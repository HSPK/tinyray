"""The user-facing API.

Deliberately shaped like Ray's, because that is the vocabulary the target users
already have: `@tinyray.remote`, `.remote()`, `ObjectRef`, `get`, `wait`.

The one thing to keep in mind while reading this file: `.remote()` never
blocks, and never raises whatever the user's method raised. Failures surface at
`get()`, carrying the remote traceback.
"""

from __future__ import annotations

import atexit
import os
import threading
from collections.abc import Iterable, Sequence
from typing import Any, Callable, NoReturn, Optional, Union

from . import serde
from ._tinyray import (
    ActorDied,
    ClientRuntime,
    CollectiveRegistry,
    NotFound,
    OwnerRef,
    TinyrayError,
)
from .head import Head, LocalNodeAgent
from .launcher import Launcher

#: Default ceiling on a blocking `get`. Long, because an RL rollout legitimately
#: takes minutes; not infinite, because a silent hang is worse than an error.
DEFAULT_TIMEOUT = 300.0


class ObjectRef:
    """A handle to a result that lives in the actor that produced it.

    Passing one of these to another actor is the point: the consumer fetches
    the value straight from the producer, so a 10 MB rollout never travels
    through the driver.
    """

    __slots__ = ("_context", "_owner")

    def __init__(self, owner: OwnerRef, context: Context) -> None:
        self._owner = owner
        self._context = context

    @property
    def task_id(self) -> str:
        return self._owner.task_id

    @property
    def owner_endpoint(self) -> str:
        return self._owner.endpoint

    def __repr__(self) -> str:
        return f"ObjectRef({self.task_id[:8]}@{self.owner_endpoint})"

    def __reduce__(self):
        # Serialised as a bare OwnerRef: the receiving actor rebuilds it against
        # its own runtime. This is what makes `learner.update.remote(refs)` work.
        return (OwnerRef, (self._owner.task_id, self._owner.actor_id, self._owner.endpoint))

    def __hash__(self) -> int:
        return hash(self._owner.task_id)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ObjectRef) and other._owner.task_id == self._owner.task_id


class Context:
    """Process-wide tinyray state: the head, the client, and the actor table."""

    def __init__(
        self,
        *,
        num_cpus: Optional[float] = None,
        num_gpus: Optional[int] = None,
        prewarm: int = 0,
        heartbeat_timeout: float = 30.0,
        supervise_interval: float = 1.0,
    ) -> None:
        self.client = ClientRuntime()
        self.launcher = Launcher()
        self.head = Head(heartbeat_timeout=heartbeat_timeout, supervise_interval=supervise_interval)
        self.collective = CollectiveRegistry()
        gpu_ids = list(range(int(num_gpus))) if num_gpus is not None else None
        self.agent = LocalNodeAgent(
            self.launcher, num_cpus=num_cpus, gpu_ids=gpu_ids, prewarm=prewarm
        )
        self.node_id = self.head.register_node(self.agent)
        self.head.set_callbacks(
            on_actor_moved=self._actor_moved,
            on_actor_lost=self._actor_lost,
        )
        self.head.start_supervisor()
        self._lost: dict[str, str] = {}
        # A restarted actor is a brand new process with no user object in it,
        # so the constructor has to be replayed. The head deliberately knows
        # nothing about user code, which leaves that job here.
        self._constructors: dict[str, tuple] = {}
        self._reconstructing: dict[str, threading.Event] = {}
        self._groups: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._shut_down = False

    def register_group(self, group: Any) -> None:
        with self._lock:
            self._groups[group.group_id] = group

    def _break_groups_containing(self, actor_id: str, reason: str) -> None:
        """A dead rank poisons every group it belonged to.

        NCCL communicators are not fault tolerant: a surviving rank that enters
        a collective on a communicator with a dead peer blocks forever. Marking
        the group broken makes the next `run()` fail fast instead.
        """
        for group_id in self.collective.groups_with(actor_id):
            self.collective.break_group(group_id, reason)

    def remember_constructor(self, actor_id: str, cls: type, args: tuple, kwargs: dict) -> None:
        with self._lock:
            self._constructors[actor_id] = (cls, args, kwargs)

    def await_reconstruction(self, actor_id: str, timeout: float = 60.0) -> None:
        """Block while a restarted actor is being reconstructed.

        Without this, a call submitted during the gap would arrive before the
        constructor and fail with a baffling "method before __init__".
        """
        with self._lock:
            event = self._reconstructing.get(actor_id)
        if event is not None:
            event.wait(timeout)

    def _actor_moved(self, actor_id: str, endpoint: str) -> None:
        """An actor restarted somewhere new; re-route and re-construct.

        Sequence numbering resets with the new process, and every reference
        into the old one is lost: its result store died with it.
        """
        # Any group this actor was in is now unusable until it is rebuilt.
        self._break_groups_containing(actor_id, "a member restarted")

        with self._lock:
            constructor = self._constructors.get(actor_id)
            event = threading.Event()
            self._reconstructing[actor_id] = event

        try:
            self.client.register_actor(actor_id, endpoint)
            if constructor is not None:
                cls, args, kwargs = constructor
                body, frames = serde.serialize(((cls, args, kwargs), {}))
                owner = self.client.submit(actor_id, "__init__", body, frames)
                # Block until the constructor has actually run, so the first
                # user call cannot overtake it.
                self.client.fetch(owner, 60.0)
        except Exception as exc:
            self._actor_lost(actor_id, f"restarted but could not be reconstructed: {exc}")
        finally:
            with self._lock:
                self._reconstructing.pop(actor_id, None)
            event.set()

    def _actor_lost(self, actor_id: str, reason: str) -> None:
        self._break_groups_containing(actor_id, reason)
        with self._lock:
            self._lost[actor_id] = reason
            self._constructors.pop(actor_id, None)
        self.client.forget_actor(actor_id)

    def loss_reason(self, actor_id: str) -> Optional[str]:
        with self._lock:
            return self._lost.get(actor_id)

    def register_endpoint(self, actor_id: str, endpoint: str) -> None:
        self.client.register_actor(actor_id, endpoint)

    def shutdown(self) -> None:
        with self._lock:
            if self._shut_down:
                return
            self._shut_down = True
        self.head.stop()
        self.launcher.shutdown()


_context: Optional[Context] = None
_context_lock = threading.Lock()

#: A client for processes that are not drivers. An actor resolving a reference
#: it was handed needs nothing but a connection pool: the reference already
#: carries the endpoint of the actor that owns the value, so no registry, no
#: head and no placement are involved.
_standalone_client: Optional[ClientRuntime] = None
_standalone_lock = threading.Lock()


def _inside_actor() -> bool:
    return os.environ.get("TINYRAY_ACTOR_NAME") is not None


def _fetch_client() -> ClientRuntime:
    """The client to resolve references with, wherever we are running."""
    global _standalone_client
    if _context is not None:
        return _context.client
    if _inside_actor():
        with _standalone_lock:
            if _standalone_client is None:
                _standalone_client = ClientRuntime()
            return _standalone_client
    return init().client


def init(
    *,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[int] = None,
    prewarm: int = 0,
    heartbeat_timeout: float = 30.0,
    supervise_interval: float = 1.0,
) -> Context:
    """Start the local tinyray runtime. Idempotent.

    `num_cpus` and `num_gpus` override what tinyray detects, which is mostly
    useful in tests that want to exercise placement without real hardware.

    `prewarm` keeps that many interpreters warm per device assignment. Worth
    setting for a hyperparameter sweep, where `import torch` in every short
    trial otherwise dominates the run; pointless for a handful of long-lived
    actors, which is why it is off by default.
    """
    global _context
    with _context_lock:
        if _context is None:
            _context = Context(
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                prewarm=prewarm,
                heartbeat_timeout=heartbeat_timeout,
                supervise_interval=supervise_interval,
            )
            atexit.register(shutdown)
        return _context


def shutdown() -> None:
    """Stop every actor this process started."""
    global _context
    with _context_lock:
        context, _context = _context, None
    if context is not None:
        context.shutdown()


def _require_context() -> Context:
    if _context is None:
        return init()
    return _context


class ActorMethod:
    """One remotely callable method, bound to one actor."""

    def __init__(self, handle: ActorHandle, method: str) -> None:
        self._handle = handle
        self._method = method

    def remote(self, *args: Any, **kwargs: Any) -> ObjectRef:
        """Submit the call. Returns immediately, without running it."""
        return self._handle._submit(self._method, args, kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise TypeError(
            f"use {self._method}.remote(...) to call an actor method; "
            "calling it directly would run it in the driver"
        )


class ActorHandle:
    """A reference to a running actor.

    The endpoint is looked up rather than cached, so a handle keeps working
    after the actor restarts somewhere else.
    """

    def __init__(self, context: Context, entry: dict[str, Any], class_name: str) -> None:
        self._context = context
        self._entry = entry
        self._class_name = class_name

    @property
    def actor_id(self) -> str:
        return self._entry["actor_id"]

    @property
    def endpoint(self) -> str:
        current = self._context.head.get_actor(self.actor_id)
        return current["endpoint"] if current else self._entry["endpoint"]

    @property
    def name(self) -> str:
        return self._entry.get("name", self._class_name)

    @property
    def pid(self) -> int:
        return self._entry["pid"]

    @property
    def gpu_ids(self) -> list[int]:
        return list(self._entry.get("gpu_ids", []))

    def __getattr__(self, name: str) -> ActorMethod:
        if name.startswith("_"):
            raise AttributeError(name)
        return ActorMethod(self, name)

    def __repr__(self) -> str:
        return f"ActorHandle({self._class_name}, {self.name}, {self.endpoint})"

    def _submit(self, method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> ObjectRef:
        self._context.await_reconstruction(self.actor_id)
        body, frames = serde.serialize((args, kwargs))
        try:
            owner = self._context.client.submit(self.actor_id, method, body, frames)
        except TinyrayError:
            # A call that lands exactly as the actor dies should say so, rather
            # than surfacing a raw connection error.
            reason = self._context.loss_reason(self.actor_id)
            if reason is not None:
                raise ActorDied(
                    f"actor {self._class_name} ({self.actor_id[:8]}) is gone: {reason}"
                ) from None
            raise
        return ObjectRef(owner, self._context)

    def introspect(self) -> str:
        """The actor's own view of its queues and store."""
        return self._context.client.get_text(self.endpoint, "/introspect")

    def is_alive(self) -> bool:
        entry = self._context.head.get_actor(self.actor_id)
        return entry is not None and entry["state"] in ("ALIVE", "STARTING", "RESTARTING")


class RemoteClass:
    """What `@tinyray.remote` produces."""

    def __init__(self, cls: type, options: dict[str, Any]) -> None:
        self._cls = cls
        self._options = options

    def options(self, **overrides: Any) -> RemoteClass:
        """Return a copy with different resources or lifetime settings."""
        merged = dict(self._options)
        merged.update(overrides)
        return RemoteClass(self._cls, merged)

    def remote(self, *args: Any, **kwargs: Any) -> ActorHandle:
        """Place, start and construct one actor."""
        context = _require_context()
        options = self._options
        lifetime = options.get("lifetime")
        if lifetime not in (None, "driver"):
            if lifetime == "detached":
                # Better to refuse than to accept the option and quietly ignore
                # it. A detached actor has to outlive the driver, which needs a
                # head running as its own process to keep owning it; until that
                # exists, "detached" would just leak a process on shutdown.
                raise NotImplementedError(
                    "lifetime='detached' needs a standalone head process, which tinyray "
                    "does not have yet. Use name= for a lookup-able actor; it will still "
                    "be stopped when the driver exits."
                )
            raise ValueError(f"unknown lifetime {lifetime!r}; expected 'driver' or None")

        entry = context.head.create_actor(
            name=options.get("name") or self._cls.__name__,
            num_cpus=options.get("num_cpus", 1.0),
            num_gpus=options.get("num_gpus", 0.0),
            memory_bytes=options.get("memory_bytes", 0),
            strategy=options.get("strategy", "SPREAD"),
            max_restarts=options.get("max_restarts", 0),
            max_pending_calls=options.get("max_pending_calls", 1000),
            store_max_bytes=options.get("store_max_bytes"),
            store_ttl_seconds=options.get("store_ttl_seconds"),
            # Naming and lifetime are independent: an actor you can look up by
            # name need not outlive the driver.
            actor_name=options.get("name"),
            detached=False,
        )
        return _construct(context, entry, self._cls, args, kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise TypeError(
            f"use {self._cls.__name__}.remote(...) to create an actor; "
            "calling the class directly would build it in the driver"
        )


def _construct(
    context: Context,
    entry: dict[str, Any],
    cls: type,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> ActorHandle:
    """Register the actor and run its constructor as an ordinary call.

    Sending the class over the wire keeps the node agent completely ignorant of
    user code, and waiting for it here means a failing ``__init__`` raises at
    ``.remote()``, which is where a user expects it.
    """
    context.register_endpoint(entry["actor_id"], entry["endpoint"])
    handle = ActorHandle(context, entry, cls.__name__)
    get(handle._submit("__init__", (cls, args, kwargs), {}))
    # Remembered only after a successful construction, so a broken __init__ is
    # not replayed forever by the supervisor.
    context.remember_constructor(entry["actor_id"], cls, args, kwargs)
    return handle


def create_actors(
    remote_class: RemoteClass,
    *args: Any,
    count: int,
    strategy: str = "SPREAD",
    **kwargs: Any,
) -> list[ActorHandle]:
    """Start `count` actors atomically, or start none at all.

    Gang placement is required rather than merely nice: a group that comes up
    halfway cannot form a collective, and the run then hangs waiting for ranks
    that will never arrive.
    """
    context = _require_context()
    options = remote_class._options
    entries = context.head.create_actors(
        count,
        name=options.get("name") or remote_class._cls.__name__,
        num_cpus=options.get("num_cpus", 1.0),
        num_gpus=options.get("num_gpus", 0.0),
        memory_bytes=options.get("memory_bytes", 0),
        strategy=strategy,
        max_restarts=options.get("max_restarts", 0),
        max_pending_calls=options.get("max_pending_calls", 1000),
    )
    return [_construct(context, entry, remote_class._cls, args, kwargs) for entry in entries]


def get_actor(name: str) -> ActorHandle:
    """Look up a named actor."""
    context = _require_context()
    entry = context.head.get_actor_by_name(name)
    if entry is None:
        raise NotFound(f"no actor is registered under the name {name!r}")
    context.register_endpoint(entry["actor_id"], entry["endpoint"])
    return ActorHandle(context, {**entry, "pid": -1}, entry.get("name") or "actor")


def transport_stats() -> dict[str, dict[str, int]]:
    """Bytes and requests this driver has exchanged with each actor.

    Answers "is my driver relaying data it should not be?". In a healthy run
    the driver's byte counts stay small: it moves references, and the actors
    move payloads between themselves.
    """
    return _require_context().client.transport_stats()


def nodes() -> list[dict]:
    """Every node in the cluster, with its resources."""
    return _require_context().head.nodes_info()


def actors() -> list[dict]:
    """Every actor the head knows about."""
    return _require_context().head.actors()


def remote(
    *decorator_args: Any, **options: Any
) -> Union[RemoteClass, Callable[[type], RemoteClass]]:
    """Mark a class as an actor.

    Usable bare or with options::

        @tinyray.remote
        class Counter: ...


        @tinyray.remote(num_gpus=1, max_restarts=3)
        class Rollout: ...
    """
    if decorator_args and callable(decorator_args[0]) and not options:
        cls = decorator_args[0]
        if not isinstance(cls, type):
            raise TypeError("@tinyray.remote applies to classes; tinyray has no tasks")
        return RemoteClass(cls, {})

    if decorator_args:
        raise TypeError("@tinyray.remote takes keyword options only")

    def wrap(cls: type) -> RemoteClass:
        if not isinstance(cls, type):
            raise TypeError("@tinyray.remote applies to classes; tinyray has no tasks")
        return RemoteClass(cls, options)

    return wrap


def get(
    refs: Union[ObjectRef, OwnerRef, Sequence[Any]],
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> Any:
    """Fetch one or more results, blocking until they are ready.

    Works in the driver and inside an actor alike. A reference carries the
    endpoint of whoever owns the value, so an actor handed a reference fetches
    straight from the producer -- which is precisely what keeps large payloads
    off the driver.

    Raises the remote exception, with the remote traceback attached.
    """
    client = _fetch_client()
    if isinstance(refs, (ObjectRef, OwnerRef)):
        return _fetch_one(client, refs, timeout)
    return [_fetch_one(client, ref, timeout) for ref in refs]


def _owner_of(ref: Union[ObjectRef, OwnerRef]) -> OwnerRef:
    if isinstance(ref, ObjectRef):
        return ref._owner
    if isinstance(ref, OwnerRef):
        # How a reference arrives after being pickled to another actor.
        return ref
    raise TypeError(f"expected an ObjectRef, got {type(ref).__name__}")


def _fetch_one(client: ClientRuntime, ref: Union[ObjectRef, OwnerRef], timeout: float) -> Any:
    body, frames = client.fetch(_owner_of(ref), timeout)
    return serde.deserialize(body, frames)


def resolve_arguments(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Replace top-level references in a call's arguments with their values.

    Only top level, matching Ray: references nested inside a list are passed
    through untouched, so an actor can receive a batch of them and decide when
    (or whether) to fetch each one.
    """
    if not any(isinstance(a, OwnerRef) for a in args) and not any(
        isinstance(v, OwnerRef) for v in kwargs.values()
    ):
        return args, kwargs
    client = _fetch_client()
    resolved_args = tuple(
        _fetch_one(client, a, DEFAULT_TIMEOUT) if isinstance(a, OwnerRef) else a for a in args
    )
    resolved_kwargs = {
        key: _fetch_one(client, value, DEFAULT_TIMEOUT) if isinstance(value, OwnerRef) else value
        for key, value in kwargs.items()
    }
    return resolved_args, resolved_kwargs


def wait(
    refs: Sequence[ObjectRef],
    *,
    num_returns: int = 1,
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[list[ObjectRef], list[ObjectRef]]:
    """Wait for `num_returns` of `refs` to settle.

    Returns `(ready, pending)`. This is how an RL loop drops stragglers: train
    on 24 of 32 rollouts instead of waiting for the slowest.

    Note that a straggler skipped here is still obliged to take part in the
    next collective barrier; see `tinyray.collective`.
    """
    context = _require_context()
    refs = list(refs)
    owners = [ref._owner for ref in refs]
    ready_owners, pending_owners = context.client.wait(owners, num_returns, timeout)

    by_task = {ref._owner.task_id: ref for ref in refs}
    ready = [by_task[owner.task_id] for owner in ready_owners]
    pending = [by_task[owner.task_id] for owner in pending_owners]
    return ready, pending


def release(refs: Union[ObjectRef, Iterable[ObjectRef]]) -> None:
    """Tell the owners these results are no longer needed.

    Best effort. The store's watermark and TTL are the real safety net; this
    just returns the memory sooner.
    """
    client = _fetch_client()
    if isinstance(refs, (ObjectRef, OwnerRef)):
        refs = [refs]
    for ref in refs:
        client.release(_owner_of(ref))


def kill(handle: ActorHandle, *, no_restart: bool = True) -> None:
    """Terminate an actor immediately.

    Needed for hyperparameter search: stopping a bad trial early is the whole
    point of the exercise.
    """
    context = _require_context()
    context.head.kill_actor(handle.actor_id)
    context.client.forget_actor(handle.actor_id)
