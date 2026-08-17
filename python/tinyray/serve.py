"""Attaching tinyray to a process it does not own.

The least intrusive way to put a training script under a controller. The script
keeps its own entrypoint, its own ``init_process_group``, its own model
construction and its own imports. tinyray adds a control port to it, and that
is all:

.. code-block:: python

    # train.py -- an ordinary Megatron/DeepSpeed/whatever script
    import torch.distributed as dist
    import tinyray

    dist.init_process_group(backend="nccl")  # yours
    trainer = build_trainer()  # yours

    tinyray.serve(trainer)  # the only tinyray line

Contrast with the actor model, where tinyray owns ``__main__``, pickles the
user's class over the wire and constructs it remotely. That works for code
written for tinyray; it does not work for a framework that expects to own its
own process, and it fails in confusing ways when the class holds CUDA state or
lives in a module the driver cannot import.

Nothing here is decorated, subclassed or serialised. `serve` looks up methods on
an object you already built.
"""

from __future__ import annotations

import os
import threading
import traceback
from typing import Any, Callable, Optional, Union

from . import mesh, serde
from ._tinyray import ActorRuntime, new_id

#: Port the driver told this process to bind. Absent means the process was not
#: started by tinyray, and `serve` picks its own.
CONTROL_PORT_ENV = "TINYRAY_CONTROL_PORT"

#: Identity the driver expects to address. Absent means self-assigned.
ACTOR_ID_ENV = "TINYRAY_ACTOR_ID"

#: Where to write the endpoint once bound, for a driver that did not pin a port.
ANNOUNCE_ENV = "TINYRAY_ANNOUNCE_FD"


class ServeError(RuntimeError):
    """The control endpoint could not be set up."""


def _resolve_target(target: Any, method: str) -> Callable[..., Any]:
    """Find `method` on whatever the caller handed us.

    Accepts an object, a mapping of names to callables, or a module. All three
    turn up in real scripts, and requiring one shape would be exactly the kind
    of intrusion this module exists to avoid.
    """
    if isinstance(target, dict):
        if method not in target:
            available = ", ".join(sorted(target))
            raise AttributeError(f"no callable named {method!r}; available: {available}")
        found = target[method]
    else:
        if method.startswith("_"):
            raise AttributeError(f"{method!r} is private and not remotely callable")
        found = getattr(target, method, None)
        if found is None:
            available = ", ".join(
                sorted(
                    name
                    for name in dir(target)
                    if not name.startswith("_") and callable(getattr(target, name, None))
                )
            )
            raise AttributeError(
                f"{type(target).__name__} has no method {method!r}; available: {available}"
            )
    if not callable(found):
        raise TypeError(f"{method!r} is not callable")
    return found


class Server:
    """A control endpoint attached to an existing process."""

    def __init__(self, target: Any, runtime: ActorRuntime, actor_id: str) -> None:
        self.target = target
        self.runtime = runtime
        self.actor_id = actor_id
        self._thread: Optional[threading.Thread] = None
        self._stopped = threading.Event()

    @property
    def endpoint(self) -> str:
        return self.runtime.endpoint

    def __repr__(self) -> str:
        return f"Server({type(self.target).__name__} at {self.endpoint})"

    def run_forever(self, poll_interval: float = 0.2) -> None:
        """Serve control calls on the calling thread until shutdown.

        The right default for an SPMD worker driven by a controller: the script
        has finished setting itself up and now does what it is told.

        The interval matters. Python only runs signal handlers while the main
        thread executes bytecode, so a loop that blocked indefinitely in Rust
        would ignore SIGTERM and every clean shutdown would become a SIGKILL.
        """
        while not self._stopped.is_set():
            task = self.runtime.next_task(timeout_seconds=poll_interval)
            if task is None:
                if self.runtime.shutting_down:
                    break
                continue
            self._dispatch(task)

    def run_in_background(self) -> Server:
        """Serve on a background thread and return immediately.

        For a script that keeps its own loop. Be aware that control calls then
        execute on that thread while your loop runs on another: anything the
        two touch needs to be safe against it. `run_forever` avoids the
        question entirely.
        """
        if self._thread is not None:
            return self
        self._thread = threading.Thread(target=self.run_forever, name="tinyray-serve", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stopped.set()
        self.runtime.begin_shutdown()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None

    def __enter__(self) -> Server:
        return self.run_in_background()

    def __exit__(self, *_exc: Any) -> None:
        self.stop()

    def _dispatch(self, task: Any) -> None:
        try:
            args, kwargs = serde.deserialize(task.body, task.frames)
        except Exception:
            self.runtime.fail(
                task.task_id.hex,
                "Internal",
                "failed to deserialise call arguments",
                traceback.format_exc(),
            )
            return

        try:
            from .api import resolve_arguments

            args, kwargs = resolve_arguments(args, kwargs)
            if task.method == mesh.LINK_METHOD:
                # Intercepted before dispatch: the roster is tinyray's business,
                # and looking a dunder up on the user's object would be exactly
                # the kind of reach-in this module exists to avoid.
                result = mesh.install_roster(*args, **kwargs)
            else:
                result = _resolve_target(self.target, task.method)(*args, **kwargs)
        except Exception as exc:
            # The remote traceback is the only artefact the controller will
            # ever see, so it travels with the error.
            self.runtime.fail(
                task.task_id.hex,
                "UserException",
                f"{type(exc).__name__}: {exc}",
                traceback.format_exc(),
            )
            return

        try:
            body, frames = serde.serialize(result)
        except Exception:
            self.runtime.fail(
                task.task_id.hex,
                "UserException",
                f"{task.method!r} returned a value that cannot be serialised",
                traceback.format_exc(),
            )
            return

        self.runtime.complete(task.task_id.hex, body, frames)


def serve(
    target: Union[object, dict],
    *,
    background: bool = False,
    bind: Optional[str] = None,
    actor_id: Optional[str] = None,
    max_pending_calls: int = 1000,
) -> Server:
    """Expose `target`'s methods on a control port.

    Call it from a script tinyray started, or from any process at all: with no
    tinyray environment present it binds a port of its own and prints the
    endpoint, which is enough to drive it by hand.

    By default this **blocks**, serving until shutdown. Pass ``background=True``
    to keep your own loop, and read :meth:`Server.run_in_background` about what
    that implies for thread safety.
    """
    port = os.environ.get(CONTROL_PORT_ENV)
    resolved_bind = bind or (f"127.0.0.1:{port}" if port else "127.0.0.1:0")
    resolved_id = actor_id or os.environ.get(ACTOR_ID_ENV) or str(new_id())

    try:
        runtime = ActorRuntime(
            actor_id=resolved_id,
            bind=resolved_bind,
            max_pending_calls=max_pending_calls,
        )
    except Exception as exc:
        raise ServeError(f"could not bind a control port at {resolved_bind}: {exc}") from exc

    server = Server(target, runtime, resolved_id)
    mesh.mark_serving()
    _announce(server)

    if background:
        return server.run_in_background()
    server.run_forever()
    return server


def _announce(server: Server) -> None:
    """Tell whoever started us where we ended up."""
    fd = os.environ.get(ANNOUNCE_ENV)
    line = f'{{"actor_id": "{server.actor_id}", "endpoint": "{server.endpoint}"}}'
    if fd is not None:
        try:
            with os.fdopen(int(fd), "w", closefd=True) as handle:
                handle.write(line + "\n")
            return
        except (OSError, ValueError):
            pass
    if CONTROL_PORT_ENV not in os.environ:
        # Started by hand rather than by tinyray; the endpoint is the only way
        # anyone will find this process.
        print(f"[tinyray] serving control on {server.endpoint}", flush=True)
