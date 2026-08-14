"""Type stubs for the tinyray Rust core.

Hand written, because PyO3 cannot generate them. Everything the extension
exports is declared here with its full signature, so `mypy`, `pyright` and
editor completion see the same API a reader of the Rust source would.

The docstrings duplicate the ones on the Rust side deliberately: a caller
reading `tinyray.Frame` in an editor should learn the ownership and mutability
rules without opening `crates/tinyray-py/src/buffers.rs`.
"""

from __future__ import annotations

import sys
from typing import Any, Optional

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:  # pragma: no cover - depends on the interpreter, not on tinyray
    from typing_extensions import Buffer

__all__ = [
    "ActorDied",
    "ActorRuntime",
    "Backpressure",
    "ClientRuntime",
    "ClusterState",
    "CollectiveRegistry",
    "Decoder",
    "Frame",
    "Id",
    "Limits",
    "MessageTooLarge",
    "NotFound",
    "ObjectLost",
    "OwnerRef",
    "ProtocolError",
    "RemoteCallError",
    "Task",
    "TinyrayError",
    "UserCodeError",
    "bench_decode_native",
    "decode_message",
    "detect_cpus",
    "detect_gpus",
    "encode_message",
    "new_id",
    "version",
]

#: Version of the compiled Rust core. May differ from ``tinyray.__version__``
#: if the extension was not rebuilt after a source change.
version: str

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class TinyrayError(Exception):
    """Base class for every tinyray error."""

class ProtocolError(TinyrayError):
    """The peer sent something that is not a valid tinyray message."""

class MessageTooLarge(ProtocolError):
    """A message exceeded the configured size limits."""

class RemoteCallError(TinyrayError):
    """A call failed on the remote actor.

    Instances carry two extra attributes, set from the wire error:

    ``kind``
        One of ``UserException``, ``ObjectLost``, ``ActorDied``, ``NotFound``,
        ``Backpressure`` or ``Internal``.
    ``remote_traceback``
        The traceback from the actor process, when the failure came from user
        code. In a distributed run this is usually the only useful artefact.
    """

    kind: str
    remote_traceback: Optional[str]

class UserCodeError(RemoteCallError):
    """The user's method raised. ``remote_traceback`` holds the actor's stack."""

class ObjectLost(RemoteCallError):
    """The result is gone: evicted, expired, released, or its owner restarted.

    Distinct from :class:`NotFound` on purpose. ``ObjectLost`` means the value
    existed and the consumer was too late, which points at the store watermark
    or TTL; ``NotFound`` points at a bug.
    """

class ActorDied(RemoteCallError):
    """The target actor is no longer alive and will not be restarted."""

class NotFound(RemoteCallError):
    """No such actor, task or method."""

class Backpressure(RemoteCallError):
    """The target is over its queue or memory watermark.

    The only failure that is safe to retry blindly; the client does so
    automatically.
    """

# ---------------------------------------------------------------------------
# Buffers and framing
# ---------------------------------------------------------------------------

class Frame:
    """A read-only view of a Rust-owned buffer, exposed with no copy.

    Supports the buffer protocol, so ``memoryview(frame)`` and
    ``pickle.loads(body, buffers=[frame])`` build numpy arrays that view Rust
    memory directly rather than copying it.

    Frames are **read-only**: one result may be served to many consumers, and
    letting any of them mutate the shared buffer would corrupt the rest. Copy
    explicitly if you need to write.
    """

    def __init__(self, data: Buffer) -> None:
        """Copy ``data`` into a Rust-owned buffer.

        Copies rather than borrows: a caller is free to mutate its array right
        after a non-blocking submit, and borrowing would make that a data race.
        Passing another :class:`Frame` shares the allocation instead.
        """

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def to_bytes(self) -> bytes:
        """Copy the contents into a regular ``bytes``.

        For tests and small payloads only; avoiding this copy is the point of
        the class.
        """

class Limits:
    """Size limits applied while decoding an untrusted byte stream.

    Every field exists to stop a malformed or hostile peer from triggering an
    unbounded allocation.
    """

    def __init__(
        self,
        max_header_len: Optional[int] = ...,
        max_frames: Optional[int] = ...,
        max_frame_len: Optional[int] = ...,
        max_message_len: Optional[int] = ...,
    ) -> None: ...
    @staticmethod
    def default() -> Limits: ...
    @property
    def max_header_len(self) -> int: ...
    @property
    def max_frames(self) -> int: ...
    @property
    def max_frame_len(self) -> int: ...
    @property
    def max_message_len(self) -> int: ...
    def __repr__(self) -> str: ...

class Decoder:
    """Incremental decoder for the tinyray framing.

    Feed it whatever arrives from the socket; it yields whole messages and
    keeps the remainder buffered.
    """

    def __init__(self, limits: Optional[Limits] = ...) -> None: ...
    def feed(self, data: Buffer) -> None:
        """Append bytes to the internal buffer."""

    def next_message(self) -> Optional[tuple[bytes, list[Frame]]]:
        """Pop the next complete message, or ``None`` if more bytes are needed.

        Raises :class:`ProtocolError` on malformed input, after which the
        decoder is poisoned: a binary framing has no resynchronisation point,
        so the connection must be closed.
        """

    @property
    def buffered(self) -> int:
        """Bytes received but not yet consumed."""

    @property
    def at_message_boundary(self) -> bool:
        """True when no partial message is in flight."""

    @property
    def poisoned(self) -> bool:
        """True once a fatal framing error has been reported."""

def encode_message(
    header: Buffer,
    frames: Any,
    limits: Optional[Limits] = ...,
) -> bytes:
    """Encode a header plus out-of-band frames into one wire buffer.

    ``frames`` is any iterable of buffer-like objects: ``bytes``, ``bytearray``,
    ``memoryview``, numpy arrays of any dtype, ``pickle.PickleBuffer``, or
    :class:`Frame`.
    """

def decode_message(
    data: Buffer,
    limits: Optional[Limits] = ...,
) -> tuple[bytes, list[Frame]]:
    """Decode exactly one complete message.

    Returns ``(header, frames)``. The frames are zero-copy views of Rust
    memory. Raises ``ValueError`` if the buffer is incomplete or has trailing
    bytes, and :class:`ProtocolError` if it is malformed.
    """

# ---------------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------------

class Id:
    """A 128-bit identifier, rendered as 32 lowercase hex characters."""

    def __init__(self, value: str) -> None:
        """Parse from hex. Raises ``ValueError`` on anything non-canonical."""

    @staticmethod
    def nil() -> Id: ...
    @property
    def hex(self) -> str: ...
    def is_nil(self) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __lt__(self, other: Id) -> bool: ...
    def __le__(self, other: Id) -> bool: ...
    def __gt__(self, other: Id) -> bool: ...
    def __ge__(self, other: Id) -> bool: ...

def new_id() -> Id:
    """Allocate a fresh, process-unique identifier."""

# ---------------------------------------------------------------------------
# Driver side
# ---------------------------------------------------------------------------

class OwnerRef:
    """Names a result and the actor that owns it.

    Small and cheap to pass around. Handing one to another actor is what keeps
    large payloads off the driver: the consumer fetches straight from the
    producer. Picklable, so it survives being sent as a call argument.
    """

    def __init__(self, task_id: str, actor_id: str, endpoint: str) -> None: ...
    @property
    def task_id(self) -> str: ...
    @property
    def actor_id(self) -> str: ...
    @property
    def endpoint(self) -> str:
        """``host:port`` of the actor holding the value."""

    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __reduce__(self) -> tuple[Any, tuple[str, str, str]]: ...

class ClientRuntime:
    """Driver-side client: submits calls and fetches results.

    Owns a pooled HTTP connection per peer. Every blocking method releases the
    GIL for the duration, so a driver waiting on 32 rollouts is not sitting on
    the interpreter.
    """

    def __init__(
        self,
        connections_per_peer: int = ...,
        request_timeout_seconds: float = ...,
        max_retries: int = ...,
    ) -> None:
        """Configure the pool.

        ``connections_per_peer`` defaults above one to dodge HTTP/1.1
        head-of-line blocking, where a 10 MB response would otherwise stall
        every small control message queued behind it.
        """

    @property
    def caller_id(self) -> str:
        """Identifies this client, and therefore its per-actor call ordering."""

    def register_actor(self, actor_id: str, endpoint: str) -> None:
        """Record where an actor lives.

        Pointing an existing actor at a new endpoint resets its sequence
        numbering, because a restarted process has no memory of what the
        previous one already ran.
        """

    def forget_actor(self, actor_id: str) -> None: ...
    def endpoint_of(self, actor_id: str) -> Optional[str]: ...
    def submit(
        self,
        actor_id: str,
        method: str,
        body: Buffer,
        frames: Any,
    ) -> OwnerRef:
        """Submit a call and return a reference to its eventual result.

        Waits only for the actor's acknowledgement, not for the method to run.
        """

    def fetch(
        self,
        reference: OwnerRef,
        timeout_seconds: float = ...,
    ) -> tuple[Frame, list[Frame]]:
        """Fetch a result, parking until it is ready.

        Returns ``(body, frames)`` as zero-copy views. Raises the remote
        exception, with the remote traceback attached.
        """

    def wait(
        self,
        refs: list[OwnerRef],
        num_returns: int,
        timeout_seconds: float = ...,
    ) -> tuple[list[OwnerRef], list[OwnerRef]]:
        """Wait for ``num_returns`` references to settle.

        Returns ``(ready, pending)``. A reference that failed counts as ready:
        it has settled, and ``fetch`` will raise.
        """

    def release(self, reference: OwnerRef) -> None:
        """Tell the owner a result is no longer needed. Best effort."""

    def get_text(self, endpoint: str, path: str) -> str:
        """Read a plain-text endpoint such as ``/health`` or ``/introspect``."""

# ---------------------------------------------------------------------------
# Actor side
# ---------------------------------------------------------------------------

class Task:
    """A call handed to the Python executor thread."""

    @property
    def task_id(self) -> Id: ...
    @property
    def method(self) -> str: ...
    @property
    def body(self) -> Frame:
        """The pickle body of the call arguments."""

    @property
    def frames(self) -> list[Frame]:
        """Out-of-band argument buffers, as zero-copy views."""

    def __repr__(self) -> str: ...

class ActorRuntime:
    """The actor-side runtime: HTTP server, result store and dispatch queue.

    Everything except the user's method runs on tokio threads that never take
    the GIL, so an actor busy with a 200 ms training step still serves result
    fetches at full speed.
    """

    def __init__(
        self,
        actor_id: str,
        bind: str = ...,
        max_pending_calls: int = ...,
        store_max_bytes: Optional[int] = ...,
        store_ttl_seconds: Optional[float] = ...,
        inline_threshold: int = ...,
    ) -> None:
        """Bind a server and start accepting calls.

        ``bind`` defaults to ``127.0.0.1:0``: the OS picks a free port, which is
        then reported through :attr:`endpoint`. Actors are addressed through the
        registry, never by a fixed port.
        """

    @property
    def endpoint(self) -> str:
        """``host:port`` this actor is reachable at."""

    @property
    def actor_id(self) -> str: ...
    def next_task(self, timeout_seconds: float = ...) -> Optional[Task]:
        """Wait for the next call, giving up after ``timeout_seconds``.

        Returns ``None`` on either a timeout or shutdown; check
        :attr:`shutting_down` to tell them apart.

        The timeout is not a nicety. Python only runs signal handlers while the
        main thread is executing bytecode, so an executor parked in Rust
        indefinitely would ignore SIGTERM and every clean shutdown would fall
        back to SIGKILL.
        """

    def complete(self, task_id: str, body: Buffer, frames: Any) -> None:
        """Publish a successful result."""

    def fail(
        self,
        task_id: str,
        kind: str,
        message: str,
        traceback: Optional[str] = ...,
    ) -> None:
        """Publish a failure.

        ``kind`` is one of ``UserException``, ``ObjectLost``, ``ActorDied``,
        ``NotFound``, ``Backpressure`` or ``Internal``. ``traceback`` travels to
        the caller verbatim.
        """

    def begin_shutdown(self) -> None:
        """Stop accepting work and fail anything still queued.

        Queued calls are failed rather than dropped, so no caller waits forever
        on a result that will never be produced.
        """

    @property
    def shutting_down(self) -> bool: ...
    def introspect(self) -> str:
        """The same JSON the ``/introspect`` endpoint serves."""

    def sweep_expired(self) -> int:
        """Drop results past their TTL. Returns how many were removed."""

# ---------------------------------------------------------------------------
# Cluster state
# ---------------------------------------------------------------------------

class ClusterState:
    """The head's bookkeeping: nodes, their resources, and where actors live.

    With no stateless tasks there is no high-frequency scheduling, so this is
    consulted only when an actor is created, looked up or dies -- never on the
    data path.
    """

    def __init__(self, heartbeat_timeout_seconds: float = ...) -> None: ...
    def register_node(
        self,
        node_id: str,
        endpoint: str,
        hostname: str,
        num_cpus: float,
        num_gpus: float = ...,
        memory_bytes: int = ...,
        gpu_ids: Optional[list[int]] = ...,
        custom: Optional[dict[str, float]] = ...,
    ) -> None: ...
    def place(
        self,
        num_cpus: float = ...,
        num_gpus: float = ...,
        memory_bytes: int = ...,
        strategy: str = ...,
        custom: Optional[dict[str, float]] = ...,
    ) -> tuple[str, str, list[int]]:
        """Reserve resources for one actor.

        Returns ``(node_id, node_endpoint, gpu_ids)``. ``strategy`` is
        ``PACK``, ``SPREAD`` or ``STRICT_SPREAD``. Requests of a whole GPU or
        more reserve devices exclusively, because two ranks sharing a device
        deadlock NCCL; fractional requests share.

        Raises :class:`TinyrayError` with the shortfall spelled out if nothing
        fits.
        """

    def place_gang(
        self,
        count: int,
        num_cpus: float = ...,
        num_gpus: float = ...,
        memory_bytes: int = ...,
        strategy: str = ...,
        custom: Optional[dict[str, float]] = ...,
    ) -> list[tuple[str, str, list[int]]]:
        """Reserve resources for ``count`` actors, all or nothing.

        Atomic by requirement, not as an optimisation: a group that comes up
        halfway cannot form a collective, and the run then hangs waiting for
        ranks that will never arrive.
        """

    def gang_capacity(
        self,
        num_cpus: float = ...,
        num_gpus: float = ...,
        memory_bytes: int = ...,
        strategy: str = ...,
        custom: Optional[dict[str, float]] = ...,
    ) -> int:
        """How many actors of this shape the cluster could currently host."""

    def add_actor(
        self,
        actor_id: str,
        node_id: str,
        endpoint: str,
        num_cpus: float = ...,
        num_gpus: float = ...,
        memory_bytes: int = ...,
        gpu_ids: Optional[list[int]] = ...,
        name: Optional[str] = ...,
        max_restarts: int = ...,
        detached: bool = ...,
    ) -> None: ...
    def note_actor_died(self, actor_id: str) -> bool:
        """Record a death. Returns True if the actor should be restarted."""

    def set_actor_endpoint(self, actor_id: str, endpoint: str) -> None:
        """Point an actor at its new address after a restart."""

    def remove_actor(self, actor_id: str) -> bool:
        """Forget an actor and return its resources to the node immediately."""

    def actor_by_name(self, name: str) -> Optional[str]: ...
    def actor(self, actor_id: str) -> Optional[dict[str, Any]]: ...
    def actors(self) -> list[dict[str, Any]]: ...
    def nodes(self) -> list[dict[str, Any]]: ...
    def heartbeat(
        self,
        node_id: str,
        num_cpus: float,
        num_gpus: float,
        free_gpu_ids: list[int],
    ) -> bool:
        """Accept a node's periodic report. False if the node is unknown."""

    def dead_nodes(self) -> list[str]:
        """Nodes that have missed their heartbeat deadline."""

    def remove_node(self, node_id: str) -> list[str]:
        """Drop a node; returns the actors that died with it."""

    def release(
        self,
        node_id: str,
        num_cpus: float,
        num_gpus: float,
        gpu_ids: list[int],
    ) -> None: ...

def detect_gpus() -> list[int]:
    """Physical GPU indices on this machine.

    Reads ``CUDA_VISIBLE_DEVICES`` if set, otherwise shells out to
    ``nvidia-smi``. Returns an empty list on a machine without GPUs, which is a
    normal CPU-only development box rather than an error.
    """

def detect_cpus() -> int:
    """Usable CPU count for this process."""

# ---------------------------------------------------------------------------
# Collective groups
# ---------------------------------------------------------------------------

class CollectiveRegistry:
    """Rank assignment and the epoch state machine for collective groups.

    tinyray implements **no collective transport**. NCCL moves the weights
    through ``torch.distributed``; this supplies the parts NCCL leaves to the
    caller: who is rank what, where they meet, and what happens when a member
    dies.
    """

    def __init__(self) -> None: ...
    def create(
        self,
        group_id: str,
        members: list[tuple[str, float, str, list[int], bool]],
        backend: str = ...,
        store_host: str = ...,
        store_port: int = ...,
    ) -> int:
        """Validate a membership and assign ranks. Returns the world size.

        Each member is ``(actor_id, num_gpus, node_id, gpu_ids, alive)``.

        Admission is strict because every rule here corresponds to a NCCL
        failure that manifests as a *hang* rather than an error: a group needs
        at least two members, each must own at least one whole GPU, and no two
        may share a device.
        """

    def rendezvous_for(self, group_id: str, actor_id: str) -> Optional[dict[str, Any]]:
        """Everything a member needs to call ``init_process_group``.

        Keys: ``group_id``, ``epoch``, ``rank``, ``world_size``,
        ``store_host``, ``store_port``, ``backend``.
        """

    def acknowledge(self, group_id: str, actor_id: str, epoch: int) -> Optional[str]:
        """Record that a member joined. Returns the group's new state.

        Acknowledgements for a stale epoch are ignored, or a rebuilding group
        would look ready before its members had actually rejoined.
        """

    def break_group(self, group_id: str, reason: str) -> list[str]:
        """Mark a group unusable; returns the members that must abort.

        Every rank must abort, not just the dead one: a NCCL communicator is
        only as alive as its least alive member, and survivors entering a
        collective on it block forever.
        """

    def begin_rebuild(self, group_id: str) -> Optional[int]:
        """Bump the epoch and start forming again. Returns the new epoch.

        Rebuilding takes seconds, which is why groups are long-lived and must
        never be rebuilt per training iteration.
        """

    def replace_member(
        self,
        group_id: str,
        old: str,
        new: str,
        node_id: str,
        gpu_ids: list[int],
    ) -> bool:
        """Swap a member in, keeping every other rank stable."""

    def groups_with(self, actor_id: str) -> list[str]:
        """Groups an actor belongs to. Used when it dies."""

    def destroy(self, group_id: str) -> list[str]: ...
    def info(self, group_id: str) -> Optional[dict[str, Any]]: ...
    def group_ids(self) -> list[str]: ...

# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------

def bench_decode_native(data: Buffer, repeats: int = ...) -> float:
    """Decode ``data`` repeatedly on a native thread; returns median seconds.

    Models the server path honestly. The worker thread never acquires the GIL,
    so the result is unaffected by other Python threads -- unlike anything
    Python initiates, which pays GIL scheduling latency no matter how little of
    it runs in the interpreter.
    """
