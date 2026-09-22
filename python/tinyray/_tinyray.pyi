"""Types for the Rust extension."""

from collections.abc import Callable
from typing import Any

class NativeMember:
    @property
    def pool(self) -> str: ...
    @property
    def id(self) -> int: ...
    @property
    def slot(self) -> int | None: ...
    @property
    def incarnation(self) -> int: ...
    @property
    def url(self) -> str | None: ...
    @property
    def ready(self) -> bool: ...
    @property
    def identity(self) -> str: ...
    @property
    def label(self) -> str: ...
    def materialize_state(self) -> Any: ...

class NativeSnapshot:
    @property
    def revision(self) -> int: ...
    @property
    def fingerprint(self) -> int: ...
    @property
    def roster(self) -> int: ...
    @property
    def size(self) -> int | None: ...
    def __len__(self) -> int: ...
    def materialize(
        self,
        factory: Callable[..., Any],
        state_factory: Callable[..., Any],
        immutable: bool = ...,
    ) -> Any: ...
    def materialize_ready(
        self, factory: Callable[..., Any], state_factory: Callable[..., Any]
    ) -> list[Any]: ...
    def slot(self, slot: int, factory: Callable[..., Any]) -> Any | None: ...
    def get(self, identity: str, factory: Callable[..., Any]) -> Any | None: ...

class NativeStateBatch:
    def materialize(self) -> bytes: ...

class NativeWait:
    def check(self, initial: bool = ...) -> tuple[int, NativeSnapshot | None, int, int, int]: ...
    def wait(
        self, timeout_ms: int | None = ..., initial: bool = ...
    ) -> tuple[int, NativeSnapshot | None, int, int, int]: ...
    def close(self) -> None: ...

class Client:
    def __init__(
        self,
        endpoint: str,
        pool: str,
        id: int,
        incarnation: int,
        policy: str,
        slot: int | None = ...,
        size: int | None = ...,
        url: str | None = ...,
        methods: list[str] = ...,
        exclusive: bool = ...,
        coalesce_ms: int = 50,
    ) -> None: ...
    @property
    def accepted(self) -> bool: ...
    @property
    def silence_ms(self) -> int: ...
    def start(self, budget_ms: int = 5_000) -> bool:
        """Sends one beat and blocks on it. False means it did not land."""

    def watch(self, pools: list[str]) -> None:
        """Raises once the subscription list is full."""

    def set_state(self, state_msgpack: bytes, ready: bool) -> bool:
        """False when the pair was already exactly this, so nothing was nudged."""

    def set_state_only(self, state_msgpack: bytes) -> bool:
        """Publish state without touching readiness."""

    def is_ready(self) -> bool: ...
    def field_digest(self, pool: str, fields: list[str]) -> int | None:
        """A hash over only these fields of every member, plus who is present."""

    def registry(self) -> tuple[int, str]:
        """(protocol, version) as last reported by the registry."""

    def publish_versions(self) -> tuple[int, int]:
        """(published locally, acked by the registry). flush() waits for the
        second to reach the first."""

    def add_wake_fd(self, fd: int) -> None:
        """Also write a byte to `fd` whenever the bell rings."""

    def drop_wake_fd(self, fd: int) -> None:
        """Stop writing to `fd`. Call before closing it."""

    def wake(self) -> None:
        """Ring the bell with nothing changed, so waiters can re-check."""

    def set_url(self, url: str | None = ...) -> None: ...
    def snapshot_view(
        self, pool: str, filter_msgpack: bytes | None = ..., require_ready: bool = ...
    ) -> NativeSnapshot | None:
        """An immutable Arc-backed view over the cached roster."""

    def count(
        self, pool: str, filter_msgpack: bytes | None = ..., require_ready: bool = ...
    ) -> int:
        """Count matching cached members without materializing Handles."""

    def count_waiter(
        self, pool: str, count: int, filter_msgpack: bytes | None = ...
    ) -> NativeWait: ...
    def departure_waiter(self, pool: str, identity: str) -> NativeWait: ...
    def replacement_waiter(
        self, pool: str, slot: int, previous: str | None = ..., capture: bool = ...
    ) -> NativeWait: ...
    def wait_epoch(
        self, pool: str, timeout_ms: int, minimum: int | None = ...
    ) -> tuple[int, NativeSnapshot | None, int, int, bool, bool, int]: ...
    def epoch_valid(self, pool: str, roster: int) -> bool: ...
    def lookup(
        self, pool: str, filter_msgpack: bytes | None = ..., require_ready: bool = ...
    ) -> bytes:
        """Matching members of `pool`, as MessagePack."""

    def choose(
        self, pool: str, filter_msgpack: bytes | None = ..., require_ready: bool = ...
    ) -> bytes | None:
        """One uniformly selected matching member, or None."""

    def choose_ref(
        self, pool: str, filter_msgpack: bytes | None = ..., require_ready: bool = ...
    ) -> NativeMember | None:
        """One uniformly selected matching native member reference, or None."""

    def lookup_slot(self, pool: str, slot: int, require_ready: bool = ...) -> bytes | None:
        """The occupant of a slot, or None."""

    def lookup_slot_ref(
        self, pool: str, slot: int, require_ready: bool = ...
    ) -> NativeMember | None:
        """The occupant of a slot as a native member reference, or None."""

    def pool_info(self, pool: str) -> tuple[int, int, int | None, list[str]] | None:
        """(version, roster, size, methods), or None if the pool is unseen."""

    def frozen(self, pool: str, require_ready: bool = ...) -> tuple[bytes, int, int, int] | None:
        """(members MessagePack, their fingerprint, the pool's, the pool's version)."""

    def cache_revision(self) -> int:
        """Moves when cached membership or lifecycle state changes."""

    def wait_revision(self, since: int, timeout_ms: int) -> int:
        """Block until the cache moves past `since`. No polling anywhere."""

    def wait_registered(self, timeout_ms: int) -> bool: ...
    def wait_publication(self, wanted: int, timeout_ms: int) -> tuple[int, bool]: ...
    def debug_beat_revision(self) -> int: ...
    def debug_wait_beat_revision(self, since: int, timeout_ms: int) -> int: ...
    def stats(self) -> dict[str, int]: ...
    def last_error(self) -> str: ...
    def refused(self) -> str: ...
    def leave(self) -> None: ...
    def abandon(self) -> None:
        """Drop the runtime without shutting it down. Forked children only."""

    def debug_registry_fds(self) -> list[int]: ...
    def debug_registry_transport(self) -> dict[str, int]: ...
    def debug_filter_index_stats(self, pool: str) -> dict[str, int]: ...
    def debug_filter_index_clear(self, pool: str) -> None: ...
    def debug_filter_scan_ids(
        self, pool: str, filter_msgpack: bytes | None = ..., require_ready: bool = ...
    ) -> list[int]: ...

class RpcOutcome:
    @property
    def kind(self) -> int: ...
    @property
    def request_id(self) -> str: ...
    @property
    def status(self) -> int | None: ...
    @property
    def error_type(self) -> str: ...
    @property
    def message(self) -> str: ...
    @property
    def traceback(self) -> str: ...
    @property
    def batch_index(self) -> int | None: ...
    @property
    def completed(self) -> int | None: ...
    @property
    def payload(self) -> bytes: ...

class RpcCompletion:
    def resolve(self, reusable: bool) -> RpcOutcome: ...

class RpcCallTicket:
    def cancel(self) -> None: ...

class RpcServer:
    def __init__(
        self,
        identity: str,
        methods: list[str],
        callback: Callable[
            ...,
            tuple[
                int,
                bytes,
                str,
                str,
                str,
                int | None,
                int | None,
                tuple[BlobRef, ...],
            ],
        ],
        owned: Callable[[], bool],
        host: str = "0.0.0.0",
        max_concurrency: int | None = None,
    ) -> None: ...
    @property
    def port(self) -> int: ...
    @property
    def identity(self) -> str: ...
    def close(self) -> None: ...
    def abandon(self) -> None: ...
    def stats(self) -> dict[str, int]: ...

def rpc_call_sync(
    endpoint: str,
    request_id: str,
    caller: str,
    target: str,
    payload: bytes,
    timeout_ms: int,
    method: str | None = None,
    batch_len: int | None = None,
    blob_owners: list[BlobRef] | tuple[BlobRef, ...] | None = None,
) -> RpcOutcome: ...
def rpc_call_async(
    loop_: Any,
    callback: Callable[[RpcCompletion], None],
    endpoint: str,
    request_id: str,
    caller: str,
    target: str,
    payload: bytes,
    timeout_ms: int,
    method: str | None = None,
    batch_len: int | None = None,
    blob_owners: list[BlobRef] | tuple[BlobRef, ...] | None = None,
) -> RpcCallTicket: ...
def rpc_reset_after_fork() -> None: ...
def rpc_shutdown() -> None: ...
def rpc_drop_endpoint(endpoint: str) -> None: ...
def rpc_debug_clear_pools() -> None: ...
def rpc_debug_state() -> dict[str, int]: ...
def rpc_debug_fds() -> list[int]: ...

RPC_OUTCOME_REPLY: int
RPC_OUTCOME_NOT_DELIVERED: int
RPC_OUTCOME_UNKNOWN: int
RPC_OUTCOME_CANCELLED: int
RPC_STATUS_SUCCESS: int
RPC_STATUS_METHOD_NOT_FOUND: int
RPC_STATUS_FENCED: int
RPC_STATUS_CALLER_FAULT: int
RPC_STATUS_CONCURRENCY_REFUSED: int
RPC_STATUS_REMOTE_ERROR: int
RPC_STATUS_MALFORMED_PROTOCOL: int
RPC_STATUS_INTERNAL: int
RPC_PROTOCOL: int
RPC_MAX_FRAME_BYTES: int
BLOB_MAX_BYTES: int
BLOB_MAX_REFS_PER_MESSAGE: int
BLOB_MAX_MAPPED_BYTES_PER_MESSAGE: int
BLOB_MAX_DECODED_HANDLES: int
BLOB_MAX_DECODED_MAPPINGS: int
BLOB_MAX_DECODED_BYTES: int
WAIT_PENDING: int
WAIT_READY: int
WAIT_TIMEOUT: int
WAIT_FENCED: int
WAIT_CLOSED: int
WAIT_STALE: int
WAIT_NO_SIZE: int
WAIT_MISMATCH: int

version: str

def serve_registry(listen: str, ttl_ms: int) -> None: ...

class BlobError(RuntimeError): ...

class BlobRef:
    @classmethod
    def create(cls, data: Any, max_bytes: int = ...) -> BlobRef: ...
    @classmethod
    def from_descriptor(cls, descriptor: bytes, max_bytes: int = ...) -> BlobRef: ...
    @property
    def closed(self) -> bool: ...
    def descriptor(self) -> bytes: ...
    def close(self) -> None: ...
    def view(self) -> memoryview: ...
    def __len__(self) -> int: ...
    def __bytes__(self) -> bytes: ...
    def __enter__(self) -> BlobRef: ...
    def __exit__(self, type: Any, value: Any, traceback: Any) -> bool: ...
    def _clone(self) -> BlobRef: ...
    def _after_fork_close(self) -> None: ...

def blob_close_after_fork() -> None: ...
