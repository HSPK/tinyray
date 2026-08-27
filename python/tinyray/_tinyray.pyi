"""Types for the Rust extension. Everything crossing the boundary is JSON or a
primitive, so the surface here is deliberately small."""

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
    ) -> None: ...
    @property
    def accepted(self) -> bool: ...
    @property
    def silence_ms(self) -> int: ...
    def start(self, budget_ms: int = 5_000) -> bool:
        """Sends one beat and blocks on it. False means it did not land."""

    def watch(self, pools: list[str]) -> None:
        """Raises once the subscription list is full."""

    def set_state(self, state_json: str, ready: bool) -> bool:
        """False when the pair was already exactly this, so nothing was nudged."""

    def set_state_only(self, state_json: str) -> bool:
        """Publish state without touching readiness."""

    def is_ready(self) -> bool: ...
    def field_digest(self, pool: str, fields: list[str]) -> int | None:
        """A hash over only these fields of every member, plus who is present."""

    def registry(self) -> tuple[int, str]:
        """(protocol, version) as last reported by the registry."""

    def add_wake_fd(self, fd: int) -> None:
        """Also write a byte to `fd` whenever the bell rings."""

    def drop_wake_fd(self, fd: int) -> None:
        """Stop writing to `fd`. Call before closing it."""

    def wake(self) -> None:
        """Ring the bell with nothing changed, so waiters can re-check."""

    def set_url(self, url: str | None = ...) -> None: ...
    def lookup(self, pool: str, filter_json: str = ..., require_ready: bool = ...) -> str:
        """Matching members of `pool`, as a JSON list."""

    def pool_info(self, pool: str) -> tuple[int, int, int | None, list[str]] | None:
        """(version, roster, size, methods), or None if the pool is unseen."""

    def frozen(self, pool: str, require_ready: bool = ...) -> tuple[str, int, int, int] | None:
        """(members JSON, their fingerprint, the pool's, the pool's version)."""

    def cache_revision(self) -> int:
        """Moves once per beat, after the ack has been applied."""

    def wait_revision(self, since: int, timeout_ms: int) -> int:
        """Block until the cache moves past `since`. No polling anywhere."""

    def stats(self) -> dict[str, int]: ...
    def last_error(self) -> str: ...
    def refused(self) -> str: ...
    def leave(self) -> None: ...
    def abandon(self) -> None:
        """Drop the runtime without shutting it down. Forked children only."""

version: str

def serve_registry(listen: str, ttl_ms: int) -> None: ...
