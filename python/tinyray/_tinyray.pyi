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
    def start(self) -> bool:
        """Sends one beat and blocks on it. False means it did not land."""

    def watch(self, pools: list[str]) -> None: ...
    def set_state(self, state_json: str, ready: bool) -> None: ...
    def set_url(self, url: str | None = ...) -> None: ...
    def lookup(self, pool: str, filter_json: str = ..., require_ready: bool = ...) -> str:
        """Matching members of `pool`, as a JSON list."""

    def pool_info(self, pool: str) -> tuple[int, int, int | None, list[str]] | None:
        """(version, roster, size, methods), or None if the pool is unseen."""

    def stats(self) -> dict[str, int]: ...
    def last_error(self) -> str: ...
    def refused(self) -> str: ...
    def leave(self) -> None: ...

version: str

def serve_registry(listen: str, ttl_ms: int) -> None: ...
