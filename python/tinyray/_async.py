"""The async half of the calling layer.

Both flavours have to exist or the first real integration fails: a trainer
loop is synchronous, a collector loop is asyncio, and they talk to each other.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from ._call import DEFAULT_TIMEOUT, MAX_BODY, _decode
from ._errors import Unreachable


async def _request(url: str, body: bytes, identity: str, timeout: float) -> Any:
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(parts.hostname, parts.port), timeout
        )
    except (OSError, asyncio.TimeoutError) as e:
        raise Unreachable(f"{identity} at {url}: {e}") from e
    try:
        head = (
            f"POST {parts.path} HTTP/1.1\r\n"
            f"host: {parts.hostname}:{parts.port}\r\n"
            "content-type: application/json\r\n"
            f"x-tinyray-target: {identity}\r\n"
            f"content-length: {len(body)}\r\n"
            "connection: close\r\n\r\n"
        ).encode()
        writer.write(head + body)
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout)
    except (OSError, asyncio.TimeoutError) as e:
        raise Unreachable(f"{identity} at {url}: {e}") from e
    finally:
        writer.close()

    head_bytes, _, payload = raw.partition(b"\r\n\r\n")
    try:
        status = int(head_bytes.split(b"\r\n", 1)[0].split()[1])
    except (IndexError, ValueError) as e:
        raise Unreachable(f"{identity} sent a malformed response") from e
    return _decode(status, payload, identity)


class AsyncBoundMethod:
    __slots__ = ("_handle", "_name", "_timeout")

    def __init__(self, handle: Any, name: str, timeout: float):
        self._handle = handle
        self._name = name
        self._timeout = timeout

    def timeout(self, seconds: float) -> AsyncBoundMethod:
        return AsyncBoundMethod(self._handle, self._name, seconds)

    def __call__(self, *args: Any, **kwargs: Any):
        if args and kwargs:
            raise TypeError("pass positional or keyword arguments, not both")
        payload = kwargs if kwargs or not args else (list(args) if len(args) > 1 else args[0])
        h = self._handle
        if h.url is None:
            raise Unreachable(f"{h} advertises no address; it was joined without serves=")
        body = json.dumps(payload).encode()
        if len(body) > MAX_BODY:
            raise ValueError(
                f"{self._name}() payload is {len(body)} bytes, over the {MAX_BODY} limit; "
                f"use {h}.url and send it yourself"
            )
        return _request(f"{h.url}/call/{self._name}", body, h.identity, self._timeout)

    def __repr__(self) -> str:
        return f"<AsyncBoundMethod {self._handle!r}.{self._name}>"


class AsyncHandleMixin:
    """Same handle, awaitable methods."""

    def __getattr__(self, name: str) -> AsyncBoundMethod:
        if name.startswith("_") or name not in self._methods:
            raise AttributeError(
                f"{self.identity} serves {sorted(self._methods) or 'no methods'}, not {name!r}"
            )
        return AsyncBoundMethod(self, name, DEFAULT_TIMEOUT)
