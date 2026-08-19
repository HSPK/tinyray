"""The calling half: a handle's attribute is a method on the far side.

This is a convention over plain HTTP, not a new protocol, so curl keeps
working:

    curl -X POST http://host:port/call/assign -d '{...}'
    curl http://host:port/_methods
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from ._errors import Fenced, RemoteError, Unreachable

MAX_BODY = 1 << 20
DEFAULT_TIMEOUT = 30.0  # The measured control-plane band is 2-30s.


def _decode(status: int, raw: bytes, target: str) -> Any:
    if status == 409:
        raise Fenced(f"{target} is held by a later tenure now; look it up again")
    try:
        body = json.loads(raw or b"{}")
    except json.JSONDecodeError as e:
        raise Unreachable(f"{target} returned unparseable body: {e}") from e
    if status == 404:
        raise AttributeError(body.get("error", "no such method"))
    if status == 413:
        raise ValueError(body.get("error", "payload too large"))
    if status != 200:
        raise Unreachable(f"{target} returned HTTP {status}")
    err = body.get("error")
    if err:
        raise RemoteError(err["type"], err["message"], err.get("traceback", ""))
    return body.get("result")


class BoundMethod:
    """Callable, and carries its own modifiers.

    Timeout is a modifier rather than a keyword argument so it cannot collide
    with a parameter of the same name on the far side.
    """

    __slots__ = ("_handle", "_name", "_timeout")

    def __init__(self, handle: Any, name: str, timeout: float):
        self._handle = handle
        self._name = name
        self._timeout = timeout

    def timeout(self, seconds: float) -> BoundMethod:
        return BoundMethod(self._handle, self._name, seconds)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if args and kwargs:
            raise TypeError("pass positional or keyword arguments, not both")
        payload = kwargs if kwargs or not args else (list(args) if len(args) > 1 else args[0])
        return invoke(self._handle, self._name, payload, self._timeout)

    def __repr__(self) -> str:
        return f"<BoundMethod {self._handle!r}.{self._name}>"


def invoke(handle: Any, name: str, payload: Any, timeout: float) -> Any:
    if handle.url is None:
        raise Unreachable(f"{handle} advertises no address; it was joined without serves=")
    body = json.dumps(payload).encode()
    if len(body) > MAX_BODY:
        # The byte budget is enforced, not merely documented. Failing loudly
        # beats getting slower and slower with nobody noticing.
        raise ValueError(
            f"{name}() payload is {len(body)} bytes, over the {MAX_BODY} limit; "
            f"use {handle}.url and send it yourself"
        )
    req = urllib.request.Request(
        f"{handle.url}/call/{name}",
        data=body,
        method="POST",
        headers={"content-type": "application/json", "x-tinyray-target": handle.identity},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return _decode(resp.status, resp.read(), handle.identity)
    except urllib.error.HTTPError as e:
        return _decode(e.code, e.read(), handle.identity)
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
        raise Unreachable(f"{handle.identity} at {handle.url}: {e}") from e
