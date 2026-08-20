"""Calling: an attribute on a handle is a method on the far side.

A convention over plain HTTP, not a new protocol, so curl still works.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from ._errors import Fenced, RemoteError, Unreachable

MAX_BODY = 1 << 20
DEFAULT_TIMEOUT = 30.0  # the measured control-plane band is 2-30s

_LIMITS = httpx.Limits(max_keepalive_connections=64, keepalive_expiry=60.0)
_sync: httpx.Client | None = None
_loops: dict[int, httpx.AsyncClient] = {}


def _sync_client() -> httpx.Client:
    # Reused: a fresh socket per call drains the ephemeral port range in
    # seconds and the server sits at 0% CPU while everything fails.
    global _sync
    if _sync is None:
        _sync = httpx.Client(limits=_LIMITS)
    return _sync


def _async_client() -> httpx.AsyncClient:
    key = id(asyncio.get_running_loop())  # a client belongs to one loop
    client = _loops.get(key)
    if client is None:
        client = _loops[key] = httpx.AsyncClient(limits=_LIMITS)
    return client


def _prepare(handle: Any, name: str, payload: Any) -> tuple[str, bytes, dict[str, str]]:
    if handle.url is None:
        raise Unreachable(f"{handle} advertises no address; it joined without serves=")
    body = json.dumps(payload).encode()
    if len(body) > MAX_BODY:
        # The byte budget is enforced, not documented: failing loudly beats
        # getting quietly slower with nobody noticing.
        raise ValueError(
            f"{name}() payload is {len(body)} bytes, over the {MAX_BODY} limit; "
            f"use {handle}.url and send it yourself"
        )
    headers = {"content-type": "application/json", "x-tinyray-target": handle.identity}
    return f"{handle.url}/call/{name}", body, headers


def _decode(status: int, raw: bytes, target: str) -> Any:
    if status == 409:
        raise Fenced(f"{target} is held by a later tenure now; look it up again")
    try:
        body = json.loads(raw or b"{}")
    except json.JSONDecodeError as e:
        raise Unreachable(f"{target} returned an unparseable body: {e}") from e
    if status == 404:
        raise AttributeError(body.get("error", "no such method"))
    if status == 422:
        raise TypeError(body.get("error", "argument does not match the signature"))
    if status == 413:
        raise ValueError(body.get("error", "payload too large"))
    if status != 200:
        raise Unreachable(f"{target} returned HTTP {status}")
    err = body.get("error")
    if err:
        raise RemoteError(err["type"], err["message"], err.get("traceback", ""))
    return body.get("result")


def invoke(handle: Any, name: str, payload: Any, timeout: float) -> Any:
    url, body, headers = _prepare(handle, name, payload)
    try:
        r = _sync_client().post(url, content=body, headers=headers, timeout=timeout)
    except httpx.HTTPError as e:
        raise Unreachable(f"{handle.identity} at {handle.url}: {e}") from e
    return _decode(r.status_code, r.content, handle.identity)


async def ainvoke(handle: Any, name: str, payload: Any, timeout: float) -> Any:
    url, body, headers = _prepare(handle, name, payload)
    try:
        r = await _async_client().post(url, content=body, headers=headers, timeout=timeout)
    except httpx.HTTPError as e:
        raise Unreachable(f"{handle.identity} at {handle.url}: {e}") from e
    return _decode(r.status_code, r.content, handle.identity)


class BoundMethod:
    """Callable, and carries its own modifiers.

    Timeout is a modifier rather than a keyword argument so it cannot collide
    with a parameter of the same name on the far side.
    """

    __slots__ = ("_handle", "_name", "_timeout", "_send")

    def __init__(self, handle: Any, name: str, timeout: float, send: Any = invoke):
        self._handle, self._name, self._timeout, self._send = handle, name, timeout, send

    def timeout(self, seconds: float) -> BoundMethod:
        return BoundMethod(self._handle, self._name, seconds, self._send)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # Explicit args/kwargs: a bare object cannot tell f({"a": 1}) from f(a=1).
        payload = {"args": list(args), "kwargs": kwargs}
        return self._send(self._handle, self._name, payload, self._timeout)

    def __repr__(self) -> str:
        return f"<BoundMethod {self._handle!r}.{self._name}>"


class AsyncHandleMixin:
    """Same handle, awaitable methods."""

    # Declared because __getattr__ below answers for every other name, which
    # otherwise makes these look like BoundMethods too.
    _methods: tuple[str, ...]
    identity: str

    def __getattr__(self, name: str) -> BoundMethod:
        if name.startswith("_") or name not in self._methods:
            raise AttributeError(
                f"{self.identity} serves {sorted(self._methods) or 'no methods'}, not {name!r}"
            )
        return BoundMethod(self, name, DEFAULT_TIMEOUT, ainvoke)
