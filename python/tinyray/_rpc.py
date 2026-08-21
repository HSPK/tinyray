"""Calling: an attribute on a handle is a method on the far side.

A convention over plain HTTP, not a new protocol, so curl still works.
"""

from __future__ import annotations

import asyncio
import json
import warnings
import weakref
from typing import Any

import httpx

from ._errors import (
    Fenced,
    NotDelivered,
    OutcomeUnknown,
    OversizeWarning,
    RemoteError,
    Unreachable,
)

# Past this a call is warned about, not refused. The control plane carries
# facts about where things are, not the things -- but a call is point to point,
# so going over is slow for the two ends and nothing else, and refusing at a
# threshold would turn a payload that grew from 900 KB to 1.1 MB into an
# outage. There is deliberately no ceiling above it.
# Nothing got onto the wire, or nothing reached the far side: the method did
# not run, whatever else happened. Everything else that httpx raises leaves the
# question open -- a read that timed out or a connection that broke mid-exchange
# may well have been acted on -- and "may have run" is the case that needs a
# request id, so the two must not share a class.
_NEVER_LEFT = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    httpx.ProxyError,
    httpx.UnsupportedProtocol,
    httpx.LocalProtocolError,
)

SOFT_BODY = 1 << 20
DEFAULT_TIMEOUT = 30.0  # the measured control-plane band is 2-30s

# Set at join() time. Travels on every call so the far side can bind a lease to
# the tenure that asked for it, instead of trusting an argument the caller had
# to remember to fill in correctly.
_identity = ""


def set_identity(who: str) -> None:
    global _identity
    _identity = who


_LIMITS = httpx.Limits(max_keepalive_connections=64, keepalive_expiry=60.0)
_sync: httpx.Client | None = None
# A client belongs to one loop, so it is cached per loop -- but held weakly and
# under a liveness check, because an id() is an address. Nothing retired an
# entry, so a program calling asyncio.run() per step (a synchronous training
# loop driving an async fleet, which is the shape this exists for) accumulated
# one client and one socket per call: measured at 100 calls, 100 entries, 100
# file descriptors, against a default limit of 1024. And the address a freed
# loop leaves behind is handed to the next one, so a later run could be given a
# pool belonging to a loop that is already closed -- 2 of 5 consecutive
# asyncio.run() calls landed on an id that had already been used.
_loops: dict[int, tuple[weakref.ref[asyncio.AbstractEventLoop], httpx.AsyncClient]] = {}


def _sync_client() -> httpx.Client:
    # Reused: a fresh socket per call drains the ephemeral port range in
    # seconds and the server sits at 0% CPU while everything fails.
    global _sync
    if _sync is None:
        _sync = httpx.Client(limits=_LIMITS)
    return _sync


def _async_client() -> httpx.AsyncClient:
    loop = asyncio.get_running_loop()
    # Drop whatever belongs to a loop that has gone. Dropping the reference is
    # what closes the sockets: the pool cannot be awaited shut once its loop
    # is closed, so this is the only lever left.
    for key, (ref, _) in list(_loops.items()):
        held = ref()
        if held is None or held.is_closed():
            del _loops[key]

    key = id(loop)
    got = _loops.get(key)
    if got is not None and got[0]() is loop:
        return got[1]
    client = httpx.AsyncClient(limits=_LIMITS)
    _loops[key] = (weakref.ref(loop), client)
    return client


def _nudge(what: str, size: int, where: str | None) -> None:
    """Oversize is worth saying and never worth refusing."""
    if size <= SOFT_BODY:
        return
    # stacklevel 4: here, invoke/ainvoke, BoundMethod.__call__, then the line
    # the application wrote -- which is why both directions are nudged from
    # invoke and not from _prepare, one frame deeper. The default filter
    # collapses repeats, so a hot loop nudges once rather than screaming.
    warnings.warn(
        f"{what} {size} bytes, past the {SOFT_BODY} the control plane is meant "
        f"for. It goes through -- a nudge, not a limit -- but consider passing a "
        f"reference and fetching the payload from {where} yourself.",
        OversizeWarning,
        stacklevel=4,
    )


def _prepare(handle: Any, name: str, payload: Any) -> tuple[str, bytes, dict[str, str]]:
    if handle.url is None:
        raise NotDelivered(f"{handle} advertises no address; it joined without serves=")
    body = json.dumps(payload).encode()
    headers = {
        "content-type": "application/json",
        "x-tinyray-target": handle.identity,
        "x-tinyray-caller": _identity,
    }
    return f"{handle.url}/call/{name}", body, headers


def _transport_error(handle: Any, name: str, exc: Exception) -> Unreachable:
    at = f"{handle.identity} at {handle.url}: {exc}"
    if isinstance(exc, _NEVER_LEFT):
        return NotDelivered(f"{name}() never reached {at}")
    return OutcomeUnknown(f"{name}() may or may not have run on {at}")


def _decode(status: int, raw: bytes, target: str) -> Any:
    if status == 409:
        raise Fenced(f"{target} is held by a later tenure now; look it up again")
    if status == 503:
        # Refused before dispatch, so nothing ran: retry here or elsewhere.
        raise NotDelivered(f"{target} is at its concurrency limit")
    if status == 411:
        # The body was never read, so neither was the call.
        raise NotDelivered(f"{target} refused the request framing: {raw[:120]!r}")
    if status >= 500:
        # The handler was already running when it came apart.
        raise OutcomeUnknown(f"{target} answered HTTP {status} partway through")
    try:
        body = json.loads(raw or b"{}")
    except json.JSONDecodeError as e:
        raise OutcomeUnknown(f"{target} answered with a body that will not parse: {e}") from e
    if status == 404:
        raise AttributeError(body.get("error", "no such method"))
    if status == 422:
        raise TypeError(body.get("error", "argument does not match the signature"))
    if status == 413:
        raise ValueError(body.get("error", "payload too large"))
    if status != 200:
        raise OutcomeUnknown(f"{target} returned HTTP {status}")
    err = body.get("error")
    if err:
        raise RemoteError(err["type"], err["message"], err.get("traceback", ""))
    return body.get("result")


def invoke(handle: Any, name: str, payload: Any, timeout: float) -> Any:
    url, body, headers = _prepare(handle, name, payload)
    _nudge(f"{name}() is sending", len(body), handle.url)
    try:
        r = _sync_client().post(url, content=body, headers=headers, timeout=timeout)
    except httpx.HTTPError as e:
        raise _transport_error(handle, name, e) from e
    # Nudged here rather than on the far side: a served process routinely has
    # its output sent to /dev/null, so a warning there is one nobody reads.
    _nudge(f"{handle.identity}.{name}() returned", len(r.content), handle.url)
    return _decode(r.status_code, r.content, handle.identity)


async def ainvoke(handle: Any, name: str, payload: Any, timeout: float) -> Any:
    url, body, headers = _prepare(handle, name, payload)
    _nudge(f"{name}() is sending", len(body), handle.url)
    try:
        r = await _async_client().post(url, content=body, headers=headers, timeout=timeout)
    except httpx.HTTPError as e:
        raise _transport_error(handle, name, e) from e
    _nudge(f"{handle.identity}.{name}() returned", len(r.content), handle.url)
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
