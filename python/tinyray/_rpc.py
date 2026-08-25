"""Calling: an attribute on a handle is a method on the far side.

A convention over plain HTTP, not a new protocol, so curl still works.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import itertools
import json
import sys
import threading
import warnings
import weakref
from collections.abc import Callable, Iterator
from typing import Any, TypeVar

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

_T = TypeVar("_T")


def _sync_client() -> httpx.Client:
    # Reused: a fresh socket per call drains the ephemeral port range in
    # seconds and the server sits at 0% CPU while everything fails.
    global _sync
    if _sync is None:
        _sync = httpx.Client(limits=_LIMITS)
    return _sync


# Eviction runs from whichever thread asks next, so two loops in two threads
# reach it together. The lock is not decoration: without it, eight threads each
# running asyncio.run() had four die on the first round with EBADF, because two
# of them evicted the same entry and closed its descriptors twice -- and a
# number freed by the first close goes straight to whoever asks next.
_per_loop_lock = threading.Lock()


def per_loop(
    cache: dict[int, tuple[weakref.ref[asyncio.AbstractEventLoop], _T]],
    make: Callable[[asyncio.AbstractEventLoop], _T],
    drop: Callable[[_T], None] = lambda _: None,
) -> _T:
    """The one `_T` belonging to the running loop, made on first ask.

    Anything held per loop -- a transport pool, a wakeup pipe -- has the same
    two problems. It must be dropped once its loop closes, or the process
    accumulates descriptors against a limit of 1024. And it cannot be found by
    id() alone, because a freed loop's address is handed to the next one: 2 of
    5 consecutive asyncio.run() calls landed on an id already used. So the
    weak reference is checked as well as the number.
    """
    loop = asyncio.get_running_loop()
    with _per_loop_lock:
        for key, (ref, held) in list(cache.items()):
            got = ref()
            if got is None or got.is_closed():
                cache.pop(key, None)
                drop(held)
        key = id(loop)
        entry = cache.get(key)
        if entry is not None and entry[0]() is loop:
            return entry[1]
        made = make(loop)
        cache[key] = (weakref.ref(loop), made)
        return made


def reset_after_fork() -> None:
    """Give the child a lock nobody holds.

    A fork can land while another thread is inside per_loop, and the child
    inherits the lock held with no thread left to release it -- the child then
    hangs on its first watch, in native code with no Python frame to say why.
    """
    global _per_loop_lock, _sync
    _per_loop_lock = threading.Lock()
    # The shared one goes the same way, and it is the one that bites: two
    # processes taking turns on one keep-alive connection had a call come back
    # with the *other* process's answer -- no exception, no warning, just the
    # wrong value. Measured at 300 calls each from parent and child.
    _sync = None
    # The transports belong to the parent's loops and speak over the parent's
    # sockets. Dropped rather than closed: the parent still owns those
    # descriptors. Left in place, both processes write down the same
    # connection -- measured over 300 calls each from parent and child, every
    # run had a request arrive garbled as HTTP 400 (an OutcomeUnknown, so the
    # call may well have run) or the loop refuse the socket with
    # FileExistsError, against no failure at all once these are dropped.
    _loops.clear()


def _async_client() -> httpx.AsyncClient:
    # Dropping the reference is what closes the sockets: the pool cannot be
    # awaited shut once its loop is closed, so this is the only lever left.
    return per_loop(_loops, lambda _: httpx.AsyncClient(limits=_LIMITS))


def _app_stacklevel() -> int:
    """How far up the first frame that is not ours is.

    A fixed number cannot be right for both directions. Synchronously the
    frames are `_nudge`, `invoke`, `BoundMethod.__call__`, then the
    application -- four. On an event loop `__call__` has already returned by
    the time the coroutine runs, so the same four lands in asyncio's
    internals: measured pointing at `asyncio/events.py:84` instead of the line
    that made the call, which also collapses every async nudge into one
    suppressed duplicate.

    Counting is cheap here because nothing reaches this unless something was
    already over a megabyte.
    """
    level = 1
    frame: Any = sys._getframe(1)
    while frame is not None and frame.f_globals.get("__name__", "").startswith("tinyray"):
        level += 1
        frame = frame.f_back
    return level


def _nudge(what: str, size: int, where: str | None) -> None:
    """Oversize is worth saying and never worth refusing."""
    if size <= SOFT_BODY:
        return
    # The default filter collapses repeats, so a hot loop nudges once rather
    # than screaming -- which only works if the location is the caller's.
    warnings.warn(
        f"{what} {size} bytes, past the {SOFT_BODY} the control plane is meant "
        f"for. It goes through -- a nudge, not a limit -- but consider passing a "
        f"reference and fetching the payload from {where} yourself.",
        OversizeWarning,
        stacklevel=_app_stacklevel(),
    )


def _prepare(handle: Any, name: str, payload: Any) -> tuple[str, bytes, dict[str, str]]:
    if handle.url is None:
        raise NotDelivered(f"{handle} advertises no address; it joined without serves=")
    body = json.dumps(payload).encode()
    headers = {
        "content-type": "application/json",
        # Plain str: a header value has to be ASCII, which is why the names
        # that end up here are refused at the point they are chosen.
        "x-tinyray-target": handle.identity,
        "x-tinyray-caller": _identity,
        # Names this attempt, so the two sides can talk about the same call.
        # Deliberately just the name: deduplicating on it would mean the
        # callee deciding what is safe to replay, and only the caller knows
        # that. OutcomeUnknown is where that decision belongs.
        "x-tinyray-request": _request_id(),
    }
    return f"{handle.url}/call/{name}", body, headers


_seq = itertools.count(1)
_pinned: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "tinyray_request_id", default=None
)


def _request_id() -> str:
    fixed = _pinned.get()
    return fixed if fixed is not None else f"{_identity or 'anon'}-{next(_seq)}"


@contextlib.contextmanager
def request_id(value: str) -> Iterator[str]:
    """Name every call made inside this block, so retries share one name.

    The generated id changes per attempt, which is right for tracing and wrong
    for idempotency: a callee that wants to recognise a repeat needs the same
    name each time, and without this the key had to travel as an ordinary
    argument -- where it is one more thing to thread through, and one more
    thing to forget on the retry path.

        with tinyray.request_id(f"commit-{batch}"):
            for attempt in range(3):
                try:
                    return h.commit(rows)
                except tinyray.NotDelivered:
                    continue

    A block rather than a per-call argument, because that is the shape retries
    already have, and because a keyword would collide with the callee's own
    parameter names. Deduplication is still nobody's job here: tinyray only
    carries the name, since only the caller knows what is safe to replay.

    A ContextVar, so it follows an await into the tasks that block starts and
    does not leak into a neighbouring one.
    """
    # Checked here, where the mistake is. A name that cannot be a header value
    # fails inside httpx otherwise, and the caller is told the peer could not
    # be reached -- which sends them to look at the network.
    if not value:
        raise ValueError("a request id has to be something; empty names nothing")
    if not value.isascii() or any(c < " " or c == "\x7f" for c in value):
        raise ValueError(
            f"a request id has to be printable ASCII; got {value!r}. It travels "
            f"as a header, where anything else cannot be encoded and a newline "
            f"would end the header."
        )
    if len(value.encode()) > 200:
        raise ValueError(
            f"a request id of {len(value.encode())} bytes is too long; keep it "
            f"under 200. It is sent on every attempt, and servers cap headers."
        )
    token = _pinned.set(value)
    try:
        yield value
    finally:
        _pinned.reset(token)


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
    if status in (400, 408, 411):
        # Every way the far side gives up before the method runs. 400 is a
        # length it cannot read or a body it cannot parse, 408 a body that
        # stopped arriving part way, 411 framing it will not take at all.
        # Measured against a real callee: a body cut short was answered 408
        # after the body timeout and a content-length of "abc" answered 400 at
        # once, and in both the method was called zero times.
        #
        # 400 and 408 used to fall through to OutcomeUnknown, which tells the
        # caller the opposite of the truth -- that it may have run, so a
        # non-idempotent call cannot simply be sent again. A stalled upload is
        # an ordinary thing for a large payload on a busy link.
        raise NotDelivered(f"{target} would not take the request: HTTP {status} {raw[:120]!r}")
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
    """Same handle, awaitable methods.

    The whole difference is which of the two senders a bound method carries,
    so that is the whole class. The lookup itself -- what counts as a served
    name, and what the AttributeError says when it is not -- lives once on
    `Handle`; it used to live here as well, word for word, which is one place
    for an improved message to be made and the other to be forgotten in.
    """

    _send = staticmethod(ainvoke)
