"""Calling: an attribute on a handle is a native framed MessagePack RPC."""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import contextvars
import hashlib
import itertools
import math
import sys
import threading
import warnings
import weakref
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, TypeVar

import msgspec

from . import _tinyray as _native
from ._errors import (
    BatchError,
    Fenced,
    NotDelivered,
    OutcomeUnknown,
    OversizeWarning,
    RemoteError,
    Unreachable,
)
from ._msgpack import convert, convert_msgpack, dumps_with_blob_refs, loads, validate_type

SOFT_BODY = 1 << 20
DEFAULT_TIMEOUT = 30.0
MAX_BATCH = 128
_MAX_REQUEST_ID = 200

_identity = ""
_T = TypeVar("_T")
_RAW_RETURN = object()


def set_identity(who: str) -> None:
    global _identity
    _identity = who


_per_loop_lock = threading.Lock()


def per_loop(
    cache: dict[int, tuple[weakref.ref[asyncio.AbstractEventLoop], _T]],
    make: Callable[[asyncio.AbstractEventLoop], _T],
    drop: Callable[[_T], None] = lambda _: None,
    reuse: Callable[[_T], bool] = lambda _: True,
) -> _T:
    """Return the cached value belonging to the current live event loop."""
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
            if reuse(entry[1]):
                return entry[1]
            cache.pop(key)
            drop(entry[1])
        made = make(loop)
        cache[key] = (weakref.ref(loop), made)
        return made


def reset_after_fork() -> None:
    """Drop inherited loop locks, runtimes, pools, listeners, and sockets."""
    global _per_loop_lock
    _per_loop_lock = threading.Lock()
    _native.rpc_reset_after_fork()


def _shutdown() -> None:
    with contextlib.suppress(Exception):
        _native.rpc_shutdown()


atexit.register(_shutdown)


def _app_stacklevel() -> int:
    level = 1
    frame: Any = sys._getframe(1)
    while frame is not None and frame.f_globals.get("__name__", "").startswith(
        ("tinyray", "asyncio")
    ):
        level += 1
        frame = frame.f_back
    return level


def _nudge(what: str, size: int, where: str | None) -> None:
    if size <= SOFT_BODY:
        return
    warnings.warn(
        f"{what} {size} bytes, past the {SOFT_BODY} the control plane is meant "
        f"for. It goes through -- a nudge, not a limit -- but consider passing a "
        f"reference and fetching the payload from {where} yourself.",
        OversizeWarning,
        stacklevel=_app_stacklevel(),
    )


def _endpoint(handle: Any) -> str:
    endpoint = handle.url
    if endpoint is None:
        raise NotDelivered(f"{handle} advertises no address; it joined without serves=")
    if "://" in endpoint:
        raise NotDelivered(
            f"{handle.identity} advertises legacy URL {endpoint!r}; method RPC is a hard "
            "cutover to native framed MessagePack and requires host:port"
        )
    if not endpoint or endpoint.strip() != endpoint or ":" not in endpoint:
        raise NotDelivered(
            f"{handle.identity} advertises invalid native method endpoint {endpoint!r}; "
            "expected host:port"
        )
    return endpoint


def _prepare(
    handle: Any,
    name: str,
    payload: Any,
    *,
    batching: bool = False,
) -> tuple[str, bytes, str, tuple[Any, ...]]:
    endpoint = _endpoint(handle)
    body, keepalive = dumps_with_blob_refs(payload)
    return endpoint, body, _request_id(), keepalive


_seq = itertools.count(1)
_pinned: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "tinyray_request_id", default=None
)


def _request_id() -> str:
    fixed = _pinned.get()
    return fixed if fixed is not None else _generated_request_id(_identity or "anon", next(_seq))


def _generated_request_id(identity: str, sequence: int) -> str:
    suffix = f"-{sequence}"
    direct = identity + suffix
    if len(direct) <= _MAX_REQUEST_ID:
        return direct
    identity_digest = hashlib.sha256(identity.encode("ascii")).hexdigest()
    fixed = f"~{identity_digest}{suffix}"
    if len(fixed) <= _MAX_REQUEST_ID:
        return identity[: _MAX_REQUEST_ID - len(fixed)] + fixed
    return hashlib.sha256(f"{identity}\0{sequence}".encode("ascii")).hexdigest()


@contextlib.contextmanager
def request_id(value: str) -> Iterator[str]:
    """Pin the protocol request id for retries and application reconciliation."""
    if not value:
        raise ValueError("a request id has to be something; empty names nothing")
    if not value.isascii() or any(c < " " or c == "\x7f" for c in value):
        raise ValueError(
            f"a request id has to be printable ASCII; got {value!r}. It travels "
            "in every native RPC envelope."
        )
    if len(value.encode()) > _MAX_REQUEST_ID:
        raise ValueError(
            f"a request id of {len(value.encode())} bytes is too long; keep it "
            "under 200. It is sent on every attempt."
        )
    token = _pinned.set(value)
    try:
        yield value
    finally:
        _pinned.reset(token)


def _batch_request_id(root: str, index: int) -> str:
    suffix = f":{index}"
    if len(root) + len(suffix) <= _MAX_REQUEST_ID:
        return root + suffix
    digest = hashlib.sha256(root.encode()).hexdigest()
    prefix = root[: _MAX_REQUEST_ID - len(digest) - len(suffix) - 1]
    return f"{prefix}~{digest}{suffix}"


def _timeout_ms(timeout: float) -> int:
    try:
        seconds = float(timeout)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"RPC timeout has to be a finite non-negative number, got {timeout!r}"
        ) from exc
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError(f"RPC timeout has to be a finite non-negative number, got {timeout!r}")
    return min(math.ceil(seconds * 1000), (1 << 64) - 1)


def _transport_error(handle: Any, name: str, outcome: Any) -> Unreachable:
    at = f"{handle.identity} at {handle.url}: {outcome.message}"
    if outcome.kind == _native.RPC_OUTCOME_NOT_DELIVERED:
        return NotDelivered(f"{name}() never reached {at}")
    return OutcomeUnknown(f"{name}() may or may not have run on {at}")


def _raise_reply(outcome: Any, target: str) -> None:
    status = outcome.status
    message = outcome.message
    if status == _native.RPC_STATUS_METHOD_NOT_FOUND:
        raise AttributeError(message or "no such method")
    if status == _native.RPC_STATUS_FENCED:
        raise Fenced(f"{target} is held by a later tenure now; look it up again")
    if status == _native.RPC_STATUS_CALLER_FAULT:
        raise TypeError(message or "argument does not match the signature")
    if status == _native.RPC_STATUS_CONCURRENCY_REFUSED:
        raise NotDelivered(f"{target} is at its concurrency limit")
    if status == _native.RPC_STATUS_REMOTE_ERROR:
        raise RemoteError(outcome.error_type or "Exception", message, outcome.traceback)
    if status == _native.RPC_STATUS_MALFORMED_PROTOCOL:
        raise NotDelivered(f"{target} refused the malformed native RPC request: {message}")
    if status == _native.RPC_STATUS_INTERNAL:
        raise OutcomeUnknown(f"{target} failed inside the native RPC listener: {message}")
    raise OutcomeUnknown(f"{target} returned unknown native RPC status {status!r}")


def _reply_payload(outcome: Any, target: str, raw: bytes | None = None) -> bytes:
    if outcome.kind != _native.RPC_OUTCOME_REPLY:
        raise AssertionError("transport outcomes are handled before reply decoding")
    if outcome.status != _native.RPC_STATUS_SUCCESS:
        _raise_reply(outcome, target)
    return bytes(outcome.payload) if raw is None else raw


def _decode(outcome: Any, target: str, raw: bytes | None = None) -> Any:
    raw = _reply_payload(outcome, target, raw)
    try:
        return loads(raw)
    except (msgspec.DecodeError, UnicodeError, ValueError, RecursionError) as exc:
        raise OutcomeUnknown(f"{target} answered with malformed MessagePack: {exc}") from exc


def _decode_typed(outcome: Any, target: str, name: str, want: Any, raw: bytes | None = None) -> Any:
    raw = _reply_payload(outcome, target, raw)
    call = f"{target}.{name}()"
    try:
        return convert_msgpack(raw, want)
    except msgspec.ValidationError as exc:
        label = getattr(want, "__qualname__", repr(want))
        raise TypeError(f"{call} returned MessagePack that does not match {label}: {exc}") from exc
    except TypeError as exc:
        label = getattr(want, "__qualname__", repr(want))
        raise TypeError(f"{call} returned MessagePack that does not match {label}: {exc}") from exc
    except (msgspec.DecodeError, UnicodeError, ValueError, RecursionError) as exc:
        raise OutcomeUnknown(f"{call} answered with malformed MessagePack: {exc}") from exc


def _decode_batch(outcome: Any, target: str, expected: int, raw: bytes | None = None) -> list[Any]:
    if outcome.kind != _native.RPC_OUTCOME_REPLY:
        raise AssertionError("transport outcomes are handled before batch decoding")
    if outcome.status == _native.RPC_STATUS_SUCCESS:
        try:
            results = loads(bytes(outcome.payload) if raw is None else raw)
        except (msgspec.DecodeError, UnicodeError, ValueError, RecursionError) as exc:
            raise OutcomeUnknown(
                f"{target} answered with an invalid batch response; item outcomes are unknown"
            ) from exc
        if not isinstance(results, list) or len(results) != expected:
            raise OutcomeUnknown(
                f"{target} answered with an invalid batch response; item outcomes are unknown"
            )
        return results

    index = outcome.batch_index
    completed = outcome.completed
    if index is None and completed is None:
        _raise_reply(outcome, target)
    invalid = f"{target} answered with an invalid batch response; item outcomes are unknown"
    if (
        type(index) is not int
        or type(completed) is not int
        or index != completed
        or not 0 <= index < expected
    ):
        raise OutcomeUnknown(invalid)
    try:
        results = loads(bytes(outcome.payload) if raw is None else raw)
    except (msgspec.DecodeError, UnicodeError, ValueError, RecursionError) as exc:
        raise OutcomeUnknown(invalid) from exc
    if not isinstance(results, list) or len(results) != completed:
        raise OutcomeUnknown(invalid)
    try:
        _raise_reply(outcome, target)
    except (AttributeError, TypeError, Fenced, RemoteError) as exc:
        raise BatchError(index, results, exc) from exc
    raise OutcomeUnknown(invalid)


def _native_sync(
    handle: Any,
    name: str,
    body: bytes,
    request: str,
    timeout: float,
    batch_size: int | None,
    blob_owners: tuple[Any, ...] = (),
) -> Any:
    endpoint = _endpoint(handle)
    outcome = _native.rpc_call_sync(
        endpoint,
        request,
        _identity,
        handle.identity,
        body,
        _timeout_ms(timeout),
        method=None if batch_size is not None else name,
        batch_len=batch_size,
        blob_owners=blob_owners,
    )
    if outcome.kind != _native.RPC_OUTCOME_REPLY:
        raise _transport_error(handle, name, outcome)
    return outcome


def invoke(
    handle: Any,
    name: str,
    payload: Any,
    timeout: float,
    *,
    _batch_size: int | None = None,
    _return_type: Any = _RAW_RETURN,
) -> Any:
    endpoint, body, request, _keepalive = _prepare(
        handle, name, payload, batching=_batch_size is not None
    )
    del payload
    _nudge(f"{name}() is sending", len(body), endpoint)
    outcome = _native_sync(handle, name, body, request, timeout, _batch_size, _keepalive)
    raw = bytes(outcome.payload)
    _nudge(f"{handle.identity}.{name}() returned", len(raw), endpoint)
    if _batch_size is not None:
        return _decode_batch(outcome, handle.identity, _batch_size, raw)
    if _return_type is not _RAW_RETURN:
        return _decode_typed(outcome, handle.identity, name, _return_type, raw)
    return _decode(outcome, handle.identity, raw)


async def _await_native(
    handle: Any,
    name: str,
    body: bytes,
    request: str,
    timeout: float,
    batch_size: int | None,
    blob_owners: tuple[Any, ...] = (),
) -> Any:
    loop = asyncio.get_running_loop()
    future: asyncio.Future[Any] = loop.create_future()

    def complete(completion: Any) -> None:
        if future.done():
            completion.resolve(False)
            return
        try:
            outcome = completion.resolve(True)
        except Exception as exc:
            future.set_exception(exc)
        else:
            future.set_result(outcome)

    ticket = _native.rpc_call_async(
        loop,
        complete,
        _endpoint(handle),
        request,
        _identity,
        handle.identity,
        body,
        _timeout_ms(timeout),
        method=None if batch_size is not None else name,
        batch_len=batch_size,
        blob_owners=blob_owners,
    )
    try:
        outcome = await future
    except asyncio.CancelledError:
        ticket.cancel()
        raise
    if outcome.kind != _native.RPC_OUTCOME_REPLY:
        raise _transport_error(handle, name, outcome)
    return outcome


async def ainvoke(
    handle: Any,
    name: str,
    payload: Any,
    timeout: float,
    *,
    _batch_size: int | None = None,
    _return_type: Any = _RAW_RETURN,
) -> Any:
    endpoint, body, request, _keepalive = _prepare(
        handle, name, payload, batching=_batch_size is not None
    )
    del payload
    _nudge(f"{name}() is sending", len(body), endpoint)
    outcome = await _await_native(handle, name, body, request, timeout, _batch_size, _keepalive)
    raw = bytes(outcome.payload)
    _nudge(f"{handle.identity}.{name}() returned", len(raw), endpoint)
    if _batch_size is not None:
        return _decode_batch(outcome, handle.identity, _batch_size, raw)
    if _return_type is not _RAW_RETURN:
        return _decode_typed(outcome, handle.identity, name, _return_type, raw)
    return _decode(outcome, handle.identity, raw)


@dataclass(frozen=True)
class Call:
    """One batch item. Only public ASCII method names can be served."""

    method: str
    args: tuple[Any, ...] | list[Any] = ()
    kwargs: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        _call_payload(self)


def _call_payload(call: Call) -> dict[str, Any]:
    name = call.method
    if not isinstance(name, str):
        raise TypeError("a batch method has to be a string")
    if not name.isascii() or not name.isidentifier() or name.startswith("_"):
        raise ValueError("a batch method has to be a public ASCII identifier")
    if not isinstance(call.args, (tuple, list)):
        raise TypeError("batch args has to be a tuple or list")
    if call.kwargs is not None and (
        not isinstance(call.kwargs, dict) or any(not isinstance(k, str) for k in call.kwargs)
    ):
        raise TypeError("batch kwargs has to be a dict with string keys, or None")
    return {"method": name, "args": list(call.args), "kwargs": dict(call.kwargs or {})}


def _batch_payload(calls: Iterable[Call]) -> list[dict[str, Any]]:
    items = []
    for index, call in enumerate(calls):
        if index >= MAX_BATCH:
            raise ValueError(f"a batch can contain at most {MAX_BATCH} calls")
        if not isinstance(call, Call):
            raise TypeError(f"batch item {index} has to be a Call")
        items.append(_call_payload(call))
    return items


def batch(handle: Any, calls: Iterable[Call], timeout: float = DEFAULT_TIMEOUT) -> list[Any]:
    """Run up to 128 calls in order, stopping at the first failure. Not atomic."""
    items = _batch_payload(calls)
    if not items:
        return []
    return invoke(handle, "batch", {"calls": items}, timeout, _batch_size=len(items))


async def abatch(handle: Any, calls: Iterable[Call], timeout: float = DEFAULT_TIMEOUT) -> list[Any]:
    """Await a native batch; cancellation stops waiting, not remote execution."""
    items = _batch_payload(calls)
    if not items:
        return []
    return await ainvoke(handle, "batch", {"calls": items}, timeout, _batch_size=len(items))


def _restore_return(value: Any, want: Any, target: str) -> Any:
    try:
        return convert(value, want)
    except (msgspec.ValidationError, TypeError) as exc:
        label = getattr(want, "__qualname__", repr(want))
        raise TypeError(
            f"{target} returned MessagePack that does not match {label}: {exc}"
        ) from exc


class BoundMethod:
    """Callable, and carries its own timeout and return-type modifiers."""

    __slots__ = ("_handle", "_name", "_timeout", "_send", "_return_type")

    def __init__(
        self,
        handle: Any,
        name: str,
        timeout: float,
        send: Any = invoke,
        return_type: Any = _RAW_RETURN,
    ):
        self._handle, self._name, self._timeout, self._send = handle, name, timeout, send
        self._return_type = return_type

    def timeout(self, seconds: float) -> BoundMethod:
        return BoundMethod(self._handle, self._name, seconds, self._send, self._return_type)

    def returns(self, return_type: Any) -> BoundMethod:
        """Restore the MessagePack result as ``return_type`` for this call."""
        validate_type(return_type)
        return BoundMethod(self._handle, self._name, self._timeout, self._send, return_type)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        payload = {"args": list(args), "kwargs": kwargs}
        if self._return_type is _RAW_RETURN:
            return self._send(self._handle, self._name, payload, self._timeout)
        return self._send(
            self._handle,
            self._name,
            payload,
            self._timeout,
            _return_type=self._return_type,
        )

    def __repr__(self) -> str:
        return f"<BoundMethod {self._handle!r}.{self._name}>"


class AsyncHandleMixin:
    """Same handle, with an async-native Tokio completion path."""

    _send = staticmethod(ainvoke)
