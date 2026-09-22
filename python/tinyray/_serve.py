"""Receiving: public Python methods behind a native Tokio MessagePack listener."""

from __future__ import annotations

import asyncio
import inspect
import threading
import traceback
import typing
from collections.abc import Callable
from typing import Any

import msgspec

from . import _tinyray as _native
from ._msgpack import (
    BlobError,
    BlobRef,
    blob_decode_scope,
    convert,
    convert_msgpack,
    dumps,
    dumps_with_blob_refs,
    is_model_type,
    loads,
)
from ._rpc import MAX_BATCH, _batch_request_id


class CallContext:
    """The self-declared caller identity and request id for one invocation."""

    __slots__ = ("identity", "pool", "slot", "incarnation", "request_id")

    def __init__(self, identity: str, request_id: str = ""):
        self.identity = identity
        self.request_id = request_id
        self.pool, _, seat = identity.partition("/")
        seat, _, tenure = seat.partition("#")
        self.slot = int(seat) if seat.isdigit() else None
        self.incarnation = int(tenure) if tenure.isdigit() else 0

    def __repr__(self) -> str:
        return f"<CallContext {self.identity or 'anonymous'}>"


_ABSENT = object()


def scan(obj: Any) -> dict[str, Callable[..., Any]]:
    """Return public callable attributes in declaration-compatible order."""
    out: dict[str, Callable[..., Any]] = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        static = inspect.getattr_static(obj, name, _ABSENT)
        if static is _ABSENT:
            attr = getattr(obj, name, None)
        elif callable(static) or isinstance(static, classmethod):
            attr = getattr(obj, name)
        else:
            continue
        if not callable(attr) or isinstance(attr, type):
            continue
        if not (name.isascii() and name.isidentifier()):
            raise ValueError(
                f"{type(obj).__name__}.{name} cannot be served: a method name "
                "travels in every native RPC envelope, so it has to be an ASCII "
                "identifier. Rename it, or make it private with a leading underscore."
            )
        out[name] = attr
    return out


_SHAPES: dict[Any, tuple[dict[str, Any], inspect.Signature | None]] = {}
_RAW_INPUTS: dict[Any, bool] = {}


class _RawCall(msgspec.Struct, forbid_unknown_fields=True):
    args: list[msgspec.Raw] | None = None
    kwargs: dict[str, msgspec.Raw] | None = None


_decode_raw_call = msgspec.msgpack.Decoder(_RawCall).decode


def _shape(fn: Callable[..., Any]) -> tuple[dict[str, Any], inspect.Signature | None]:
    key = getattr(fn, "__func__", fn)
    got = _SHAPES.get(key)
    if got is None:
        try:
            hints = typing.get_type_hints(fn)
        except Exception:
            hints = {}
        try:
            sig: inspect.Signature | None = inspect.signature(fn)
        except (TypeError, ValueError):
            sig = None
        got = _SHAPES[key] = (hints, sig)
    return got


def _takes_model(fn: Callable[..., Any]) -> bool:
    key = getattr(fn, "__func__", fn)
    if key not in _RAW_INPUTS:
        hints, sig = _shape(fn)
        _RAW_INPUTS[key] = sig is not None and any(
            is_model_type(hints.get(name)) for name in sig.parameters
        )
    return _RAW_INPUTS[key]


def _coerce_value(value: Any, want: Any) -> Any:
    if isinstance(value, msgspec.Raw):
        return loads(bytes(value)) if want is None else convert_msgpack(value, want)
    return value if want is None else convert(value, want)


def _coerce(
    fn: Callable[..., Any], payload: Any, caller: str = "", request_id: str = ""
) -> tuple[list[Any], dict[str, Any]]:
    """Unpack an argument envelope and validate it against the Python signature."""
    raw_values = isinstance(payload, _RawCall)
    if raw_values:
        args = list(payload.args or [])
        kwargs = dict(payload.kwargs or {})
    elif isinstance(payload, dict) and set(payload) <= {"args", "kwargs"}:
        given_args, given_kwargs = payload.get("args"), payload.get("kwargs")
        if given_args is not None and not isinstance(given_args, list):
            raise msgspec.ValidationError(
                f"'args' has to be an array, got {type(given_args).__name__}"
            )
        if given_kwargs is not None and not isinstance(given_kwargs, dict):
            raise msgspec.ValidationError(
                f"'kwargs' has to be a map, got {type(given_kwargs).__name__}"
            )
        args = list(given_args or [])
        kwargs = dict(given_kwargs or {})
    elif isinstance(payload, dict):
        args, kwargs = [], dict(payload)
    elif isinstance(payload, list):
        args, kwargs = list(payload), {}
    else:
        args, kwargs = [payload], {}

    hints, sig = _shape(fn)
    if sig is None:
        return args, kwargs
    kinds = inspect.Parameter
    injected = {name for name in sig.parameters if hints.get(name) is CallContext}
    for name in injected:
        if sig.parameters[name].kind in (kinds.VAR_POSITIONAL, kinds.VAR_KEYWORD):
            raise msgspec.ValidationError("CallContext needs a named, non-variadic parameter")
    public = (
        sig.replace(parameters=[p for p in sig.parameters.values() if p.name not in injected])
        if injected
        else sig
    )
    try:
        public_kwargs = {key: value for key, value in kwargs.items() if key not in injected}
        bound = public.bind(*args, **public_kwargs)
    except (BlobError, TypeError) as exc:
        raise msgspec.ValidationError(str(exc)) from None

    for name, value in bound.arguments.items():
        want = hints.get(name)
        if want is None and not raw_values:
            continue
        kind = sig.parameters[name].kind
        if kind is kinds.VAR_POSITIONAL:
            bound.arguments[name] = tuple(_coerce_value(item, want) for item in value)
        elif kind is kinds.VAR_KEYWORD:
            bound.arguments[name] = {key: _coerce_value(item, want) for key, item in value.items()}
        else:
            bound.arguments[name] = _coerce_value(value, want)

    if injected:
        bound.apply_defaults()
        context = CallContext(caller, request_id)
        bound = inspect.BoundArguments(
            sig,
            {
                name: context if name in injected else bound.arguments[name]
                for name in sig.parameters
            },
        )
    return list(bound.args), bound.kwargs


class Counters:
    """Serving counters; MethodServer snapshots the native listener atomically."""

    __slots__ = (
        "calls",
        "refused",
        "failed",
        "in_flight",
        "peak_in_flight",
        "busy_ns",
        "_lock",
        "_native",
    )

    def __init__(self, native: Any = None) -> None:
        self.calls = 0
        self.refused = 0
        self.failed = 0
        self.in_flight = 0
        self.peak_in_flight = 0
        self.busy_ns = 0
        self._lock = threading.Lock()
        self._native = native

    def entered(self) -> None:
        with self._lock:
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)

    def answered(self, failed: bool) -> None:
        with self._lock:
            self.calls += 1
            self.failed += failed

    def left(self, spent_ns: int) -> None:
        with self._lock:
            self.in_flight -= 1
            self.busy_ns += spent_ns

    def refuse(self) -> None:
        with self._lock:
            self.refused += 1

    def snapshot(self) -> dict[str, int]:
        if self._native is not None:
            return dict(self._native.stats())
        with self._lock:
            return {
                "calls": self.calls,
                "refused": self.refused,
                "failed": self.failed,
                "in_flight": self.in_flight,
                "peak_in_flight": self.peak_in_flight,
                "busy_ms": self.busy_ns // 1_000_000,
            }


_Reply = tuple[
    int,
    bytes,
    str,
    str,
    str,
    int | None,
    int | None,
    tuple[BlobRef, ...],
]


def _reply(
    status: int,
    payload: bytes = b"",
    *,
    error_type: str = "",
    message: str = "",
    traceback_text: str = "",
    batch_index: int | None = None,
    completed: int | None = None,
    blob_owners: tuple[BlobRef, ...] = (),
) -> _Reply:
    return (
        status,
        payload,
        error_type,
        message,
        traceback_text,
        batch_index,
        completed,
        blob_owners,
    )


class _Dispatch:
    def __init__(
        self,
        dispatch: dict[str, Callable[..., Any]],
        identity: str,
        loop: asyncio.AbstractEventLoop | None,
    ):
        self.dispatch = dispatch
        self.identity = identity
        self.loop = loop
        self.still_ours: Callable[[], bool] = lambda: True

    def owned(self) -> bool:
        return self.still_ours()

    def __call__(
        self,
        operation: str,
        method: str | None,
        raw: bytes,
        caller: str,
        request_id: str,
        batch_len: int | None,
    ) -> _Reply:
        if operation == "batch":
            return self._batch(raw, caller, request_id, batch_len)
        if operation != "call" or method is None:
            return _reply(
                _native.RPC_STATUS_MALFORMED_PROTOCOL,
                error_type="ProtocolError",
                message="invalid call metadata",
            )
        fn = self.dispatch.get(method)
        if fn is None:
            return _reply(
                _native.RPC_STATUS_METHOD_NOT_FOUND,
                error_type="AttributeError",
                message=f"no method {method!r}",
            )
        return self._call(fn, method, raw, caller, request_id)

    def _call(
        self,
        fn: Callable[..., Any],
        name: str,
        raw: bytes,
        caller: str,
        request_id: str,
    ) -> _Reply:
        with blob_decode_scope():
            payload = None
            if raw and _takes_model(fn):
                try:
                    payload = _decode_raw_call(raw)
                except msgspec.DecodeError:
                    pass
            if payload is None:
                try:
                    payload = loads(raw)
                except (BlobError, TypeError) as exc:
                    return _reply(
                        _native.RPC_STATUS_CALLER_FAULT,
                        error_type="TypeError",
                        message=f"{name}(): malformed MessagePack: {exc}",
                    )
                except (msgspec.DecodeError, UnicodeError, ValueError, RecursionError) as exc:
                    return _reply(
                        _native.RPC_STATUS_CALLER_FAULT,
                        error_type="ValidationError",
                        message=f"{name}(): malformed MessagePack: {exc}",
                    )
            return self._invoke(fn, name, payload, caller, request_id)

    def _invoke(
        self,
        fn: Callable[..., Any],
        name: str,
        payload: Any,
        caller: str,
        request_id: str,
    ) -> _Reply:
        try:
            args, kwargs = _coerce(fn, payload, caller, request_id)
        except (BlobError, msgspec.ValidationError, TypeError, ValueError) as exc:
            return _reply(
                _native.RPC_STATUS_CALLER_FAULT,
                error_type="TypeError",
                message=f"{name}(): {exc}",
            )
        try:
            result = fn(*args, **kwargs)
            if inspect.iscoroutine(result):
                loop = self.loop
                if loop is None or not loop.is_running():
                    result = asyncio.run(result)
                else:
                    result = asyncio.run_coroutine_threadsafe(result, loop).result()
        except Exception as exc:
            return _reply(
                _native.RPC_STATUS_REMOTE_ERROR,
                error_type=type(exc).__name__,
                message=str(exc),
                traceback_text=traceback.format_exc(),
            )
        try:
            payload, blob_owners = dumps_with_blob_refs(result)
            return _reply(
                _native.RPC_STATUS_SUCCESS,
                payload,
                blob_owners=blob_owners,
            )
        except (TypeError, ValueError, OverflowError, RecursionError) as exc:
            return _reply(
                _native.RPC_STATUS_REMOTE_ERROR,
                error_type="TypeError",
                message=f"the return value cannot be sent as MessagePack: {exc}",
            )

    def _batch(
        self,
        raw: bytes,
        caller: str,
        request_id: str,
        batch_len: int | None,
    ) -> _Reply:
        try:
            payload = loads(raw)
        except (BlobError, TypeError) as exc:
            return _reply(
                _native.RPC_STATUS_CALLER_FAULT,
                error_type="TypeError",
                message=f"malformed batch MessagePack: {exc}",
            )
        except (msgspec.DecodeError, UnicodeError, ValueError, RecursionError) as exc:
            return _reply(
                _native.RPC_STATUS_CALLER_FAULT,
                error_type="ValidationError",
                message=f"malformed batch MessagePack: {exc}",
            )
        if (
            not isinstance(payload, dict)
            or set(payload) != {"calls"}
            or not isinstance(payload["calls"], list)
        ):
            return _reply(
                _native.RPC_STATUS_CALLER_FAULT,
                error_type="ValidationError",
                message="a batch needs one 'calls' array",
            )
        calls = payload["calls"]
        if batch_len != len(calls):
            return _reply(
                _native.RPC_STATUS_MALFORMED_PROTOCOL,
                error_type="ProtocolError",
                message="batch metadata does not match the application payload",
            )
        if len(calls) > MAX_BATCH:
            return _reply(
                _native.RPC_STATUS_CALLER_FAULT,
                error_type="ValueError",
                message=f"a batch can contain at most {MAX_BATCH} calls",
            )
        for item in calls:
            if not isinstance(item, dict) or set(item) != {"method", "args", "kwargs"}:
                return _reply(
                    _native.RPC_STATUS_CALLER_FAULT,
                    error_type="ValidationError",
                    message="each batch item needs method, args and kwargs",
                )
            name = item["method"]
            if (
                not isinstance(name, str)
                or not name.isascii()
                or not name.isidentifier()
                or name.startswith("_")
                or not isinstance(item["args"], list)
                or not isinstance(item["kwargs"], dict)
                or any(not isinstance(key, str) for key in item["kwargs"])
            ):
                return _reply(
                    _native.RPC_STATUS_CALLER_FAULT,
                    error_type="ValidationError",
                    message="invalid batch method or argument envelope",
                )

        completed: list[msgspec.Raw] = []
        blob_owners: list[BlobRef] = []
        for index, item in enumerate(calls):
            if not self.still_ours():
                return _reply(
                    _native.RPC_STATUS_FENCED,
                    dumps(completed),
                    error_type="Fenced",
                    message=f"{self.identity} is held by a later tenure",
                    batch_index=index,
                    completed=index,
                    blob_owners=tuple(blob_owners),
                )
            name = item["method"]
            fn = self.dispatch.get(name)
            if fn is None:
                return _reply(
                    _native.RPC_STATUS_METHOD_NOT_FOUND,
                    dumps(completed),
                    error_type="AttributeError",
                    message=f"no method {name!r}",
                    batch_index=index,
                    completed=index,
                    blob_owners=tuple(blob_owners),
                )
            result = self._invoke(
                fn,
                name,
                {"args": item["args"], "kwargs": item["kwargs"]},
                caller,
                _batch_request_id(request_id, index),
            )
            if result[0] != _native.RPC_STATUS_SUCCESS:
                return _reply(
                    result[0],
                    dumps(completed),
                    error_type=result[2],
                    message=result[3],
                    traceback_text=result[4],
                    batch_index=index,
                    completed=index,
                    blob_owners=tuple(blob_owners),
                )
            completed.append(msgspec.Raw(result[1]))
            blob_owners.extend(result[7])
        return _reply(
            _native.RPC_STATUS_SUCCESS,
            dumps(completed),
            blob_owners=tuple(blob_owners),
        )


class MethodServer:
    """One native Tokio listener per serving process."""

    def __init__(
        self,
        obj: Any,
        identity: str,
        host: str = "0.0.0.0",
        max_concurrency: int | None = None,
    ):
        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError("max_concurrency has to be positive or None")
        self.dispatch = scan(obj)
        try:
            loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        self._dispatch = _Dispatch(self.dispatch, identity, loop)
        self._srv = _native.RpcServer(
            identity,
            sorted(self.dispatch),
            self._dispatch,
            self._dispatch.owned,
            host,
            max_concurrency,
        )
        self.counters = Counters(self._srv)
        self.limit = max_concurrency
        self.port = self._srv.port
        self._endpoints: set[str] = set()
        self._closed = False

    @property
    def still_ours(self) -> Callable[[], bool]:
        return self._dispatch.still_ours

    @still_ours.setter
    def still_ours(self, value: Callable[[], bool]) -> None:
        self._dispatch.still_ours = value

    @property
    def methods(self) -> list[str]:
        return sorted(self.dispatch)

    def url(self, advertise: str) -> str:
        endpoint = f"{advertise}:{self.port}"
        self.track_endpoint(endpoint)
        return endpoint

    def track_endpoint(self, endpoint: str) -> None:
        self._endpoints.add(endpoint)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._srv.close()
        for endpoint in self._endpoints:
            _native.rpc_drop_endpoint(endpoint)
        self._endpoints.clear()
        self.dispatch.clear()

    def abandon(self) -> None:
        self._closed = True
        self._srv.abandon()
