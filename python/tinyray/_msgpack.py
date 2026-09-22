"""MessagePack application values for native method RPC.

Rust frames and correlates these bytes but never deserializes them. Python
owns standard dataclass serialization and annotation conversion.
"""

from __future__ import annotations

import sys
import threading
import typing
from contextlib import contextmanager
from dataclasses import fields, is_dataclass
from functools import lru_cache
from typing import Any

import msgspec

from . import _tinyray as _native

_EXT_BIG_INT = 121
_EXT_BIG_DICT = 122
_EXT_BIG_SET = 123
_EXT_BLOB_REF = 124
_EXT_TUPLE_KEY = 125
_EXT_FROZENSET_KEY = 126

BlobRef = _native.BlobRef
BlobError = _native.BlobError
_MAX_BLOB_REFS_PER_MESSAGE = _native.BLOB_MAX_REFS_PER_MESSAGE
_MAX_BLOB_MAPPED_BYTES_PER_MESSAGE = _native.BLOB_MAX_MAPPED_BYTES_PER_MESSAGE
_BLOB_HEADER_BYTES = 32
_codec_local = threading.local()

_PYDANTIC_ERROR = (
    "Pydantic models are unsupported in tinyray 0.18; use a standard "
    "dataclass, NamedTuple, TypedDict, or another MessagePack-compatible type"
)


class _BlobScopeRequired(Exception):
    pass


def _decode_ext(code: int, data: memoryview) -> Any:
    if code == _EXT_BLOB_REF:
        if len(data) > 4096:
            raise BlobError("BlobRef descriptor exceeds 4096 bytes")
        raw = bytes(data)
        state = getattr(_codec_local, "decode", None)
        if state is not None:
            state["refs"] += 1
            if state["refs"] > _MAX_BLOB_REFS_PER_MESSAGE:
                raise BlobError(
                    f"MessagePack contains more than {_MAX_BLOB_REFS_PER_MESSAGE} BlobRef values"
                )
            cached = state["cache"].get(raw)
            if cached is not None:
                return cached._clone()
            size = _descriptor_size(raw)
            if size is not None:
                mapped = state["mapped_bytes"] + _BLOB_HEADER_BYTES + size
                if mapped > _MAX_BLOB_MAPPED_BYTES_PER_MESSAGE:
                    raise BlobError(
                        "MessagePack BlobRef mappings exceed the "
                        f"{_MAX_BLOB_MAPPED_BYTES_PER_MESSAGE}-byte aggregate limit"
                    )
                state["mapped_bytes"] = mapped
        blob = BlobRef.from_descriptor(raw)
        if state is not None:
            state["cache"][raw] = blob
        return blob
    raw = bytes(data)
    if code == _EXT_BIG_INT:
        return int(raw.decode("ascii"))
    if code == _EXT_BIG_DICT:
        pairs = msgspec.msgpack.decode(raw)
        return {_decoder.decode(key): _decoder.decode(value) for key, value in pairs}
    if code == _EXT_BIG_SET:
        return {_decoder.decode(item) for item in msgspec.msgpack.decode(raw)}
    if code == _EXT_TUPLE_KEY:
        return tuple(_decoder.decode(raw))
    if code == _EXT_FROZENSET_KEY:
        return frozenset(_decoder.decode(raw))
    return msgspec.msgpack.Ext(code, raw)


def _decode_ext_fast(code: int, data: memoryview) -> Any:
    raise _BlobScopeRequired


_decoder = msgspec.msgpack.Decoder(ext_hook=_decode_ext)
_fast_decoder = msgspec.msgpack.Decoder(ext_hook=_decode_ext_fast)


def _descriptor_size(raw: bytes) -> int | None:
    try:
        descriptor = msgspec.msgpack.decode(raw)
    except msgspec.DecodeError:
        return None
    if not isinstance(descriptor, dict):
        return None
    size = descriptor.get("size")
    return size if type(size) is int and size >= 0 else None


@contextmanager
def blob_decode_scope():
    current = getattr(_codec_local, "decode", None)
    if current is not None:
        yield
        return
    _codec_local.decode = {"refs": 0, "mapped_bytes": 0, "cache": {}}
    try:
        yield
    finally:
        del _codec_local.decode


def _decode_with_scope(decoder: msgspec.msgpack.Decoder[Any], raw: Any) -> Any:
    with blob_decode_scope():
        return decoder.decode(raw)


def _decode_maybe_scoped(
    decoder: msgspec.msgpack.Decoder[Any],
    fast_decoder: msgspec.msgpack.Decoder[Any],
    raw: Any,
) -> Any:
    try:
        return fast_decoder.decode(raw)
    except _BlobScopeRequired:
        return _decode_with_scope(decoder, raw)


def _is_pydantic_type(value: Any) -> bool:
    return isinstance(value, type) and (
        hasattr(value, "__pydantic_validator__")
        or hasattr(value, "__pydantic_core_schema__")
        or hasattr(value, "__pydantic_model__")
        or hasattr(value, "model_fields")
        and hasattr(value, "model_validate")
        or any(base.__module__.startswith("pydantic.") for base in value.__mro__)
    )


def _reject_pydantic_values(value: Any) -> None:
    if "pydantic" not in sys.modules:
        return
    pending = [value]
    seen: set[int] = set()
    while pending:
        value = pending.pop()
        kind = type(value)
        if _is_pydantic_type(kind):
            raise TypeError(_PYDANTIC_ERROR)
        if kind not in (list, tuple, set, frozenset, dict) and not (
            is_dataclass(value) and not isinstance(value, type)
        ):
            continue
        marker = id(value)
        if marker in seen:
            continue
        seen.add(marker)
        if kind is dict:
            pending.extend(value.keys())
            pending.extend(value.values())
        elif kind in (list, tuple, set, frozenset):
            pending.extend(value)
        else:
            pending.extend(getattr(value, field.name) for field in fields(value))


def _contains_pydantic_type(value: Any, seen: set[int]) -> bool:
    marker = id(value)
    if marker in seen:
        return False
    seen.add(marker)
    if _is_pydantic_type(value):
        return True
    if any(_contains_pydantic_type(arg, seen) for arg in typing.get_args(value)):
        return True
    if not isinstance(value, type) or not is_dataclass(value):
        return False
    try:
        annotations = typing.get_type_hints(value)
    except (NameError, TypeError):
        annotations = getattr(value, "__annotations__", {})
    return any(_contains_pydantic_type(field, seen) for field in annotations.values())


@lru_cache(maxsize=256)
def _uses_pydantic_type(value: Any) -> bool:
    return _contains_pydantic_type(value, set())


def _reject_pydantic_type(value: Any) -> None:
    if _uses_pydantic_type(value):
        raise TypeError(_PYDANTIC_ERROR)


def _contains_dataclass(value: Any, seen: set[int]) -> bool:
    marker = id(value)
    if marker in seen:
        return False
    seen.add(marker)
    if _is_pydantic_type(value):
        return False
    if isinstance(value, type) and is_dataclass(value):
        return True
    if any(_contains_dataclass(arg, seen) for arg in typing.get_args(value)):
        return True
    if not isinstance(value, type) or not getattr(value, "__annotations__", None):
        return False
    try:
        annotations = typing.get_type_hints(value)
    except (NameError, TypeError):
        annotations = value.__annotations__
    return any(_contains_dataclass(field, seen) for field in annotations.values())


@lru_cache(maxsize=256)
def is_model_type(value: Any) -> bool:
    return _contains_dataclass(value, set())


def _encoded_item(value: Any) -> bytes:
    return _encoder.encode(value)


def _prepare_hashable(value: Any, active: set[int]) -> Any:
    kind = type(value)
    if kind not in (tuple, frozenset):
        return _prepare_bigints(value, active)
    marker = id(value)
    if marker in active:
        raise ValueError("recursive values cannot be sent as MessagePack")
    active.add(marker)
    try:
        items = [_prepare_hashable(item, active) for item in value]
        code = _EXT_TUPLE_KEY if kind is tuple else _EXT_FROZENSET_KEY
        return msgspec.msgpack.Ext(code, _encoder.encode(items))
    finally:
        active.remove(marker)


def _big_dict(value: dict[Any, Any], active: set[int]) -> msgspec.msgpack.Ext:
    pairs = [
        (
            _encoded_item(_prepare_hashable(key, active)),
            _encoded_item(_prepare_bigints(item, active)),
        )
        for key, item in value.items()
    ]
    return msgspec.msgpack.Ext(_EXT_BIG_DICT, msgspec.msgpack.encode(pairs))


def _big_set(value: set[Any] | frozenset[Any], active: set[int]) -> msgspec.msgpack.Ext:
    items = [_encoded_item(_prepare_hashable(item, active)) for item in value]
    return msgspec.msgpack.Ext(_EXT_BIG_SET, msgspec.msgpack.encode(items))


def _prepare_bigints(value: Any, active: set[int]) -> Any:
    if type(value) is int and not -(1 << 63) <= value < (1 << 64):
        return msgspec.msgpack.Ext(_EXT_BIG_INT, str(value).encode("ascii"))
    kind = type(value)
    if kind not in (list, tuple, set, frozenset, dict) and not (
        is_dataclass(value) and not isinstance(value, type)
    ):
        return value
    marker = id(value)
    if marker in active:
        raise ValueError("recursive values cannot be sent as MessagePack")
    active.add(marker)
    try:
        if kind is dict:
            converted = [
                (_prepare_bigints(key, active), _prepare_bigints(item, active))
                for key, item in value.items()
            ]
            try:
                return dict(converted)
            except TypeError:
                return _big_dict(value, active)
        if kind in (set, frozenset):
            converted = [_prepare_bigints(item, active) for item in value]
            try:
                return kind(converted)
            except TypeError:
                return _big_set(value, active)
        if kind is list:
            return [_prepare_bigints(item, active) for item in value]
        if kind is tuple:
            return tuple(_prepare_bigints(item, active) for item in value)
        return {
            field.name: _prepare_bigints(getattr(value, field.name), active)
            for field in fields(value)
        }
    finally:
        active.remove(marker)


def _enc_hook(value: Any) -> Any:
    if type(value) is BlobRef:
        if value.closed:
            raise BlobError("cannot encode a closed BlobRef")
        state = getattr(_codec_local, "encode", None)
        if state is not None and id(value) not in state["seen"]:
            state["seen"].add(id(value))
            state["owners"].append(value)
        return msgspec.msgpack.Ext(_EXT_BLOB_REF, value.descriptor())
    if _is_pydantic_type(type(value)):
        raise TypeError(_PYDANTIC_ERROR)
    raise TypeError(f"Encoding objects of type {type(value).__name__} is unsupported")


def _enc_hook_fast(value: Any) -> Any:
    if type(value) is BlobRef:
        raise _BlobScopeRequired
    return _enc_hook(value)


_encoder = msgspec.msgpack.Encoder(enc_hook=_enc_hook)
_fast_encoder = msgspec.msgpack.Encoder(enc_hook=_enc_hook_fast)


def _encode_with_blob_scope(value: Any) -> tuple[bytes, tuple[BlobRef, ...]]:
    current = getattr(_codec_local, "encode", None)
    own_scope = current is None
    if own_scope:
        current = {"owners": [], "seen": set()}
        _codec_local.encode = current
    assert current is not None
    try:
        try:
            encoded = _encoder.encode(value)
        except OverflowError:
            encoded = _encoder.encode(_prepare_bigints(value, set()))
        return encoded, tuple(current["owners"])
    finally:
        if own_scope:
            del _codec_local.encode


def _encode_prepared(value: Any) -> tuple[bytes, tuple[BlobRef, ...]]:
    try:
        return _fast_encoder.encode(value), ()
    except (_BlobScopeRequired, OverflowError):
        return _encode_with_blob_scope(value)


def dumps(value: Any) -> bytes:
    """Encode one application value with native MessagePack semantics."""
    _reject_pydantic_values(value)
    return _encode_prepared(value)[0]


def dumps_with_blob_refs(value: Any) -> tuple[bytes, tuple[BlobRef, ...]]:
    """Encode one value and return the exact BlobRef owners encountered."""
    _reject_pydantic_values(value)
    return _encode_prepared(value)


def blob(data: Any, *, max_bytes: int = _native.BLOB_MAX_BYTES) -> BlobRef:
    """Copy one bytes-like value into a sealed same-host Linux memfd."""
    return BlobRef.create(data, max_bytes=max_bytes)


def blob_refs(value: Any) -> tuple[BlobRef, ...]:
    """BlobRefs nested in one outgoing application value, retained until delivery."""
    pending = [value]
    seen: set[int] = set()
    found: list[BlobRef] = []
    while pending:
        current = pending.pop()
        if type(current) is BlobRef:
            if current.closed:
                raise BlobError("cannot send a closed BlobRef")
            found.append(current)
            continue
        if not isinstance(current, (list, tuple, set, frozenset, dict)) and not (
            is_dataclass(current) and not isinstance(current, type)
        ):
            continue
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        if isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
        elif isinstance(current, (list, tuple, set, frozenset)):
            pending.extend(current)
        else:
            pending.extend(getattr(current, field.name) for field in fields(current))
    return tuple(found)


def close_blobs_after_fork() -> None:
    _native.blob_close_after_fork()


def loads(raw: Any) -> Any:
    """Decode one complete application value."""
    return _decode_maybe_scoped(_decoder, _fast_decoder, raw)


def convert(value: Any, want: Any) -> Any:
    """Restore an already-decoded MessagePack value as ``want``."""
    _reject_pydantic_type(want)
    return msgspec.convert(value, want, strict=False)


def validate_type(want: Any) -> None:
    """Reject model types that are not part of the supported RPC type surface."""
    _reject_pydantic_type(want)


@lru_cache(maxsize=128)
def _typed_decoder(want: Any) -> msgspec.msgpack.Decoder:
    return msgspec.msgpack.Decoder(want, strict=False, ext_hook=_decode_ext)


@lru_cache(maxsize=128)
def _typed_fast_decoder(want: Any) -> msgspec.msgpack.Decoder:
    return msgspec.msgpack.Decoder(want, strict=False, ext_hook=_decode_ext_fast)


def convert_msgpack(raw: Any, want: Any) -> Any:
    """Restore directly from one MessagePack value, avoiding an untyped graph."""
    _reject_pydantic_type(want)
    try:
        return _decode_maybe_scoped(_typed_decoder(want), _typed_fast_decoder(want), raw)
    except msgspec.ValidationError:
        # Custom extensions for arbitrary-size integers and hashable composite
        # keys are materialized by ext_hook before ordinary conversion.
        return convert(loads(raw), want)
