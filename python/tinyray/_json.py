"""Fast JSON for RPC values, with the stdlib as the compatibility boundary.

The encoder fast path is reserved for payloads with an ASCII string of at least
1 KiB; small messages retain the stdlib's wire formatting. It only accepts exact
builtin types, ASCII strings, finite floats and 64-bit integers. Msgspec's extra
support for bytes and dates must not make invalid ordinary RPC values valid.
Dataclass and Pydantic model subtrees are the deliberate exception: they are
normalized to plain JSON values, while unrelated siblings retain stdlib semantics.
Non-string keys, subclasses, cycles and large/deep containers stay with JSONEncoder.

Decoding uses Python's float conversion and msgspec's exact integer decoding.
Unicode, NaN/Infinity literals and large/deep containers go straight to json.loads;
that decoder also supplies every parsing error. Tokens inside strings may select
the legacy path conservatively. Guards do bounded Python work and never walk a
decoded value. Large ASCII strings need no Python-level character scan.
"""

from __future__ import annotations

import json
import math
import sys
import typing
from dataclasses import is_dataclass
from functools import lru_cache
from typing import Any

import msgspec

_GUARD_NODES = 32
# float_hook is supported by msgspec 0.18.0. Its builtin float callback uses
# Python's conversion even for overflow, subnormals and negative zero. Untyped
# integers already use PyLong_FromString, not the floating-point decoder.
_decode_fast = msgspec.json.Decoder(float_hook=float).decode
_PYDANTIC_BASE: type[Any] | None = None


def _pydantic_base() -> type[Any] | None:
    """Find Pydantic only after the application has imported it."""
    global _PYDANTIC_BASE
    if _PYDANTIC_BASE is None:
        module = sys.modules.get("pydantic")
        candidate = getattr(module, "BaseModel", None)
        if isinstance(candidate, type):
            _PYDANTIC_BASE = candidate
    return _PYDANTIC_BASE


def _pydantic_type(value: Any, base: type[Any]) -> bool:
    return isinstance(value, type) and (
        issubclass(value, base) or is_dataclass(value) and hasattr(value, "__pydantic_validator__")
    )


def _contains_pydantic(value: Any, base: type[Any], seen: set[int]) -> bool:
    marker = id(value)
    if marker in seen:
        return False
    seen.add(marker)
    if _pydantic_type(value, base):
        return True
    if any(_contains_pydantic(arg, base, seen) for arg in typing.get_args(value)):
        return True
    annotations = getattr(value, "__annotations__", None)
    if not isinstance(value, type) or not annotations:
        return False
    try:
        annotations = typing.get_type_hints(value)
    except (NameError, TypeError):
        pass
    return any(_contains_pydantic(field, base, seen) for field in annotations.values())


@lru_cache(maxsize=256)
def _uses_pydantic(value: Any) -> bool:
    base = _pydantic_base()
    return base is not None and _contains_pydantic(value, base, set())


@lru_cache(maxsize=128)
def _type_adapter(value: Any) -> Any:
    return sys.modules["pydantic"].TypeAdapter(value)


def _validate(validator: Any, value: Any) -> Any:
    try:
        return validator(value)
    except Exception as exc:
        validation_error = getattr(sys.modules.get("pydantic"), "ValidationError", None)
        if isinstance(validation_error, type) and isinstance(exc, validation_error):
            raise msgspec.ValidationError(str(exc)) from None
        raise


def convert(value: Any, want: Any) -> Any:
    """Restore a JSON value, using Pydantic only for types that contain it."""
    base = _pydantic_base()
    if base is not None:
        if isinstance(want, type) and issubclass(want, base):
            model_type: Any = want
            return _validate(model_type.model_validate, value)
        if _uses_pydantic(want):
            return _validate(_type_adapter(want).validate_python, value)
    return msgspec.convert(value, want, strict=False)


class _ModelEncoder(json.JSONEncoder):
    def default(self, value: Any) -> Any:
        base = _pydantic_base()
        if base is not None and isinstance(value, base):
            return value.model_dump(mode="json", by_alias=True)
        if is_dataclass(value) and not isinstance(value, type):
            kind = type(value)
            if base is not None and _uses_pydantic(kind):
                return _type_adapter(kind).dump_python(value, mode="json", by_alias=True)
            return msgspec.to_builtins(value, enc_hook=self.default)
        return super().default(value)


_encode_legacy = _ModelEncoder().encode


def _compatible(value: Any) -> bool:
    pending = [value]
    remaining = _GUARD_NODES
    large_string = False
    while pending:
        value = pending.pop()
        remaining -= 1
        kind = type(value)
        if kind is str:
            if not value.isascii():
                return False
            large_string = large_string or len(value) >= 1024
        elif value is None or kind is bool:
            pass
        elif kind is int:
            if not -(1 << 63) <= value < (1 << 64):
                return False
        elif kind is float:
            if not math.isfinite(value):
                return False
        elif kind in (list, tuple, dict):
            # Reserve room for queued siblings as well as this container's
            # children, so nested small containers cannot evade the work bound.
            if len(value) > remaining - len(pending):
                return False
            if kind is dict:
                for key, item in value.items():
                    if type(key) is not str or not key.isascii():
                        return False
                    pending.append(item)
            else:
                pending.extend(value)
        else:
            return False
    return large_string


def _many_values(raw: str | bytes | bytearray) -> bool:
    # A native bounded-prefix count handles dense numeric containers at once.
    # Native find skips long strings cheaply, stopping after 32 structural
    # characters. Counting openings as well as commas also bounds nesting,
    # without first parsing and then walking the decoded tree.
    separators: Any = (",", "[", "{") if isinstance(raw, str) else (b",", b"[", b"{")
    remaining = _GUARD_NODES
    for separator in separators:
        remaining -= raw.count(separator, 0, 1024)
        position = 1024
        while remaining > 0:
            position = raw.find(separator, position)
            if position < 0:
                break
            remaining -= 1
            position += 1
        if remaining <= 0:
            return True
    return False


def dumps(value: Any) -> bytes:
    """Encode ordinary JSON compatibly, plus declared application models."""
    if _compatible(value):
        return msgspec.json.encode(value)
    return _encode_legacy(value).encode()


def loads(raw: Any) -> Any:
    """Decode with json.loads' values and exceptions, without optional hooks."""
    if type(raw) not in (str, bytes, bytearray):
        return json.loads(raw)
    legacy_tokens = (
        (("\\", "\\u"), ("N", "NaN"), ("I", "Infinity"))
        if isinstance(raw, str)
        else ((b"\\", b"\\u"), (b"N", b"NaN"), (b"I", b"Infinity"))
    )
    # Single-character searches skip bulk ASCII cheaply when a legacy token
    # cannot be present, before attempting the longer substring search.
    if not raw.isascii() or any(first in raw and token in raw for first, token in legacy_tokens):
        return json.loads(raw)
    if _many_values(raw):
        return json.loads(raw)
    try:
        return _decode_fast(raw)
    except (msgspec.DecodeError, UnicodeError, RecursionError):
        # NaN/Infinity, alternate encodings, malformed JSON and msgspec's own
        # integer digit ceiling belong to the legacy decoder's values/errors.
        return json.loads(raw)
