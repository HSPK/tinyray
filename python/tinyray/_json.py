"""Fast JSON for ordinary RPC values, with the stdlib as the compatibility boundary.

The encoder fast path is reserved for payloads with an ASCII string of at least
1 KiB; small messages retain the stdlib's wire formatting. It only accepts exact
builtin types, ASCII strings, finite floats and 64-bit integers. Msgspec's extra
support for dataclasses, bytes and dates must not make invalid RPC values valid.
Non-string keys, subclasses, cycles and large/deep containers stay with json.dumps.

Decoding uses Python's float conversion and msgspec's exact integer decoding.
Unicode, NaN/Infinity literals and large/deep containers go straight to json.loads;
that decoder also supplies every parsing error. Tokens inside strings may select
the legacy path conservatively. Guards do bounded Python work and never walk a
decoded value. Large ASCII strings need no Python-level character scan.
"""

from __future__ import annotations

import json
import math
from typing import Any

import msgspec

_GUARD_NODES = 32
# float_hook is supported by msgspec 0.18.0. Its builtin float callback uses
# Python's conversion even for overflow, subnormals and negative zero. Untyped
# integers already use PyLong_FromString, not the floating-point decoder.
_decode_fast = msgspec.json.Decoder(float_hook=float).decode


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
    """Encode with json.dumps' accepted values and wire meaning, not its spacing."""
    if _compatible(value):
        return msgspec.json.encode(value)
    return json.dumps(value).encode()


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
