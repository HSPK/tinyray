"""Serialisation for tinyray.

This is the one part of the data path that has to stay in Python: only Python
can pickle arbitrary Python objects. What we *can* keep out of Python is the
byte shuffling, so this module is deliberately small and does exactly two
things:

* split a value into a small pickle body plus large out-of-band buffers, using
  pickle protocol 5, so tensors never get copied through the pickle stream;
* put it back together on the other side, where the buffers are Rust-owned
  :class:`tinyray.Frame` objects and the reconstructed arrays are views onto
  them.

Arrays reconstructed from a frame are **read-only**, because several consumers
may share one result buffer. Copy explicitly if you need to mutate.
"""

from __future__ import annotations

import pickle
from collections.abc import Iterable, Sequence
from typing import Any

import cloudpickle

#: Pickle protocol that supports out-of-band buffers. Non-negotiable: without
#: it, a 10 MB tensor would be memcpy'd into the pickle stream.
PROTOCOL = 5

#: Buffers at least this large travel out-of-band. Smaller ones ride inside the
#: pickle body, where the per-frame overhead would dominate.
DEFAULT_MIN_OOB_SIZE = 4096


class SerializationError(Exception):
    """Raised when a value cannot be serialised or restored."""


def serialize(obj: Any, *, min_oob_size: int = DEFAULT_MIN_OOB_SIZE) -> tuple[bytes, list[Any]]:
    """Split ``obj`` into a pickle body and a list of out-of-band buffers.

    Returns ``(body, frames)``. ``frames`` entries are buffer-like objects that
    must be transmitted in order, alongside ``body``.
    """
    buffers: list[Any] = []

    def collect(pickle_buffer: pickle.PickleBuffer) -> bool:
        # Careful: pickle inverts the sense of this return value. A *true*
        # result keeps the buffer inline in the pickle stream; a false result
        # makes it out-of-band. Getting this backwards silently doubles the
        # payload, because the buffer is then both inlined and collected.
        with pickle_buffer.raw() as view:
            if view.nbytes < min_oob_size:
                return True
        buffers.append(pickle_buffer)
        return False

    try:
        body = cloudpickle.dumps(obj, protocol=PROTOCOL, buffer_callback=collect)
    except Exception as exc:
        raise SerializationError(f"failed to serialise {type(obj).__name__}: {exc}") from exc

    return body, [b.raw() for b in buffers]


def deserialize(body: Any, frames: Sequence[Any] = ()) -> Any:
    """Rebuild a value from ``body`` and its out-of-band ``frames``.

    ``body`` is any bytes-like object. In practice it arrives as a
    :class:`tinyray.Frame`, a zero-copy view of the Rust buffer it was received
    into, so the pickle stream is never copied on the way in.
    """
    try:
        return pickle.loads(body, buffers=list(frames))
    except Exception as exc:
        raise SerializationError(f"failed to deserialise payload: {exc}") from exc


def payload_size(body: bytes, frames: Iterable[Any]) -> int:
    """Total bytes a serialised value will occupy, excluding framing overhead."""
    return len(body) + sum(memoryview(f).nbytes for f in frames)
