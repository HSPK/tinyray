"""Framed MessagePack client helpers for direct registry protocol tests."""

from __future__ import annotations

import itertools
import socket
import time
from typing import Any

import msgspec

LENGTH_PREFIX_BYTES = 4
MAX_RESPONSE_FRAME_BYTES = 64 << 20

OP_BEAT = "beat"
OP_BEAT_ACK = "beat_ack"
OP_HEALTH = "health"
OP_HEALTH_ACK = "health_ack"
OP_DEBUG_POOLS = "debug_pools"
OP_DEBUG_POOLS_ACK = "debug_pools_ack"
OP_ERROR = "error"

_ids = itertools.count(1)


class RegistryWireError(RuntimeError):
    def __init__(self, code: str, message: str, request_id: int = 0):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.request_id = request_id


def _address(endpoint: str) -> tuple[str, int]:
    if "://" in endpoint:
        raise ValueError(f"registry endpoint must be host:port, not {endpoint!r}")
    host, separator, port = endpoint.rpartition(":")
    if not separator or not host or not port:
        raise ValueError(f"registry endpoint must be host:port, not {endpoint!r}")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    return host, int(port)


def _remaining(deadline: float) -> float:
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError("registry request deadline expired")
    return left


def read_exact(sock: socket.socket, count: int) -> bytes:
    out = bytearray()
    while len(out) < count:
        part = sock.recv(count - len(out))
        if not part:
            raise EOFError(f"socket ended after {len(out)} of {count} bytes")
        out.extend(part)
    return bytes(out)


def read_frame(sock: socket.socket, maximum: int = MAX_RESPONSE_FRAME_BYTES) -> bytes:
    prefix = read_exact(sock, LENGTH_PREFIX_BYTES)
    length = int.from_bytes(prefix, "big")
    if length == 0:
        raise RegistryWireError("empty_frame", "zero-length frames are not valid")
    if length > maximum:
        raise RegistryWireError(
            "frame_too_large", f"frame declares {length} bytes, over the {maximum}-byte limit"
        )
    return read_exact(sock, length)


def write_frame(sock: socket.socket, payload: bytes) -> None:
    if len(payload) > 0xFFFFFFFF:
        raise RegistryWireError("invalid_frame", f"cannot frame {len(payload)} payload bytes")
    sock.sendall(len(payload).to_bytes(LENGTH_PREFIX_BYTES, "big") + payload)


def decode_envelope(payload: bytes) -> dict[str, Any]:
    try:
        envelope = msgspec.msgpack.decode(payload)
    except msgspec.DecodeError as exc:
        raise RegistryWireError("malformed_frame", str(exc)) from exc
    if not isinstance(envelope, dict):
        raise RegistryWireError("malformed_frame", "registry envelope is not a map")
    return envelope


def raw_exchange(endpoint: str, payload: bytes, timeout: float = 5.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    sock = socket.create_connection(_address(endpoint), timeout=_remaining(deadline))
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(_remaining(deadline))
        send_error: OSError | None = None
        try:
            write_frame(sock, payload)
        except OSError as exc:
            # An oversized request may be rejected from its prefix while this
            # side is still writing the body. Prefer the structured reply if
            # it made it back before the close.
            send_error = exc
        sock.settimeout(_remaining(deadline))
        try:
            return decode_envelope(read_frame(sock))
        except (EOFError, OSError) as exc:
            if send_error is not None:
                raise send_error from exc
            raise
    finally:
        sock.close()


def request(
    endpoint: str,
    operation: str,
    payload: Any,
    *,
    timeout: float = 5.0,
    request_id: int | None = None,
) -> Any:
    request_id = next(_ids) if request_id is None else request_id
    encoded = msgspec.msgpack.encode(
        {"request_id": request_id, "operation": operation, "payload": payload}
    )
    envelope = raw_exchange(endpoint, encoded, timeout)
    reply_id = envelope.get("request_id")
    reply_operation = envelope.get("operation")
    reply_payload = envelope.get("payload")
    if reply_operation == OP_ERROR:
        if reply_id not in (0, request_id):
            raise RegistryWireError(
                "correlation_mismatch",
                f"error reply named request {reply_id!r}, expected {request_id} or 0",
                request_id,
            )
        if not isinstance(reply_payload, dict):
            raise RegistryWireError(
                "malformed_error", f"error payload is {reply_payload!r}", request_id
            )
        raise RegistryWireError(
            str(reply_payload.get("code", "protocol_error")),
            str(reply_payload.get("message", "")),
            int(reply_id or 0),
        )
    if reply_id != request_id:
        raise RegistryWireError(
            "correlation_mismatch",
            f"reply named request {reply_id!r}, expected {request_id}",
            request_id,
        )
    expected = {
        OP_BEAT: OP_BEAT_ACK,
        OP_HEALTH: OP_HEALTH_ACK,
        OP_DEBUG_POOLS: OP_DEBUG_POOLS_ACK,
    }.get(operation)
    if reply_operation != expected:
        raise RegistryWireError(
            "operation_mismatch",
            f"reply operation is {reply_operation!r}, expected {expected!r}",
            request_id,
        )
    return reply_payload


def beat(endpoint: str, payload: dict[str, Any], *, timeout: float = 5.0) -> dict[str, Any]:
    reply = request(endpoint, OP_BEAT, payload, timeout=timeout)
    if not isinstance(reply, dict):
        raise RegistryWireError("malformed_beat_ack", f"BeatAck payload is {reply!r}")
    return reply


def health(endpoint: str, *, timeout: float = 5.0) -> dict[str, Any]:
    reply = request(endpoint, OP_HEALTH, None, timeout=timeout)
    if not isinstance(reply, dict):
        raise RegistryWireError("malformed_health", f"health payload is {reply!r}")
    return reply


def debug_pools(endpoint: str, *, timeout: float = 5.0) -> dict[str, Any]:
    reply = request(endpoint, OP_DEBUG_POOLS, None, timeout=timeout)
    if not isinstance(reply, dict) or not isinstance(reply.get("pools"), dict):
        raise RegistryWireError("malformed_debug_pools", f"debug-pools payload is {reply!r}")
    return reply["pools"]
