from __future__ import annotations

import socket
import struct
from typing import Any

import msgspec

PROTOCOL = 1
MAX_FRAME = 32 << 20


def frame(value: Any) -> bytes:
    body = msgspec.msgpack.encode(value)
    return struct.pack(">I", len(body)) + body


def request(
    *,
    request_id: str = "raw-1",
    caller: str = "caller/0#1",
    target: str = "service/0#1",
    method: str | None = "ping",
    batch_len: int | None = None,
    body: bytes = b"\x80",
    version: int = PROTOCOL,
) -> dict[str, Any]:
    return {
        "v": version,
        "id": request_id,
        "from": caller,
        "to": target,
        "op": "batch" if batch_len is not None else "call",
        **({"method": method} if method is not None else {}),
        **({"batch": batch_len} if batch_len is not None else {}),
        "body": body,
    }


def recv_exact(connection: socket.socket, length: int) -> bytes:
    chunks = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise EOFError(f"stream ended with {remaining} bytes left")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_frame(connection: socket.socket) -> bytes:
    length = struct.unpack(">I", recv_exact(connection, 4))[0]
    if length > MAX_FRAME:
        raise ValueError(f"reply frame declares {length} bytes")
    return recv_exact(connection, length)


def recv_reply(connection: socket.socket) -> dict[str, Any]:
    return msgspec.msgpack.decode(recv_frame(connection))


def exchange(
    endpoint: str,
    envelope: dict[str, Any],
    *,
    timeout: float = 5.0,
) -> dict[str, Any]:
    host, port = endpoint.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=timeout) as connection:
        connection.sendall(frame(envelope))
        return recv_reply(connection)
