"""Inspect the native framed MessagePack method protocol by hand.

python examples/15_native_rpc.py
"""

from __future__ import annotations

import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

import msgspec

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402

from tests.support.registry_wire import debug_pools, health  # noqa: E402


def run_service(_: list[str]) -> None:
    class Dispatcher:
        def assign(self, task: str, retries: int = 0) -> dict:
            return {"took": task, "retries": retries}

        def depth(self) -> int:
            return 3

        def _private(self) -> str:
            return "not reachable"

    with tinyray.join("dispatcher", "stateful", slot=0, serves=Dispatcher()) as me:
        me.ready(dp_rank=0)
        print(f"ENDPOINT {tinyray.pool('dispatcher').slot(0).url}", flush=True)
        time.sleep(8)


def recv_exact(connection: socket.socket, length: int) -> bytes:
    chunks = []
    while length:
        chunk = connection.recv(length)
        if not chunk:
            raise EOFError("native RPC reply ended early")
        chunks.append(chunk)
        length -= len(chunk)
    return b"".join(chunks)


def raw_call(endpoint: str, target: str, method: str, payload: dict, request_id: str) -> dict:
    envelope = {
        "v": 1,
        "id": request_id,
        "from": "example/0#1",
        "to": target,
        "op": "call",
        "method": method,
        "body": msgspec.msgpack.encode(payload),
    }
    body = msgspec.msgpack.encode(envelope)
    host, port = endpoint.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=5) as connection:
        connection.sendall(struct.pack(">I", len(body)) + body)
        length = struct.unpack(">I", recv_exact(connection, 4))[0]
        return msgspec.msgpack.decode(recv_exact(connection, length))


def run_client(argv: list[str]) -> None:
    endpoint, registry = argv[0], argv[1]
    with tinyray.join("client", "churn") as me:
        me.ready()
        handle = tinyray.pool("dispatcher").wait(count=1, timeout=20)[0]

        print(f"[native] advertised endpoint: {endpoint}", flush=True)
        assert "://" not in endpoint

        reply = raw_call(
            endpoint,
            handle.identity,
            "assign",
            {"args": [], "kwargs": {"task": "t-7", "retries": 2}},
            "manual-1",
        )
        result = msgspec.msgpack.decode(reply["body"])
        print(f"[native] assign -> {result}", flush=True)
        assert reply["status"] == "success"
        assert result == {"took": "t-7", "retries": 2}

        missing = raw_call(endpoint, handle.identity, "_private", {}, "manual-2")
        print(f"[native] private method -> {missing['status']}", flush=True)
        assert missing["status"] == "malformed_protocol"

        print(f"[registry] pools={debug_pools(registry)}", flush=True)
        print(f"[registry] health={health(registry)}", flush=True)


def driver() -> int:
    with Fleet() as fleet:
        svc = subprocess.Popen(
            [sys.executable, __file__, "service"],
            env=fleet.env,
            stdout=subprocess.PIPE,
            text=True,
        )
        fleet.procs.append(("service", svc))
        endpoint = svc.stdout.readline().split()[1]
        fleet.spawn(__file__, "client", endpoint, fleet.endpoint, label="client")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"service": run_service, "client": run_client}, driver))
