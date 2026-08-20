"""Addressing across machines.

A member advertises the address other machines should use. There is no
loopback default, because publishing 127.0.0.1 from a multi-node job is silent
misrouting: peers elsewhere reach whatever happens to listen on that port on
their own machine, which is worse than failing.

    python examples/16_cross_node.py
"""

from __future__ import annotations

import os
import socket
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402


def run_worker(argv: list[str]) -> None:
    node, advertise = argv[0], argv[1]
    if advertise != "auto":
        os.environ["TINYRAY_ADVERTISE"] = advertise

    class Worker:
        def where(self) -> str:
            return node

    with tinyray.join("worker", "churn", serves=Worker()) as me:
        me.ready(node=node)
        print(f"[{node}] joined", flush=True)
        time.sleep(6)


def run_client(_: list[str]) -> None:
    with tinyray.join("client", "churn") as me:
        me.ready()
        pool = tinyray.pool("worker")
        workers = pool.wait(count=2, timeout=20)
        for h in sorted(workers, key=lambda x: x.state["node"]):
            host = h.url.split("//")[1].split(":")[0]
            print(f"[client] {h.state['node']:<8} advertises {h.url}", flush=True)
            assert not host.startswith("127."), "a loopback address was published"
            assert h.where() == h.state["node"]

        # What the default resolves to, and why.
        auto = tinyray._advertise()
        print(
            f"[client] with no TINYRAY_ADVERTISE the address comes from the routing table: {auto}",
            flush=True,
        )
        with socket.socket() as s:
            s.bind((auto, 0))  # it really is one of ours
        print("[client] and it is genuinely a local address", flush=True)

        # An explicit override is what a container with a mapped port needs.
        os.environ["TINYRAY_ADVERTISE"] = "10.0.0.42"
        assert tinyray._advertise() == "10.0.0.42"
        del os.environ["TINYRAY_ADVERTISE"]
        print("[client] TINYRAY_ADVERTISE overrides it for NAT or containers", flush=True)


def driver() -> int:
    # One process advertises whatever the routing table says (the default), the
    # other is told explicitly -- the shape a container with a mapped port has.
    host_ip = tinyray._advertise()
    with Fleet() as fleet:
        fleet.spawn(__file__, "worker", "node-a", "auto", label="node-a")
        fleet.spawn(__file__, "worker", "node-b", host_ip, label="node-b")
        time.sleep(0.6)
        fleet.spawn(__file__, "client", label="client")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"worker": run_worker, "client": run_client}, driver))
