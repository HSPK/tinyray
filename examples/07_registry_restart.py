"""Losing the phone book, and getting it back.

The registry holds nothing that needs recovering: every record is re-asserted
by its owner each heartbeat. So killing it is not an outage, it is a gap --
lookups keep working from cache, and the roster refills itself when it returns.

    python examples/07_registry_restart.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402

PEERS = 3


def run_peer(argv: list[str]) -> None:
    name = argv[0]

    class Peer:
        def ping(self) -> str:
            return name

    with tinyray.join("peer", "churn", serves=Peer()) as me:
        me.ready(name=name)
        time.sleep(12)


def run_observer(_: list[str]) -> None:
    with tinyray.join("observer", "churn") as me:
        me.ready()
        peers = tinyray.pool("peer")
        before = peers.wait(count=PEERS, timeout=20)
        print(f"[observer] sees {len(before)} peers", flush=True)
        print("MARK ready", flush=True)

        # The driver kills the registry here.
        time.sleep(5)

        after = peers.all()
        print(
            f"[observer] registry down: still sees {len(after)} peers, "
            f"beats_failed={me.stats()['beats_failed']}",
            flush=True,
        )
        assert len(after) == PEERS, "the cache expired along with the registry"
        # And they are still reachable: calls never went through the registry.
        replies = sorted(h.ping() for h in after)
        print(f"[observer] calls still work: {replies}", flush=True)
        print(f"[observer] silence_ms={me.silence_ms}", flush=True)
        assert me.silence_ms > 500, "silence should be visible while it is down"

        print("MARK survived", flush=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if me.stats()["beats_ok"] > 1 and me.silence_ms < 2000:
                break
            time.sleep(0.1)
        print(
            f"[observer] registry back: {len(peers.all())} peers, silence_ms={me.silence_ms}",
            flush=True,
        )
        assert len(peers.all()) == PEERS, "the roster did not regrow"


def driver() -> int:
    with Fleet(ttl_ms=2000) as fleet:
        for i in range(PEERS):
            fleet.spawn(__file__, "peer", f"p{i}", label=f"peer{i}")
        time.sleep(0.5)
        import subprocess

        obs = subprocess.Popen(
            [sys.executable, __file__, "observer"],
            env=fleet.env,
            stdout=subprocess.PIPE,
            text=True,
        )
        fleet.procs.append(("observer", obs))
        for line in obs.stdout:
            print(line.rstrip(), flush=True)
            if line.startswith("MARK ready"):
                print("[driver] killing the registry", flush=True)
                fleet.stop_registry()
            elif line.startswith("MARK survived"):
                print("[driver] starting it again", flush=True)
                fleet.start_registry()
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"peer": run_peer, "observer": run_observer}, driver))
