"""Why every identity carries a tenure number.

A machine stalls, gets declared dead, a replacement starts -- and then the old
process wakes up still believing it owns the seat. Without tenure numbers both
write their address into the phone book and callers get whichever one wrote
last. The symptom is "occasional timeouts" and it takes days to find.

    python examples/06_fencing.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, role_main  # noqa: E402

import tinyray  # noqa: E402


def run_worker(argv: list[str]) -> None:
    generation = argv[0]

    class Worker:
        def whoami(self) -> str:
            return generation

    with tinyray.join("worker", "stateful", slot=0, serves=Worker()) as me:
        me.ready(generation=generation)
        print(f"[{generation}] holding seat 0 as {me!r}", flush=True)
        # Stays up and keeps listening for the whole run, on purpose. This is
        # the process that "came back": nothing about it looks broken, its port
        # is open, and it will answer anyone who still has its address. It only
        # learns it is a ghost from the registry's reply to its own heartbeat.
        announced = False
        deadline = time.monotonic() + 14
        while time.monotonic() < deadline:
            if not announced and not me.accepted:
                print(f"[{generation}] the seat moved on without me, "
                      f"still listening though", flush=True)
                announced = True
            time.sleep(0.05)


def run_client(_: list[str]) -> None:
    with tinyray.join("client", "churn") as me:
        me.ready()
        pool = tinyray.pool("worker")
        first = pool.wait(count=1, timeout=20)[0]
        print(f"[client] talking to {first.label}: {first.whoami()}", flush=True)

        # Hold on to the stale handle while a replacement takes the seat.
        deadline = time.monotonic() + 20
        second = None
        while time.monotonic() < deadline:
            now = pool.all()
            if now and now[0].incarnation != first.incarnation:
                second = now[0]
                break
            time.sleep(0.05)
        assert second is not None, "the replacement never took the seat"
        print(f"[client] seat 0 is now {second.label}: {second.whoami()}", flush=True)

        # The old handle still has a working address -- the old process is
        # alive and listening. Only the tenure number tells them apart.
        assert first.url != second.url or first.incarnation != second.incarnation
        try:
            first.whoami()
        except tinyray.Fenced as exc:
            print(f"[client] stale handle refused: {exc}", flush=True)
        else:
            raise AssertionError("a stale tenure was served, which is the bug")

        # Fenced means "look it up again", not "give up".
        print(f"[client] after re-lookup: {pool.slot(0).whoami()}", flush=True)
        time.sleep(0.3)


def driver() -> int:
    # The lease has to outlast the run so the old process is never merely
    # expired -- we want it alive and wrong. But it also sets the beat interval
    # (lease/4), and with it the time for a replacement to become visible, so
    # "very long" makes the example look broken rather than safe.
    with Fleet(ttl_ms=8000) as fleet:
        fleet.spawn(__file__, "worker", "gen-1", label="gen1")
        time.sleep(0.6)
        fleet.spawn(__file__, "client", label="client")
        time.sleep(0.8)
        fleet.spawn(__file__, "worker", "gen-2", label="gen2")
        return fleet.wait_all(timeout=90)


if __name__ == "__main__":
    raise SystemExit(role_main({"worker": run_worker, "client": run_client}, driver))
