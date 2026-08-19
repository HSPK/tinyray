"""Leader election.

Seats are last-writer-wins by default, because a rank that restarts has to
reclaim its seat while the dead one's lease is still running. An election wants
the opposite, so it asks for the seat `exclusive=True`: first to take seat 0
wins, and the standby only gets in once that lease lapses. The old leader
cannot come back, because the seat remembers how far it has moved on.

    python examples/04_singleton_leader.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, role_main  # noqa: E402

import tinyray  # noqa: E402


def run_candidate(argv: list[str]) -> None:
    name, die_after = argv[0], float(argv[1])

    class Controller:
        def whoami(self) -> str:
            return name

    started = time.monotonic()
    while True:
        try:
            # exclusive=: take seat 0 only if it is free. Without it seats are
            # last-writer-wins, which is what a restarting rank needs and the
            # opposite of what an election needs.
            me = tinyray.join("controller", "stateful", slot=0, serves=Controller(),
                              exclusive=True)
        except tinyray.SeatTaken:
            time.sleep(0.1)
            if time.monotonic() - started > 20:
                raise
            continue
        me.ready(leader=name)
        # accepted goes false the moment a later tenure takes the seat.
        if me.accepted:
            print(f"[{name}] elected after {time.monotonic() - started:.1f}s", flush=True)
        while me.accepted:
            if die_after and time.monotonic() - started > die_after:
                print(f"[{name}] dying while leader", flush=True)
                os._exit(0)  # no farewell: only the lease can notice
            time.sleep(0.05)
        print(f"[{name}] lost the seat", flush=True)
        me.leave()
        return


def run_watcher(_: list[str]) -> None:
    with tinyray.join("watcher", "churn") as me:
        me.ready()
        pool = tinyray.pool("controller")
        seen: list[str] = []
        deadline = time.monotonic() + 9
        while time.monotonic() < deadline:
            for h in pool.all():
                who = h.state.get("leader")
                if who and (not seen or seen[-1] != who):
                    seen.append(who)
                    print(f"[watcher] leader is now {who} ({h.label})", flush=True)
            time.sleep(0.05)
        print(f"[watcher] leadership sequence: {seen}", flush=True)
        assert len(seen) >= 2, "the standby never took over"
        assert len(set(seen)) == len(seen), "a leader came back after being replaced"


def driver() -> int:
    with Fleet(ttl_ms=1500) as fleet:
        fleet.spawn(__file__, "watcher", label="watcher")
        time.sleep(0.3)
        fleet.spawn(__file__, "candidate", "alice", 2.0, label="alice")
        time.sleep(0.6)
        fleet.spawn(__file__, "candidate", "bob", 0, label="bob")
        return fleet.wait_all(timeout=90)


if __name__ == "__main__":
    raise SystemExit(role_main({"candidate": run_candidate, "watcher": run_watcher}, driver))
