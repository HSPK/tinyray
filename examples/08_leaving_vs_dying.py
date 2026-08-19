"""Two ways to go, and how long each takes to notice.

Leaving says goodbye on the way out, so the seat frees immediately. Being
killed says nothing, and only the lease can notice. Both work; they differ by
about a lease.

    python examples/08_leaving_vs_dying.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, role_main  # noqa: E402

import tinyray  # noqa: E402


def run_member(argv: list[str]) -> None:
    name, how = argv[0], argv[1]
    me = tinyray.join("crowd", "churn")
    me.ready(name=name)
    print(f"READY {name}", flush=True)
    sys.stdin.readline()
    if how == "leave":
        me.leave()  # a farewell beat: the seat is free before this returns
    else:
        os._exit(0)  # nothing is sent; the lease has to run out


def run_observer(argv: list[str]) -> None:
    lease = float(argv[0])
    with tinyray.join("observer", "churn") as me:
        me.ready()
        crowd = tinyray.pool("crowd")
        crowd.wait(count=2, timeout=20)
        print("MARK ready", flush=True)

        for label in ("leave", "kill"):
            sys.stdin.readline()  # the driver tells the member to go
            t0 = time.monotonic()
            deadline = t0 + lease * 4 + 5
            before = len(crowd.all())
            while len(crowd.all()) == before and time.monotonic() < deadline:
                time.sleep(0.01)
            took = time.monotonic() - t0
            print(f"[observer] {label}: noticed in {took * 1000:.0f} ms", flush=True)
            if label == "leave":
                assert took < lease, f"a farewell should beat the {lease}s lease"
            else:
                assert took > lease / 2, "a kill cannot be noticed before the lease"
            print(f"MARK done-{label}", flush=True)


def driver() -> int:
    import subprocess

    lease_ms = 1500
    with Fleet(ttl_ms=lease_ms) as fleet:
        members = {}
        for how in ("leave", "kill"):
            p = subprocess.Popen(
                [sys.executable, __file__, "member", how, how],
                env=fleet.env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            assert p.stdout.readline().startswith("READY")
            members[how] = p
            fleet.procs.append((how, p))

        obs = subprocess.Popen(
            [sys.executable, __file__, "observer", str(lease_ms / 1000)],
            env=fleet.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        fleet.procs.append(("observer", obs))
        for line in obs.stdout:
            print(line.rstrip(), flush=True)
            if line.startswith("MARK ready"):
                members["leave"].stdin.write("\n")
                members["leave"].stdin.flush()
                obs.stdin.write("\n")
                obs.stdin.flush()
            elif line.startswith("MARK done-leave"):
                members["kill"].stdin.write("\n")
                members["kill"].stdin.flush()
                obs.stdin.write("\n")
                obs.stdin.flush()
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"member": run_member, "observer": run_observer}, driver))
