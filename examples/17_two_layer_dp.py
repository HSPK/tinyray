"""Asynchronous data parallelism, as two layers rather than a fifth policy.

One global collective would mean any rank dying stops everyone. Splitting it in
two says what is actually true: inside a group everyone must be present, and
between groups they are independent.

    python examples/17_two_layer_dp.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402

GROUPS = 3
PER_GROUP = 2
STEPS = 25


def run_rank(argv: list[str]) -> None:
    group, local, die_at = int(argv[0]), int(argv[1]), int(argv[2])
    inner = f"dp{group}_ranks"

    with tinyray.join(inner, "collective", slot=local, size=PER_GROUP) as me:
        me.ready(group=group)
        ep = tinyray.pool(inner).epoch(timeout=30)
        if local == 0:
            print(f"[dp{group}] inner round with {sorted(h.slot for h in ep)}", flush=True)

        step = 0
        while step < STEPS and ep.valid:
            if die_at and step >= die_at:
                print(f"[dp{group}/{local}] dying at step {step}", flush=True)
                os._exit(0)
            step += 1
            me.ready(group=group, step=step)
            # Slow enough that a lost partner is noticed before the work ends;
            # otherwise the run simply outpaces its own lease.
            time.sleep(0.13)

        if not ep.valid:
            # Only this group is affected. The others never noticed.
            print(f"[dp{group}/{local}] inner round broke at step {step}", flush=True)
            assert group == 1, "a group broke that should not have"
        else:
            print(f"[dp{group}/{local}] finished {step} steps", flush=True)
            assert step == STEPS
        time.sleep(1.2)


def run_leader(argv: list[str]) -> None:
    """One representative per group, in a pool where losing one is survivable."""
    group = int(argv[0])
    with tinyray.join("dp_groups", "stateful", slot=group) as me:
        me.ready(group=group)
        groups = tinyray.pool("dp_groups")
        groups.wait(count=GROUPS, timeout=30)
        print(f"[leader {group}] all {GROUPS} groups present", flush=True)

        seen_min = GROUPS
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            seen_min = min(seen_min, len(groups.all()))
            time.sleep(0.05)
        if group == 0:
            print(f"[leader 0] fewest groups seen at once: {seen_min} of {GROUPS}", flush=True)
            print("[leader 0] losing a rank ended one group, not the job", flush=True)


def driver() -> int:
    with Fleet(ttl_ms=1500) as fleet:
        for g in range(GROUPS):
            fleet.spawn(__file__, "leader", g, label=f"leader{g}")
            for local in range(PER_GROUP):
                die = 8 if (g == 1 and local == 1) else 0
                fleet.spawn(__file__, "rank", g, local, die, label=f"dp{g}/{local}")
        return fleet.wait_all(timeout=120)


if __name__ == "__main__":
    raise SystemExit(role_main({"rank": run_rank, "leader": run_leader}, driver))
