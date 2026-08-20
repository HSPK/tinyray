"""Rebuilding a round instead of abandoning it.

tinyray only flips a boolean when the occupants change. Whether that means
"this run is over" or "regroup with whoever is left" is the application's call,
and epoch(min=) is how you say the second one.

    python examples/05_elastic_dp.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402

WORLD = 4
MIN_WORLD = 3
STEPS = 40


def run_rank(argv: list[str]) -> None:
    rank, die_at = int(argv[0]), int(argv[1])
    with tinyray.join("trainer", "collective", slot=rank, size=WORLD) as me:
        me.ready(step=0)
        rounds = 0
        step = 0
        while step < STEPS:
            # The first round must wait for every seat. Opening it with min=
            # lets two ranks freeze different lists while members are still
            # arriving -- one saw [1,2,3] and another [0,1,2,3] -- and two
            # different lists is a deadlock, not a smaller group.
            #
            # Rebuilds are the opposite: the group was whole and lost somebody,
            # so min= is exactly right.
            trainers = tinyray.pool("trainer")
            ep = (
                trainers.epoch(timeout=30)
                if rounds == 0
                else trainers.epoch(min=MIN_WORLD, timeout=30)
            )
            rounds += 1
            members = sorted(h.slot for h in ep)
            if rank == members[0]:
                print(f"[rank {rank}] round {rounds} with {members}", flush=True)
            while step < STEPS and ep.valid:
                if die_at and step >= die_at:
                    print(f"[rank {rank}] leaving mid-round at step {step}", flush=True)
                    os._exit(0)
                step += 1
                me.ready(step=step)
                time.sleep(0.12)  # slow enough that the lease can expire mid-run
            if not ep.valid and step < STEPS:
                # Not an error: somebody joined or left, so regroup.
                time.sleep(0.2)
        print(f"[rank {rank}] finished {step} steps across {rounds} rounds", flush=True)
        assert rounds >= 2 or die_at, "the round never had to be rebuilt"
        time.sleep(0.5)


def driver() -> int:
    with Fleet(ttl_ms=1500) as fleet:
        for r in range(WORLD):
            fleet.spawn(__file__, "rank", r, 8 if r == 3 else 0, label=f"rank{r}")
        return fleet.wait_all(timeout=120)


if __name__ == "__main__":
    raise SystemExit(role_main({"rank": run_rank}, driver))
