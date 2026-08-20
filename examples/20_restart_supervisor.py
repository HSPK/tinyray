"""Restart policy is per pool, because the three groups die differently.

tinyray restarts nothing -- it only says who is missing. What that means is the
supervisor's decision, and it is a different decision for each pool:

    env       replace it, nobody notices
    engine    replace it, but it cannot take work until it has weights
    trainer   the round is void; restart the group, not the member

    python examples/20_restart_supervisor.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402


def run_env(argv: list[str]) -> None:
    name, die_after = argv[0], float(argv[1])
    with tinyray.join("env", "churn") as me:
        me.ready(name=name)
        if die_after:
            time.sleep(die_after)
            os._exit(1)
        time.sleep(12)


def run_engine(argv: list[str]) -> None:
    name, die_after = argv[0], float(argv[1])
    me = tinyray.join("engine", "serving")
    # A replacement is present long before it is useful. Until the weights are
    # loaded it must not be picked.
    time.sleep(0.8)
    me.ready(name=name, model_version=5)
    if die_after:
        time.sleep(die_after)
        os._exit(1)
    time.sleep(12)
    me.leave()


def run_trainer(argv: list[str]) -> None:
    rank, die_after = int(argv[0]), float(argv[1])
    with tinyray.join("trainer", "collective", slot=rank, size=2) as me:
        me.ready()
        ep = tinyray.pool("trainer").epoch(timeout=20)
        if die_after:
            time.sleep(die_after)
            os._exit(1)
        deadline = time.monotonic() + 10
        while ep.valid and time.monotonic() < deadline:
            time.sleep(0.05)
        print(f"[trainer/{rank}] round valid={ep.valid}", flush=True)
        time.sleep(0.5)


def driver() -> int:
    with Fleet(ttl_ms=1500) as fleet:
        fleet.spawn(__file__, "env", "e0", 2.0, label="env0")
        fleet.spawn(__file__, "engine", "g0", 2.5, label="engine0")
        fleet.spawn(__file__, "trainer", 0, 0, label="trainer0")
        fleet.spawn(__file__, "trainer", 1, 3.0, label="trainer1")

        import tinyray as tr

        os.environ["TINYRAY_REGISTRY"] = fleet.endpoint
        me = tr.join("supervisor", "churn")
        me.ready()
        pools = {n: tr.pool(n) for n in ("env", "engine", "trainer")}
        pools["trainer"].wait(count=2, timeout=25)

        seen: dict[str, bool] = {}
        replaced = 0
        deadline = time.monotonic() + 14
        while time.monotonic() < deadline:
            if not seen.get("env") and not pools["env"].all():
                seen["env"] = True
                print("[supervisor] env gone -> replace it, nothing else to do", flush=True)
                fleet.spawn(__file__, "env", "e1", 0, label="env1")
                replaced += 1
            if not seen.get("engine") and not pools["engine"].all():
                seen["engine"] = True
                print(
                    "[supervisor] engine gone -> replace it, but it stays out of "
                    "rotation until it reports a model version",
                    flush=True,
                )
                fleet.spawn(__file__, "engine", "g1", 0, label="engine1")
                replaced += 1
            if not seen.get("trainer") and len(pools["trainer"].all()) < 2:
                seen["trainer"] = True
                print(
                    "[supervisor] trainer gone -> the round is void; restart the "
                    "group, not the member",
                    flush=True,
                )
            if len(seen) == 3:
                break
            time.sleep(0.05)

        assert len(seen) == 3, f"only saw {sorted(seen)}"
        print(
            f"[supervisor] replaced {replaced} members individually, and declared 1 round void",
            flush=True,
        )

        # The replacements come back, and the engine only becomes eligible once
        # it has weights -- present is not the same as usable.
        pools["env"].wait(count=1, timeout=15)
        engine = pools["engine"].wait(count=1, timeout=15, model_version=5)[0]
        print(
            f"[supervisor] env is back; engine {engine.label} is back and now "
            f"reports model_version=5",
            flush=True,
        )
        print(
            "[supervisor] the trainer group is not something you patch member by member", flush=True
        )
        me.leave()
        return fleet.wait_all(timeout=60, expect_nonzero=("env0", "engine0", "trainer1"))


if __name__ == "__main__":
    raise SystemExit(
        role_main({"env": run_env, "engine": run_engine, "trainer": run_trainer}, driver)
    )
