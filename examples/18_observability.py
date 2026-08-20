"""What to look at when something is wrong.

Everything here is either a plain HTTP endpoint or a number the client already
tracks. There is no dashboard and no event stream, because a phone book that
needed its own observability stack would be the wrong size.

    python examples/18_observability.py
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402


def run_engine(argv: list[str]) -> None:
    index = int(argv[0])

    class Engine:
        def generate(self, p: str) -> str:
            return f"answer({p})"

    with tinyray.join("engine", "serving", serves=Engine()) as me:
        me.ready(index=index, model_version=7)
        time.sleep(7)


def run_inspector(argv: list[str]) -> None:
    registry = argv[0]
    with tinyray.join("inspector", "churn") as me:
        me.ready()
        engines = tinyray.pool("engine")
        engines.wait(count=2, timeout=20)

        print("--- what this process knows about itself ---", flush=True)
        print(f"  identity   {me!r}", flush=True)
        print(f"  accepted   {me.accepted}   (false once a later tenure took the seat)", flush=True)
        print(f"  silence_ms {me.silence_ms}   (since the registry last answered)", flush=True)
        print(f"  stats      {me.stats()}", flush=True)

        print("--- what it knows about a pool ---", flush=True)
        version, roster, size, methods = engines._c.pool_info("engine")
        print(f"  version {version}   bumped by anything a peer should learn", flush=True)
        print(f"  roster  {roster}   changes only when the occupants change", flush=True)
        print(f"  size    {size}      declared by the pool, None for churn", flush=True)
        print(f"  methods {methods}", flush=True)
        for h in engines.all():
            print(
                f"  member  {h.label:<18} ready={h.ready} state={h.state} url={h.url}", flush=True
            )

        print("--- and from outside, with no client at all ---", flush=True)
        with urllib.request.urlopen(f"http://{registry}/v1/pools", timeout=5) as r:
            for name, info in sorted(json.loads(r.read()).items()):
                print(f"  {name:<12} {info}", flush=True)

        print("--- the two numbers, and why there are two ---", flush=True)
        before_v, before_r, _, _ = engines._c.pool_info("engine")
        me.ready(note="progress")
        deadline = time.monotonic() + 5
        while engines._c.pool_info("engine")[0] == before_v and time.monotonic() < deadline:
            time.sleep(0.02)
        # Our own state change bumps the pool we belong to, not this one.
        insp_v, insp_r, _, _ = tinyray.pool("inspector")._c.pool_info("inspector")
        print(f"  publishing state moved inspector version to {insp_v}", flush=True)
        print(f"  but its roster stayed {insp_r}: the same people are still here", flush=True)
        print("  a frozen round compares the roster, a cache compares the version", flush=True)


def driver() -> int:
    with Fleet() as fleet:
        for i in range(2):
            fleet.spawn(__file__, "engine", i, label=f"engine{i}")
        time.sleep(0.6)
        fleet.spawn(__file__, "inspector", fleet.endpoint, label="inspector")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"engine": run_engine, "inspector": run_inspector}, driver))
