"""Rolling a new model version through a pool of engines.

An engine loading weights is present but must not take work. Readiness here is
a claim about state, not a pulse, so `pick(model_version=N)` routes only to
engines that actually hold N.

    python examples/02_weight_rollout.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, role_main  # noqa: E402

import tinyray  # noqa: E402

VERSIONS = 3
ENGINES = 3


def run_engine(argv: list[str]) -> None:
    index, stagger = int(argv[0]), float(argv[1])

    class Engine:
        version = -1

        def generate(self, prompt: str) -> dict:
            return {"text": f"answer({prompt})", "version": self.version}

    svc = Engine()
    with tinyray.join("engine", "serving", serves=svc) as me:
        for v in range(VERSIONS):
            time.sleep(stagger)  # engines never finish loading at the same moment
            # Present but not eligible. pick() must not route here.
            me.unready()
            time.sleep(0.25)
            svc.version = v
            me.ready(model_version=v)
            print(f"[engine {index}] serving v{v}", flush=True)
            time.sleep(0.9)
        time.sleep(0.6)


def run_client(_: list[str]) -> None:
    with tinyray.join("client", "churn") as me:
        me.ready()
        engines = tinyray.pool("engine")
        engines.wait(count=ENGINES, timeout=20)
        seen: dict[int, int] = {}
        stale = 0
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            # Ask for the newest version anyone has, and never accept older.
            newest = max((h.state.get("model_version", -1) for h in engines.all()), default=-1)
            if newest < 0:
                time.sleep(0.02)
                continue
            try:
                out = engines.pick(model_version=newest).generate("q")
            except (tinyray.NotFound, tinyray.Unreachable, tinyray.Fenced):
                time.sleep(0.02)
                continue
            seen[out["version"]] = seen.get(out["version"], 0) + 1
            if out["version"] < newest:
                stale += 1
            time.sleep(0.01)
        print(f"[client] answers per version {dict(sorted(seen.items()))}", flush=True)
        print(f"[client] answers from an engine older than requested: {stale}", flush=True)
        assert stale == 0, "readiness gating failed: a loading engine took work"


def driver() -> int:
    with Fleet() as fleet:
        for i in range(ENGINES):
            fleet.spawn(__file__, "engine", i, i * 0.3, label=f"engine{i}")
        time.sleep(0.5)
        fleet.spawn(__file__, "client", label="client")
        return fleet.wait_all(timeout=90)


if __name__ == "__main__":
    raise SystemExit(role_main({"engine": run_engine, "client": run_client}, driver))
