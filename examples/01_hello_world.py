"""The smallest thing that works: one process offers a method, another calls it.

    python examples/01_hello_world.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, role_main  # noqa: E402

import tinyray  # noqa: E402


def run_greeter(_: list[str]) -> None:
    class Greeter:
        def hello(self, name: str) -> str:
            return f"hello, {name}"

    # serves= is what makes this process callable. Without it the member is
    # present and findable but advertises no address.
    with tinyray.join("greeter", "serving", serves=Greeter()) as me:
        me.ready()
        print("[greeter] up", flush=True)
        time.sleep(3)


def run_caller(_: list[str]) -> None:
    with tinyray.join("caller", "churn") as me:
        me.ready()
        # pick() blocks for nobody: wait() is the one that waits, and its
        # failure names who it was waiting for.
        greeter = tinyray.pool("greeter").wait(count=1, timeout=15)[0]
        print(f"[caller] found {greeter.label} at {greeter.url}", flush=True)
        print(f"[caller] {greeter.hello('world')}", flush=True)


def driver() -> int:
    with Fleet() as fleet:
        fleet.spawn(__file__, "greeter", label="greeter")
        time.sleep(0.4)
        fleet.spawn(__file__, "caller", label="caller")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"greeter": run_greeter, "caller": run_caller}, driver))
