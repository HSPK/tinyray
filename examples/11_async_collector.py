"""An asyncio process on both sides of a call.

A trainer loop is synchronous and a collector loop is asyncio, and they call
each other, so both flavours have to exist. The callee's coroutines run on the
loop that was already there -- inventing a new one would strand every client
the application had bound to its own.

    python examples/11_async_collector.py
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, role_main  # noqa: E402

import tinyray  # noqa: E402

TASKS = 30


def run_collector(argv: list[str]) -> None:
    index = int(argv[0])

    class Collector:
        def __init__(self) -> None:
            self.loop_id = 0
            self.handled = 0

        async def rollout(self, prompt: str) -> dict:
            await asyncio.sleep(0.01)  # a real one would await an engine
            self.handled += 1
            return {"text": f"answer({prompt})", "loop": id(asyncio.get_running_loop())}

        def handled_count(self) -> int:
            return self.handled

    async def main() -> None:
        svc = Collector()
        svc.loop_id = id(asyncio.get_running_loop())
        with tinyray.join("collector", "churn", serves=svc) as me:
            me.ready(index=index, loop=svc.loop_id)
            drivers = tinyray.pool("driver")
            met = False
            while True:
                alive = drivers.all()
                if alive:
                    met = True
                    if all(h.state.get("done") for h in alive):
                        break
                elif met:
                    break
                await asyncio.sleep(0.02)
            print(f"[collector {index}] handled {svc.handled}", flush=True)
            await asyncio.sleep(0.3)

    asyncio.run(main())


def run_driver(_: list[str]) -> None:
    async def main() -> None:
        with tinyray.join("driver", "churn") as me:
            me.ready()
            # apool gives handles whose methods are awaitable. Same lookups --
            # they read the local cache and never block.
            collectors = tinyray.apool("collector")
            collectors.wait(count=2, timeout=20)

            handles = collectors.all()
            # Thirty rollouts in flight at once over two collectors.
            results = await asyncio.gather(
                *(handles[i % len(handles)].rollout(f"q{i}") for i in range(TASKS))
            )
            assert len(results) == TASKS
            for h in handles:
                on_its_own_loop = any(r["loop"] == h.state["loop"] for r in results)
                assert on_its_own_loop, "a coroutine ran on a loop we invented"
            print(f"[driver] {TASKS} concurrent rollouts, all on the collectors' "
                  f"own loops", flush=True)

            # A synchronous caller talks to the same async methods.
            sync = tinyray.pool("collector").pick()
            print(f"[driver] blocking call works too: {sync.rollout('sync')['text']}",
                  flush=True)
            me.ready(done=True)
            await asyncio.sleep(0.4)

    asyncio.run(main())


def driver() -> int:
    with Fleet() as fleet:
        for i in range(2):
            fleet.spawn(__file__, "collector", i, label=f"collector{i}")
        time.sleep(0.6)
        fleet.spawn(__file__, "driver", label="driver")
        return fleet.wait_all(timeout=90)


if __name__ == "__main__":
    raise SystemExit(role_main({"collector": run_collector, "driver": run_driver}, driver))
