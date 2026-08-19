"""A small async-RL fleet, shaped like a real one.

Four kinds of process, three pools, spread over two simulated nodes:

    node 0                          node 1
    trainer rank 0,1  (collective)  trainer rank 2,3
    engine 0          (serving)     engine 1
    agent worker x3   (churn)       agent worker x3

The agents pull work rather than having it pushed at them: whoever is free
takes the next attempt, which caps the queue at one and keeps a task from
ageing against a model version while it waits.

Run it:  python examples/async_rl_fleet.py
"""

from __future__ import annotations

import os
import random
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = Path(sys.executable).parent / "tinyray"


# --------------------------------------------------------------------------
# The dispatcher: one per data-parallel group, addressed by seat, never by
# "any of them". Sending DP 3's work to DP 5 silently corrupts a batch.
# --------------------------------------------------------------------------
def run_dispatcher(dp_rank: int, total_tasks: int) -> None:
    import tinyray

    class Dispatcher:
        """Work is leased, not given away.

        An agent that dies holding an attempt would otherwise strand it
        forever. tinyray reports that the worker is gone, but only the
        application knows what its disappearance means for the work it held --
        so re-queueing lives here, not in the library.
        """

        LEASE = 3.0

        def __init__(self) -> None:
            self.queue = [f"dp{dp_rank}-task-{i}" for i in range(total_tasks)]
            self.leased: dict[str, tuple[str, float]] = {}
            self.done: list[dict] = []
            self.requeued = 0

        def take(self, worker: str) -> dict | None:
            """An idle agent asks for work. Nothing is handed out in advance."""
            self._reclaim()
            if not self.queue:
                return None
            task = self.queue.pop(0)
            self.leased[task] = (worker, time.monotonic() + self.LEASE)
            return {"task": task, "worker": worker}

        def submit(self, task: str, reward: float, model_version: int) -> dict:
            self.leased.pop(task, None)
            self.done.append({"task": task, "reward": reward, "version": model_version})
            return {"remaining": len(self.queue), "collected": len(self.done)}

        def progress(self) -> dict:
            return {"remaining": len(self.queue), "collected": len(self.done)}

        def _reclaim(self) -> None:
            now = time.monotonic()
            for task, (_, deadline) in list(self.leased.items()):
                if deadline <= now:
                    del self.leased[task]
                    self.queue.append(task)
                    self.requeued += 1

        def outstanding(self) -> int:
            self._reclaim()
            return len(self.queue) + len(self.leased)

    svc = Dispatcher()
    with tinyray.join("dispatcher", "stateful", slot=dp_rank, serves=svc) as me:
        me.ready(dp_rank=dp_rank)
        while svc.outstanding():
            time.sleep(0.05)
        me.ready(finished=True)
        rewards = [d["reward"] for d in svc.done]
        print(
            f"[dispatcher/{dp_rank}] collected {len(svc.done)}/{total_tasks} "
            f"requeued={svc.requeued} mean_reward={sum(rewards) / len(rewards):.3f}",
            flush=True,
        )
        time.sleep(0.5)


# --------------------------------------------------------------------------
# The engine: interchangeable, but only once it holds the right weights.
# Readiness here is a claim about state, not just a pulse.
# --------------------------------------------------------------------------
def run_engine(index: int, versions: int) -> None:
    import tinyray

    class Engine:
        version = 0

        def generate(self, prompt: str) -> dict:
            return {"text": f"answer({prompt})", "version": self.version, "engine": index}

    svc = Engine()
    with tinyray.join("engine", "serving", serves=svc) as me:
        for v in range(versions):
            # While loading weights the engine is present but not eligible:
            # pick() must not route to it.
            me.unready()
            time.sleep(0.15)
            svc.version = v
            me.ready(model_version=v)
            time.sleep(1.2)
        time.sleep(0.5)


# --------------------------------------------------------------------------
# The agent worker: pure churn. Nobody is named, nobody is missed, and one
# dying costs exactly one attempt.
# --------------------------------------------------------------------------
def run_agent(worker_id: str, dp_groups: int, crash_after: int = 0) -> None:
    import tinyray

    with tinyray.join("agent", "churn") as me:
        me.ready(worker=worker_id)
        dispatchers = tinyray.pool("dispatcher")
        engines = tinyray.pool("engine")
        engines.wait(count=1, timeout=30)

        # An empty pool means two opposite things -- "not started yet" and
        # "finished and went home" -- and the phone book cannot tell them
        # apart, because job lifecycle is not its business. So remember
        # whether we ever saw them: that one bit is what distinguishes the
        # two, and it has to live here.
        met_dispatchers = False
        startup_deadline = time.monotonic() + 30

        handled = 0
        while True:
            alive = dispatchers.all()
            if alive:
                met_dispatchers = True
            elif met_dispatchers:
                break  # they finished and left
            elif time.monotonic() > startup_deadline:
                info = tinyray.pool("dispatcher")._c.pool_info("dispatcher")
                print(
                    f"[agent {worker_id}] DEBUG no dispatcher: pool_info={info} "
                    f"stats={me.stats()} raw={tinyray.pool('dispatcher')._members({}, False)}",
                    flush=True,
                )
                break
            else:
                time.sleep(0.05)
                continue

            work = None
            for dp in random.sample(range(dp_groups), dp_groups):
                try:
                    work = dispatchers.slot(dp).take(worker_id)
                except (tinyray.NotFound, tinyray.Unreachable, tinyray.Fenced):
                    continue
                if work:
                    break
            if work is None:
                if all(h.state.get("finished") for h in alive):
                    break
                time.sleep(0.05)
                continue

            try:
                engine = engines.pick()
                out = engine.generate(work["task"])
                time.sleep(0.05)  # pretend the rollout took a moment
            except (tinyray.NotFound, tinyray.Unreachable, tinyray.Fenced):
                # A stale address is normal. Look it up again next round; the
                # attempt is simply retried.
                time.sleep(0.05)
                continue

            dispatchers.slot(dp).submit(
                task=work["task"], reward=random.random(), model_version=out["version"]
            )
            handled += 1
            if crash_after and handled >= crash_after:
                print(f"[agent {worker_id}] dying after {handled} attempts", flush=True)
                os._exit(1)  # no farewell: only the lease can notice
        print(f"[agent {worker_id}] finished {handled} attempts", flush=True)


# --------------------------------------------------------------------------
# The trainer: a seat is a rank, and every rank must agree on the same list
# before a collective can be built.
# --------------------------------------------------------------------------
def run_trainer(rank: int, world: int) -> None:
    import tinyray

    with tinyray.join("trainer", "collective", slot=rank, size=world) as me:
        me.ready(step=0)
        peers = tinyray.pool("trainer").wait(count=world, timeout=30)
        assert len(peers) == world
        if rank == 0:
            seats = sorted(h.slot for h in peers)
            print(f"[trainer/0] all {world} ranks present: {seats}", flush=True)
        for step in range(1, 4):
            tinyray.pool("trainer").all(step=step)
            time.sleep(0.4)
            me.ready(step=step)
        time.sleep(1.0)

    print(f"[trainer/{rank}] finished", flush=True)


ROLES = {
    "dispatcher": lambda a: run_dispatcher(int(a[0]), int(a[1])),
    "engine": lambda a: run_engine(int(a[0]), int(a[1])),
    "agent": lambda a: run_agent(a[0], int(a[1]), int(a[2])),
    "trainer": lambda a: run_trainer(int(a[0]), int(a[1])),
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] in ROLES:
        ROLES[sys.argv[1]](sys.argv[2:])
        return 0

    if not REGISTRY.exists():
        print(f"install first: maturin develop --release ({REGISTRY} missing)")
        return 1

    port = free_port()
    registry = subprocess.Popen(
        [str(REGISTRY), "--listen", f"127.0.0.1:{port}", "--ttl-ms", "3000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    env = dict(os.environ, TINYRAY_REGISTRY=f"127.0.0.1:{port}")
    time.sleep(0.8)

    dp_groups, world, engines, agents_per_node = 2, 4, 2, 3
    plan: list[list[str]] = []
    for dp in range(dp_groups):
        plan.append(["dispatcher", str(dp), "40"])
    for rank in range(world):
        plan.append(["trainer", str(rank), str(world)])
    for e in range(engines):
        plan.append(["engine", str(e), "3"])
    for node in range(2):
        for i in range(agents_per_node):
            # One agent is killed mid-flight to show that churn costs nothing.
            crash = "2" if (node, i) == (1, 0) else "0"
            plan.append(["agent", f"n{node}-w{i}", str(dp_groups), crash])

    print(f"registry on 127.0.0.1:{port}; starting {len(plan)} processes\n", flush=True)
    procs = [
        subprocess.Popen([sys.executable, __file__, *args], env=env, cwd=str(ROOT)) for args in plan
    ]
    t0 = time.monotonic()
    rc = 0
    for p, args in zip(procs, plan):
        code = p.wait(timeout=120)
        # The deliberately killed agent is expected to exit non-zero.
        if code != 0 and not (args[0] == "agent" and args[3] != "0"):
            print(f"!! {' '.join(args)} exited {code}")
            rc = 1
    print(f"\nfleet finished in {time.monotonic() - t0:.1f}s")
    registry.terminate()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
