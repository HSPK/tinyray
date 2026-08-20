"""The worker side. Its whole liveness story is `join` plus `ready`."""

from __future__ import annotations

import time

import tinyray


class Worker:
    """Exposed so the pool can reach in and stop an attempt promptly."""

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.active: dict[str, bool] = {}

    def kill(self, attempt_id: str) -> dict:
        self.active.pop(attempt_id, None)
        return {"killed": attempt_id}


def run(worker_id: str, slots: int, capability: str, fingerprint: str, num_tasks: int) -> int:
    me_svc = Worker(worker_id)
    done = 0
    with tinyray.join("agent", "churn", serves=me_svc) as me:
        me.ready(worker=worker_id, slots=slots)
        pool = tinyray.pool("agent_pool")
        pool.wait(count=1, timeout=30)
        pool.slot(0).announce(capability=capability, fingerprint=fingerprint, num_tasks=num_tasks)

        while True:
            handles = pool.all()
            if not handles or handles[0].state.get("finished"):
                break
            work = pool.slot(0).pull_work(worker=worker_id)
            if work is None:
                time.sleep(0.05)
                continue
            attempt_id = work["attempt_id"]
            me_svc.active[attempt_id] = True
            time.sleep(0.02)  # the attempt itself
            if attempt_id not in me_svc.active:
                continue  # cancelled while we ran
            me_svc.active.pop(attempt_id, None)
            pool.slot(0).submit_result(
                worker=worker_id, attempt_id=attempt_id, result={"status": "ok", "reward": 1.0}
            )
            done += 1
    return done
