"""The pool side of an agent tier from a real async RL framework, ported onto tinyray.

Only the domain is left here. Who exists, who is still alive, and how to reach
them are no longer this file's problem.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Literal

import tinyray

State = Literal["pending", "running", "completed", "cancelled"]


@dataclass
class Attempt:
    attempt_id: str
    payload: dict[str, Any]
    state: State = "pending"
    worker: str | None = None
    result: dict[str, Any] | None = None


@dataclass
class Catalog:
    """Agreed once by the first worker; later disagreement is a real bug."""

    capability: str
    fingerprint: str
    num_tasks: int


@dataclass
class AgentPool:
    capacity: int
    catalog: Catalog
    queue: list[str] = field(default_factory=list)
    attempts: dict[str, Attempt] = field(default_factory=dict)
    stopping: bool = False
    # A worker we have never heard of is not a worker that died. Membership is
    # eventually consistent, and a newcomer can pull work before the pool's own
    # cache has caught up with its arrival.
    last_seen: dict[str, float] = field(default_factory=dict)

    # ---- domain API, callable from any worker ----------------------------
    def announce(self, capability: str, fingerprint: str, num_tasks: int) -> dict:
        c = self.catalog
        if (capability, fingerprint, num_tasks) != (c.capability, c.fingerprint, c.num_tasks):
            raise ValueError(
                f"catalog mismatch: pool has {c}, worker has "
                f"{Catalog(capability, fingerprint, num_tasks)}"
            )
        return {"num_tasks": c.num_tasks}

    def submit_attempt(self, attempt_id: str, payload: dict[str, Any]) -> dict:
        if len(self.queue) >= self.capacity:
            raise RuntimeError(f"queue is full at {self.capacity}")
        self.attempts[attempt_id] = Attempt(attempt_id, payload)
        self.queue.append(attempt_id)
        return {"queued": len(self.queue)}

    def pull_work(self, worker: str) -> dict | None:
        if self.stopping or not self.queue:
            return None
        record = self.attempts[self.queue.pop(0)]
        record.state, record.worker = "running", worker
        return {"attempt_id": record.attempt_id, "payload": record.payload}

    def submit_result(self, worker: str, attempt_id: str, result: dict[str, Any]) -> dict:
        record = self.attempts[attempt_id]
        if record.worker != worker:
            raise ValueError(f"{attempt_id} belongs to {record.worker}, not {worker}")
        if record.state in ("completed", "cancelled"):
            # A terminal state is terminal. The worker's own "was I cancelled?"
            # check only works if the kill arrived, and the kill is the one call
            # here that may fail without saying whether it ran. So the record
            # decides, not the survivor.
            return {"state": record.state, "outstanding": self.outstanding()}
        record.state, record.result = "completed", result
        return {"state": record.state, "outstanding": self.outstanding()}

    def cancel(self, attempt_id: str, detail: str) -> dict:
        record = self.attempts[attempt_id]
        if record.state in ("completed", "cancelled"):
            return {"state": record.state}
        record.state = "cancelled"
        record.result = {"status": "cancelled", "detail": detail}
        # Reaching the worker directly rather than waiting for it to poll: a
        # worker with every slot busy is not asking for anything. Fungible
        # members have no seat, so they are addressed by published state.
        if record.worker:
            try:
                tinyray.pool("agent").pick(worker=record.worker).kill(attempt_id)
            except tinyray.NotDelivered:
                pass  # it never left, and the worker it was for is gone
            except (tinyray.NotFound, tinyray.OutcomeUnknown, tinyray.Fenced):
                pass  # we do not know whether it landed; submit_result refuses
                # the attempt either way, so a survivor cannot finish it
        return {"state": record.state}

    def outstanding(self) -> int:
        return sum(1 for a in self.attempts.values() if a.state in ("pending", "running"))

    # ---- the one thing tinyray cannot decide -----------------------------
    def reclaim(self, live: set[str], grace: float) -> int:
        """A worker vanished. tinyray reports that; what happens to the work it
        held is domain knowledge, so it lives here.

        Two guards, both learned the hard way: never reclaim from a worker we
        have not seen yet, and wait out a grace period so a slow heartbeat is
        not read as a death.
        """
        now = time.monotonic()
        for worker in live:
            if worker:
                self.last_seen[worker] = now
        lost = 0
        for record in self.attempts.values():
            if record.state != "running" or record.worker in live:
                continue
            seen = self.last_seen.get(record.worker)
            if seen is None or now - seen < grace:
                continue
            record.state = "pending"
            record.worker = None
            self.queue.append(record.attempt_id)
            lost += 1
        return lost


def serve(capacity: int, catalog: Catalog, total: int) -> AgentPool:
    svc = AgentPool(capacity=capacity, catalog=catalog)
    for i in range(total):
        svc.submit_attempt(f"a-{i}", {"task": i})

    with tinyray.join("agent_pool", "stateful", slot=0, serves=svc) as me:
        me.ready(capability=catalog.capability, num_tasks=catalog.num_tasks)
        agents = tinyray.pool("agent")
        grace = 2.0
        while svc.outstanding():
            svc.reclaim({h.state.get("worker") for h in agents.all()}, grace)
            time.sleep(0.1)
        svc.stopping = True
        me.ready(finished=True)
        time.sleep(0.5)
    return svc
