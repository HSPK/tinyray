"""Refusing work instead of queueing it forever.

tinyray hands out addresses; it has no opinion about how much work you accept.
But the shape it does enforce -- a bounded call, an explicit failure -- makes
saying no cheap, and saying no is what keeps a queue from becoming staleness.

    python examples/10_backpressure.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402

CAPACITY = 8
PRODUCERS = 3
PER_PRODUCER = 40


def run_queue(_: list[str]) -> None:
    class Queue:
        def __init__(self) -> None:
            self.items: list[str] = []
            self.accepted = 0
            self.refused = 0
            self.done = 0

        def offer(self, item: str) -> dict:
            if len(self.items) >= CAPACITY:
                self.refused += 1
                # An explicit refusal, not a silent wait. The producer decides
                # what to do about it, because only it knows if the work can
                # wait or should go somewhere else.
                raise RuntimeError(f"full at {CAPACITY}")
            self.items.append(item)
            self.accepted += 1
            return {"depth": len(self.items)}

        def take(self) -> str | None:
            return self.items.pop(0) if self.items else None

        def stats(self) -> dict:
            return {
                "accepted": self.accepted,
                "refused": self.refused,
                "depth": len(self.items),
                "done": self.done,
            }

    svc = Queue()
    with tinyray.join("queue", "stateful", slot=0, serves=svc) as me:
        me.ready(capacity=CAPACITY)
        total = PRODUCERS * PER_PRODUCER
        deadline = time.monotonic() + 25
        while svc.done < total and time.monotonic() < deadline:
            item = svc.take()
            if item is None:
                time.sleep(0.005)
                continue
            time.sleep(0.004)  # the consumer is slower than the producers
            svc.done += 1
        s = svc.stats()
        print(
            f"[queue] accepted={s['accepted']} refused={s['refused']} "
            f"consumed={svc.done} final_depth={s['depth']}",
            flush=True,
        )
        assert s["refused"] > 0, "the producers never outran the consumer"
        assert s["depth"] <= CAPACITY, "the bound did not hold"
        me.ready(finished=True)
        time.sleep(0.4)


def run_producer(argv: list[str]) -> None:
    name = argv[0]
    with tinyray.join("producer", "churn") as me:
        me.ready(name=name)
        queue = tinyray.pool("queue")
        queue.wait(count=1, timeout=20)
        sent = retried = 0
        for i in range(PER_PRODUCER):
            while True:
                try:
                    queue.slot(0).offer(item=f"{name}-{i}")
                    sent += 1
                    break
                except tinyray.RemoteError as exc:
                    # A refusal is a business answer, so tinyray did not retry
                    # it for us. Backing off is our call.
                    assert "full" in exc.message
                    retried += 1
                    time.sleep(0.01)
        print(f"[producer {name}] sent {sent}, backed off {retried} times", flush=True)


def driver() -> int:
    with Fleet() as fleet:
        fleet.spawn(__file__, "queue", label="queue")
        time.sleep(0.5)
        for i in range(PRODUCERS):
            fleet.spawn(__file__, "producer", f"p{i}", label=f"producer{i}")
        return fleet.wait_all(timeout=90)


if __name__ == "__main__":
    raise SystemExit(role_main({"queue": run_queue, "producer": run_producer}, driver))
