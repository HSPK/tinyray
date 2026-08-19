"""Annotations are the schema.

No IDL, no code generation, no hand-written validator. The method signature is
the interface, and a caller that sends the wrong shape is told so before the
method runs.

    python examples/12_typed_payloads.py
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, role_main  # noqa: E402

import tinyray  # noqa: E402


@dataclass
class Task:
    task_id: str
    weight: float
    tags: list[str]


@dataclass
class Verdict:
    task_id: str
    reward: float
    accepted: bool


def run_judge(_: list[str]) -> None:
    class Judge:
        def score(self, task: Task, strict: bool = False) -> dict:
            reward = task.weight * (0.5 if strict else 1.0)
            return {"task_id": task.task_id, "reward": reward,
                    "accepted": reward > 0.3, "tags": task.tags}

        def batch(self, tasks: list[Task]) -> int:
            return len(tasks)

        def untyped(self, anything):  # no annotation: nothing is checked
            return type(anything).__name__

    with tinyray.join("judge", "stateful", slot=0, serves=Judge()) as me:
        me.ready()
        print("READY", flush=True)
        time.sleep(6)


def run_client(_: list[str]) -> None:
    with tinyray.join("client", "churn") as me:
        me.ready()
        judge = tinyray.pool("judge").wait(count=1, timeout=20)[0]

        # A plain dict becomes the annotated dataclass on the far side.
        out = judge.score(task={"task_id": "t-1", "weight": 0.8, "tags": ["math"]})
        print(f"[client] score -> {out}", flush=True)
        assert out["accepted"] is True

        out = judge.score(task={"task_id": "t-2", "weight": 0.4, "tags": []}, strict=True)
        assert out["accepted"] is False
        print(f"[client] strict scoring -> reward {out['reward']}", flush=True)

        # Nested containers are checked too.
        assert judge.batch(tasks=[{"task_id": f"t{i}", "weight": 0.1, "tags": []}
                                  for i in range(5)]) == 5
        print("[client] a list of dataclasses arrived as a list of dataclasses", flush=True)

        for bad, why in (
            ({"task_id": "t-3"}, "missing a required field"),
            ({"task_id": 7, "weight": 0.1, "tags": []}, "task_id is not a string"),
            ({"task_id": "t", "weight": "heavy", "tags": []}, "weight is not a number"),
            ({"task_id": "t", "weight": 0.1, "tags": "math"}, "tags is not a list"),
        ):
            try:
                judge.score(task=bad)
            except TypeError as exc:
                print(f"[client] refused ({why}): {str(exc)[:58]}", flush=True)
            else:
                raise AssertionError(f"{why} was accepted")

        # Without an annotation nothing is checked -- opt in, not imposed.
        print(f"[client] untyped method still takes anything: "
              f"{judge.untyped({'a': 1})}", flush=True)


def driver() -> int:
    with Fleet() as fleet:
        fleet.spawn(__file__, "judge", label="judge")
        time.sleep(0.6)
        fleet.spawn(__file__, "client", label="client")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"judge": run_judge, "client": run_client}, driver))
