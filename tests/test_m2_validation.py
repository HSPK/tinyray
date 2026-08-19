"""Annotations are the schema.

rl-bridge hand-wrote a 56-line TypedDict validator because it needed typed RPC
and had none. Repeating that would defeat the purpose.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

import tinyray

SERVER = textwrap.dedent(
    """
    import sys
    from dataclasses import dataclass
    import tinyray

    @dataclass
    class Task:
        id: str
        weight: float

    class Dispatcher:
        def assign(self, task: Task, retries: int = 0) -> dict:
            return {"id": task.id, "weight": task.weight, "retries": retries}
        def add(self, a: int, b: int) -> int:
            return a + b
        def untyped(self, whatever):
            return repr(whatever)

    me = tinyray.join("typed", "stateful", slot=0, serves=Dispatcher())
    me.ready()
    print("READY", flush=True)
    sys.stdin.readline()
    """
)


@pytest.fixture
def typed(registry):
    me = tinyray.join("driver", "churn")
    me.ready()
    proc = subprocess.Popen(
        [sys.executable, "-c", SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert proc.stdout.readline().strip() == "READY"
    tinyray.pool("typed").wait(count=1, timeout=10)
    try:
        yield tinyray.pool("typed").slot(0)
    finally:
        try:
            proc.stdin.write("\n")
            proc.stdin.flush()
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        me.leave()


def test_a_plain_dict_becomes_the_annotated_type(typed):
    got = typed.assign({"id": "t-1", "weight": 0.5})
    assert got == {"id": "t-1", "weight": 0.5, "retries": 0}


def test_a_bad_payload_is_the_callers_fault_not_a_business_failure(typed):
    with pytest.raises(TypeError, match="assign"):
        typed.assign({"id": "t-1"})  # missing a required field
    with pytest.raises(TypeError):
        typed.assign({"id": 7, "weight": "heavy"})  # wrong types
    # Not RemoteError: the method never ran.
    try:
        typed.add("x", 1)
    except TypeError:
        pass
    else:
        pytest.fail("a string where an int was declared must be rejected")


def test_checking_does_not_get_in_the_way_of_untyped_methods(typed):
    assert typed.untyped({"anything": [1, 2]}) == "{'anything': [1, 2]}"
    assert typed.add(2, 3) == 5


def test_the_process_survives_a_rejected_call(typed):
    with pytest.raises(TypeError):
        typed.assign({"id": "t"})
    assert typed.add(1, 1) == 2
