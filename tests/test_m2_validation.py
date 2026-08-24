"""Annotations are the schema.

rl-bridge hand-wrote a 56-line TypedDict validator because it needed typed RPC
and had none. Repeating that would defeat the purpose.
"""

from __future__ import annotations

import functools
import subprocess
import sys
import textwrap

import pytest
import tinyray
from tinyray import _serve

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


def test_a_property_on_a_served_object_is_never_evaluated(registry):
    """发现方法时读的是类，不是实例。

    `getattr` 会执行描述符，所以逐个读公开名字等于把每个 property 都求值一遍 ——
    而这个领域里的服务对象满是 `device`、`model`、`step` 这样的 property。

    实测过三种后果：有副作用的 property 在发现阶段被触发一次；会抛的那种直接
    把 `join(serves=...)` 带下水，报出 `RuntimeError: no GPU on this box` ——
    一次应用根本没发起的调用；返回 callable 的那种被登记成了可远程调用的方法。
    """
    fired = []

    class Worker:
        def ping(self) -> int:
            return 1

        @property
        def device(self) -> int:
            fired.append("device")
            return 0

        @property
        def gpu(self) -> int:
            raise RuntimeError("no GPU on this box")

        @property
        def handler(self):
            return lambda: "not a method"

        @functools.cached_property
        def expensive(self) -> int:
            fired.append("expensive")
            return 2

    found = _serve.scan(Worker())
    assert fired == [], f"发现方法时求值了 {fired}"
    assert sorted(found) == ["ping"], f"多收或少收了: {sorted(found)}"

    # 整条路走一遍：带这种对象 join 不能失败。
    with tinyray.join("svc", "stateful", slot=0, serves=Worker()) as me:
        me.ready()
        h = tinyray.pool("svc").wait(count=1, timeout=15)[0]
        assert h.ping() == 1
        assert fired == [], f"服务起来之后又求值了 {fired}"
        with pytest.raises(AttributeError):
            h.device()  # property 不是方法，不该出现在对面


def test_the_kinds_of_method_are_all_still_found(registry):
    """对偶：修复不能把真方法挡掉。classmethod 尤其容易 —— 它本身不可调用。"""

    class Every:
        def plain(self) -> int:
            return 1

        @staticmethod
        def stat() -> int:
            return 2

        @classmethod
        def cls_(cls) -> int:
            return 3

        def __init__(self) -> None:
            self.assigned = lambda: 4

    found = _serve.scan(Every())
    assert sorted(found) == ["assigned", "cls_", "plain", "stat"], sorted(found)


def test_a_proxy_that_answers_through_getattr_still_works(registry):
    """`__dir__` 加 `__getattr__` 的代理没有静态属性可查，只能问实例。
    修复不能顺手把这种也挡掉。"""

    class Proxy:
        def __dir__(self):
            return ["dynamic"]

        def __getattr__(self, name: str):
            if name == "dynamic":
                return lambda: "answered"
            raise AttributeError(name)

    found = _serve.scan(Proxy())
    assert sorted(found) == ["dynamic"], sorted(found)
