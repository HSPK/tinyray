"""state 会被复制给每个订阅者，所以它的大小要乘上观众人数。"""

from __future__ import annotations

import json

import httpx
import pytest

import tinyray


def _beat(registry, **kw):
    body = dict(
        pool="p",
        id=1,
        incarnation=100,
        policy="churn",
        ready=True,
        leaving=False,
        exclusive=False,
        methods=[],
        watch=[],
        seen={},
        state={},
    )
    body.update(kw)
    return httpx.post(
        f"http://{registry.endpoint}/v1/beat",
        content=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        timeout=30,
    )


def test_the_registry_refuses_to_carry_a_large_state(registry):
    """实测：一个成员 6MB 的 state 在 0.9s 内变成推给 20 个订阅者的 120MB。"""
    assert _beat(registry, state={"blob": "z" * (6 << 20)}).status_code == 413
    assert _beat(registry, id=2, state={"blob": "z" * (17 << 10)}).json()["accepted"] is False
    assert _beat(registry, id=3, state={"blob": "z" * (8 << 10)}).json()["accepted"] is True


def test_the_registry_refuses_oversized_urls_and_method_lists(registry):
    """url 和 methods 同样是照抄给每个订阅者的。"""
    assert _beat(registry, id=4, url="http://" + "x" * 600).json()["accepted"] is False
    assert _beat(registry, id=5, methods=[f"m{i}" for i in range(300)]).json()["accepted"] is False
    assert _beat(registry, id=6, methods=["z" * 600]).json()["accepted"] is False
    assert _beat(registry, id=7, url="http://10.0.0.1:9000", methods=["ok"]).json()["accepted"]


def test_an_oversized_state_fails_at_the_call_that_did_it(registry):
    """注册中心也会拒，但那是在后台线程里悄悄发生的。"""
    with tinyray.join("c", "churn") as me:
        me.ready(host="h1", gpu="a100")
        with pytest.raises(ValueError, match="over the .* limit"):
            me.ready(blob="z" * (1 << 20))


def test_one_oversized_call_does_not_poison_the_member(registry):
    """先改 state 再检查，会把超限的内容留在原地，之后每次 ready() 都失败。"""
    with tinyray.join("c", "churn") as me:
        me.ready(host="h1")
        with pytest.raises(ValueError):
            me.ready(blob="z" * (1 << 20))
        me.ready(step=7)
        assert me.state == {"host": "h1", "step": 7}, me.state
        me.unready()
