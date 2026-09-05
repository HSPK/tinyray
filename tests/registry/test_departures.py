"""Registry fencing and departure invariants."""

from __future__ import annotations

import json
import time
import urllib.request

import tinyray
from tinyray import _tinyray


def _beat(endpoint: str, **kw) -> dict:
    body = dict(
        pool="t",
        id=0,
        slot=0,
        incarnation=1,
        policy="stateful",
        url=None,
        state={},
        ready=True,
        leaving=False,
        methods=[],
        watch=["t"],
        seen={},
    )
    body.update(kw)
    req = urllib.request.Request(
        f"http://{endpoint}/v1/beat",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def _pools(endpoint: str) -> dict:
    with urllib.request.urlopen(f"http://{endpoint}/v1/pools", timeout=5) as r:
        return json.loads(r.read())


def test_a_superseded_tenure_cannot_take_the_seat_back(registry):
    """The record disappears when a member is reaped, so the pool must keep a
    high-water mark. Otherwise the process that was replaced only has to wait
    for its replacement to die."""
    ep = registry.endpoint
    _beat(ep, incarnation=1)
    _beat(ep, incarnation=2)
    assert _beat(ep, incarnation=1)["accepted"] is False

    deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 2
    while _pools(ep)["t"]["members"] and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _pools(ep)["t"]["members"] == 0, "the replacement should have expired"

    assert _beat(ep, incarnation=1)["accepted"] is False, "the ghost came back"
    assert _pools(ep)["t"]["members"] == 0


def test_leaving_with_a_mismatched_tenure_keeps_the_fingerprint_clean(registry):
    """XOR-ing out the beat's tenure instead of the stored one would leave the
    roster permanently wrong, and a wrong roster voids rounds forever."""
    ep = registry.endpoint
    _beat(ep, pool="r", id=7, incarnation=10)
    assert _pools(ep)["r"]["roster"] != 0
    _beat(ep, pool="r", id=7, incarnation=99, leaving=True)
    assert _pools(ep)["r"]["roster"] == 0


def test_a_beat_still_in_flight_cannot_undo_a_leave(registry):
    """离开之后，一个还在路上的心跳不许把它重新注册回来。

    `leave()` 发出最后一拍，但心跳循环可能已经把下一拍送出去了 —— 那一拍带着
    同样的 (id, 任期)，到达时座位刚被腾空，于是又被当成一次正常注册收下。原始
    实测是 300 次 leave 里有 6 次留下了没走掉的成员。

    这里不去复现那个竞态 —— 2% 的概率意味着测试要跑几百轮才看得见。改成直接量
    机制：注册表把刚离开的任期记住一个租约的时间，之后带着那个任期来的心跳一律
    当幽灵。用 `Client` 直接指定任期就能确定地送出这样一拍。
    """
    # 座位号得落在世界里 —— 写这条测试时随手挑了 4242 配 size=1，被后来加的
    # 形状校验当场拦下。座位是 0 号，id 就是 0，补那一拍照样构造得出来。
    ident = 0
    with tinyray.join("late", "stateful", slot=ident, size=1) as me:
        me.ready()
        tenure = me.incarnation
        pool = tinyray.pool("late")
        assert len(pool.wait(count=1, timeout=15)) == 1
        me.leave()

    # 座位确实空了。
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and _pools(registry.endpoint)["late"]["members"]:
        time.sleep(0.05)
    assert _pools(registry.endpoint)["late"]["members"] == 0, "leave() 之后成员还在"

    # 现在补上那一拍：同样的座位、同样的任期，就像它从未离开。
    straggler = _tinyray.Client(
        endpoint=f"http://{registry.endpoint}",
        pool="late",
        id=ident,
        incarnation=tenure,
        policy="stateful",
        slot=ident,
        size=1,
    )
    try:
        straggler.start()
        time.sleep(0.5)
        assert not straggler.accepted, "刚离开的任期又被收下了"
        assert _pools(registry.endpoint)["late"]["members"] == 0, (
            "一个在途的心跳把已经离开的成员复活了"
        )
    finally:
        straggler.abandon()
