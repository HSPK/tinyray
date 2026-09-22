"""Beats that would damage state, rather than merely being wrong, are refused.

Everything here is something a buggy client can send by accident. The registry
has no authentication and is not trying to have any; the point is that one bad
peer must not be able to make a seat unusable or grow memory without bound.
"""

from __future__ import annotations

import time

import msgspec
import pytest

from tests.support.registry_wire import OP_ERROR, RegistryWireError, debug_pools, raw_exchange
from tests.support.registry_wire import beat as registry_beat


def beat(endpoint: str, **kw) -> dict:
    body = {
        "pool": "t",
        "id": 0,
        "slot": 0,
        "incarnation": 1,
        "policy": "stateful",
        "url": None,
        "state": {},
        "ready": True,
        "leaving": False,
        "exclusive": False,
        "methods": [],
        "watch": [],
        "seen": {},
    }
    body.update(kw)
    return registry_beat(endpoint, body)


def pools(endpoint: str) -> dict:
    return debug_pools(endpoint)


def test_a_runaway_clock_cannot_lock_a_seat_forever(registry):
    """The high-water mark is permanent, so accepting a tenure from the year
    50,000 would mean nothing healthy could ever take that seat again. For a
    trainer rank that is a job which can never be restarted."""
    ep = registry.endpoint
    assert beat(ep, incarnation=2**64 - 1)["accepted"] is False
    assert "t" not in pools(ep), "a refused beat still created the pool"

    now = int(time.time() * 1000) << 20
    assert beat(ep, incarnation=now)["accepted"] is True

    # And a restart still takes the seat back, which is the whole reason the
    # default is last-writer-wins.
    assert beat(ep, incarnation=now + 1)["accepted"] is True


def test_a_plausible_future_tenure_is_still_accepted(registry):
    """The bound has to be loose enough that clock skew is not an outage."""
    ep = registry.endpoint
    an_hour_ahead = (int(time.time() * 1000) + 3_600_000) << 20
    assert beat(ep, incarnation=an_hour_ahead)["accepted"] is True


def test_absurd_names_are_refused_rather_than_stored(registry):
    ep = registry.endpoint
    before = len(pools(ep))
    assert beat(ep, pool="x" * 100_000, id=1)["accepted"] is False
    assert beat(ep, pool="ok", id=1, watch=["y" * 100_000])["accepted"] is False
    assert len(pools(ep)) == before, "a refused beat created a pool anyway"


def test_watching_everything_is_refused(registry):
    """A subscriber is sent the whole roster of each pool it watches, so an
    unbounded watch list is an unbounded response."""
    ep = registry.endpoint
    assert beat(ep, pool="w", id=1, watch=[f"p{i}" for i in range(10_000)])["accepted"] is False
    assert beat(ep, pool="w", id=1, watch=[f"p{i}" for i in range(8)])["accepted"] is True


def test_malformed_bodies_are_rejected_without_killing_anything(registry):
    ep = registry.endpoint
    malformed = (
        (b"", 0, {"empty_frame"}),
        (b"\xc1", 0, {"malformed_frame"}),
        (msgspec.msgpack.encode([1, 2, 3]), 0, {"malformed_frame"}),
        (
            msgspec.msgpack.encode({"request_id": 7, "operation": "beat"}),
            7,
            {"malformed_request"},
        ),
        (
            msgspec.msgpack.encode(
                {"request_id": 8, "operation": "beat", "payload": {"pool": "p"}}
            ),
            8,
            {"malformed_request"},
        ),
        (
            msgspec.msgpack.encode(
                {
                    "request_id": 9,
                    "operation": "beat",
                    "payload": {
                        "pool": "p",
                        "id": "one",
                        "incarnation": 1,
                        "policy": "churn",
                    },
                },
            ),
            9,
            {"malformed_request"},
        ),
        (
            msgspec.msgpack.encode({"request_id": 10, "operation": "teleport"}),
            10,
            {"unknown_operation"},
        ),
        (
            msgspec.msgpack.encode({"request_id": 11, "operation": 7, "payload": None}),
            11,
            {"malformed_request"},
        ),
    )
    for raw, request_id, codes in malformed:
        reply = raw_exchange(ep, raw)
        assert reply["operation"] == OP_ERROR
        assert reply["request_id"] == request_id
        assert reply["payload"]["code"] in codes
    # Still serving afterwards.
    assert beat(ep, pool="after", id=1)["accepted"] is True


def test_a_body_too_big_to_be_a_beat_is_refused_before_it_is_read(registry):
    """一个心跳能有多大是有上限的，而且上限在读之前就生效。

    没有它，任何人都能声明一个任意大的请求体，注册中心会一路读进内存 —— 这正是
    这个文件开头那句"一个坏 peer 不能让内存无界增长"。

    我们自己的客户端撞不到：状态受 `MAX_STATE`（16 KiB）管着，watch 列表最多 64
    个名字。所以这条只有手写请求能试，而它防的就是手写请求。

    实测（上限 512 KiB）：1 KiB 正常受理；400 KiB 进得来但因为状态超标被拒
    （`accepted=False`，是内容的问题不是尺寸的问题）；600 KiB 收到结构化的
    `frame_too_large`，而且服务端没有按声明的尺寸分配。
    """
    # 尺寸之内、内容超标：413 之外的另一条路，确认这两件事没有混为一谈。
    fat_state = beat(registry.endpoint, pool="big", state={"pad": "x" * (400 << 10)})
    assert fat_state["accepted"] is False, "400 KiB 的状态该因为内容被拒"

    with pytest.raises(RegistryWireError) as caught:
        beat(registry.endpoint, pool="big", state={"pad": "x" * (600 << 10)})
    assert caught.value.code == "frame_too_large"
