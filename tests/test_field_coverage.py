"""One test per wire field, so no field exists without a reason anyone can see."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import time
import urllib.request

import pytest
import tinyray


def beat(endpoint: str, **kw) -> dict:
    body = {
        "pool": "f",
        "id": 1,
        "incarnation": 100,
        "policy": "churn",
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
    req = urllib.request.Request(
        f"http://{endpoint}/v1/beat",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


# ---- Beat ----------------------------------------------------------------


def test_pool_and_id_are_the_key(registry):
    beat(registry.endpoint, pool="a", id=1)
    beat(registry.endpoint, pool="b", id=1)  # same id, different pool
    ack = beat(registry.endpoint, pool="a", id=2, watch=["a", "b"])
    assert len(ack["pools"]["a"]["changed"]) == 2
    assert len(ack["pools"]["b"]["changed"]) == 1


def test_slot_is_carried_through_for_display(registry):
    ack = beat(registry.endpoint, pool="s", id=3, slot=3, policy="stateful", watch=["s"])
    assert ack["pools"]["s"]["changed"][0]["slot"] == 3


def test_incarnation_orders_occupants(registry):
    beat(registry.endpoint, pool="i", id=1, incarnation=5)
    assert beat(registry.endpoint, pool="i", id=1, incarnation=4)["accepted"] is False
    assert beat(registry.endpoint, pool="i", id=1, incarnation=6)["accepted"] is True


def test_policy_and_size_are_pool_wide(registry):
    beat(registry.endpoint, pool="p", id=1, policy="collective", size=8, slot=0)
    ack = beat(registry.endpoint, pool="p", id=2, slot=1, policy="collective", watch=["p"])
    assert ack["pools"]["p"]["policy"] == "collective"
    assert ack["pools"]["p"]["size"] == 8


def test_url_and_state_and_ready_travel_to_peers(registry):
    beat(registry.endpoint, pool="u", id=1, url="http://h:1", state={"v": 7}, ready=False)
    m = beat(registry.endpoint, pool="u", id=2, watch=["u"])["pools"]["u"]["changed"]
    mine = [x for x in m if x["id"] == 1][0]
    assert mine["url"] == "http://h:1" and mine["state"] == {"v": 7}
    assert mine["ready"] is False


def test_leaving_frees_the_record_at_once(registry):
    beat(registry.endpoint, pool="l", id=1)
    beat(registry.endpoint, pool="l", id=1, leaving=True)
    ack = beat(registry.endpoint, pool="l", id=2, watch=["l"])
    assert [x["id"] for x in ack["pools"]["l"]["changed"]] == [2]


def test_exclusive_refuses_an_occupied_seat(registry):
    beat(registry.endpoint, pool="x", id=0, slot=0, policy="stateful", incarnation=10)
    taken = beat(
        registry.endpoint, pool="x", id=0, slot=0, policy="stateful", incarnation=11, exclusive=True
    )
    assert taken["accepted"] is False


def test_methods_are_stored_once_per_pool(registry):
    beat(registry.endpoint, pool="m", id=1, methods=["a", "b"])
    ack = beat(registry.endpoint, pool="m", id=2, watch=["m"])
    assert ack["pools"]["m"]["methods"] == ["a", "b"]
    # Not repeated on every member.
    assert all("methods" not in x for x in ack["pools"]["m"]["changed"])


def test_watch_decides_what_comes_back(registry):
    beat(registry.endpoint, pool="w1", id=1)
    beat(registry.endpoint, pool="w2", id=1)
    assert set(beat(registry.endpoint, pool="w1", id=2, watch=["w2"])["pools"]) == {"w2"}
    assert beat(registry.endpoint, pool="w1", id=2, watch=[])["pools"] == {}


def test_seen_turns_a_snapshot_into_a_delta(registry):
    beat(registry.endpoint, pool="d", id=1)
    first = beat(registry.endpoint, pool="d", id=2, watch=["d"])["pools"]["d"]
    assert first["full"] is True and len(first["changed"]) == 2

    caught_up = beat(registry.endpoint, pool="d", id=2, watch=["d"], seen={"d": first["version"]})
    assert "d" not in caught_up["pools"], "nothing changed, so nothing should be sent"

    beat(registry.endpoint, pool="d", id=3)
    delta = beat(registry.endpoint, pool="d", id=2, watch=["d"], seen={"d": first["version"]})[
        "pools"
    ]["d"]
    assert delta["full"] is False
    assert [x["id"] for x in delta["changed"]] == [3]


# ---- BeatAck -------------------------------------------------------------


def test_ttl_ms_tells_the_client_how_often_to_beat(registry):
    assert beat(registry.endpoint, pool="t", id=1)["ttl_ms"] == registry.ttl_ms


def test_accepted_is_false_only_for_a_ghost(registry):
    assert beat(registry.endpoint, pool="g", id=1, incarnation=9)["accepted"] is True
    assert beat(registry.endpoint, pool="g", id=1, incarnation=8)["accepted"] is False


def test_removed_carries_departures(registry):
    beat(registry.endpoint, pool="r", id=1)
    first = beat(registry.endpoint, pool="r", id=2, watch=["r"])["pools"]["r"]
    beat(registry.endpoint, pool="r", id=1, leaving=True)
    delta = beat(registry.endpoint, pool="r", id=2, watch=["r"], seen={"r": first["version"]})[
        "pools"
    ]["r"]
    assert delta["removed"] == [1]


def test_version_and_roster_answer_different_questions(registry):
    beat(registry.endpoint, pool="n", id=1, state={"step": 1})
    a = beat(registry.endpoint, pool="n", id=2, watch=["n"])["pools"]["n"]
    beat(registry.endpoint, pool="n", id=1, state={"step": 2})  # same person, new state
    b = beat(registry.endpoint, pool="n", id=2, watch=["n"], seen={"n": a["version"]})["pools"]["n"]
    assert b["version"] > a["version"], "peers must learn about the new state"
    assert b["roster"] == a["roster"], "the same people are still here"

    beat(registry.endpoint, pool="n", id=3)  # a new person
    c = beat(registry.endpoint, pool="n", id=2, watch=["n"], seen={"n": b["version"]})["pools"]["n"]
    assert c["roster"] != b["roster"]


def test_full_says_to_drop_what_you_had(registry):
    """Two ways to get a whole roster: never having seen the pool, or falling
    so far behind that the change log no longer reaches back to you.

    什么时候要紧，是可以量的：让一个成员在你走神期间离场，再把它的离场记录挤出
    日志。拿旧位置回来的时候，只有"整份重发"能让你知道它走了。实测把日志窗口那
    条判断拆掉：答复是 `full=False changed=[1]`，观察者**永远以为它还在**。
    """
    beat(registry.endpoint, pool="F", id=1)
    fresh = beat(registry.endpoint, pool="F", id=2, watch=["F"])["pools"]["F"]
    assert fresh["full"] is True

    # Still inside the log: a delta is enough.
    beat(registry.endpoint, pool="F", id=3)
    near = beat(registry.endpoint, pool="F", id=2, watch=["F"], seen={"F": fresh["version"]})[
        "pools"
    ]["F"]
    assert near["full"] is False

    # id=3 走了，然后把它的离场挤出日志的另一头。
    beat(registry.endpoint, pool="F", id=3, leaving=True)

    # Past the end of it: the registry gives up on catching us up piecemeal.
    # LOG_CAP is 4096 entries, and each state change is one entry.
    for i in range(4200):
        beat(registry.endpoint, pool="F", id=1, state={"i": i})
    far = beat(registry.endpoint, pool="F", id=2, watch=["F"], seen={"F": fresh["version"]})[
        "pools"
    ]["F"]
    assert far["full"] is True
    left = sorted(m["id"] for m in far["changed"])
    assert left == [1, 2], f"整份名册应该只剩 1 和 2，拿到的是 {left}"
    assert 3 not in left, "3 号的离场掉出了日志，观察者却还以为它在"


def test_expires_at_never_crosses_the_wire(registry):
    ack = beat(registry.endpoint, pool="e", id=1, watch=["e"])
    member = ack["pools"]["e"]["changed"][0]
    assert set(member) <= {"id", "slot", "incarnation", "url", "state", "ready"}
    assert "expires_at" not in member


# ---- local objects -------------------------------------------------------

SERVER = textwrap.dedent(
    """
    import sys, tinyray
    class S:
        def echo(self, x: int) -> int: return x
    with tinyray.join("h", "collective", slot=0, size=1, serves=S()) as me:
        me.ready(k="v")
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


@pytest.fixture
def peer(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    me = tinyray.join("obs", "churn")
    me.ready()
    tinyray.pool("h").wait(count=1, timeout=15)
    try:
        yield me
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
        me.leave()


def test_every_handle_field_is_populated(peer):
    h = tinyray.pool("h").slot(0)
    for field in ("pool", "id", "slot", "incarnation", "url", "state", "ready"):
        assert getattr(h, field) is not None, field
    assert h.state == {"k": "v"}
    assert h.identity and h.label


def test_every_epoch_field_is_populated(peer):
    ep = tinyray.pool("h").epoch(timeout=10)
    assert ep.pool == "h"
    assert len(ep.members) == 1
    assert isinstance(ep.roster, int) and ep.roster != 0
    assert ep.valid is True


def test_a_departure_stops_mattering_once_its_lease_would_have_run_out(registry):
    """说过再见的任期要记住一阵子，但**只是一阵子**。

    记住是为了挡住迟到的心跳：调用方那边已经超时放弃，请求却还在路上，说着
    `leaving=false`、带着刚刚告别的那个任期，一到就能把成员塞回去，白占一个租约。

    忘掉则是为了两件事。一是这张表按成员 id 存，而 churn 成员每次加入都换一个
    随机 id —— 不忘就是无界增长：实测 5 万轮 churn，会忘记时 RSS 涨 312 kB，
    不忘时涨 **2200 kB**（每轮 45 字节对 6 字节）。二是下面这条更直接：租约都过完
    了，同一个任期本来就该能重新加入。

    断言的是后者，因为它是确定的 —— 不用去数一个谁也没暴露出来的表。
    """
    assert beat(registry.endpoint, pool="G", id=77, incarnation=500)["accepted"] is True
    assert beat(registry.endpoint, pool="G", id=77, incarnation=500, leaving=True)["accepted"]

    # 迟到的心跳：同一个任期，刚说完再见就回来，不能复活它。
    late = beat(registry.endpoint, pool="G", id=77, incarnation=500)
    assert late["accepted"] is False, "刚告别的任期被一个迟到的心跳原地复活了"

    # 租约过完之后，这条记录就没有意义了。
    time.sleep(registry.ttl_ms / 1000 + 1.5)
    again = beat(registry.endpoint, pool="G", id=77, incarnation=500)
    assert again["accepted"] is True, (
        "过了一整个租约，同一个任期还是被挡着 —— 离场记录没有被忘掉，"
        "而它是按每个 id 存的，churn 成员每次都换 id"
    )
