"""One test per public name, so nothing ships undocumented or unexercised."""

from __future__ import annotations

import asyncio
import inspect
import subprocess
import sys
import textwrap
import time

import pytest
import tinyray

SERVER = textwrap.dedent(
    """
    import sys, tinyray
    class S:
        def echo(self, x: int) -> int: return x
        def boom(self): raise ValueError("nope")
    with tinyray.join("svc", "stateful", slot=0, size=1, serves=S()) as me:
        me.ready(v=1)
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


@pytest.fixture
def svc(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    me = tinyray.join("driver", "churn")
    me.ready()
    tinyray.pool("svc").wait(count=1, timeout=15)
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


# ---- the exported surface ------------------------------------------------


def test_all_exports_exist_and_are_documented():
    for name in tinyray.__all__:
        obj = getattr(tinyray, name, None)
        assert obj is not None, f"{name} is exported but missing"
        assert obj.__doc__, f"{name} has no docstring"


def test_nothing_public_is_missing_from_all():
    public = {
        n
        for n in dir(tinyray)
        if not n.startswith("_") and not inspect.ismodule(getattr(tinyray, n))
    }
    # POLICIES is a constant, not part of the surface users compose with.
    # POLICIES is a constant rather than something users compose with, and
    # `Any`/`annotations` are typing imports Python leaves in the namespace.
    undeclared = public - set(tinyray.__all__) - {"POLICIES", "annotations", "Any"}
    assert not undeclared, f"public but not in __all__: {sorted(undeclared)}"


# ---- module level --------------------------------------------------------


def test_join_returns_a_member(registry):
    me = tinyray.join("env", "churn")
    assert isinstance(me, tinyray.Member)
    assert me.pool == "env" and me.slot is None
    me.leave()


def test_join_rejects_an_unknown_policy(registry):
    with pytest.raises(tinyray.PolicyError, match="policy must be one of"):
        tinyray.join("env", "sideways")


def test_pool_and_apool_return_the_right_views(svc):
    assert isinstance(tinyray.pool("svc"), tinyray.Pool)
    assert isinstance(tinyray.apool("svc"), tinyray.AsyncPool)
    assert isinstance(tinyray.pool("svc").slot(0), tinyray.Handle)
    assert isinstance(tinyray.apool("svc").slot(0), tinyray.AsyncHandle)


# ---- Member --------------------------------------------------------------


def test_member_ready_merges_state(registry):
    me = tinyray.join("env", "churn")
    me.ready(a=1)
    me.ready(b=2)
    assert me.state == {"a": 1, "b": 2}
    me.leave()


def test_member_unready_hides_it_from_pick(registry):
    me = tinyray.join("env", "serving")
    me.ready()
    assert len(tinyray.pool("env").wait(count=1, timeout=10)) == 1
    me.unready()
    deadline = time.monotonic() + 5
    while tinyray.pool("env").all() and time.monotonic() < deadline:
        time.sleep(0.02)
    with pytest.raises(tinyray.NotFound):
        tinyray.pool("env").pick()
    me.leave()


def test_member_leave_is_idempotent(registry):
    me = tinyray.join("env", "churn")
    me.ready()
    me.leave()
    me.leave()


def test_member_works_as_a_context_manager(registry):
    with tinyray.join("env", "churn") as me:
        me.ready()
        assert tinyray.pool("env").wait(count=1, timeout=10)
    assert tinyray._client is None


def test_member_accepted_and_silence_and_stats(svc):
    assert svc.accepted is True
    assert svc.silence_ms >= 0
    stats = svc.stats()
    # 钉的是精确集合，不是"至少包含"。多出一个键没人会发现，而它一旦出现就是
    # 公开接口的一部分 —— 加键要在这里过一遍，是故意的摩擦。
    assert set(stats) == {
        "beats_ok",
        "beats_failed",
        "interval_ms",
        "silence_ms",
        "watch_wakeups",
        "state_bytes",
        "pool_revision",
        "watched_pools",
        # 这个 fixture 交出来的是 driver，它没有 serves=，所以没有服务端那一半。
        # 服务端计数器由 tests/test_stats.py 单独盯。
    }
    assert stats["beats_ok"] >= 1


# ---- Pool ----------------------------------------------------------------


def test_pool_pick_slot_all_and_len(svc):
    pool = tinyray.pool("svc")
    assert pool.pick().slot == 0
    assert pool.slot(0).slot == 0
    assert len(pool.all()) == 1
    assert len(pool) == 1
    assert "svc" in repr(pool)


def test_pool_filters_by_published_state(svc):
    assert len(tinyray.pool("svc").all(v=1)) == 1
    assert tinyray.pool("svc").all(v=2) == []


def test_pool_wait_times_out_with_a_useful_message(svc):
    with pytest.raises(TimeoutError, match="nobody-here"):
        tinyray.pool("nobody-here").wait(count=1, timeout=0.5)


def test_pool_epoch_freezes_and_validates(svc):
    ep = tinyray.pool("svc").epoch(timeout=10)
    assert isinstance(ep, tinyray.Epoch)
    assert len(ep) == 1 and ep.valid
    assert [h.slot for h in ep] == [0]
    assert ep.slot(0).slot == 0
    assert "valid" in repr(ep)
    with pytest.raises(tinyray.NotFound):
        ep.slot(9)


# ---- Handle --------------------------------------------------------------


def test_handle_fields_and_calling(svc):
    h = tinyray.pool("svc").slot(0)
    assert h.pool == "svc" and h.slot == 0
    assert h.incarnation > 0 and h.ready is True
    assert h.url.startswith("http://") and h.state == {"v": 1}
    assert h.identity == f"svc/0#{h.incarnation}"
    assert h.label.startswith("svc/0#") and len(h.label) < 20
    assert h.echo(7) == 7
    assert h.echo.timeout(5)(8) == 8
    assert h == tinyray.pool("svc").slot(0)
    assert hash(h) == hash(tinyray.pool("svc").slot(0))


@pytest.mark.parametrize("kind", ["sync", "async"])
def test_both_handle_flavours_call(svc, kind):
    if kind == "sync":
        assert tinyray.pool("svc").slot(0).echo(3) == 3
    else:
        assert asyncio.run(tinyray.apool("svc").slot(0).echo(3)) == 3


# ---- exceptions ----------------------------------------------------------


def test_every_exception_is_reachable(svc):
    h = tinyray.pool("svc").slot(0)
    with pytest.raises(tinyray.RemoteError):
        h.boom()
    with pytest.raises(tinyray.NotFound):
        tinyray.pool("svc").slot(9)
    with pytest.raises(tinyray.Unreachable):
        tinyray.Handle(
            "svc",
            {
                "id": 0,
                "slot": 0,
                "incarnation": h.incarnation,
                "url": "http://127.0.0.1:1",
                "ready": True,
            },
            ("echo",),
        ).echo(1)
    with pytest.raises(tinyray.Fenced):
        tinyray.Handle(
            "svc",
            {"id": 0, "slot": 0, "incarnation": h.incarnation - 1, "url": h.url, "ready": True},
            ("echo",),
        ).echo(1)
    # SeatTaken needs a process that has not joined yet; it is covered on its
    # own in test_seats.py.
    assert issubclass(tinyray.SeatTaken, tinyray.TinyrayError)
    assert issubclass(tinyray.Unreachable, tinyray.TinyrayError)
    assert issubclass(tinyray.Stale, tinyray.TinyrayError)
