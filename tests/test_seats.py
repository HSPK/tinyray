"""Seat semantics: last-writer-wins by default, first-wins on request."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time

import pytest
import tinyray

HOLDER = textwrap.dedent(
    """
    import sys, tinyray
    kw = {"exclusive": True} if sys.argv[1] == "exclusive" else {}
    me = tinyray.join("seat", "stateful", slot=0, **kw)
    me.ready(who=sys.argv[2])
    print("HELD", flush=True)
    sys.stdin.readline()
    """
)


def _hold(mode: str, who: str) -> subprocess.Popen:
    p = subprocess.Popen(
        [sys.executable, "-c", HOLDER, mode, who],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert p.stdout.readline().strip() == "HELD"
    return p


def _release(p: subprocess.Popen) -> None:
    try:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=5)
    except Exception:
        p.kill()


def test_a_restarting_member_reclaims_its_seat(registry):
    """The default has to be last-writer-wins: a rank that comes back must get
    its seat even though the dead one's lease is still running."""
    first = _hold("default", "first")
    me = tinyray.join("watch", "churn")
    me.ready()
    held = tinyray.pool("seat").wait(count=1, timeout=10)[0]
    assert held.state["who"] == "first"

    first.kill()  # lease still ticking
    second = _hold("default", "second")
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            seat = tinyray.pool("seat").all()
            if seat and seat[0].state.get("who") == "second":
                break
            time.sleep(0.05)
        assert tinyray.pool("seat").slot(0).state["who"] == "second"
    finally:
        _release(second)
        me.leave()


def test_exclusive_refuses_an_occupied_seat(registry):
    first = _hold("exclusive", "first")
    try:
        with pytest.raises(tinyray.SeatTaken, match="already held"):
            tinyray.join("seat", "stateful", slot=0, exclusive=True)
    finally:
        _release(first)


def test_exclusive_succeeds_once_the_lease_lapses(registry):
    first = _hold("exclusive", "first")
    first.kill()  # no farewell, so only expiry frees the seat

    deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 3
    took = None
    while time.monotonic() < deadline:
        try:
            took = tinyray.join("seat", "stateful", slot=0, exclusive=True)
            break
        except tinyray.SeatTaken:
            time.sleep(0.1)
    assert took is not None, "the seat never freed up"
    took.ready(who="second")
    took.leave()


def test_the_short_label_stays_readable(registry):
    me = tinyray.join("engine", "serving", slot=3)
    me.ready()
    h = tinyray.pool("engine").wait(count=1, timeout=10)[0]
    assert h.label.startswith("engine/3#")
    assert len(h.label) < 24, f"{h.label} is not a label a human reads"
    # identity stays exact: it is the fencing token, not a display string.
    assert h.identity == f"engine/3#{h.incarnation}"
    me.leave()


SURVIVOR = textwrap.dedent(
    """
    import sys, time, tinyray
    class W:
        def whoami(self) -> str:
            return sys.argv[1]
    me = tinyray.join("ghost", "stateful", slot=0, serves=W())
    me.ready(gen=sys.argv[1])
    print(f"HELD {tinyray.pool('ghost').slot(0).url}", flush=True)
    # Deliberately keeps running and listening after being replaced.
    sys.stdin.readline()
    """
)


def test_a_superseded_process_stops_answering(long_lease):
    """The registry refuses a ghost's heartbeat, but nothing stops the ghost
    itself: it keeps running with its port open, and a caller holding the old
    handle would get a cheerful reply from the wrong process. The identity
    header cannot catch this -- the ghost's identity is exactly what the stale
    handle asks for -- so the process has to notice it lost the seat.
    """
    first = subprocess.Popen(
        [sys.executable, "-c", SURVIVOR, "gen-1"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert first.stdout.readline().startswith("HELD")

    me = tinyray.join("watch", "churn")
    me.ready()
    stale = tinyray.pool("ghost").wait(count=1, timeout=10)[0]
    assert stale.whoami() == "gen-1"

    second = subprocess.Popen(
        [sys.executable, "-c", SURVIVOR, "gen-2"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert second.stdout.readline().startswith("HELD")

        # Hammer *now*, before anything else. The old process is alive and
        # still listening, and only the registry's answer to its own heartbeat
        # tells it otherwise -- but that heartbeat is parked, and taking the
        # seat drains the pool's waiters before the taker's reply is even
        # built, so it learns without waiting for its next beat.
        #
        # A long lease is what makes the two cases far apart enough to tell
        # apart. At the default 2s lease the interval is 500ms and a ghost that
        # is *not* woken still finds out within a fraction of a second, so the
        # only thing separating them is how many calls slip through -- and that
        # is a rate, which moves with the machine. Calibrated on an idle box it
        # said 0; run beside a benchmark it said 59, and the assertion was
        # wrong rather than the code. Here the interval is 5s, so a ghost that
        # has to wait for its own beat waits seconds, not milliseconds.
        elapsed = None
        answered = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < 20.0:
            try:
                stale.whoami()
                answered += 1
            except tinyray.Fenced:
                elapsed = time.monotonic() - t0
                break
        assert elapsed is not None, (
            f"the ghost kept serving its old identity for {answered} call(s)"
        )
        assert elapsed < 1.0, (
            f"the ghost took {elapsed * 1000:.0f}ms and {answered} call(s) to notice, "
            f"which is its own beat telling it rather than the seat being taken"
        )

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            live = tinyray.pool("ghost").all()
            if live and live[0].incarnation != stale.incarnation:
                break
            time.sleep(0.05)
        assert tinyray.pool("ghost").slot(0).whoami() == "gen-2"
    finally:
        _release(second)
        _release(first)
        me.leave()


def test_flush_says_the_seat_was_taken_rather_than_blaming_the_registry(registry):
    """被抢座的成员会停止心跳，所以 flush() 等的那两拍永远不会来。

    没有这个守卫，它就一直等到超时，然后报 TimeoutError 说"注册表不收这份
    state"—— 而注册表好得很，`last_error` 是空的，等于什么都没说。实测同一场景：

        有守卫  SeatTaken   92ms   "seat 0 was taken while publishing"
        无守卫  TimeoutError 10000ms "waited 10.0s for the registry to take this
                                     state; last error was ''"

    快 100 倍只是顺带的，真正的差别是把锅扣在对的地方。
    """
    me = tinyray.join("seat", "stateful", slot=0)
    later = None
    try:
        me.ready(who="mine")
        me.flush()

        later = _hold("default", "later")  # 后来者拿走这个座位

        me.update(who="mine-again")
        t0 = time.monotonic()
        with pytest.raises(tinyray.SeatTaken, match="taken while publishing"):
            me.flush(timeout=10.0)
        elapsed = time.monotonic() - t0
        assert elapsed < 5.0, f"应该在下一拍就知道，却等了 {elapsed:.1f}s"
    finally:
        if later is not None:
            _release(later)
        try:
            me.leave()
        except Exception:
            pass


def test_a_superseded_member_stops_beating_instead_of_hammering_the_registry(registry):
    """被顶替之后再拍下去，只是在等接任者死掉好把座位抢回来。

    这条一直没人钉，而少了它不是"慢一点"：实测 ttl 2000（间隔 500ms），
    被顶替后 6 秒内

        有 `if !alive { return }`   beats_ok +0
        没有                        beats_ok +117

    每秒约 19.5 个请求，而且已经不受心跳间隔约束了 —— 注册表对一个不被接受的
    成员不会 park 它，于是循环退化成一个不会停的压测器。
    """
    me = tinyray.join("seat", "stateful", slot=0)
    later = None
    try:
        me.ready(who="mine")
        me.flush()
        c = tinyray._client
        assert c is not None

        later = _hold("default", "later")
        deadline = time.monotonic() + 15
        while c.accepted and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not c.accepted, "座位没有被接管，这条测试没测到东西"

        before = c.stats()
        time.sleep(2.0)  # 至少四个心跳间隔
        after = c.stats()

        sent = (after["beats_ok"] - before["beats_ok"]) + (
            after["beats_failed"] - before["beats_failed"]
        )
        assert sent == 0, f"被顶替后两秒里还发了 {sent} 次心跳"
    finally:
        if later is not None:
            _release(later)
        try:
            me.leave()
        except Exception:
            pass


def _fenced_by_a_replacement():
    """把自己的座位让给后来者，返回 (me, holder)。"""
    me = tinyray.join("seat", "stateful", slot=0)
    me.ready(who="mine")
    me.flush()
    holder = _hold("default", "later")
    deadline = time.monotonic() + 15
    while tinyray._client.accepted and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not tinyray._client.accepted, "座位没有被接管，这条测试没测到东西"
    return me, holder


def test_every_wait_says_it_was_fenced_rather_than_blaming_the_pool(registry):
    """`until()` 的文档把"让 Fenced 透出去、而不是当成条件还没满足"列为每个
    等待必须做对的四件事之一。`wait()` 从前自己手写循环，是全库唯一做不到的
    那个。实测（被围栏的进程要五个成员，预算 4 秒）：

        wait()        TimeoutError 4000ms，"saw 0"
        await_ready() Fenced 1ms
        until()       Fenced 0ms

    而且那句 "saw 0" 是双重误导：池子里有接任者，只是这个进程的缓存被冻住了。
    池子空和"你看不见了"是两件事。
    """
    me, holder = _fenced_by_a_replacement()
    try:
        pool = tinyray.pool("seat")
        t0 = time.monotonic()
        with pytest.raises(tinyray.Fenced, match="lost its seat"):
            pool.wait(count=5, timeout=4.0)
        assert time.monotonic() - t0 < 2.0, "被围栏了还等满超时"

        with pytest.raises(tinyray.Fenced):
            pool.until(lambda s: len(s) >= 5, timeout=4.0)

        import asyncio

        with pytest.raises(tinyray.Fenced):
            asyncio.run(tinyray.apool("seat").await_ready(count=5, timeout=4.0))
    finally:
        _release(holder)
        try:
            me.leave()
        except Exception:
            pass
