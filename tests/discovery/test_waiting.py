"""等待条件：一个底座，三种问法。

以前 `AsyncPool` 继承的是阻塞版 `wait()`。它在事件循环上不是"不够优雅"，是
**停掉整个 loop** —— 实测一秒的 `apool.wait()` 只放过 5 次 10ms 的 tick，本该有
一百次。让调用方用 `asyncio.to_thread` 包一层，是库把自己的活推给了调用方，而且
那期间一个 executor 线程被占着。

`until()` 是另外两条的底座。每个手写的等待循环都要做对同样四件事：先看已经成立
没有、把 revision 无缝交接过去、被 close 时停下、`Fenced` 要放出去而不是当成
"条件还没满足"。第二件做错最有意思 —— 池子在"先看一眼"和"订阅"之间动了，等待
就会为一个立刻成立的条件白等满整个超时。
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap
import threading
import time

import pytest
import tinyray

PEER = textwrap.dedent(
    """
    import os, sys, tinyray
    os.environ["TINYRAY_REGISTRY"] = "{endpoint}"
    m = tinyray.join("{pool}", "churn")
    m.ready(who="{who}")
    print(m.identity, flush=True)
    sys.stdin.readline()
    m.leave()
    print("GONE", flush=True)
    """
)


def _spawn(registry, pool: str, who: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", PEER.format(endpoint=registry.endpoint, pool=pool, who=who)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )


def _stop(p: subprocess.Popen) -> None:
    try:
        p.stdin.write("\n")
        p.stdin.close()
        p.stdout.readline()
        p.wait(timeout=10)
    except Exception:
        p.kill()


def test_until_returns_at_once_when_it_is_already_true(registry):
    """已经成立就不该等。等一拍才发现"本来就成立"，是每个手写循环的第一个坑。"""
    with tinyray.join("p", "churn") as me:
        me.ready(role="a")
        me.flush()
        pool = tinyray.pool("p")
        t0 = time.monotonic()
        snap = pool.until(lambda s: len(s.ready()) >= 1, timeout=10)
        took = (time.monotonic() - t0) * 1000
        assert len(snap.ready()) >= 1
        # 一拍是 ttl/4 = 500ms。要求远快于此。
        assert took < 100, f"条件本来就成立，却等了 {took:.0f}ms"


def test_until_does_not_miss_a_change_that_lands_while_it_looks(registry):
    """ "先看一眼"和"开始订阅"之间池子动了，那次变化不能丢。

    交接的是快照当时的 revision，不是订阅那一刻的。差别只在几微秒，但丢掉的
    是"条件刚好在这一瞬成立"的那一次 —— 于是等待白等满整个超时。
    """
    with tinyray.join("p", "churn") as me:
        me.ready(role="a")
        pool = tinyray.pool("p")
        peer = _spawn(registry, "p", "b")
        try:
            peer.stdout.readline()
            t0 = time.monotonic()
            snap = pool.until(lambda s: len(s.ready()) >= 2, timeout=15)
            assert len(snap.ready()) >= 2
            assert time.monotonic() - t0 < 15
        finally:
            _stop(peer)


def test_until_says_what_it_was_waiting_for(registry):
    with tinyray.join("p", "churn") as me:
        me.ready()
        pool = tinyray.pool("p")
        with pytest.raises(TimeoutError) as caught:
            pool.until(lambda s: len(s) >= 9, timeout=0.4, describe="nine members")
        assert "nine members" in str(caught.value), str(caught.value)
        assert "'p'" in str(caught.value)


def test_until_lets_fenced_through(registry):
    """丢座位不是"条件还没满足"，不能被当成超时吞掉。"""
    me = tinyray.join("seats", "collective", slot=0, size=2)
    me.ready()
    pool = tinyray.pool("seats")
    pool.snapshot()
    thief = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                f"""
                import os, sys, tinyray
                os.environ["TINYRAY_REGISTRY"] = "{registry.endpoint}"
                m = tinyray.join("seats", "collective", slot=0, size=2)
                m.ready()
                print("READY", flush=True)
                sys.stdin.readline()
                """
            ),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert thief.stdout.readline().strip() == "READY"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and me.accepted:
            time.sleep(0.05)
        assert not me.accepted
        with pytest.raises(tinyray.Fenced):
            pool.until(lambda s: len(s) >= 99, timeout=10)
    finally:
        thief.stdin.write("\n")
        thief.stdin.close()
        thief.wait(timeout=10)
        try:
            me.leave()
        except Exception:
            pass


def test_wait_departure_answers_when_nobody_takes_over(registry):
    """旧 owner 只是走了、没人接任 —— `wait_replacement` 回答不了这个问题。

    实测：它会等满整个超时然后返回 None，因为它等的是接任者。而要接手工作的
    那一方通常只需要知道前任已经不在了。
    """
    with tinyray.join("bq", "churn") as me:
        me.ready(role="waiter")
        pool = tinyray.pool("bq")
        owner = _spawn(registry, "bq", "old")
        who = owner.stdout.readline().strip()
        pool.wait(count=2, timeout=15)

        # 先证明这个问题用 wait_replacement 问不出来。
        t0 = time.monotonic()
        assert pool.wait_replacement(identity=who, timeout=1.0) is None
        assert time.monotonic() - t0 >= 0.9, "它应该是等满了超时"

        _stop(owner)
        t0 = time.monotonic()
        assert pool.wait_departure(who, timeout=15) is True
        took = time.monotonic() - t0
        # 对方主动 leave()，注册中心立刻知道，不必等租约到期。
        assert took < registry.ttl_ms / 1000, f"等了 {took:.1f}s，像是在等租约过期"


def test_wait_departure_is_about_the_tenure_not_the_seat(registry):
    """座位换人了，原来那一任同样算"走了"。"""
    with tinyray.join("seats", "collective", slot=1, size=2) as me:
        me.ready()
        pool = tinyray.pool("seats")
        first = subprocess.Popen(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    f"""
                    import os, sys, tinyray
                    os.environ["TINYRAY_REGISTRY"] = "{registry.endpoint}"
                    m = tinyray.join("seats", "collective", slot=0, size=2)
                    m.ready()
                    print(m.identity, flush=True)
                    sys.stdin.readline()
                    """
                ),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        who = first.stdout.readline().strip()
        pool.wait(count=2, timeout=15)
        first.stdin.write("\n")
        first.stdin.close()
        first.wait(timeout=10)
        second = subprocess.Popen(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    f"""
                    import os, sys, tinyray
                    os.environ["TINYRAY_REGISTRY"] = "{registry.endpoint}"
                    m = tinyray.join("seats", "collective", slot=0, size=2)
                    m.ready()
                    print(m.identity, flush=True)
                    sys.stdin.readline()
                    """
                ),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            other = second.stdout.readline().strip()
            assert other != who
            assert pool.wait_departure(who, timeout=15) is True
        finally:
            second.stdin.write("\n")
            second.stdin.close()
            second.wait(timeout=10)


def test_wait_departure_says_no_rather_than_hanging(registry):
    with tinyray.join("bq", "churn") as me:
        me.ready()
        pool = tinyray.pool("bq")
        t0 = time.monotonic()
        assert pool.wait_departure(me.identity, timeout=0.4) is False
        assert time.monotonic() - t0 < 5


def test_await_ready_leaves_the_event_loop_turning(registry):
    """这条才是重点：`AsyncPool` 继承的阻塞 `wait()` 会停掉整个 loop。"""

    async def body() -> tuple[int, int]:
        me = tinyray.join("p", "churn")
        me.ready()
        ap = tinyray.apool("p")
        ticks = 0

        async def tick() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1

        t = asyncio.create_task(tick())
        await asyncio.sleep(0.2)

        before = ticks
        with pytest.raises(TimeoutError):
            await ap.await_ready(count=9, timeout=1.0)
        turning = ticks - before

        before = ticks
        try:
            ap.wait(count=9, timeout=1.0)  # 继承来的阻塞版，作为对照
        except TimeoutError:
            pass
        blocked = ticks - before

        t.cancel()
        me.leave()
        return turning, blocked

    turning, blocked = asyncio.run(body())
    # 一秒里本该有约 100 次 tick。
    assert turning > 60, f"await_ready 期间 loop 只跑了 {turning} 次"
    assert blocked < turning / 2, (
        f"阻塞版跑了 {blocked} 次、异步版 {turning} 次 —— 差距不够大，这条测试没测到东西"
    )


def test_await_ready_returns_the_matching_handles(registry):
    async def body() -> list:
        me = tinyray.join("p", "churn")
        me.ready(role="trainer", shard=3)
        got = await tinyray.apool("p").await_ready(count=1, timeout=15, role="trainer")
        me.leave()
        return got

    got = asyncio.run(body())
    assert len(got) == 1
    assert got[0].state["role"] == "trainer"


def test_await_departure_matches_the_sync_one(registry):
    async def body() -> bool:
        me = tinyray.join("bq", "churn")
        me.ready()
        ap = tinyray.apool("bq")
        out = await ap.await_departure(me.identity, timeout=0.4)
        me.leave()
        return out

    assert asyncio.run(body()) is False


def test_await_ready_holds_no_executor_thread(long_lease):
    """`asyncio.to_thread` 包一层并不会卡住 loop —— 那正是它的用途 —— 所以上一条
    测试抓不到它。它的代价在别处：取消一个 `to_thread` 并不会停掉底下那个线程，
    于是等待期间一直占着默认 executor 的一个 worker。

    这是 0.8.0 给 `achanges()` 修过、0.9.1 给 `await_fenced()` 修过的同一件事。
    第三次了，所以这次连测试一起补上。
    """

    async def body() -> float:
        me = tinyray.join("p", "churn")
        me.ready()
        ap = tinyray.apool("p")
        # 每一个都不会满足，于是全都停在等待里。
        waiters = [asyncio.create_task(ap.await_ready(count=99, timeout=5)) for _ in range(40)]
        await asyncio.sleep(0.5)
        for t in waiters:
            t.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        t0 = time.monotonic()
        await asyncio.to_thread(lambda: None)
        cost = (time.monotonic() - t0) * 1000
        me.leave()
        return cost

    cost = asyncio.run(body())
    assert cost < 500, f"取消 40 个 await_ready 之后，一次 to_thread 等了 {cost:.0f}ms"


def test_until_hands_the_revision_over_without_leaving_a_gap(registry):
    """这个文件开头说"第二件做错最有意思"，但一直没人测它。

    缺口不是理论上的一瞬：`predicate` 是调用方写的，跑多久由调用方说了算，
    所以"先看一眼"和"订阅"之间的距离就是 predicate 的耗时。这里让第一次
    predicate 跑 1.5s，池子在这期间动。

    实测：交回 revision 的版本 1500ms 就返回（predicate 自己睡完立刻拿到）；
    把 `since=snap.revision` 换成 `since=None`，同一个场景 7501ms 抛 TimeoutError
    —— 条件其实已经成立了 6 秒。
    """
    with tinyray.join("p", "churn") as me:
        me.ready(step=0)
        me.flush()
        pool = tinyray.pool("p")
        pool.until(lambda s: bool(s.ready()) and s.ready()[0].state.get("step") == 0, timeout=5)

        predicate_running = threading.Event()

        def mover():
            predicate_running.wait(5)
            me.update(step=1)
            me.flush()

        t = threading.Thread(target=mover, daemon=True)
        t.start()

        first = [True]

        def stepped(snap):
            if first[0]:
                first[0] = False
                predicate_running.set()
                time.sleep(1.5)  # 调用方的 predicate 本身就是那个缺口
            return any(h.state.get("step") == 1 for h in snap)

        t0 = time.monotonic()
        snap = pool.until(stepped, timeout=6.0, describe="step==1")
        elapsed = time.monotonic() - t0
        t.join(timeout=5)

        assert any(h.state.get("step") == 1 for h in snap)
        # 白等满超时是这个 bug 的样子，1.5s 的 predicate 之后应该立刻拿到。
        assert elapsed < 4.0, f"条件在 predicate 跑的时候就成立了，却等了 {elapsed:.1f}s"


def test_auntil_hands_the_revision_over_as_well(registry):
    """异步那条是同一行代码抄的第二遍，所以也是同一个缺口。

    发现方式是变异门的锚点检查报了"匹配 2 次"：一条 mutant 打算钉住的地方，
    实际上有两处。第二处一样没人看着。
    """

    async def body() -> float:
        me = tinyray.join("ap", "churn")
        try:
            me.ready(step=0)
            me.flush()
            apool = tinyray.apool("ap")
            await apool.auntil(
                lambda s: bool(s.ready()) and s.ready()[0].state.get("step") == 0, timeout=5
            )

            first = [True]

            def stepped(snap):
                if first[0]:
                    first[0] = False
                    # 缺口在这里：predicate 是同步的，跑多久由调用方说了算
                    me.update(step=1)
                    me.flush()
                return any(h.state.get("step") == 1 for h in snap)

            t0 = time.monotonic()
            snap = await apool.auntil(stepped, timeout=6.0, describe="step==1")
            assert any(h.state.get("step") == 1 for h in snap)
            return time.monotonic() - t0
        finally:
            me.leave()

    elapsed = asyncio.run(body())
    assert elapsed < 4.0, f"条件在 predicate 跑的时候就成立了，却等了 {elapsed:.1f}s"


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_the_budget_is_a_deadline_not_an_allowance_on_top(registry, flavour):
    """`until()` 算了 `deadline` 却把原始的 `timeout` 转手交给 `changes()`，
    于是那个变量只是错误消息里的一个装饰 —— 真正生效的是 watch 自己重算的
    一份全新预算。settle 池子和跑 predicate 都发生在 `timeout` 里面，却不计入。

    实测 predicate 睡 1 秒：

        until(timeout=0.3)  修前 1300ms   修后 1000ms

    修后那 1000ms 全是调用方自己的代码，库一分钟也没有另加。
    首次遇到的池子同样：342ms -> 301ms（那 42ms 是 settle 的一个往返）。
    """
    with tinyray.join("b", "churn") as me:
        me.ready()
        me.flush()

        def slow(_snap) -> bool:
            time.sleep(1.0)
            return False

        pool = tinyray.pool("b")
        t0 = time.monotonic()
        if flavour == "sync":
            with pytest.raises(TimeoutError):
                pool.until(slow, timeout=0.3)
        else:

            async def body():
                with pytest.raises(TimeoutError):
                    await tinyray.apool("b").auntil(slow, timeout=0.3)

            asyncio.run(body())
        elapsed = time.monotonic() - t0

        assert elapsed >= 1.0, "predicate 本来就要跑 1 秒"
        assert elapsed < 1.25, (
            f"预算 0.3s 全花在 predicate 上了，库不该再加一份：实际 {elapsed:.2f}s"
        )


@pytest.mark.parametrize("flavour", ["sync", "async"])
@pytest.mark.parametrize("fields", [None, ["step"]])
def test_watch_deadline_wins_over_available_changes(registry, flavour, fields):
    with tinyray.join("deadline") as me:
        me.ready(step=0).flush()
        pool = tinyray.apool(me.pool)
        watch = (
            pool.changes(timeout=0.02, fields=fields)
            if flavour == "sync"
            else pool.achanges(timeout=0.02, fields=fields)
        )
        try:
            time.sleep(0.04)
            me.update(step=1).flush()
            if flavour == "sync":
                with pytest.raises(StopIteration):
                    next(watch)
            else:
                with pytest.raises(StopAsyncIteration):
                    asyncio.run(anext(watch))
        finally:
            watch.close()


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_until_does_not_reenter_a_predicate_after_expiry(registry, flavour):
    with tinyray.join("busy-deadline") as me:
        me.ready(step=0).flush()
        calls = 0

        def predicate(_snapshot):
            nonlocal calls
            calls += 1
            assert calls == 1, "A ready change bypassed the exhausted deadline"
            me.update(step=1).flush()
            time.sleep(0.04)
            return False

        with pytest.raises(TimeoutError):
            if flavour == "sync":
                tinyray.pool(me.pool).until(predicate, timeout=0.02)
            else:
                asyncio.run(tinyray.apool(me.pool).auntil(predicate, timeout=0.02))
        assert calls == 1
