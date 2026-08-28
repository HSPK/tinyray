"""watcher 必须能被停下来。

以前不能。`changes()` 返回一个生成器，阻塞在 Rust 的等待里而不是停在 yield 上，
所以别的线程既 close 不了它（`generator already executing`），也没有任何标志位
能让它看到。实测：一个非 daemon 线程在 `changes()` 里跑，进程再也退不出来，
`leave()` 也解不开。

异步侧是另一半：`achanges()` 走 `asyncio.to_thread`，取消 awaitable 并不会停掉
底下那个线程。实测 24 核机器上取消 40 个 watcher 之后，紧接着一次
`asyncio.to_thread` 等了 3092ms —— 默认 executor 的 28 个 worker 全卡在里面。
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time

import pytest
import tinyray


def test_close_releases_a_blocked_watcher(long_lease):
    """没有变化时 watcher 是阻塞的，close() 必须把它拽回来。

    必须用长租约。短租约下心跳本来就密，"被 close 拽回来"和"碰巧被下一拍
    叫醒"只差一百多毫秒，分不开 —— 第一版就是这样，把 wake() 去掉照样通过。
    ttl=20s 时前者是 0ms，后者要等一次挂起，差三个数量级。"""
    with tinyray.join("p", slot=0, size=1) as m:
        m.ready()
        w = tinyray.pool("p").changes()
        done = threading.Event()

        def drain() -> None:
            for _ in w:
                pass
            done.set()

        t = threading.Thread(target=drain, daemon=True)
        t.start()
        time.sleep(0.5)  # 确实已经停在等待里
        assert not done.is_set()

        t0 = time.monotonic()
        w.close()
        assert done.wait(5), "close() 没能结束一个阻塞中的 watcher"
        took = (time.monotonic() - t0) * 1000
        # 一拍是 ttl/4 = 5s；要求远快于此，否则只是碰巧被心跳唤醒。
        assert took < 500, f"close() 用了 {took:.0f}ms，像是在等下一拍"


def test_leave_ends_live_watchers(registry):
    """离开之后 watcher 再等下去没有意义，也拦着进程退出。"""
    m = tinyray.join("p", slot=0, size=1)
    m.ready()
    w = tinyray.pool("p").changes()
    done = threading.Event()

    def drain() -> None:
        for _ in w:
            pass
        done.set()

    threading.Thread(target=drain, daemon=True).start()
    time.sleep(0.5)
    assert not done.is_set()
    m.leave()
    assert done.wait(5), "leave() 之后 watcher 还在等"


NON_DAEMON = textwrap.dedent(
    """
    import os, threading, tinyray
    os.environ["TINYRAY_REGISTRY"] = "{endpoint}"
    m = tinyray.join("p", slot=0, size=1)
    m.ready()
    w = tinyray.pool("p").changes()
    threading.Thread(target=lambda: [s for s in w], daemon=False).start()
    m.leave()
    """
)


def test_a_non_daemon_watcher_does_not_pin_the_process(registry):
    """这条以前会永远挂住 —— 测过 25s 没有任何进展。"""
    code = NON_DAEMON.format(endpoint=registry.endpoint)
    t0 = time.monotonic()
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    assert time.monotonic() - t0 < 15, "进程被一个停不下来的 watcher 拖住了"


def test_context_manager_closes_on_the_way_out(registry):
    with tinyray.join("p", slot=0, size=1) as m:
        m.ready()
        with tinyray.pool("p").changes() as w:
            pass
        assert w._closed


def test_async_watchers_hold_no_executor_thread(long_lease):
    """取消一批 achanges 之后，别的东西还能立刻拿到 executor 线程。

    这条量的是取消的代价，不是取消本身：以前 to_thread 起的线程还阻塞在 Rust
    的等待里，谁也拿不到它们。

    必须用长租约。铃每拍响一次，被卡住的线程也就最多卡一拍 —— ttl=2s 时那是
    500ms，噪声都盖过去了。ttl=20s 下同样的错误要卡 3 秒以上。
    """

    async def body() -> float:
        m = tinyray.join("p", slot=0, size=1)
        m.ready()
        p = tinyray.apool("p")

        async def watch() -> None:
            async for _ in p.achanges():
                pass

        tasks = [asyncio.create_task(watch()) for _ in range(40)]
        await asyncio.sleep(1.0)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        t0 = time.monotonic()
        await asyncio.to_thread(lambda: None)
        cost = (time.monotonic() - t0) * 1000
        m.leave()
        return cost

    cost = asyncio.run(body())
    assert cost < 500, f"取消 40 个 watcher 之后，一次 to_thread 等了 {cost:.0f}ms"


def test_async_close_ends_the_stream(registry):
    async def body() -> bool:
        m = tinyray.join("p", slot=0, size=1)
        m.ready()
        w = tinyray.apool("p").achanges()
        seen = asyncio.Event()

        async def drain() -> None:
            async for _ in w:
                pass
            seen.set()

        task = asyncio.create_task(drain())
        await asyncio.sleep(0.5)
        assert not seen.is_set()
        w.close()
        try:
            await asyncio.wait_for(seen.wait(), 5)
        finally:
            task.cancel()
            m.leave()
        return True

    assert asyncio.run(body())


@pytest.mark.parametrize("kind", ["slot", "identity"])
def test_wait_replacement_names_the_new_tenure(registry, kind):
    """座位换人和座位空着是两回事，只有 incarnation 分得清。"""
    peer = textwrap.dedent(
        f"""
        import os, sys, tinyray
        os.environ["TINYRAY_REGISTRY"] = "{registry.endpoint}"
        m = tinyray.join("p", slot=1, size=2)
        m.ready(who=sys.argv[1])
        print(m.identity, flush=True)
        sys.stdin.readline()
        """
    )
    with tinyray.join("p", slot=0, size=2) as me:
        me.ready()
        pool = tinyray.pool("p")
        first = subprocess.Popen(
            [sys.executable, "-c", peer, "first"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        was = first.stdout.readline().strip()
        pool.wait(count=2, timeout=10)

        target = {"slot": {"slot": 1}, "identity": {"identity": was}}[kind]
        got: list = []
        done = threading.Event()

        def watch() -> None:
            got.append(pool.wait_replacement(timeout=20, **target))
            done.set()

        threading.Thread(target=watch, daemon=True).start()
        time.sleep(0.3)
        assert not done.is_set(), "座位还没换人就返回了"

        # 池子动一下，但那个座位还是原来那一任。只看"池子变了"的实现会在这里
        # 交出原任期；只有比对 incarnation 才知道该继续等。
        me.update(nudge=1)
        time.sleep(0.5)
        assert not done.is_set(), "池子只是动了一下，座位还没换人"

        first.stdin.write("\n")
        first.stdin.close()
        first.wait(timeout=10)
        second = subprocess.Popen(
            [sys.executable, "-c", peer, "second"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        now = second.stdout.readline().strip()
        assert done.wait(20), "接任者已经到位，wait_replacement 没有返回"
        assert got[0] is not None
        assert got[0].identity == now
        assert got[0].identity != was, "返回的还是原来那一任"
        second.stdin.write("\n")
        second.stdin.close()
        second.wait(timeout=10)


def test_wait_replacement_wants_exactly_one_of_slot_or_identity(registry):
    """错误里要说出**是哪个函数** —— 两条路各自把自己的名字传给
    `_replacement_target`，而这段管线除了这里没人守。只认 TypeError 的话，
    异步那条报成同步那条的名字也一样绿。"""
    with tinyray.join("p", slot=0, size=1) as m:
        m.ready()
        pool = tinyray.pool("p")
        for bad in ({}, {"slot": 0, "identity": "p/0#1"}):
            with pytest.raises(TypeError, match=r"wait_replacement\(\) takes exactly one"):
                pool.wait_replacement(**bad)
            with pytest.raises(TypeError, match=r"await_replacement\(\) takes exactly one"):
                asyncio.run(tinyray.apool("p").await_replacement(**bad))


def test_await_fenced_holds_no_executor_thread_either(long_lease):
    """`await_fenced()` 也不能借 executor 线程。

    它一直没有任何测试，所以当 `achanges()` 从 `asyncio.to_thread` 搬走时，
    它被落下了 —— 同一个毛病，只是没人在看。

    等待者带 timeout 是必须的：不带的话，走线程的实现会让 40 个 worker 永远
    卡在里面，解释器退出时等它们，测试变成**挂死**而不是失败。挂死的测试比
    没有测试更糟 —— CI 只会超时，不会告诉你哪里错了。
    """

    async def body() -> tuple[float, bool]:
        me = tinyray.join("seat", "stateful", slot=0)
        me.ready()
        waiters = [asyncio.create_task(me.await_fenced(timeout=5)) for _ in range(40)]
        await asyncio.sleep(0.5)
        for t in waiters:
            t.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        t0 = time.monotonic()
        await asyncio.to_thread(lambda: None)
        cost = (time.monotonic() - t0) * 1000
        # 没被顶替时要如实返回 False，而不是一直挂着。
        timely = await me.await_fenced(timeout=0.3)
        me.leave()
        return cost, timely

    cost, timely = asyncio.run(body())
    assert timely is False, "没被顶替却报告被顶替了"
    assert cost < 500, f"取消 40 个 await_fenced 之后，一次 to_thread 等了 {cost:.0f}ms"


def test_await_fenced_still_reports_a_takeover(long_lease):
    """对偶：真被顶替时必须返回 True，而且要快。"""

    async def body() -> tuple[bool, float]:
        me = tinyray.join("seat", "stateful", slot=0)
        me.ready()
        waiting = asyncio.create_task(me.await_fenced(timeout=20))
        await asyncio.sleep(0.3)
        assert not waiting.done(), "还没人抢座就返回了"
        t0 = time.monotonic()
        taker = subprocess.Popen(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    f"""
                    import os, sys, tinyray
                    os.environ["TINYRAY_REGISTRY"] = "{long_lease.endpoint}"
                    m = tinyray.join("seat", "stateful", slot=0)
                    m.ready()
                    print("TOOK", flush=True)
                    sys.stdin.readline()
                    """
                ),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            taker.stdout.readline()
            got = await asyncio.wait_for(waiting, 20)
            took = (time.monotonic() - t0) * 1000
        finally:
            taker.stdin.write("\n")
            taker.stdin.close()
            taker.wait(timeout=10)
        return got, took

    got, took = asyncio.run(body())
    assert got is True, "座位被抢了却没报告"
    # 一拍是 ttl/4 = 5s；靠铃唤醒应该快两三个数量级。
    assert took < 1000, f"过了 {took:.0f}ms 才知道，像是在等下一拍"


def test_achanges_with_a_timeout_ends_rather_than_raises(long_lease):
    """超时是流的正常结局，不是异常。

    异步侧等的是管道，而 `asyncio.wait_for` 超时会抛 —— 一路放出去的话，
    `achanges(timeout=)` 就变成了抛 `TimeoutError`，而同步的 `changes(timeout=)`
    只是安静地结束。两边必须一样。
    """

    async def body() -> float:
        me = tinyray.join("p", slot=0, size=1)
        me.ready()
        me.flush()
        t0 = time.monotonic()
        # 不接 pytest.raises：要的是它**正常走完**，产出几次都无所谓。
        async for _ in tinyray.apool("p").achanges(timeout=0.3):
            pass
        took = time.monotonic() - t0
        me.leave()
        return took

    took = asyncio.run(body())
    assert 0.3 <= took < 3.0, f"流在 {took:.2f}s 结束，既不像超时也不像正常收尾"


SEAT_TAKER = textwrap.dedent(
    """
    import os, sys, tinyray
    os.environ["TINYRAY_REGISTRY"] = "{endpoint}"
    m = tinyray.join("seats", "collective", slot=0, size=2)
    m.ready()
    print("READY", flush=True)
    sys.stdin.readline()
    """
)


def test_the_three_ways_a_stream_ends_are_told_apart(registry):
    """超时、被 close、被顶替，是三件不相干的事。

    过去三种都是"循环安静地退出"，消费者只能事后去查 `Member.accepted` 才知道
    自己是不是丢了座位 —— 而丢座位意味着缓存从此冻结，之后每一次查询都是陈旧的
    却不声张。把它和"超时到了、一切正常"混在一起，是这套 API 里最贵的一次混同。

    这条测试的价值全在**对比**：只测被顶替会抛，不足以说明另外两种不会抛。
    """
    # 1) 超时：正常收尾
    with tinyray.join("solo", slot=0, size=1) as me:
        me.ready()
        for _ in tinyray.pool("solo").changes(timeout=0.2):
            pass  # 走到这里就说明没抛

    # 2) close()：正常收尾，哪怕从别的线程关
    with tinyray.join("solo", slot=0, size=1) as me:
        me.ready()
        w = tinyray.pool("solo").changes()
        ended: list = []

        def drain() -> None:
            try:
                for _ in w:
                    pass
                ended.append(None)
            except BaseException as exc:  # noqa: BLE001
                ended.append(exc)

        t = threading.Thread(target=drain, daemon=True)
        t.start()
        time.sleep(0.3)
        w.close()
        t.join(timeout=10)
        assert ended == [None], f"close() 不该抛: {ended}"

    # 3) 被顶替：必须抛，而且要说得出为什么
    me = tinyray.join("seats", "collective", slot=0, size=2)
    me.ready()
    pool = tinyray.pool("seats")
    pool.snapshot()
    thief = subprocess.Popen(
        [sys.executable, "-c", SEAT_TAKER.format(endpoint=registry.endpoint)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert thief.stdout.readline().strip() == "READY"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and me.accepted:
            time.sleep(0.05)
        assert not me.accepted, "没有被顶替，这条测试就没测到东西"
        with pytest.raises(tinyray.Fenced) as caught:
            for _ in pool.changes():
                pass
        assert "seats" in str(caught.value), "报错没有说是哪个池子"
    finally:
        thief.stdin.write("\n")
        thief.stdin.close()
        thief.wait(timeout=10)
        try:
            me.leave()
        except Exception:
            pass


def test_the_async_stream_ends_the_same_three_ways(registry):
    """异步侧必须和同步侧一致。两条路各写一遍，是它们分头跑偏的开始。"""

    async def timed_out() -> None:
        me = tinyray.join("solo", slot=0, size=1)
        me.ready()
        async for _ in tinyray.apool("solo").achanges(timeout=0.2):
            pass
        me.leave()

    asyncio.run(timed_out())

    async def closed() -> None:
        me = tinyray.join("solo", slot=0, size=1)
        me.ready()
        w = tinyray.apool("solo").achanges()
        w.close()
        async for _ in w:
            pass
        me.leave()

    asyncio.run(closed())

    me = tinyray.join("seats", "collective", slot=0, size=2)
    me.ready()
    tinyray.apool("seats").snapshot()
    thief = subprocess.Popen(
        [sys.executable, "-c", SEAT_TAKER.format(endpoint=registry.endpoint)],
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

        async def fenced() -> None:
            async for _ in tinyray.apool("seats").achanges():
                pass

        with pytest.raises(tinyray.Fenced):
            asyncio.run(fenced())
    finally:
        thief.stdin.write("\n")
        thief.stdin.close()
        thief.wait(timeout=10)
        try:
            me.leave()
        except Exception:
            pass


def _drain_into(w, out: list) -> threading.Thread:
    def go() -> None:
        try:
            for snap in w:
                out.append(snap)
        except BaseException:  # noqa: BLE001 - 由调用方断言
            pass

    t = threading.Thread(target=go, daemon=True)
    t.start()
    return t


def test_a_watch_on_named_fields_ignores_the_rest(registry):
    """只关心两个 key 的 watcher，不该为第三个 key 的变化付整份快照的钱。

    比较必须在 Rust 缓存里做。放在 Python 里做等于白做 —— predicate 要拿到
    `Snapshot` 才能判断，而那时候钱已经花完了。实测 5,000 成员：
    `snapshot()` 8.78ms，`field_digest(['role','ready'])` 0.40ms，差 22 倍。
    """
    with tinyray.join("p", "churn") as me:
        me.ready(role="trainer", step=0)
        me.flush()
        pool = tinyray.pool("p")

        got: list = []
        w = pool.changes(fields=["role"])
        _drain_into(w, got)
        time.sleep(0.3)

        for i in range(1, 6):
            me.update(step=i)  # 没人订阅的字段
            time.sleep(0.1)
        me.flush()
        time.sleep(0.3)
        assert got == [], f"无关字段的变化产出了 {len(got)} 个快照"

        me.update(role="rollout")  # 订阅了的字段
        me.flush()
        time.sleep(0.5)
        w.close()
        assert len(got) >= 1, "订阅的字段变了，却没有产出"
        assert any(h.state.get("role") == "rollout" for h in got[-1].members)


def test_a_watch_on_fields_notices_a_seat_changing_hands(registry):
    """换人也算数 —— 哪怕接任者发布的字段和前任一模一样。

    要真正隔离出"身份"，订阅的必须是一个**谁都不发布**的字段。否则总有别的东西
    在变，测试就会靠它蒙对：
      - 只测"新来一个成员" —— 人数变了，digest 自然变；
      - 让前一任先退出再进新的 —— 中途只剩一个人，人数又变了；
      - 订阅 role —— 新任期是先入座、后 `ready(role=...)`，中间有一瞬 role 是
        缺失的。
    三种写法我都试过，把身份从 digest 里删掉，三种照样通过。
    """
    with tinyray.join("seats", "collective", slot=1, size=2) as me:
        me.ready(role="trainer")
        pool = tinyray.pool("seats")

        def peer() -> subprocess.Popen:
            return subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    textwrap.dedent(
                        f"""
                        import os, sys, tinyray
                        os.environ["TINYRAY_REGISTRY"] = "{registry.endpoint}"
                        m = tinyray.join("seats", "collective", slot=0, size=2)
                        m.ready(role="trainer")
                        print(m.identity, flush=True)
                        sys.stdin.readline()
                        """
                    ),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )

        first = peer()
        was = first.stdout.readline().strip()
        pool.wait(count=2, timeout=15)
        me.flush()

        got: list = []
        w = pool.changes(fields=["nobody_publishes_this"])
        _drain_into(w, got)
        time.sleep(0.3)
        assert got == []

        # 故意不让前一任先退出：座位是后来者居上，所以中途池子始终是两个人，
        # role 也始终是 trainer。变的只有任期 —— 这才逼着 digest 必须把身份
        # 算进去，而不是靠"人数变了"蒙对。
        second = peer()
        try:
            now = second.stdout.readline().strip()
            assert now != was
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and not got:
                time.sleep(0.05)
            assert got, "座位换人了，watcher 却没看见 —— digest 没把身份算进去"
            assert all(len(sn.members) == 2 for sn in got), (
                f"过程中人数变过，这条测试就可能是靠人数蒙对的: {[len(sn.members) for sn in got]}"
            )
        finally:
            w.close()
            for p_ in (first, second):
                try:
                    p_.stdin.write("\n")
                    p_.stdin.close()
                    p_.wait(timeout=10)
                except Exception:
                    p_.kill()


def test_a_watch_on_fields_does_not_lose_what_happened_before_since(registry):
    """`since=` 的承诺是"接着往下看"，`fields=` 不该把这个承诺吃掉。

    `_seen` 来自过去（调用方交回来的 revision），digest 却是构造时现取的，于是
    间隙里对订阅字段的改动被自己的基线抵消，watcher 永远不会为它醒来。实测：
    不带 fields= 0ms 就拿到，带 fields=["step"] 等满 2000ms 一无所获。

    没有基线时只能选一边错：多产一个重复快照，还是漏掉一次变化。重复的调用方
    能自己去重，漏掉的没人救得回来。
    """
    with tinyray.join("p", "churn") as me:
        me.ready(step=0)
        me.flush()
        pool = tinyray.pool("p")
        pool.until(lambda s: bool(s.ready()) and s.ready()[0].state.get("step") == 0, timeout=5)
        rev = pool.snapshot().revision

        # 调用方拿着 revision 走开去干活，期间订阅的字段变了
        me.update(step=1)
        me.flush()
        time.sleep(0.3)

        with pool.changes(since=rev, fields=["step"], timeout=3.0) as w:
            got = next(w, None)

        assert got is not None, "since= 之前发生的字段变化被自己的基线吞掉了"
        assert any(h.state.get("step") == 1 for h in got.members)


def test_the_missing_baseline_costs_one_snapshot_not_a_stream(registry):
    """补偿只该发生一次。永远产出等于把 fields= 退化成没有。"""
    with tinyray.join("p", "churn") as me:
        me.ready(step=0, other=0)
        me.flush()
        pool = tinyray.pool("p")
        pool.until(lambda s: bool(s.ready()) and s.ready()[0].state.get("step") == 0, timeout=5)
        rev = pool.snapshot().revision

        me.update(other=1)  # 间隙里动的是**没订阅**的字段
        me.flush()
        time.sleep(0.3)

        with pool.changes(since=rev, fields=["step"], timeout=1.5) as w:
            assert next(w, None) is not None, "没有基线时第一次要产出"

            got: list = []
            _drain_into(w, got)
            for i in range(2, 5):
                me.update(other=i)
                time.sleep(0.1)
            me.flush()
            time.sleep(0.5)
            assert got == [], f"补偿之后又为无关字段产出了 {len(got)} 个快照"


def test_a_watch_on_fields_still_sees_people_come_and_go(registry):
    """成员进出永远算数，不管订阅了哪些字段 —— 否则"谁在池子里"就成了盲区。"""
    with tinyray.join("p", "churn") as me:
        me.ready(role="trainer")
        me.flush()
        pool = tinyray.pool("p")
        got: list = []
        w = pool.changes(fields=["role"])
        _drain_into(w, got)
        time.sleep(0.3)
        assert got == []

        peer = subprocess.Popen(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    f"""
                    import os, sys, tinyray
                    os.environ["TINYRAY_REGISTRY"] = "{registry.endpoint}"
                    m = tinyray.join("p", "churn")
                    m.ready(role="trainer")
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
            peer.stdout.readline()
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and not got:
                time.sleep(0.05)
            assert got, "来了一个新成员，订阅字段的 watcher 却没看见"
        finally:
            w.close()
            peer.stdin.write("\n")
            peer.stdin.close()
            peer.wait(timeout=10)


def test_readiness_can_be_watched_by_name(registry):
    """`ready` 和 `url` 是成员自己的一部分，不在 state 里，但一样能点名。"""
    with tinyray.join("p", "churn") as me:
        me.ready(step=0)
        me.flush()
        pool = tinyray.pool("p")
        got: list = []
        w = pool.changes(fields=["ready"])
        _drain_into(w, got)
        time.sleep(0.3)

        me.update(step=1)
        me.flush()
        time.sleep(0.3)
        assert got == [], "改的是 step，不该惊动订阅 ready 的 watcher"

        me.unready()
        me.flush()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not got:
            time.sleep(0.05)
        w.close()
        assert got, "readiness 变了却没有产出"


def test_each_event_loop_leaves_nothing_behind(registry):
    """每个 `asyncio.run()` 用过 watch 之后，它的铃和管道都得还回去。

    实测过没还的样子：101 次 `asyncio.run()` 之后 101 个铃、**210 个文件描述符**，
    而且心跳每一拍都要往那 101 个死管道各写一次。

    原来的清理只看弱引用死没死，而那一支**永远不会触发** —— 铃自己持有它的
    loop，于是那条记录把 loop 一直吊着。真正会发生的是 loop 被**关闭**，
    `asyncio.run()` 每次结束都会关。

    这个坑本会话踩过一次：RPC 那个按 loop id 存的传输缓存，100 次调用留下 100
    个条目、100 个 fd，而上限是 1024。同一个形状，换了个地方。
    """
    import gc

    with tinyray.join("p", "churn") as me:
        me.ready()

        async def touch_a_watch() -> None:
            async for _ in tinyray.apool("p").achanges(timeout=0.05):
                pass

        def open_fds() -> int:
            return len(os.listdir(f"/proc/{os.getpid()}/fd"))

        asyncio.run(touch_a_watch())
        gc.collect()
        settled_fds, settled_bells = open_fds(), len(tinyray._bells)

        for _ in range(30):
            asyncio.run(touch_a_watch())
        gc.collect()

        assert len(tinyray._bells) <= settled_bells, (
            f"30 个事件循环之后还留着 {len(tinyray._bells)} 个铃"
        )
        # 一次一个管道两个 fd，所以三十次没回收会是 60 个。
        assert open_fds() <= settled_fds + 4, f"文件描述符从 {settled_fds} 涨到了 {open_fds()}"


def test_loops_in_many_threads_do_not_close_each_others_pipes(registry):
    """一个线程回收旧铃的时候，别把另一个线程的描述符一起关了。

    每个事件循环一个铃，而回收是**下一个来问的线程**顺手做的 —— 所以两个线程
    会同时走到那段"先列出来，再逐个关掉"的代码。两个人关同一个铃，`os.close()`
    就调了两次；而第一次释放掉的号码，内核会立刻发给下一个要 fd 的人。

    实测没加锁的样子：8 个线程各跑 25 轮 `asyncio.run()`，**4 个线程在第 1 轮
    就死了**，`OSError: [Errno 9] Bad file descriptor`。EBADF 只是能看见的那
    一面：真正危险的是第二次 close 悄悄关掉了别人刚拿到的 fd。
    """
    with tinyray.join("threads", "churn") as me:
        me.ready()
        errors: list[str] = []

        def worker() -> None:
            async def go() -> None:
                async for _ in tinyray.apool("threads").achanges(timeout=0.02):
                    pass

            for _ in range(25):
                try:
                    asyncio.run(go())
                except Exception as exc:
                    errors.append(f"{type(exc).__name__}: {exc}")
                    return

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        assert not errors, f"8 个线程并发用 watch，{len(errors)} 个出错：{errors[:4]}"


FORK_WHILE_LOCKED = textwrap.dedent(
    """
    import asyncio, os, sys, threading, time, tinyray
    from tinyray import _rpc

    me = tinyray.join("forklock", "churn")
    me.ready()

    held = threading.Event()
    def hog():
        _rpc._per_loop_lock.acquire()   # 拿住不放，模拟 fork 正好撞上
        held.set()
        time.sleep(120)
    threading.Thread(target=hog, daemon=True).start()
    held.wait()

    pid = os.fork()
    if pid == 0:
        try:
            kid = tinyray.join("forklock", "churn")
            kid.ready()
            async def go():
                async for _ in tinyray.apool("forklock").achanges(timeout=0.02):
                    pass
            asyncio.run(go())
        except BaseException:
            os._exit(3)
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    print("CHILD_EXITED", os.waitstatus_to_exitcode(status), flush=True)
    """
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork()")
def test_a_child_forked_while_the_lock_was_held_can_still_watch(registry):
    """锁是跟着内存复制过去的，持锁的那个线程不是。

    fork 只带走调用线程。如果分叉的那一刻另一个线程正在 per_loop 里面，子进程
    拿到的就是一把**永远锁着**的锁，没有任何线程还能去释放它 —— 子进程第一次
    用 watch 就永久卡住。实测：不重置是 5 秒后仍卡着（只能 kill），重置了是
    0.10 秒正常退出。
    """
    p = subprocess.Popen(
        [sys.executable, "-c", FORK_WHILE_LOCKED],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        out, err = p.communicate(timeout=25)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        try:
            out, err = p.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            out, err = "", ""
        raise AssertionError(
            "fork 时锁正被别的线程持有，子进程卡在了第一次 watch 上："
            f"stdout={out!r} stderr={err[-400:]!r}"
        ) from None
    assert "CHILD_EXITED 0" in out, f"stdout={out!r} stderr={err[-800:]!r}"


@pytest.mark.parametrize("kind", ["sync", "async"])
def test_a_replacement_that_already_happened_is_not_missed(registry, kind):
    """问"谁接手了"的时候，接手常常已经发生了。

    典型顺序就是这样：一次调用撞上 `Fenced`，**然后**才去问新任期是谁。等待
    如果只订阅"接下来的变化"，就会错过那件已经发生的事，白等满整个超时。

    实测同一个已完成的换人、且池子已经安静下来：同步和异步**都坐满 5s 超时
    然后返回 None**，5/5 复现。两个都是自己驱动 watch，只订阅"接下来的变化"，
    从不先看一眼当下已经成立没有。

    这个 bug 难看见，是因为随后任何一条无关变化都会让它蒙对（离场者被清理就是
    一条）。第一次测量时同步版 0.04s 就答对了，害我以为只有异步版坏 —— 所以
    下面要先等池子安静。
    """
    peer = textwrap.dedent(
        f"""
        import os, sys, tinyray
        os.environ["TINYRAY_REGISTRY"] = "{registry.endpoint}"
        m = tinyray.join("late", slot=1, size=2)
        m.ready(who=sys.argv[1])
        print(m.identity, flush=True)
        sys.stdin.readline()
        """
    )

    def start(tag: str):
        p = subprocess.Popen(
            [sys.executable, "-c", peer, tag],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        return p, p.stdout.readline().strip()

    def stop(p) -> None:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=10)

    with tinyray.join("late", slot=0, size=2) as me:
        me.ready()
        pool = tinyray.pool("late")
        first, was = start("first")
        pool.wait(count=2, timeout=10)
        stop(first)
        second, now = start("second")
        # 先确认换人**彻底完成**了，再去问 —— 这才是要测的那个顺序。
        pool.until(lambda s: s.slot(1) is not None and s.slot(1).identity == now, timeout=15)
        # 再等池子彻底安静。旧写法只订阅"接下来的变化"，所以只要随后还有任何
        # 一条无关变化到达（离场者被清理就是一条），谓词早已成立，它就会**碰巧
        # 答对** —— 这正是这个 bug 时灵时不灵的原因，也是写这条测试第一版没抓到
        # 它的原因。
        rev, since = pool.snapshot().revision, time.monotonic()
        while time.monotonic() - since < 1.5:
            time.sleep(0.1)
            if pool.snapshot().revision != rev:
                rev, since = pool.snapshot().revision, time.monotonic()

        started = time.monotonic()
        if kind == "sync":
            got = pool.wait_replacement(identity=was, timeout=5)
        else:
            got = asyncio.run(tinyray.apool("late").await_replacement(identity=was, timeout=5))
        spent = time.monotonic() - started
        stop(second)

    assert got is not None, f"{kind}: 换人已经发生了却答 None，白等了 {spent:.2f}s"
    assert got.identity == now, f"{kind}: 答的是 {got.identity!r}，新任期是 {now!r}"
    assert spent < 2, f"{kind}: 答案早就摆在那了，却等了 {spent:.2f}s"
