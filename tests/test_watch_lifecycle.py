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
    with tinyray.join("p", slot=0, size=1) as m:
        m.ready()
        pool = tinyray.pool("p")
        with pytest.raises(TypeError):
            pool.wait_replacement()
        with pytest.raises(TypeError):
            pool.wait_replacement(slot=0, identity="p/0#1")


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
