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


def test_close_releases_a_blocked_watcher(registry):
    """没有变化时 watcher 是阻塞的，close() 必须把它拽回来。"""
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
        # 一拍是 ttl/4 = 500ms；要求远快于此，否则只是碰巧被心跳唤醒。
        assert took < 200, f"close() 用了 {took:.0f}ms，像是在等下一拍"


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
