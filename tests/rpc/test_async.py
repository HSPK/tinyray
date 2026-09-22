"""Both flavours must exist or the first real integration fails: a trainer
loop is synchronous, a collector loop is asyncio, and they call each other."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap

import pytest
import tinyray

ASYNC_SERVER = textwrap.dedent(
    """
    import asyncio, sys, tinyray

    class Collector:
        async def assign(self, task):
            await asyncio.sleep(0.01)
            return {"took": task, "loop": id(asyncio.get_running_loop())}
        def sync_too(self, x):
            return x * 2
        async def boom(self):
            raise KeyError("no such rollout")

    async def main():
        me = tinyray.join("acollector", "stateful", slot=0, serves=Collector())
        me.ready(loop=id(asyncio.get_running_loop()))
        print("READY", flush=True)
        await asyncio.get_running_loop().run_in_executor(None, sys.stdin.readline)

    asyncio.run(main())
    """
)


@pytest.fixture
def async_peer(registry):
    me = tinyray.join("driver", "churn")
    me.ready()
    proc = subprocess.Popen(
        [sys.executable, "-c", ASYNC_SERVER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout.readline().strip() == "READY"
    tinyray.pool("acollector").wait(count=1, timeout=10)
    try:
        yield proc
    finally:
        try:
            proc.stdin.write("\n")
            proc.stdin.flush()
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        me.leave()


def test_async_methods_run_on_the_callers_existing_loop(async_peer):
    """Never build a new loop: the user's clients are bound to theirs."""
    h = tinyray.pool("acollector").slot(0)
    result = h.assign("task-1")
    assert result["took"] == "task-1"
    assert result["loop"] == h.state["loop"], "ran on a loop we invented"


def test_sync_and_async_methods_coexist_on_one_object(async_peer):
    h = tinyray.pool("acollector").slot(0)
    assert h.sync_too(21) == 42
    assert h.assign("t")["took"] == "t"


def test_async_exceptions_still_arrive_as_remote_errors(async_peer):
    h = tinyray.pool("acollector").slot(0)
    with pytest.raises(tinyray.RemoteError) as e:
        h.boom()
    assert e.value.type == "KeyError"


def test_an_async_caller_awaits_the_same_methods(async_peer):
    async def drive():
        h = tinyray.apool("acollector").slot(0)
        pending = h.assign("task-9")
        assert asyncio.iscoroutine(pending), "an async handle must hand back an awaitable"
        assert (await pending)["took"] == "task-9"
        assert await h.sync_too(3) == 6
        with pytest.raises(tinyray.RemoteError):
            await h.boom()
        with pytest.raises(AttributeError):
            h.nope()

    asyncio.run(drive())


def test_async_timeout_is_bounded(async_peer):
    async def drive():
        h = tinyray.apool("acollector").slot(0)
        bad = tinyray.AsyncHandle(
            "acollector",
            {
                "id": 0,
                "slot": 0,
                "incarnation": h.incarnation,
                "url": "127.0.0.1:1",
                "ready": True,
            },
            ("assign",),
        )
        with pytest.raises(tinyray.Unreachable):
            await bad.assign.timeout(0.3)("x")

    asyncio.run(drive())


def test_repeated_event_loops_do_not_accumulate_clients(async_peer):
    """The native pool belongs to the process, not to short-lived Python loops."""
    import gc
    import os

    def open_fds() -> int:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))

    async def one(n: int) -> dict:
        return await tinyray.apool("acollector").slot(0).assign(f"t{n}")

    # Warm up first: the first call opens a connection to the peer that later
    # ones are entitled to keep, so it is not part of what must stay flat.
    assert asyncio.run(one(0))["took"] == "t0"
    gc.collect()
    before_fds = open_fds()

    for i in range(1, 40):
        assert asyncio.run(one(i))["took"] == f"t{i}"

    gc.collect()
    leaked = open_fds() - before_fds
    assert leaked <= 3, f"39 more event loops leaked {leaked} file descriptors"


def test_async_calls_do_not_use_asyncio_threads_or_the_default_executor(async_peer, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("native async RPC must not use asyncio.to_thread")

    monkeypatch.setattr(asyncio, "to_thread", forbidden)

    async def drive():
        loop = asyncio.get_running_loop()

        def forbidden_executor(*args, **kwargs):
            pytest.fail("native async RPC must not use run_in_executor")

        monkeypatch.setattr(loop, "run_in_executor", forbidden_executor)
        h = tinyray.apool("acollector").slot(0)
        assert (await h.assign("native"))["took"] == "native"

    asyncio.run(drive())


STOPPED_LOOP_SERVER = textwrap.dedent(
    """
    import asyncio, os, sys, tinyray
    os.environ["TINYRAY_REGISTRY"] = "{endpoint}"
    class S:
        async def doubled(self, x: int) -> int: return x * 2
        def plain(self, x: int) -> int: return x + 1
    async def setup():
        m = tinyray.join("stopped", "stateful", slot=0, size=1, serves=S(), max_concurrency=2)
        m.ready()
        return m
    if sys.argv[1] == "closed":
        m = asyncio.run(setup())              # 循环被关掉了
    else:
        loop = asyncio.new_event_loop()       # 循环还在，只是没人再跑它
        m = loop.run_until_complete(setup())
    print("READY", flush=True)
    sys.stdin.readline()
    """
)


@pytest.mark.parametrize("how", ["closed", "idle"])
def test_a_member_still_answers_after_the_loop_it_joined_on_stops(registry, how):
    """`serves=` 的成员在 `asyncio.run()` 块结束之后还活着，循环却不转了。

    async 方法原来是交给"join 时那个循环"跑的，只看它当初存在过，不看它现在还
    转不转。交给一个没人转的循环，`run_coroutine_threadsafe(...).result()`
    **没有超时**，于是永不返回。

    实测 `max_concurrency=2`：两次 async 调用就把两个槽位永久占死，之后这个成员
    对**任何**调用都答 `at its concurrency limit`，连同步方法也一样 —— 而它还在
    注册、还在心跳、还在广告地址。循环被关掉那种也没好到哪去：调用方收到的是
    `RemoteError: Event loop is closed`，好像是它的方法抛了异常，其实方法压根
    没跑。

    判断的依据应该是"**现在**转不转"，不转就自己开一个跑完 —— 和 join 时根本
    没有循环时做的事一样。
    """
    p = subprocess.Popen(
        [sys.executable, "-c", STOPPED_LOOP_SERVER.format(endpoint=registry.endpoint), how],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        p.stdout.readline()
        with tinyray.join("asker", "churn") as me:
            me.ready()
            tinyray.pool("stopped").wait(count=1, timeout=15)
            h = tinyray.pool("stopped").slot(0)
            assert h.doubled.timeout(5)(21) == 42
            assert h.doubled.timeout(5)(3) == 6
            # 槽位得还回来。带着 bug 时这一句是 `at its concurrency limit`。
            assert h.plain(1) == 2, "两次 async 调用之后，这个成员再也不答话了"
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=8)
        except Exception:
            p.kill()


ASYNC_PEER = textwrap.dedent(
    """
    import sys, tinyray
    class S:
        def ping(self, x: int) -> int: return x
    with tinyray.join("aconn", "stateful", slot=0, serves=S()) as me:
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


def test_async_calls_reuse_one_connection(registry):
    """Two hundred awaited calls reuse the process-global native TCP pool."""
    me = tinyray.join("driver", "churn")
    me.ready()
    proc = subprocess.Popen(
        [sys.executable, "-c", ASYNC_PEER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout.readline().strip() == "READY"
    tinyray.pool("aconn").wait(count=1, timeout=10)

    def time_wait() -> int:
        out = subprocess.run(["ss", "-tan"], capture_output=True, text=True).stdout
        return out.count("TIME-WAIT")

    async def drive() -> None:
        h = tinyray.apool("aconn").slot(0)
        for i in range(200):
            assert await h.ping(i) == i

    before = time_wait()
    asyncio.run(drive())
    churn = (time_wait() - before) / 200
    try:
        assert churn < 0.1, f"{churn:.2f} new sockets per call; keep-alive is not working"
    finally:
        proc.stdin.write("\n")
        proc.stdin.flush()
        proc.wait(timeout=5)
        me.leave()
