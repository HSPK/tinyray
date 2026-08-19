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
            {"id": 0, "slot": 0, "incarnation": h.incarnation, "url": "http://127.0.0.1:1", "ready": True},
            ("assign",),
        )
        with pytest.raises(tinyray.Unreachable):
            await bad.assign.timeout(0.3)("x")

    asyncio.run(drive())
