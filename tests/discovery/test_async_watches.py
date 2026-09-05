"""Async watch ownership, rejoining and notification races."""

from __future__ import annotations

import asyncio

import pytest
import tinyray
from tinyray._tinyray import Client


def test_async_rejoin_replaces_the_bell_and_releases_old_waiters(long_lease):
    async def run():
        first = tinyray.join("first")
        first.ready().flush()
        old_watch = tinyray.apool("first").achanges(timeout=20)
        old_task = asyncio.create_task(anext(old_watch))
        try:
            await asyncio.sleep(0)
            old_bell = tinyray._loop_bell(first._c)
            first.leave()
            with tinyray.join("second") as second:
                second.ready(step=0).flush()
                # Replace before yielding, so the old pipe has not been read.
                new_bell = tinyray._loop_bell(second._c)
                assert new_bell is not old_bell
                assert new_bell._client is second._c
                with pytest.raises(StopAsyncIteration):
                    await asyncio.wait_for(old_task, timeout=1)
                pool = tinyray.apool("second")
                async with pool.achanges(timeout=20) as watch:
                    task = asyncio.create_task(anext(watch))
                    await asyncio.sleep(0)
                    second.update(step=1).flush()
                    snapshot = await asyncio.wait_for(task, timeout=1)
                    assert snapshot.get(second.identity).state["step"] == 1
        finally:
            first.leave()
            old_watch.close()
            if not old_task.done():
                old_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await old_task

    asyncio.run(run())


def test_retired_async_operations_cannot_replace_the_current_bell(long_lease):
    async def run():
        with tinyray.join("retired") as first:
            first.ready().flush()
            old_pool = tinyray.apool(first.pool)
            old_watch = old_pool.achanges()
        with tinyray.join("current") as current:
            current.ready(step=0).flush()
            pool = tinyray.apool(current.pool)
            async with pool.achanges(timeout=20) as watch:
                for step in (1, 2):
                    task = asyncio.create_task(anext(watch))
                    try:
                        await asyncio.sleep(0)
                        with pytest.raises(StopAsyncIteration):
                            await anext(old_watch)
                        with pytest.raises(RuntimeError, match="has left"):
                            await first.await_fenced(timeout=1)
                        async with old_pool.achanges(timeout=1) as retired:
                            with pytest.raises(RuntimeError, match="has left"):
                                await anext(retired)
                        current.update(step=step).flush()
                        snapshot = await asyncio.wait_for(task, timeout=1)
                        assert snapshot.get(current.identity).state["step"] == step
                    finally:
                        if not task.done():
                            task.cancel()
                            with pytest.raises(asyncio.CancelledError):
                                await task

    asyncio.run(run())


@pytest.mark.parametrize("flavour", ["fence", "watch"])
def test_first_async_subscription_cannot_miss_fencing(registry, monkeypatch, flavour):
    with tinyray.join("first-bell", "stateful", slot=0) as me:
        me.ready().flush()
        pool = tinyray.apool(me.pool)
        replacement = Client(
            endpoint=f"http://{registry.endpoint}",
            pool=me.pool,
            id=0,
            incarnation=me.incarnation + 1,
            policy="stateful",
            slot=0,
        )
        original = tinyray._loop_bell
        registered = False

        def register(client):
            nonlocal registered
            if not registered:
                registered = True
                assert replacement.start(2000)
                assert me.wait_fenced(timeout=5)
            return original(client)

        monkeypatch.setattr(tinyray, "_loop_bell", register)

        async def run():
            if flavour == "fence":
                assert await asyncio.wait_for(me.await_fenced(timeout=20), timeout=1)
            else:
                async with pool.achanges(timeout=20) as watch:
                    with pytest.raises(tinyray.Fenced):
                        await asyncio.wait_for(anext(watch), timeout=1)

        try:
            asyncio.run(run())
            assert registered
        finally:
            replacement.leave()
