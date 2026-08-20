"""查询读缓存，但订阅和查询是同一口气发生的 —— 事件循环上这会变成一次停顿。"""

from __future__ import annotations

import asyncio
import time

import pytest

import tinyray


async def _stall(fn) -> tuple[float, float]:
    """跑一个 5ms 心跳的 ticker，量 fn 造成的最长循环停顿。"""
    gaps: list[float] = []
    stop = asyncio.Event()

    async def ticker() -> None:
        last = time.monotonic()
        while not stop.is_set():
            await asyncio.sleep(0.005)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    t = asyncio.create_task(ticker())
    await asyncio.sleep(0.1)
    gaps.clear()
    t0 = time.monotonic()
    fn()
    spent = time.monotonic() - t0
    await asyncio.sleep(0.1)
    stop.set()
    await t
    return spent, max(gaps)


def test_priming_a_pool_keeps_the_first_async_lookup_off_the_loop(registry):
    """构造 Pool 就是订阅，所以启动时建好要用的池子，热路径上就不必等。
    没预热时实测 4 个池子共停顿 169ms，预热后 0ms。"""

    async def main() -> None:
        with tinyray.join("c", "churn") as me:
            me.ready()

            cold_spent, cold_gap = await _stall(
                lambda: [tinyray.apool(f"cold{i}").all() for i in range(4)]
            )

            [tinyray.pool(f"warm{i}") for i in range(4)]  # 只构造，不查询
            await asyncio.sleep(0.5)
            warm_spent, warm_gap = await _stall(
                lambda: [tinyray.apool(f"warm{i}").all() for i in range(4)]
            )

            assert cold_gap > 0.05, f"没预热却只停顿了 {cold_gap*1000:.0f}ms，这个测试失去了判别力"
            assert warm_gap < cold_gap / 3, (
                f"预热后仍停顿 {warm_gap*1000:.0f}ms，对比没预热的 {cold_gap*1000:.0f}ms"
            )
            assert warm_spent < cold_spent / 3

    asyncio.run(main())


def test_a_cold_async_lookup_still_answers_correctly(registry):
    """停顿是为了不撒谎换来的，所以答案必须是对的。"""

    async def main() -> None:
        with tinyray.join("c", "churn") as me:
            me.ready(host="h1")
            await asyncio.sleep(0.5)
            found = tinyray.apool("c").all()
            assert len(found) == 1, "首次异步查询把一个有人的池子报成了空的"
            assert isinstance(found[0], tinyray.AsyncHandle)

    asyncio.run(main())
