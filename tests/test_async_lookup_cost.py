"""查询读缓存，但订阅和查询是同一口气发生的 —— 事件循环上这会变成一次停顿。"""

from __future__ import annotations

import asyncio
import time

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

    量的是 `spent` —— `fn()` 跑在事件循环这根线程上，它自己花掉的时间**就是**循环
    被堵住的时间。旁边那个 5ms ticker 量到的 `gap` 是同一件事的粗刻度版本，只作
    失败时的参考：它看不见 5ms 以下的东西，而现在的冷成本经常就在那个量级。

    **改过两次校准，两次都是被测量推着走的：**

    1. 原来用 4 个池子，判据是"没预热必须停顿 >50ms"，依据是实测 169ms。后来
       给心跳连接开了 `TCP_NODELAY`，同样 4 个池子降到 5.6ms —— 那 169ms 里
       每个池子约 42ms，**几乎全是 Nagle 与对端 delayed ACK 的固定停顿**。冷热
       两侧都变成 5.5ms，这条测试当场失去全部判别力（它自己的守卫报的警）。
    2. 池子提到 16 个之后判别力回来了，但冷成本是**双峰**的：五次实测
       4.5 / 96.5 / 4.0 / 96.8 / 97.3 ms —— 16 次订阅要么并进一拍，要么各自串行。
       所以这里不设"冷必须超过多少毫秒"的绝对线，只留一个很低的牙齿守卫。

    真正稳的是预热侧：`warm_spent` 五次都是 **0.2ms**，比值因此从不小于 7.5 倍。
    """
    pools = 16

    async def main() -> None:
        with tinyray.join("c", "churn") as me:
            me.ready()

            cold_spent, cold_gap = await _stall(
                lambda: [tinyray.apool(f"cold{i}").all() for i in range(pools)]
            )

            [tinyray.pool(f"warm{i}") for i in range(pools)]  # 只构造，不查询
            await asyncio.sleep(0.5)
            warm_spent, warm_gap = await _stall(
                lambda: [tinyray.apool(f"warm{i}").all() for i in range(pools)]
            )

            assert cold_spent > 0.002, (
                f"没预热却只堵了 {cold_spent * 1000:.1f}ms，这个测试失去了判别力（实测下界 4.0ms）"
            )
            assert warm_spent < cold_spent / 3, (
                f"预热后仍堵了 {warm_spent * 1000:.2f}ms，对比没预热的 "
                f"{cold_spent * 1000:.1f}ms；循环停顿 {warm_gap * 1000:.1f}ms / "
                f"{cold_gap * 1000:.1f}ms"
            )

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
