"""查询读缓存，但订阅和查询是同一口气发生的 —— 事件循环上这会变成一次停顿。"""

from __future__ import annotations

import asyncio

import tinyray


def test_priming_a_pool_keeps_the_first_async_lookup_off_the_loop():
    """构造 Pool 就是订阅，所以启动时建好要用的池子，热路径上就不必等。

    这个机制不能用墙钟时间钉：连接、Nagle 和调度优化已经把冷路径从 169ms 降到
    GitHub runner 上的 1.3ms，任何绝对阈值都会把优化误判成测试失去判别力。
    用一个可控 client 直接钉协议：构造时订阅；未知 pool 第一次查询等一次 revision；
    已收到首答的 pool 不等。
    """

    class Client:
        def __init__(self):
            self.info = {}
            self.watched = []
            self.waited = []
            self.revision = 0
            self.silence_ms = 0

        def watch(self, pools):
            self.watched.extend(pools)

        def cache_revision(self):
            return self.revision

        def pool_info(self, name):
            return self.info.get(name)

        def stats(self):
            return {"interval_ms": 500}

        def wait_revision(self, revision, timeout_ms):
            self.waited.append((revision, timeout_ms))
            self.revision += 1
            self.info["cold"] = (0, 0, None, [])
            return self.revision

    client = Client()
    cold = tinyray.AsyncPool("cold", client)
    assert client.watched == ["cold"]
    cold._settle()
    assert len(client.waited) == 1

    warm = tinyray.AsyncPool("warm", client)
    client.info["warm"] = (0, 0, None, [])
    warm._settle()
    assert len(client.waited) == 1


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
