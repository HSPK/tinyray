"""走一趟，什么都别留下。

前两轮各修了一处"资源没还回去"，而两轮**都是第一次只修对了一半** —— 每次都是
临时探针，盯着的东西刚好漏掉了另一条路。所以这里不再逐个猜，把判据本身写下来：
一轮 join/leave 走完，进程里该数得出来的东西都得回到原处。

盯的是计数，不是内存。RSS 会因为分配器的脾气上下跳，而"多了一条线程"、"多了两个
文件描述符"、"多了一个 MethodServer"是确定的。
"""

from __future__ import annotations

import asyncio
import gc
import os
import threading

import pytest
import tinyray
from tinyray import _rpc, _serve

_KINDS = {
    "Member": tinyray.Member,
    "MethodServer": _serve.MethodServer,
    "Watching": tinyray._Watching,
    "Handle": tinyray.Handle,
    "LoopBell": tinyray._LoopBell,
}


def census() -> dict[str, int]:
    gc.collect()
    out = {name: sum(1 for o in gc.get_objects() if type(o) is cls) for name, cls in _KINDS.items()}
    out["线程"] = threading.active_count()
    out["fd"] = len(os.listdir(f"/proc/{os.getpid()}/fd"))
    out["_bells"] = len(tinyray._bells)
    out["_loops"] = len(_rpc._loops)
    out["_SHAPES"] = len(_serve._SHAPES)
    return out


class Served:
    def echo(self, x: int) -> int:
        return x


def _plain(name: str) -> None:
    m = tinyray.join(name, "churn")
    m.ready()
    m.leave()


def _with_call(name: str) -> None:
    m = tinyray.join(name, "stateful", slot=0, size=1, serves=Served())
    m.ready()
    assert tinyray.pool(name).slot(0).echo(1) == 1
    m.leave()


def _with_watch(name: str) -> None:
    m = tinyray.join(name, "churn")
    m.ready()
    for _ in tinyray.pool(name).changes(timeout=0.02):
        pass
    m.leave()


def _with_loop(name: str) -> None:
    m = tinyray.join(name, "churn")
    m.ready()

    async def go() -> None:
        async for _ in tinyray.apool(name).achanges(timeout=0.02):
            pass

    asyncio.run(go())
    m.leave()


@pytest.mark.parametrize(
    "tag,round_trip",
    [
        ("p", _plain),
        ("c", _with_call),
        ("w", _with_watch),
        ("l", _with_loop),
    ],
)
def test_a_round_of_membership_leaves_nothing_behind(registry, tag, round_trip):
    """做完一轮该有的事，再走，然后什么都不剩。

    带调用那一种是这套判据挣来的第一个：keep-alive 让处理线程停在读上等下一个
    请求，而关掉监听 socket 碰不到它。实测 30 轮 join/一次调用/leave 之后，留下
    30 条线程、30 个方法服务器和 60 个文件描述符 —— 而描述符默认上限是 1024。
    一份工作换一份工作的进程就是这个形状。

    先空跑几轮再取基准：第一次难免有一次性的分配（连接池、事件循环的自管道），
    那不是泄漏，泄漏是**每轮都多一点**。
    """
    for i in range(3):
        round_trip(f"{tag}warm{i}")
    before = census()

    rounds = 12
    for i in range(rounds):
        round_trip(f"{tag}{i}")
    after = census()

    grew = {k: (before[k], after[k]) for k in before if after[k] > before[k]}
    assert not grew, f"{rounds} 轮之后没回到原处：" + ", ".join(
        f"{k} {b}->{a}（每轮 +{(a - b) / rounds:.1f}）" for k, (b, a) in grew.items()
    )
