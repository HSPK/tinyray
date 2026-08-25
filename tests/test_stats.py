"""计数器存在，是为了让"要不要拆独立通道"这个问题有答案。

`max_concurrency` 挡住的是无限堆积，不是隔离：并发槽被长驻调用占满之后，control
调用和别的调用一样吃 503。这件事是不是正在发生，不该靠感觉判断 —— 所以
`refused` 要和 `peak_in_flight` 摆在一起看。
"""

from __future__ import annotations

import threading
import time

import pytest
import tinyray


class Slow:
    def quick(self) -> int:
        return 1

    def slow(self, ms: int) -> int:
        time.sleep(ms / 1000)
        return ms

    def boom(self) -> None:
        raise ValueError("business problem")


def test_stats_counts_what_was_served(registry):
    with tinyray.join("svc", "stateful", slot=0, serves=Slow()) as me:
        me.ready()
        h = tinyray.pool("svc").slot(0)
        before = me.stats()
        assert before["calls"] == 0
        for _ in range(5):
            h.quick()
        with pytest.raises(tinyray.RemoteError):
            h.boom()
        got = me.stats()
        assert got["calls"] == 6, got
        assert got["failed"] == 1, "抛异常的那次没被算成失败"
        assert got["in_flight"] == 0, "调用都结束了，在飞的却不是 0"
        assert got["busy_ms"] >= 0


def test_stats_shows_saturation_rather_than_leaving_it_to_guesswork(registry):
    """槽位被占满时，control 调用同样会吃 503 —— 这正是要能看见的东西。"""
    with tinyray.join("svc", "stateful", slot=0, serves=Slow(), max_concurrency=2) as me:
        me.ready()
        h = tinyray.pool("svc").slot(0)
        assert me.stats()["concurrency_limit"] == 2

        errors: list = []

        def hog() -> None:
            try:
                h.slow(600)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        holders = [threading.Thread(target=hog) for _ in range(2)]
        for t in holders:
            t.start()
        time.sleep(0.25)  # 两个槽都占住了

        peak = me.stats()["peak_in_flight"]
        with pytest.raises(tinyray.NotDelivered):
            h.quick()  # 一次"控制面"调用，同样被挡在外面

        for t in holders:
            t.join(timeout=10)
        got = me.stats()
        assert not errors, errors
        assert peak == 2, f"占满时 peak_in_flight 是 {peak}，应该是 2"
        assert got["refused"] == 1, f"被拒的次数记成了 {got['refused']}"
        assert got["peak_in_flight"] == 2


def test_stats_reports_what_this_member_publishes(registry):
    with tinyray.join("p", "churn") as me:
        me.ready()
        me.flush()
        small = me.stats()["state_bytes"]
        me.update(blob="x" * 4096)
        me.flush()
        big = me.stats()["state_bytes"]
        assert big > small + 4000, f"state_bytes 没跟着走: {small} -> {big}"
        assert me.stats()["watched_pools"] >= 1


def test_stats_counts_the_wakeups_watching_costs(registry):
    with tinyray.join("p", "churn") as me:
        me.ready()
        pool = tinyray.pool("p")
        pool.snapshot()
        before = me.stats()["watch_wakeups"]
        for i in range(5):
            me.update(step=i)
            time.sleep(0.05)
        me.flush()
        after = me.stats()["watch_wakeups"]
        assert after > before, "发布了五次，一次唤醒都没记上"


def test_a_member_without_serves_has_no_serving_counters(registry):
    """不提供服务的成员不该凭空多出一堆恒为零的键 —— 那会让人以为量过了。"""
    with tinyray.join("p", "churn") as me:
        me.ready()
        got = me.stats()
        assert "calls" not in got
        assert "beats_ok" in got


def test_a_call_you_have_the_answer_to_is_already_counted(registry):
    """应答到了调用方手里，那次调用就必须已经在 `stats()` 里。

    原来计数发生在应答**写出去之后**：服务端把字节推上 socket，然后才更新计数器。
    于是调用方先拿到答案、再去读 `stats()`，就可能读到一个还没算上自己的数字。
    实测普通调用 0.2%、抛异常的调用 0.8% —— 对断言它的东西是抛硬币，对读它的
    东西是个错数。CI 上就是这么红的：五次 quick 加一次 boom，`calls` 报 5。

    改法是把 `calls`/`failed` 挪到写之前。`in_flight` 和 `busy_ns` 留在写之后，
    因为它们问的是"有几个 handler 在跑、跑了多久"，而写一个 16 MiB 的应答确实
    占着那条线程。
    """
    with tinyray.join("svc", "stateful", slot=0, serves=Slow()) as me:
        me.ready()
        h = tinyray.pool("svc").slot(0)
        rounds, late = 250, []
        for i in range(rounds):
            # 抛异常那条路上原来最容易漏（0.8% 对 0.2%）。
            with pytest.raises(tinyray.RemoteError):
                h.boom()
            seen = me.stats()["calls"]
            if seen != i + 1:
                late.append((i + 1, seen))
        assert not late, f"{rounds} 次调用里有 {len(late)} 次拿到答案时还没被算上：{late[:4]}"
