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
