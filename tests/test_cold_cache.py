"""首次查询一个陌生池：订阅和查询是同一口气发生的。"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time

import pytest

import tinyray

SERVER = textwrap.dedent(
    """
    import sys, tinyray
    class S:
        def ping(self) -> str: return "pong"
    with tinyray.join("s", "stateful", slot=0, size=1, serves=S()) as me:
        me.ready(); print("READY", flush=True); sys.stdin.readline()
    """
)


@pytest.fixture
def served(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    time.sleep(1.0)  # 让服务端彻底上报，排除"它还没到"的可能
    try:
        yield p
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


def test_first_lookup_does_not_call_a_full_pool_empty(served):
    """一个满员两秒的池子，首次查询不能说它是空的。"""
    with tinyray.join("c", "churn") as me:
        me.ready()
        assert len(tinyray.pool("s").all()) == 1
        assert tinyray.pool("s").slot(0).ping() == "pong"
        tinyray.pool("s").pick()


def test_an_empty_pool_answers_fast_and_stays_fast(registry):
    """空池必须与冷缓存区分开，否则每次查询都要等满超时。"""
    with tinyray.join("c", "churn") as me:
        me.ready()
        t0 = time.monotonic()
        assert tinyray.pool("ghost").all() == []
        first = time.monotonic() - t0
        assert first < 1.0, f"空池首次查询花了 {first:.2f}s"
        t0 = time.monotonic()
        for _ in range(100):
            tinyray.pool("ghost").all()
        assert time.monotonic() - t0 < 0.5, "空池的答案没被记住"
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            tinyray.pool("ghost").wait(count=1, timeout=1.0)
        assert 0.9 < time.monotonic() - t0 < 2.0, "wait 的超时被等待淹没了"


def test_a_dead_registry_does_not_stall_lookups(registry):
    """注册中心可以死而不停工 —— 包括不把查询变成一连串超时。"""
    with tinyray.join("c", "churn") as me:
        me.ready()
        tinyray.pool("warm").all()
        registry.stop()
        time.sleep(1.5)
        assert tinyray.pool("warm").all() == []  # 缓存仍可读
        tinyray.pool("cold0").all()  # 第一次要察觉到失联
        t0 = time.monotonic()
        for i in range(1, 6):
            tinyray.pool(f"cold{i}").all()
        spent = time.monotonic() - t0
        assert spent < 1.0, f"注册中心死后查 5 个陌生池子花了 {spent:.1f}s"
