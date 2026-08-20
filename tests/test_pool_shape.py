"""池子的形状由现在在场的人决定，不是由历史上第一个到达者决定。"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time

import pytest

import tinyray

HOLD = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join("t", "collective", slot=0, size=4) as m:
        m.ready(); print("READY", flush=True); sys.stdin.readline()
    """
)


@pytest.fixture
def opened(registry):
    """一个已经以 collective/size=4 开张的池子。"""
    p = subprocess.Popen(
        [sys.executable, "-c", HOLD], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    try:
        yield p
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


def test_a_different_size_is_refused_at_join(opened):
    """修前它被静默丢弃：这个成员照常运行，而它的声明无人理会。"""
    with pytest.raises(tinyray.PolicyError, match="opened with size 4, this member says 8"):
        tinyray.join("t", "collective", slot=1, size=8)


@pytest.mark.parametrize("policy", ["stateful", "churn"])
def test_a_different_policy_is_refused_at_join(opened, policy):
    kw = {"slot": 1, "size": 4} if policy == "stateful" else {}
    with pytest.raises(tinyray.PolicyError, match="running as"):
        tinyray.join("t", policy, **kw)


def test_not_declaring_a_size_is_not_a_disagreement(opened):
    """size 常常来自环境变量，没设不等于反对。"""
    with tinyray.join("t", "collective", slot=1) as m:
        m.ready()
        assert tinyray.pool("t")._c.pool_info("t")[2] == 4


def test_an_empty_pool_can_be_reshaped(registry):
    """同一个注册中心跨作业复用时，改了 world size 却悄悄沿用旧值
    是最难查的一类：所有 rank 都在等一个永远凑不齐的人数。"""
    with tinyray.join("t", "collective", slot=0, size=4) as m:
        m.ready()
        assert tinyray.pool("t")._c.pool_info("t")[2] == 4
    # 等租约过期，池子彻底空掉（leave() 之后本进程不能再查，那是另一条断言）
    time.sleep(registry.ttl_ms / 1000 + 1.0)
    with tinyray.join("t", "collective", slot=0, size=2) as m:
        m.ready()
        time.sleep(0.5)
        assert tinyray.pool("t")._c.pool_info("t")[2] == 2, "空池仍钉在旧的 size 上"


SERVELESS = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join("s", "stateful", slot=0, size=2) as m:
        m.ready(); print("READY", flush=True); sys.stdin.readline()
    """
)


def test_the_first_member_through_does_not_decide_the_pool_serves_nothing(registry):
    """只有一部分成员传 serves= 是常态。"""
    p = subprocess.Popen(
        [sys.executable, "-c", SERVELESS], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        assert p.stdout.readline().strip() == "READY"

        class S:
            def ping(self) -> str:
                return "pong"

        with tinyray.join("s", "stateful", slot=1, size=2, serves=S()) as m:
            m.ready()
            time.sleep(0.5)
            assert "ping" in tinyray.pool("s")._c.pool_info("s")[3], "后来者带来的方法被丢了"
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
