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


def test_a_world_of_zero_seats_is_refused(registry):
    """`WORLD_SIZE=0` 是这几个坏值里最危险的，因为它不报错。

    池子声明零个座位，`epoch()` 的目标就是零 —— 于是它对着"碰巧在场的人"
    立刻冻结。实测：一个孤零零的成员 82ms 就冻结了一轮，而那本该是个多人的
    世界。"等齐所有人"悄悄变成了"自己往下走"，而 `epoch()` 存在的全部意义
    就是前者。
    """
    with pytest.raises(tinyray.PolicyError, match="at least one seat"):
        tinyray.join("z", "collective", slot=0, size=0)


def test_a_seat_outside_the_world_is_refused(registry):
    """4 个人的世界里没有第 9 号座位。"""
    with pytest.raises(tinyray.PolicyError, match="outside a world"):
        tinyray.join("w", "collective", slot=9, size=4)
    with pytest.raises(tinyray.PolicyError, match="outside a world"):
        tinyray.join("w", "collective", slot=4, size=4)


def test_a_negative_seat_is_refused(registry):
    with pytest.raises(tinyray.PolicyError, match="between 0 and"):
        tinyray.join("w", "collective", slot=-1, size=4)


@pytest.mark.parametrize(
    ("rank", "size"),
    [("-1", "2"), ("1", "-2"), ("99999999999999999999", "2")],
)
def test_a_launcher_variable_that_cannot_be_a_seat_says_which_one(
    registry, monkeypatch, rank, size
):
    """报的要是"哪个变量、什么值"，不是一个转换错误。

    以前这些值一路走到 Rust 边界，回来的是
    `OverflowError: can't convert negative int to unsigned` —— 既不提变量名
    也不提值，还来自一次应用根本没发起的调用。
    """
    monkeypatch.setenv("RANK", rank)
    monkeypatch.setenv("WORLD_SIZE", size)
    with pytest.raises(ValueError, match="usable seat or world size") as caught:
        tinyray.join("w", "collective")
    said = str(caught.value)
    assert ("RANK" in said) or ("WORLD_SIZE" in said), said
