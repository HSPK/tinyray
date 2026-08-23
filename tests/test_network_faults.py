"""Behaviour on a network that misbehaves.

Every other test runs on loopback, where nothing is slow, dropped or cut off.
These put a deliberately faulty proxy in front of the registry, which is how
both bugs in here were found.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time

import pytest
from faulty_net import FaultyProxy

JOIN_AND_REPORT = textwrap.dedent(
    """
    import sys, time, tinyray
    seconds = float(sys.argv[1])
    me = tinyray.join("n", "churn")
    me.ready(v=1)
    t0 = time.monotonic()
    while time.monotonic() - t0 < seconds:
        tinyray.pool("n").all()
        time.sleep(0.05)
    s = me.stats()
    print(f"{s['beats_ok']} {s['beats_failed']} {len(tinyray.pool('n').all())}")
    me.leave()
    """
)


def run_against(proxy: FaultyProxy, seconds: float, timeout: float) -> tuple[int, int, int]:
    env = dict(os.environ, TINYRAY_REGISTRY=proxy.endpoint)
    out = subprocess.run(
        [sys.executable, "-c", JOIN_AND_REPORT, str(seconds)],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert out.returncode == 0, out.stderr[-800:]
    ok, failed, seen = out.stdout.strip().split()
    return int(ok), int(failed), int(seen)


def test_a_dropped_packet_does_not_hang_join_forever(registry):
    """join() blocks on its first beat. With no deadline on the request, one
    lost packet hung it for as long as anyone was willing to wait.

    只有"挂住"才算失败。40% 丢包下单次启动本来就有约 0.8% 的概率以 Unreachable
    收场 —— 那个数字是下面 test_the_first_beat_deadline_survives_a_lossy_link
    量出来的，也正是它为什么要容忍一次失败。这条测的是"有没有界"，不是"那道界
    有多宽"，所以一个有界的放弃同样证明了要证明的事；照抄零容忍会让它每约 125
    次全量跑就乱叫一次，而教人忽略一条测试比没有它更糟。实测撞到过一次。
    """
    proxy = FaultyProxy(registry.endpoint, drop_rate=0.4, seed=1)
    try:
        # The timeout here is the assertion: without a bounded beat this never
        # returns at all. TimeoutExpired 会穿出去，那才是这条测试要抓的。
        try:
            run_against(proxy, seconds=3, timeout=60)
        except AssertionError as gave_up:
            # 有界地放弃是通过；别的非零退出不是，所以这里只认这一种。
            assert "Unreachable" in str(gave_up), gave_up
        assert proxy.stats()["dropped"] > 0, "the proxy never actually dropped anything"
    finally:
        proxy.close()


def test_a_lossy_link_still_lets_a_member_start(registry):
    """一条只是丢包的链路不该把 rank 拦在门外。

    这是冒烟，不是回归：把截止时间调回 10 秒它照样通过。真正有判别力的那条在
    下面，带 slow 标记。

    五个种子跑在一条测试里，容忍一次失败 —— 因为下面那条量过：30 秒截止时间、
    40% 丢包下，单次启动仍有约 0.8% 会失败，没有归零。五次零容忍就是每跑一次
    套件约 4% 概率乱叫，而这不是假设：它在 CI 上拦下过一次本该发布的版本
    （0.7.0，seed 12，双核 runner）。

    教人忽略一条测试比没有它更糟 —— 这是同一条原则，下面那条测试写过一次。
    代价是判别力：容忍一次时，它抓住"启动在丢包链路上彻底坏掉"的能力仍然很强
    （五次里要坏两次才会红），但抓不住"失败率从 0.8% 涨到 20%"这种程度的退化。
    那种退化归下面那条 slow 测试管。
    """
    failures = []
    for seed in (11, 12, 13, 14, 15):
        proxy = FaultyProxy(registry.endpoint, drop_rate=0.4, seed=seed)
        try:
            # run_against 已经断言了退出码 —— join() 抛 Unreachable 就是失败。
            # 不断言"能看见自己"：ready() 要等下一拍才生效，那是另一件事的时序。
            ok, failed, _ = run_against(proxy, seconds=3, timeout=90)
            assert failed > 0, f"seed={seed} 没丢到包，等于没测"
            assert ok >= 1, f"seed={seed} 一拍都没落地却返回了成功"
        except AssertionError as e:
            failures.append(f"seed={seed}: {e}"[-400:])
        finally:
            proxy.close()
    assert len(failures) <= 1, f"{len(failures)}/5 次启动失败，超过实测到的 1/120:\n" + "\n".join(
        failures[:2]
    )


BLACKHOLE_PROBE = textwrap.dedent(
    """
    import sys, time, tinyray
    flag, seconds = sys.argv[1], float(sys.argv[2])
    me = tinyray.join("n", "churn")
    me.ready()
    time.sleep(1.0)
    before = me.stats()
    open(flag, "w").write("GO")
    t0 = time.monotonic()
    while time.monotonic() - t0 < seconds:
        time.sleep(0.02)
    s = me.stats()
    print(f"{s['beats_failed'] - before['beats_failed']} {s['beats_ok'] - before['beats_ok']}")
    me.leave()
    """
)


def test_a_beat_gives_up_inside_its_own_interval(registry, tmp_path):
    """The beat loop is serial, so a request deadline is also how long a lost
    packet stops us beating. A five-second deadline against a 500ms interval
    meant one drop took the member out of the roster.

    Counted attempts under a total blackhole rather than beats through random
    loss: at 30% drop the loss pattern dominates and healthy and broken builds
    overlap (10-19 beats against 3-7). Swallowing everything makes it
    arithmetic -- deadline plus interval per attempt -- and the two builds
    separate with no variance at all: 9, 9, 9 attempts in eight seconds
    against 1, 1, 1 with the deadline restored to five seconds.
    """
    flag = tmp_path / "go"
    proxy = FaultyProxy(registry.endpoint)
    try:
        env = dict(os.environ, TINYRAY_REGISTRY=proxy.endpoint)
        p = subprocess.Popen(
            [sys.executable, "-c", BLACKHOLE_PROBE, str(flag), "8"],
            env=env,
            stdout=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 30
        while not flag.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert flag.exists(), "the probe never got going"
        proxy.drop_rate = 1.0
        failed, ok = (int(x) for x in p.communicate(timeout=60)[0].split())
    finally:
        proxy.close()
    assert ok == 0, f"{ok} beats got through a total blackhole; the probe is wrong"
    assert failed >= 5, (
        f"only {failed} attempts in 8s with everything dropped; the loop is "
        f"stalling behind its own timeout instead of giving up inside the interval"
    )


@pytest.mark.parametrize(
    "name,kwargs",
    [
        ("latency", {"delay_ms": 150}),
        ("resets", {"reset_rate": 0.15, "seed": 3}),
        ("fragmentation", {"chunk_bytes": 7}),
        ("everything", {"drop_rate": 0.2, "reset_rate": 0.1, "delay_ms": 40, "seed": 4}),
    ],
)
def test_membership_survives_a_broken_network(registry, name, kwargs):
    proxy = FaultyProxy(registry.endpoint, **kwargs)
    try:
        ok, failed, seen = run_against(proxy, seconds=6, timeout=60)
        assert ok > 0, f"{name}: not one beat got through"
        assert seen == 1, f"{name}: the member lost sight of itself"
    finally:
        proxy.close()


def test_calls_are_bounded_when_the_far_side_stops_answering(registry):
    """A call to a black hole must fail on its own budget, not hang."""
    import tinyray

    me = tinyray.join("client", "churn")
    me.ready()
    try:
        black_hole = tinyray.Handle(
            "svc",
            {"id": 0, "slot": 0, "incarnation": 1, "url": "http://10.255.255.1:9", "ready": True},
            ("anything",),
        )
        t0 = time.monotonic()
        with pytest.raises(tinyray.Unreachable):
            black_hole.anything.timeout(1.0)()
        assert time.monotonic() - t0 < 5
    finally:
        me.leave()


@pytest.mark.slow
def test_the_first_beat_deadline_survives_a_lossy_link(registry):
    """启动 40 次，因为要防的是一个罕见事件，而它量的是比率不是有无。

    截止时间既要识破"注册中心根本不存在"，又要熬过丢包。10 秒正好坐在抛硬币
    的位置上：40% 丢包下首拍落地中位 5.0s、p90 9.8s、最慢 12.3s，实测 15 次
    启动失败 1 次（6.7%），在套件里表现为偶发。

    改成 30 秒之后，三轮共 120 次启动失败 1 次（约 0.8%）—— 降了，没有归零。
    所以这里允许 1 次失败：断言"一次都不许失败"会让这条测试自己有 4% 的概率
    乱叫，而教人忽略一条测试比没有它更糟。

    代价是判别力：允许 1 次时，它抓住 10 秒那个回归的概率是 75%（40 次试验、
    6.7% 失败率）。这条数字写在这里，免得下次有人以为它是把铁锁。
    """
    launches, tolerated = 40, 1
    failures = []
    for seed in range(200, 200 + launches):
        proxy = FaultyProxy(registry.endpoint, drop_rate=0.4, seed=seed)
        try:
            run_against(proxy, seconds=1, timeout=120)
        except AssertionError as e:
            failures.append(f"seed={seed}: {e}"[-600:])
        finally:
            proxy.close()
    assert len(failures) <= tolerated, (
        f"{len(failures)}/{launches} 次启动失败，超过实测到的 1/120:\n" + "\n".join(failures[:3])
    )
