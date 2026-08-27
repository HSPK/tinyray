"""Behaviour on a network that misbehaves.

Every other test runs on loopback, where nothing is slow, dropped or cut off.
These put a deliberately faulty proxy in front of the registry, which is how
both bugs in here were found.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import threading
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


JOIN_AND_WAIT_FOR_SIGINT = textwrap.dedent(
    """
    import sys, tinyray
    print("go", flush=True)
    try:
        tinyray.join("s", "churn", timeout=60.0)
    except KeyboardInterrupt:
        sys.exit(3)
    sys.exit(4)
    """
)


TIMED_JOIN = textwrap.dedent(
    """
    import sys, time, tinyray
    t0 = time.monotonic()
    m = tinyray.join("l", "churn", timeout=30.0)
    print(f"{time.monotonic() - t0:.2f}", flush=True)
    m.leave()
    """
)


def test_the_first_beat_is_retried_rather_than_waited_out(registry):
    """`join(timeout=)` 是整通调用的上界，但**怎么花**这份预算是另一件事。

    首拍是同步的。把整份预算交给它，丢包链路上就变成一次长等而没有任何重试：
    实测 40% 丢包、`timeout=30`，12 次启动中位 **32.7s**，合计 317s —— 每次都
    把截止时间花满，还得靠后台循环偷偷补上一拍才算成功。慢测试也因此从 7:22
    变成 17:06。

    首拍改成只拿 `min(timeout, 5s)` 之后，丢了的那一拍由循环每个间隔重发：
    同样 12 次启动中位 **5.1s**、合计 85s，失败仍是 0。后来首拍又学会了听循环
    的 ack（见 `test_join_returns_when_the_loop_registers_...`），同样六个种子
    中位降到 **1.75s**、最慢 4.26s。

    这条测试盯的是比率而不是有无，所以取中位数：一次长等的中位会贴着预算，
    快速重试的中位贴着一次往返。

    它自己没有变异体，是有意的：15s 这个界现在离常态有八倍余量，能翻红的只有
    "丢包链路上 join 整体垮掉"这种粗故障，而"该快不快"的锐利断言在上面那条
    确定性测试里。随机丢包的界收紧到贴着中位只会换来抖动——它在满量跑里已经
    红过一次。
    """
    took = []
    for seed in range(400, 406):
        proxy = FaultyProxy(registry.endpoint, drop_rate=0.4, seed=seed)
        try:
            env = dict(os.environ, TINYRAY_REGISTRY=proxy.endpoint)
            out = subprocess.run(
                [sys.executable, "-c", TIMED_JOIN],
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            assert out.returncode == 0, out.stderr[-500:]
            took.append(float(out.stdout.strip()))
        finally:
            proxy.close()

    took.sort()
    median = took[len(took) // 2]
    assert median < 15.0, f"首拍在把预算等满而不是重试：中位 {median:.1f}s，全部 {took}"


def test_join_returns_when_the_loop_registers_not_when_the_first_beat_gives_up(registry):
    """首拍和后台循环是**并发**的，所以注册成功可能来自首拍不知道的那一拍。

    首拍存在的理由只有一个：返回时调用者已经在册。一旦循环替它做到了，继续
    等下去就是纯粹的空等。实测 40% 丢包、`join(timeout=30)` 跑六个种子：循环
    在 **0.01s** 就拿到了 ack，首拍照样把 5s 预算等满，六次里占了三次；另有一
    次循环 1.75s 成功而首拍等到 5.0s，白白多花 3.25s。

    这里把链路先整个黑掉再放开，让"循环成功"必然发生在首拍之外：黑洞期间首拍
    的字节被吞掉，它只能等满 min(timeout, 5s)；放开后循环下一次重试就成功。
    等满的那种实测 5.0s，被叫醒的这种 1.75s。
    """
    proxy = FaultyProxy(registry.endpoint, drop_rate=1.0)
    opener = threading.Timer(1.2, lambda: setattr(proxy, "drop_rate", 0.0))
    try:
        env = dict(os.environ, TINYRAY_REGISTRY=proxy.endpoint)
        opener.start()
        out = subprocess.run(
            [sys.executable, "-c", TIMED_JOIN],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert out.returncode == 0, out.stderr[-500:]
        took = float(out.stdout.strip())
    finally:
        opener.cancel()
        proxy.close()
    assert took < 4.0, f"首拍在等满自己的预算而不是听循环的：{took:.2f}s"


def test_ctrl_c_during_join_is_not_swallowed_until_the_timeout(registry):
    """首拍在 Rust 里释放 GIL 阻塞，期间 Python 收不到信号——所以它拿多久的
    预算，就是 Ctrl-C 最坏要等多久。

    `join(timeout=)` 是给"等多久算失败"用的，不是给"多久之后才理你"用的。一个
    等着 300s 预算的 join 对 Ctrl-C 无动于衷 300s，看起来就是卡死。

    实测：链路整个黑掉、`join(timeout=60)`、两秒后 SIGINT —— 首拍只拿
    `min(timeout, 5s)` 时进程 **3.07s** 后退出；把整份预算交给首拍则要
    **58.06s**。

    这条测的是切片，不是重试：`test_join_returns_when_the_loop_registers_...`
    之后，丢包链路上的等待已经由循环兜住了，只有信号还必须穿过首拍。
    """
    proxy = FaultyProxy(registry.endpoint, drop_rate=1.0)
    try:
        env = dict(os.environ, TINYRAY_REGISTRY=proxy.endpoint)
        child = subprocess.Popen(
            [sys.executable, "-c", JOIN_AND_WAIT_FOR_SIGINT],
            env=env,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            assert child.stdout.readline().strip() == "go"
            time.sleep(2.0)
            sent = time.monotonic()
            child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=90)
            except subprocess.TimeoutExpired:
                child.kill()
                pytest.fail("join() swallowed Ctrl-C for at least 90s")
            waited = time.monotonic() - sent
        finally:
            if child.poll() is None:
                child.kill()
            child.stdout.close()
    finally:
        proxy.close()
    assert child.returncode == 3, f"没走到 KeyboardInterrupt：rc={child.returncode}"
    assert waited < 15.0, f"Ctrl-C 被首拍吞了 {waited:.1f}s"
