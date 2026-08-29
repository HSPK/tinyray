"""从未联系上注册中心，和联系上之后失联，是两件不同的事。"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import textwrap
import threading
import time

import pytest
import tinyray
from conftest import BIN, free_port

PROBE = textwrap.dedent(
    """
    import sys, time, tinyray
    kw = {} if len(sys.argv) < 2 else {"timeout": float(sys.argv[1])}
    t0 = time.monotonic()
    try:
        me = tinyray.join("c", "churn", **kw)
        me.ready()
        print(f"OK {time.monotonic()-t0:.1f} {me.stats()['beats_ok']}")
        me.leave()
    except tinyray.Unreachable as e:
        print(f"UNREACHABLE {time.monotonic()-t0:.1f} {e}")
    """
)


def _run(
    endpoint: str, timeout: float = 90, join_timeout: float | None = None
) -> tuple[str, float, str]:
    env = dict(os.environ, TINYRAY_REGISTRY=endpoint)
    argv = [sys.executable, "-c", PROBE]
    if join_timeout is not None:
        argv.append(str(join_timeout))
    out = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=timeout)
    assert out.returncode == 0, out.stderr[-500:]
    kind, secs, rest = out.stdout.strip().split(" ", 2)
    return kind, float(secs), rest


@pytest.mark.parametrize(
    "endpoint",
    ["127.0.0.1:%d", "10.255.255.1:8760", "no-such-host-xyz.invalid:8760"],
    ids=["nobody-listening", "unroutable", "unresolvable"],
)
def test_join_refuses_to_pretend(endpoint):
    """修前三种情况都在 0.0-5.0s 内"成功"返回，accepted=True，零心跳，
    自己的池子里 0 个成员 —— 应用会一路装作正常直到几分钟后 wait() 超时。"""
    if "%d" in endpoint:
        endpoint = endpoint % free_port()
    kind, secs, msg = _run(endpoint)
    assert kind == "UNREACHABLE", f"{secs}s 后返回了 {kind}"
    assert "no answer from the registry" in msg


def test_a_registry_that_starts_late_is_waited_for():
    """启动顺序不该是承重结构：先起的 rank 要等得到后起的注册中心。"""
    port = free_port()
    started: list[subprocess.Popen] = []

    def later():
        time.sleep(4.0)
        started.append(
            subprocess.Popen(
                [str(BIN), "--listen", f"127.0.0.1:{port}", "--ttl-ms", "4000"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )

    t = threading.Thread(target=later)
    t.start()
    try:
        kind, secs, rest = _run(f"127.0.0.1:{port}")
        assert kind == "OK", rest
        assert 3.0 < secs < 9.0, f"等了 {secs}s，与注册中心的 4s 启动延迟对不上"
        assert int(rest) >= 1, "没有一拍心跳落地"
    finally:
        t.join()
        for p in started:
            p.terminate()


def test_losing_the_registry_after_joining_is_still_survivable(registry):
    """区别在于：已经联系上过的进程，缓存能把它带下去。"""
    with tinyray.join("c", "churn") as me:
        me.ready(host="h1")
        tinyray.pool("c").wait(count=1, timeout=10)
        registry.stop()
        time.sleep(2.0)
        assert me.silence_ms > 500
        assert len(tinyray.pool("c").all()) == 1, "缓存没有把它带下去"


def test_join_timeout_bounds_the_wait():
    """等待是常态，但等多久该由调用方决定：地址写错时想立刻知道，
    注册中心晚起时想等下去。"""
    dead = f"127.0.0.1:{free_port()}"
    for want in (1.0, 4.0):
        kind, secs, msg = _run(dead, join_timeout=want)
        assert kind == "UNREACHABLE", msg
        assert want <= secs < want + 3.0, f"要求等 {want}s，实际 {secs}s"
        assert "join(timeout=)" in msg, "报错没说怎么等更久"


def test_a_longer_timeout_outlasts_a_late_registry():
    """默认 30s 之外，调用方还能自己加码。"""
    port = free_port()
    started: list[subprocess.Popen] = []

    def later():
        time.sleep(8.0)
        started.append(
            subprocess.Popen(
                [str(BIN), "--listen", f"127.0.0.1:{port}", "--ttl-ms", "4000"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )

    t = threading.Thread(target=later)
    t.start()
    try:
        kind, secs, rest = _run(f"127.0.0.1:{port}", join_timeout=25.0)
        assert kind == "OK", rest
        assert 7.0 < secs < 15.0, f"等了 {secs}s，与 8s 的启动延迟对不上"
    finally:
        t.join()
        for p in started:
            p.terminate()


@pytest.mark.parametrize("want", [0.5, 2.0])
def test_the_budget_covers_reaching_the_registry_not_just_the_wait(want):
    """上面那条测试用的是"没人监听"的端口 —— 连接立刻被拒，所以两个写死的
    五秒预算根本没走到。真正会走到的是**接受连接却从不回话**的注册中心：
    挂死的进程、accept 之后黑洞的防火墙、后端全不健康的负载均衡器。

    修前实测（预算恒等于要求值加十秒）：

        join(timeout=0.5) 10502ms    join(timeout=3) 13003ms
        join(timeout=1.0) 11004ms    join(timeout=8) 18004ms

    十秒是两个五秒：第一拍一个，失败路径上 leave() 的告别一个 —— 而那是在
    替一个从来没注册上的成员道别。
    """
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    port = srv.getsockname()[1]
    srv.listen(16)
    held: list[socket.socket] = []
    stop = threading.Event()

    def accept_and_say_nothing():
        while not stop.is_set():
            try:
                held.append(srv.accept()[0])
            except OSError:
                return

    t = threading.Thread(target=accept_and_say_nothing, daemon=True)
    t.start()
    try:
        kind, secs, msg = _run(f"127.0.0.1:{port}", join_timeout=want, timeout=60)
        assert kind == "UNREACHABLE", msg
        assert secs < want + 1.5, f"要求等 {want}s，实际 {secs}s —— 预算之外还有固定开销"
    finally:
        stop.set()
        srv.close()
        for c in held:
            c.close()


def test_registry_url_beats_the_environment_and_does_not_touch_it(registry):
    """用环境变量配置一个库，是**进程级**的副作用，而且它比这次调用活得久。

    `TINYRAY_REGISTRY` 依然是常规通道 —— launcher 一次给所有 rank 设好，没人想
    在代码里逐个写。这个参数是给用不了它的调用方：嵌在别人进程里的库，为了调一次
    `join()` 去改 `os.environ`，改的是整个进程。

    所以这里把环境变量指向一个**没人监听**的地址，再用参数指向真的那个：能报到，
    就说明参数确实压过了环境变量；而且环境变量本身**不能被动过**。
    """
    dead = f"127.0.0.1:{free_port()}"
    os.environ["TINYRAY_REGISTRY"] = dead
    try:
        with tinyray.join("p", "churn", registry_url=registry.endpoint, timeout=20) as me:
            me.ready(who="mine")
            assert me.stats()["beats_ok"] > 0, "参数没有压过环境变量"
        assert os.environ["TINYRAY_REGISTRY"] == dead, "join() 动了调用方的环境变量"
    finally:
        os.environ["TINYRAY_REGISTRY"] = registry.endpoint


def test_the_unreachable_message_names_the_address_it_actually_dialled():
    """报错必须说出**真正拨过的**那个地址，而不是此刻环境变量里写着什么。

    地址有两个来源之后，这就是最容易出的那种错：错误路径重新读一次环境变量，于是
    `join(registry_url=X)` 失败时报的是 Y。这个代码库被"同一个事实两种拼法"咬过
    ——围栏令牌曾经有四种写法 —— 所以地址只解析一次，之后所有人都读那一份。
    """
    asked = f"127.0.0.1:{free_port()}"
    misleading = f"127.0.0.1:{free_port()}"
    os.environ["TINYRAY_REGISTRY"] = misleading
    try:
        with pytest.raises(tinyray.Unreachable) as e:
            tinyray.join("p", "churn", registry_url=asked, timeout=1.0)
        assert asked in str(e.value), f"报错没说出实际拨的地址：{e.value}"
        assert misleading not in str(e.value), f"报错说的是环境变量里的地址：{e.value}"
    finally:
        os.environ.pop("TINYRAY_REGISTRY", None)


def test_a_list_of_registries_is_refused_instead_of_dialled():
    """给一串地址会拼出 `http://a:1,b:2` —— 一个谁也连不上的 URL，而进程只会报
    "注册中心没应答"，这话是真的但没用。

    文档里这一条是自己挖的坑：它把变量写成 `TINYRAY_REGISTRY=host:port,...`，
    那个逗号看起来就是可以填多个。而故意不做故障转移是有理由的（增量游标是按
    注册中心算的，换一份会静默冻住缓存），所以正确的回答是**明确拒绝**。
    """
    for where in ("param", "env"):
        with pytest.raises(ValueError, match="only ever one registry"):
            if where == "param":
                tinyray.join("p", "churn", registry_url="127.0.0.1:1,127.0.0.1:2")
            else:
                os.environ["TINYRAY_REGISTRY"] = "127.0.0.1:1,127.0.0.1:2"
                try:
                    tinyray.join("p", "churn")
                finally:
                    os.environ.pop("TINYRAY_REGISTRY", None)
