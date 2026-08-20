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
    import time, tinyray
    t0 = time.monotonic()
    try:
        me = tinyray.join("c", "churn")
        me.ready()
        print(f"OK {time.monotonic()-t0:.1f} {me.stats()['beats_ok']}")
        me.leave()
    except tinyray.Unreachable as e:
        print(f"UNREACHABLE {time.monotonic()-t0:.1f} {e}")
    """
)


def _run(endpoint: str, timeout: float = 90) -> tuple[str, float, str]:
    env = dict(os.environ, TINYRAY_REGISTRY=endpoint)
    out = subprocess.run(
        [sys.executable, "-c", PROBE], env=env, capture_output=True, text=True, timeout=timeout
    )
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
