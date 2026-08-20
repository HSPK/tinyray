"""失败的原因必须指向真凶。"""

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
from conftest import free_port

PROBE = textwrap.dedent(
    """
    import tinyray
    try:
        me = tinyray.join("c", "churn")
        print("JOINED")
        me.leave()
    except tinyray.Unreachable as e:
        print(str(e))
    """
)


class _Listener(threading.Thread):
    """接受连接，然后按 mode 行事。"""

    def __init__(self, mode: str):
        super().__init__(daemon=True)
        self.mode = mode
        self.held: list[socket.socket] = []
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.sock.listen(64)

    def run(self) -> None:
        while True:
            try:
                c, _ = self.sock.accept()
            except OSError:
                return
            if self.mode == "blackhole":
                self.held.append(c)
            else:
                threading.Thread(target=self._http11, args=(c,), daemon=True).start()

    def _http11(self, c: socket.socket) -> None:
        try:
            c.settimeout(5)
            c.recv(65536)
            body = b'{"epoch":1,"ttl_ms":4000,"accepted":true,"pools":{}}'
            c.sendall(
                b"HTTP/1.1 200 OK\r\ncontent-length: "
                + str(len(body)).encode()
                + b"\r\n\r\n"
                + body
            )
        except OSError:
            pass
        finally:
            c.close()

    def close(self) -> None:
        self.sock.close()
        for c in self.held:
            c.close()


def _reason(endpoint: str) -> str:
    env = dict(os.environ, TINYRAY_REGISTRY=endpoint)
    out = subprocess.run(
        [sys.executable, "-c", PROBE], env=env, capture_output=True, text=True, timeout=90
    )
    assert out.returncode == 0, out.stderr[-400:]
    got = out.stdout.strip()
    assert got != "JOINED", f"{endpoint} 居然 join 成功了"
    return got


def test_an_unreachable_port_is_not_blamed_on_the_protocol():
    """h2c 的提示挂在所有连接错误上，会把人送去查一个不存在的代理。"""
    got = _reason(f"127.0.0.1:{free_port()}")
    assert "cannot reach it" in got
    assert "h2c" not in got


def test_a_peer_that_only_speaks_http11_says_so():
    """假注册中心答了每一次连接 —— 网络完全通，谈不拢的是协议。
    修前这里报的是"注册中心没有回答"，运维会去查防火墙和 DNS。"""
    srv = _Listener("http11")
    srv.start()
    try:
        got = _reason(f"127.0.0.1:{srv.port}")
        assert "h2c" in got, got
        assert "the connection came up" in got, got
    finally:
        srv.close()


def test_a_peer_that_never_answers_is_reported_as_a_timeout():
    srv = _Listener("blackhole")
    srv.start()
    try:
        got = _reason(f"127.0.0.1:{srv.port}")
        assert "no reply within" in got, got
    finally:
        srv.close()


def test_last_error_describes_the_break_and_silence_says_if_it_is_current(registry):
    """两个一起读才有意义：silence_ms 说现在健康与否，last_error 说最近一次
    断裂长什么样。断言"健康时它必须是空的"是错的 —— 首拍在负载下偶尔失败
    再重试成功，本来就该留下记录（全量跑时这条断言挂过一次）。"""
    with tinyray.join("c", "churn") as me:
        me.ready()
        # 静默值在每拍之前天然会摸到一个完整间隔，所以阈值要从间隔推出来，
        # 不能写死一个正好等于间隔的数。
        healthy = me.stats()["interval_ms"] * 2
        deadline = time.monotonic() + 15
        while me.silence_ms > healthy and time.monotonic() < deadline:
            time.sleep(0.05)
        assert me.silence_ms <= healthy, "联系是健康的，这是后面比较的前提"
        registry.stop()
        # 等静默值增长，不能等 last_error 出现 —— 它保留历史，启动时的一次
        # 瞬时失败会让"等它出现"的循环立刻退出，那时最近一次成功才过去几毫秒。
        deadline = time.monotonic() + 15
        while me.silence_ms <= healthy and time.monotonic() < deadline:
            time.sleep(0.05)
        assert me.silence_ms > healthy, "注册中心死了 15s，静默值却没涨"
        assert "cannot reach it" in me.last_error or "no reply" in me.last_error, me.last_error
