"""方法服务端是手写的 HTTP：直接对它发原始报文，绕过 SDK。"""

from __future__ import annotations

import socket
import subprocess
import sys
import textwrap
import time

import pytest
import tinyray

SERVER = textwrap.dedent(
    """
    import sys, threading, tinyray
    class S:
        def ping(self) -> str: return "pong"
        def threads(self) -> int: return threading.active_count()
        def unserializable(self): return object()
        def selfref(self):
            d = {}; d["me"] = d; return d
    m = tinyray.join("s", "stateful", slot=0, size=1, serves=S())
    m.ready()
    print(m._server.port, flush=True)
    sys.stdin.readline()
    """
)


@pytest.fixture
def served(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    port = int(p.stdout.readline().strip())
    try:
        yield port
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


def _raw(port: int, head: bytes, body: bytes = b"", timeout: float = 20.0) -> bytes:
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    try:
        s.sendall(head + body)
        return s.recv(8192)
    finally:
        s.close()


def _post(path: bytes, cl: bytes) -> bytes:
    return b"POST " + path + b" HTTP/1.1\r\nHost: x\r\ncontent-length: " + cl + b"\r\n\r\n"


@pytest.mark.parametrize(
    "cl,expect",
    [(b"abc", b"400"), (b"-5", b"400"), (b"2", b"200")],
)
def test_a_malformed_content_length_gets_an_answer(served, cl, expect):
    """处理线程死掉时调用方看到的是连接被重置 —— 那读起来像"对端挂了"。"""
    got = _raw(served, _post(b"/call/ping", cl), b"{}")
    assert got.split(b"\r\n")[0].split()[1] == expect, got[:80]


def test_a_body_that_never_arrives_releases_the_thread(served):
    """声明 body 却不发，实测能占住 200 个线程直到攻击方自己松手。"""
    before = tinyray_call(served, b"/call/threads")
    holders = [socket.create_connection(("127.0.0.1", served), timeout=30) for _ in range(20)]
    for s in holders:
        s.sendall(_post(b"/call/ping", b"999999"))
    time.sleep(0.5)
    during = tinyray_call(served, b"/call/threads")
    assert during >= before + 15, f"线程没有被占住，测试本身失效了: {before} -> {during}"
    holders[0].settimeout(40)
    got = holders[0].recv(4096)
    assert b"408" in got.split(b"\r\n")[0], got[:80]
    for s in holders:
        s.close()


def tinyray_call(port: int, path: bytes) -> int:
    import json

    raw = _raw(port, _post(path, b"2"), b"{}")
    return json.loads(raw.split(b"\r\n\r\n")[-1])["result"]


@pytest.mark.parametrize("method", ["unserializable", "selfref"])
def test_a_return_value_json_cannot_carry_is_reported_not_dropped(served, method):
    """否则调用方拿到的是 Unreachable，指向错误的方向。"""
    with tinyray.join("c", "churn") as me:
        me.ready()
        h = tinyray.pool("s").wait(count=1, timeout=15)[0]
        with pytest.raises(tinyray.RemoteError, match="cannot be sent as JSON"):
            getattr(h, method)()
        assert h.ping() == "pong", "报错之后连接必须还能用"


STALE_SRV = textwrap.dedent(
    """
    import sys, tinyray
    class S:
        def who(self) -> str: return "ok"
    with tinyray.join("s", "stateful", slot=0, size=1, serves=S()) as m:
        m.ready(); print(m._server.port, flush=True); sys.stdin.readline()
    """
)


def test_a_fenced_call_does_not_poison_the_next_one(registry):
    """提前回复而不读掉请求体，会让 keep-alive 的下一个请求从残字节开始解析。

    实测表现为完美交替：过期句柄第 1、3、5 轮拿到 Fenced，第 2、4、6 轮拿到
    Unreachable（空 body），因为每个被围栏拒绝的请求都毁掉了它后面那一个。
    """
    p = subprocess.Popen(
        [sys.executable, "-c", STALE_SRV], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        port = int(p.stdout.readline().strip())
        with tinyray.join("c", "churn") as me:
            me.ready()
            good = tinyray.pool("s").wait(count=1, timeout=15)[0]
            stale = tinyray.Handle(
                "s",
                {
                    "id": 0,
                    "slot": 0,
                    "incarnation": good.incarnation - 1,
                    "url": f"http://127.0.0.1:{port}",
                    "ready": True,
                    "state": {},
                },
                ("who",),
            )
            for i in range(6):
                with pytest.raises(tinyray.Fenced):
                    stale.who()
                assert good.who() == "ok", f"第 {i + 1} 轮：围栏拒绝毁掉了下一个请求"
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
