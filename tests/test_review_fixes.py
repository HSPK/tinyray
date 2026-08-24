"""Regressions for bugs found by reading the code rather than running it."""

from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import sys
import textwrap
import threading
import time
import urllib.request

import pytest
import tinyray
from tinyray import _tinyray


def _beat(endpoint: str, **kw) -> dict:
    body = dict(
        pool="t",
        id=0,
        slot=0,
        incarnation=1,
        policy="stateful",
        url=None,
        state={},
        ready=True,
        leaving=False,
        methods=[],
        watch=["t"],
        seen={},
    )
    body.update(kw)
    req = urllib.request.Request(
        f"http://{endpoint}/v1/beat",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def _pools(endpoint: str) -> dict:
    with urllib.request.urlopen(f"http://{endpoint}/v1/pools", timeout=5) as r:
        return json.loads(r.read())


def test_a_superseded_tenure_cannot_take_the_seat_back(registry):
    """The record disappears when a member is reaped, so the pool must keep a
    high-water mark. Otherwise the process that was replaced only has to wait
    for its replacement to die."""
    ep = registry.endpoint
    _beat(ep, incarnation=1)
    _beat(ep, incarnation=2)
    assert _beat(ep, incarnation=1)["accepted"] is False

    deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 2
    while _pools(ep)["t"]["members"] and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _pools(ep)["t"]["members"] == 0, "the replacement should have expired"

    assert _beat(ep, incarnation=1)["accepted"] is False, "the ghost came back"
    assert _pools(ep)["t"]["members"] == 0


def test_leaving_with_a_mismatched_tenure_keeps_the_fingerprint_clean(registry):
    """XOR-ing out the beat's tenure instead of the stored one would leave the
    roster permanently wrong, and a wrong roster voids rounds forever."""
    ep = registry.endpoint
    _beat(ep, pool="r", id=7, incarnation=10)
    assert _pools(ep)["r"]["roster"] != 0
    _beat(ep, pool="r", id=7, incarnation=99, leaving=True)
    assert _pools(ep)["r"]["roster"] == 0


def test_joining_twice_is_refused_rather_than_leaking_a_heartbeat(registry):
    me = tinyray.join("env", "churn")
    me.ready()
    with pytest.raises(RuntimeError, match="already joined"):
        tinyray.join("env", "churn")
    me.leave()
    again = tinyray.join("env", "churn")  # allowed once the first one left
    again.leave()


def test_tenure_differs_for_two_processes_in_the_same_millisecond():
    """A millisecond timestamp alone collides on a fast restart, and equal
    tenures mean the old occupant keeps the seat.

    The threshold comes from the birthday bound rather than taste: with n
    draws from the 20 random bits, expected collisions are n^2 / 2^21, so
    5,000 draws inside one millisecond collide about twelve times. Anything
    near that is fine; a timestamp alone would collide thousands of times.
    """
    n = 5000
    src = textwrap.dedent(
        f"""
        import random, time
        vals = set()
        for _ in range({n}):
            vals.add(((time.time_ns() // 1_000_000) << 20) | random.getrandbits(20))
        print(len(vals))
        """
    )
    out = subprocess.run([sys.executable, "-c", src], capture_output=True, text=True)
    unique = int(out.stdout)
    expected_collisions = n * n / 2**21
    assert unique >= n - expected_collisions * 5, (
        f"{n - unique} collisions, far above the ~{expected_collisions:.0f} the "
        f"birthday bound predicts"
    )

    # And the point of the random bits: a bare millisecond timestamp is not
    # enough on its own.
    bare = textwrap.dedent(
        f"""
        import time
        vals = set()
        for _ in range({n}):
            vals.add(time.time_ns() // 1_000_000)
        print(len(vals))
        """
    )
    bare_unique = int(
        subprocess.run([sys.executable, "-c", bare], capture_output=True, text=True).stdout
    )
    assert bare_unique < unique / 10, "the timestamp alone was suspiciously unique"


def test_advertised_address_is_reachable_from_elsewhere(registry):
    """Publishing 127.0.0.1 from a multi-node job is silent misrouting."""
    addr = tinyray._advertise()
    assert not addr.startswith("127."), f"advertising loopback address {addr}"
    with socket.socket() as s:
        s.bind((addr, 0))  # it must actually be one of ours


ASYNC_PEER = textwrap.dedent(
    """
    import sys, tinyray
    class S:
        def ping(self, x: int) -> int: return x
    with tinyray.join("aconn", "stateful", slot=0, serves=S()) as me:
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


def test_async_calls_reuse_one_connection(registry):
    """The sync path was fixed first; the async path kept sending
    `connection: close` and burned a socket per call."""
    me = tinyray.join("driver", "churn")
    me.ready()
    proc = subprocess.Popen(
        [sys.executable, "-c", ASYNC_PEER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout.readline().strip() == "READY"
    tinyray.pool("aconn").wait(count=1, timeout=10)

    def time_wait() -> int:
        out = subprocess.run(["ss", "-tan"], capture_output=True, text=True).stdout
        return out.count("TIME-WAIT")

    async def drive() -> None:
        h = tinyray.apool("aconn").slot(0)
        for i in range(200):
            assert await h.ping(i) == i

    before = time_wait()
    asyncio.run(drive())
    churn = (time_wait() - before) / 200
    try:
        assert churn < 0.1, f"{churn:.2f} new sockets per call; keep-alive is not working"
    finally:
        proc.stdin.write("\n")
        proc.stdin.flush()
        proc.wait(timeout=5)
        me.leave()


def test_a_watchdog_thread_survives_shutdown(registry):
    """The documented way to use ep.valid is a background thread, so the
    library must not blow up in that thread when the member leaves.

    leave() used to hold pyo3's mutable borrow for the whole of a network
    round trip, and anything touching the client in that window raised
    "Already mutably borrowed". The window is sub-millisecond, so this polls
    flat out from several threads across several cycles: with the bug present
    that catches it every time, with it fixed, never.
    """
    errors: list[str] = []
    for _ in range(6):
        me = tinyray.join("trainer", "collective", slot=0, size=1)
        me.ready()
        ep = tinyray.pool("trainer").epoch(timeout=10)
        stop = threading.Event()

        def watchdog(stop: threading.Event = stop, ep: tinyray.Epoch = ep) -> None:
            # 绑成默认参数：闭包捕获循环变量，下一轮会把它们换掉。
            while not stop.is_set():
                try:
                    _ = ep.valid  # 读它就是在查注册中心，这正是要压的那条路
                except BaseException as exc:  # noqa: BLE001 - any failure counts
                    errors.append(repr(exc))
                    return

        threads = [threading.Thread(target=watchdog, daemon=True) for _ in range(4)]
        for t in threads:
            t.start()
        try:
            time.sleep(0.02)
            me.leave()  # a blocking round trip, with watchdogs hammering away
            time.sleep(0.02)
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=5)
            me.leave()
        if errors:
            break
    assert not errors, f"watchdog died during shutdown: {errors[0]}"


def test_a_beat_still_in_flight_cannot_undo_a_leave(registry):
    """离开之后，一个还在路上的心跳不许把它重新注册回来。

    `leave()` 发出最后一拍，但心跳循环可能已经把下一拍送出去了 —— 那一拍带着
    同样的 (id, 任期)，到达时座位刚被腾空，于是又被当成一次正常注册收下。原始
    实测是 300 次 leave 里有 6 次留下了没走掉的成员。

    这里不去复现那个竞态 —— 2% 的概率意味着测试要跑几百轮才看得见。改成直接量
    机制：注册表把刚离开的任期记住一个租约的时间，之后带着那个任期来的心跳一律
    当幽灵。用 `Client` 直接指定任期就能确定地送出这样一拍。
    """
    # 座位号得落在世界里 —— 写这条测试时随手挑了 4242 配 size=1，被后来加的
    # 形状校验当场拦下。座位是 0 号，id 就是 0，补那一拍照样构造得出来。
    ident = 0
    with tinyray.join("late", "stateful", slot=ident, size=1) as me:
        me.ready()
        tenure = me.incarnation
        pool = tinyray.pool("late")
        assert len(pool.wait(count=1, timeout=15)) == 1
        me.leave()

    # 座位确实空了。
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and _pools(registry.endpoint)["late"]["members"]:
        time.sleep(0.05)
    assert _pools(registry.endpoint)["late"]["members"] == 0, "leave() 之后成员还在"

    # 现在补上那一拍：同样的座位、同样的任期，就像它从未离开。
    straggler = _tinyray.Client(
        endpoint=f"http://{registry.endpoint}",
        pool="late",
        id=ident,
        incarnation=tenure,
        policy="stateful",
        slot=ident,
        size=1,
    )
    try:
        straggler.start()
        time.sleep(0.5)
        assert not straggler.accepted, "刚离开的任期又被收下了"
        assert _pools(registry.endpoint)["late"]["members"] == 0, (
            "一个在途的心跳把已经离开的成员复活了"
        )
    finally:
        straggler.abandon()
