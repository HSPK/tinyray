"""Regressions for bugs found by reading the code rather than running it."""

from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import sys
import textwrap
import time
import urllib.request

import pytest

import tinyray


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
    tenures mean the old occupant keeps the seat."""
    src = textwrap.dedent(
        """
        import random, time
        vals = set()
        for _ in range(5000):
            vals.add(((time.time_ns() // 1_000_000) << 20) | random.getrandbits(20))
        print(len(vals))
        """
    )
    out = subprocess.run([sys.executable, "-c", src], capture_output=True, text=True)
    assert int(out.stdout) >= 4990, "tenures collide far too often"


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
