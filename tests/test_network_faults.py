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
    lost packet hung it for as long as anyone was willing to wait."""
    proxy = FaultyProxy(registry.endpoint, drop_rate=0.4, seed=1)
    try:
        # The timeout here is the assertion: without a bounded beat this never
        # returns at all.
        run_against(proxy, seconds=3, timeout=45)
        assert proxy.stats()["dropped"] > 0, "the proxy never actually dropped anything"
    finally:
        proxy.close()


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
