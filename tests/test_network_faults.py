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


def test_lost_packets_do_not_cost_the_lease(registry):
    """The beat loop is serial, so a request deadline is also how long a lost
    packet stops us beating. A five-second deadline against a 500 ms interval
    meant one drop took the member out of the roster."""
    clean = FaultyProxy(registry.endpoint)
    try:
        baseline, _, _ = run_against(clean, seconds=8, timeout=60)
    finally:
        clean.close()

    lossy = FaultyProxy(registry.endpoint, drop_rate=0.3, seed=2)
    try:
        ok, failed, seen = run_against(lossy, seconds=8, timeout=60)
    finally:
        lossy.close()

    assert failed > 0, "no packets were lost, so this proved nothing"
    # Measured against a clean run in the same session rather than a fixed
    # number: an absolute threshold holds on an idle machine and not under
    # load. Stalling behind the timeout took this to one beat in thirteen
    # seconds; keeping the deadline inside the interval keeps most of them.
    assert ok >= baseline / 3, (
        f"{ok} beats through with loss against {baseline} clean; the loop is "
        f"stalling behind its own timeout"
    )
    assert seen == 1, "the member could not see itself in its own pool"


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
