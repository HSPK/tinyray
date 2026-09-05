"""Subscribing to a pool must not cost a whole heartbeat interval.

An earlier version only updated the watch list locally and let the beat loop
notice when it next woke up. Discovery then took up to a full interval, which
is long enough for short-lived peers to appear and vanish unseen.

The measurement is repeated across several pools because a single sample can
land just before a scheduled beat and look fast by luck.
"""

from __future__ import annotations

import random
import subprocess
import sys
import textwrap
import time

import tinyray

PEER = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join(sys.argv[1], "churn") as me:
        me.ready(tag=sys.argv[1])
        print("READY", flush=True)
        sys.stdin.readline()
    """
)
ROUNDS = 8


def test_watching_a_new_pool_takes_effect_immediately(registry):
    """Every subscription is fast, not just the lucky ones."""
    random.seed(0)
    procs = []
    for i in range(ROUNDS):
        p = subprocess.Popen(
            [sys.executable, "-c", PEER, f"late{i}"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert p.stdout.readline().strip() == "READY"
        procs.append(p)

    me = tinyray.join("driver", "churn")
    me.ready()
    interval = registry.ttl_ms / 1000 / 4
    samples = []
    try:
        for i in range(ROUNDS):
            # Sleep a random slice of the interval. A fixed pause lands on
            # the same phase every round and would look fast either way.
            time.sleep(random.uniform(0.0, interval))
            t0 = time.monotonic()
            found = tinyray.pool(f"late{i}").wait(count=1, timeout=10)
            samples.append(time.monotonic() - t0)
            assert len(found) == 1
    finally:
        for p in procs:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        me.leave()

    # Measured: waking the loop gives 50ms flat (the polling granularity),
    # while leaving it to its own schedule gives a median of 451ms against a
    # 500ms interval. A quarter of an interval separates the two cleanly.
    worst = max(samples)
    assert worst < interval / 4, (
        f"slowest subscription took {worst:.3f}s against a {interval:.3f}s "
        f"heartbeat interval; samples={[round(s, 3) for s in samples]}"
    )


def test_declaring_readiness_reaches_peers_promptly(registry):
    """ready() publishes; it must not sit in a buffer until the next tick."""
    random.seed(1)
    me = tinyray.join("engine", "serving")
    tinyray.pool("engine")  # watch before anyone is ready
    interval = registry.ttl_ms / 1000 / 4
    samples = []
    for version in range(ROUNDS):
        time.sleep(random.uniform(0.0, interval))
        t0 = time.monotonic()
        me.ready(model_version=version)
        found = tinyray.pool("engine").wait(count=1, timeout=10, model_version=version)
        samples.append(time.monotonic() - t0)
        assert len(found) == 1
    me.leave()

    worst = max(samples)
    assert worst < interval / 4, (
        f"slowest publish took {worst:.3f}s against a {interval:.3f}s interval; "
        f"samples={[round(s, 3) for s in samples]}"
    )
