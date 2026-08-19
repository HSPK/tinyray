"""The two acceptance criteria M1 exists to prove."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time

import tinyray

PEER = textwrap.dedent(
    """
    import os, sys, tinyray
    me = tinyray.join("env", "churn")
    me.ready(worker=os.getpid())
    print("READY", flush=True)
    sys.stdin.readline()
    """
)


def test_registry_can_die_and_peers_still_find_each_other(registry):
    """The promise: kill every replica and already-connected work continues."""
    me = tinyray.join("env", "churn")
    me.ready(role="driver")
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", PEER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
        )
        for _ in range(3)
    ]
    try:
        for p in procs:
            assert p.stdout.readline().strip() == "READY"
        before = tinyray.pool("env").wait(count=4, timeout=10)
        assert len(before) == 4

        registry.stop()
        # Long enough that every lease would have expired had the registry
        # been alive to notice.
        time.sleep(registry.ttl_ms / 1000 * 2)

        after = tinyray.pool("env").all()
        assert len(after) == 4, "cache must survive the registry, not expire with it"
        assert {h.url for h in after} == {h.url for h in before}
        assert {h.incarnation for h in after} == {h.incarnation for h in before}

        failed = me.stats()["beats_failed"]
        assert failed > 0, "beats should be visibly failing while the registry is down"

        # And it regrows from soft state: nothing was persisted anywhere.
        registry.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if len(tinyray.pool("env").all()) == 4 and me.stats()["beats_ok"] > 1:
                break
            time.sleep(0.1)
        assert len(tinyray.pool("env").all()) == 4
    finally:
        for p in procs:
            try:
                p.stdin.write("\n")
                p.stdin.flush()
                p.wait(timeout=5)
            except Exception:
                p.kill()
        me.leave()


def test_heartbeat_survives_the_main_thread_holding_the_gil(registry):
    """The whole reason the heartbeat is Rust.

    A rank inside ``dist.all_reduce`` holds the GIL for a long time. A Python
    heartbeat thread would stall, the lease would expire, and a healthy rank
    would be declared dead. A native thread does not need the GIL.
    """
    me = tinyray.join("trainer", "collective", slot=0, size=1)
    me.ready()
    tinyray.pool("trainer").wait(count=1, timeout=10)

    hold = registry.ttl_ms / 1000 * 5  # 10s against a 2s lease
    start_beats = me.stats()["beats_ok"]

    t0 = time.monotonic()
    x = 0
    while time.monotonic() - t0 < hold:  # pure Python, never yields to a peer
        for _ in range(50_000):
            x = (x * 31 + 7) % 1000003

    assert me.accepted, "the seat must not have been handed to anyone else"
    beats = me.stats()["beats_ok"] - start_beats
    expected = hold / (registry.ttl_ms / 1000 / 4)
    assert beats >= expected * 0.5, f"only {beats} beats in {hold}s while the GIL was held"
    assert me.stats()["beats_failed"] == 0

    assert len(tinyray.pool("trainer").all()) == 1, "we were declared dead while alive"
    me.leave()
