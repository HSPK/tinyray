"""M1 acceptance: report in, find each other, survive the registry dying."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time

import pytest
import tinyray

DRIVER = textwrap.dedent(
    """
    import json, os, sys, time
    import tinyray
    pool_name, policy = sys.argv[1], sys.argv[2]
    count, hold_s = int(sys.argv[3]), float(sys.argv[4])
    me = tinyray.join(pool_name, policy)
    me.ready(worker=os.getpid())
    print("READY", flush=True)
    sys.stdin.readline()
    peers = tinyray.pool(pool_name).all()
    print(json.dumps({"seen": len(peers), "beats": me.stats()}), flush=True)
    sys.stdin.readline()
    """
)


def _spawn(n: int, pool_name: str, policy: str = "churn") -> list[subprocess.Popen]:
    procs = []
    for _ in range(n):
        p = subprocess.Popen(
            [sys.executable, "-c", DRIVER, pool_name, policy, "1", "0"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        procs.append(p)
    for p in procs:
        assert p.stdout.readline().strip() == "READY"
    return procs


def _shutdown(procs):
    for p in procs:
        try:
            p.stdin.write("\n\n")
            p.stdin.flush()
        except (BrokenPipeError, ValueError):
            pass
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def test_join_then_find_each_other(registry):
    me = tinyray.join("env", "churn")
    me.ready(role="driver")
    procs = _spawn(3, "env")
    try:
        peers = tinyray.pool("env").wait(count=4, timeout=10)
        assert len(peers) == 4
        assert all(h.ready for h in peers)
        assert {h.incarnation for h in peers}, "every member carries a tenure"
    finally:
        _shutdown(procs)
        me.leave()


def test_leaving_is_noticed_sooner_than_dying(registry):
    """A farewell beat frees the seat at once; a kill can only be noticed when
    the lease runs out.

    The two are measured against each other rather than against the clock. An
    absolute threshold looks fine on an idle machine and fails under load,
    while the gap between the two paths survives both.
    """

    def time_departure(how: str) -> float:
        procs = _spawn(1, "env")
        tinyray.pool("env").wait(count=2, timeout=15)
        t0 = time.monotonic()
        if how == "leave":
            _shutdown(procs)  # exits normally, so atexit sends the farewell
        else:
            procs[0].kill()
            procs[0].wait(timeout=5)
        deadline = t0 + registry.ttl_ms / 1000 * 4 + 5
        while len(tinyray.pool("env").all()) > 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(tinyray.pool("env").all()) == 1, f"{how} was never noticed"
        return time.monotonic() - t0

    me = tinyray.join("env", "churn")
    me.ready()
    try:
        graceful = time_departure("leave")
        killed = time_departure("kill")
        assert graceful < killed / 2, (
            f"a farewell took {graceful:.2f}s against {killed:.2f}s for a kill; "
            f"it should not be waiting out the lease"
        )
    finally:
        me.leave()


def test_dead_member_expires_without_a_supervisor(registry):
    me = tinyray.join("env", "churn")
    me.ready()
    procs = _spawn(2, "env")
    tinyray.pool("env").wait(count=3, timeout=10)
    for p in procs:  # SIGKILL: no farewell beat, only the lease can reap it
        p.kill()
        p.wait(timeout=5)

    deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 2
    while len(tinyray.pool("env").all()) > 1 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert len(tinyray.pool("env").all()) == 1
    me.leave()


def test_filter_by_state(registry):
    me = tinyray.join("engine", "serving")
    me.ready(model_version=17)
    tinyray.pool("engine").wait(count=1, timeout=10)

    assert len(tinyray.pool("engine").all(model_version=17)) == 1
    assert tinyray.pool("engine").all(model_version=18) == []
    with pytest.raises(tinyray.NotFound):
        tinyray.pool("engine").pick(model_version=18)

    me.ready(model_version=18)
    deadline = time.monotonic() + 5
    while not tinyray.pool("engine").all(model_version=18) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert len(tinyray.pool("engine").all(model_version=18)) == 1
    me.leave()


def test_unready_members_are_not_picked(registry):
    me = tinyray.join("engine", "serving")
    # Registered but never declared ready: present, but not eligible.
    deadline = time.monotonic() + 5
    while tinyray.pool("engine")._c.pool_info("engine") is None and time.monotonic() < deadline:
        time.sleep(0.02)
    with pytest.raises(tinyray.NotFound):
        tinyray.pool("engine").pick()
    me.ready()
    assert len(tinyray.pool("engine").wait(count=1, timeout=5)) == 1
    me.leave()


def test_missing_seat_raises_instead_of_substituting(registry):
    me = tinyray.join("dispatcher", "stateful", slot=0)
    me.ready()
    tinyray.pool("dispatcher").wait(count=1, timeout=10)
    assert tinyray.pool("dispatcher").slot(0).slot == 0
    with pytest.raises(tinyray.NotFound):
        tinyray.pool("dispatcher").slot(3)
    me.leave()


def test_slotted_policy_requires_a_seat(registry):
    with pytest.raises(tinyray.PolicyError):
        tinyray.join("trainer", "collective")


def test_lookup_before_join_is_explicit():
    import importlib

    mod = importlib.reload(tinyray)
    with pytest.raises(RuntimeError):
        mod.pool("anything")
