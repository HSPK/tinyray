"""Guards against the failure mode that produced 2.275 GB of roster pushes.

If a routine heartbeat counted as a change, every client would receive every
member on every beat -- O(N^2) traffic that looks correct and is fatal.
"""

from __future__ import annotations

import time

import tinyray


def _version(pool_name: str) -> int:
    return tinyray.pool(pool_name)._c.pool_info(pool_name)[0]


def test_idle_heartbeats_do_not_bump_the_version(registry):
    me = tinyray.join("env", "churn")
    me.ready(role="driver")
    tinyray.pool("env").wait(count=1, timeout=10)

    # Let the join-and-ready sequence finish landing. wait() returns on the
    # first ready member the cache shows, which can be a beat before the last
    # of that sequence has reached the registry -- and a baseline taken then
    # counts the tail of the arrival as if it were an idle beat.
    time.sleep(registry.ttl_ms / 1000 / 2)
    start_version = _version("env")
    start_beats = me.stats()["beats_ok"]
    time.sleep(registry.ttl_ms / 1000 * 3)  # a dozen heartbeats, no changes

    beats = me.stats()["beats_ok"] - start_beats
    assert beats >= 3, f"expected several heartbeats, saw {beats}"
    assert _version("env") == start_version, (
        f"{beats} idle heartbeats bumped the version by "
        f"{_version('env') - start_version}; expiry time must stay out of it"
    )
    me.leave()


def test_state_changes_bump_the_version_but_not_the_roster(registry):
    """Two numbers because one cannot answer both questions.

    Publishing progress must reach other processes, and must not invalidate a
    frozen round: the same event has to count as changed and unchanged.
    """
    me = tinyray.join("engine", "serving")
    me.ready(model_version=17)
    tinyray.pool("engine").wait(count=1, timeout=10)

    v0, roster0, _, _ = tinyray.pool("engine")._c.pool_info("engine")

    me.ready(model_version=18)
    deadline = time.monotonic() + 5
    while tinyray.pool("engine")._c.pool_info("engine")[0] == v0 and time.monotonic() < deadline:
        time.sleep(0.02)

    v1, roster1, _, _ = tinyray.pool("engine")._c.pool_info("engine")
    assert v1 > v0, "peers must learn about the new model version"
    assert roster1 == roster0, "the same people are still here; a round must not be voided"
    me.leave()


def test_roster_changes_when_someone_arrives(registry):
    import subprocess
    import sys
    import textwrap

    me = tinyray.join("env", "churn")
    me.ready()
    tinyray.pool("env").wait(count=1, timeout=10)
    _, roster0, _, _ = tinyray.pool("env")._c.pool_info("env")

    peer = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import sys, tinyray
                me = tinyray.join("env", "churn"); me.ready()
                print("READY", flush=True); sys.stdin.readline()
                """
            ),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert peer.stdout.readline().strip() == "READY"
        tinyray.pool("env").wait(count=2, timeout=10)
        _, roster1, _, _ = tinyray.pool("env")._c.pool_info("env")
        assert roster1 != roster0, "a new occupant must change the fingerprint"
    finally:
        peer.stdin.write("\n")
        peer.stdin.flush()
        peer.wait(timeout=5)

    # XOR is self-inverse, so losing the newcomer restores the original value.
    deadline = time.monotonic() + 10
    while tinyray.pool("env")._c.pool_info("env")[1] != roster0 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert tinyray.pool("env")._c.pool_info("env")[1] == roster0
    me.leave()
