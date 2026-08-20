"""A restarted registry counts from zero again.

Losing the registry is meant to be a gap rather than an outage, and mostly it
is. But its change counter restarts with it, and a client holding a higher
number asks for changes since a version the new process has never reached --
gets told there are none, and keeps a stale roster forever with nothing
anywhere to say so.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import time
import urllib.request

import tinyray
from conftest import BIN

BUMP = textwrap.dedent(
    """
    import sys, time, tinyray
    with tinyray.join("w", "churn") as m:
        for i in range(int(sys.argv[1])):
            m.ready(n=i)
            time.sleep(0.002)
        print("DONE", flush=True)
        sys.stdin.readline()
    """
)

NEWCOMER = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join("w", "churn") as m:
        m.ready(who="newcomer")
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


def _stop(p: subprocess.Popen) -> None:
    try:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=10)
    except Exception:
        p.kill()


def test_a_restart_does_not_freeze_the_cache(registry):
    """The client's position has to be read against the registry that issued
    it, or a restart with fewer changes than before is invisible."""
    me = tinyray.join("obs", "churn")
    me.ready()
    watched = tinyray.pool("w")

    # Push the pool's counter well past what a fresh registry will reach.
    bumper = subprocess.Popen(
        [sys.executable, "-c", BUMP, "200"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert bumper.stdout.readline().strip() == "DONE"
        # Wait on the counter itself: an entry now appears as soon as the
        # registry answers, empty pool included, so its presence says nothing
        # about how far the counter got.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            info = watched._c.pool_info("w")
            if info is not None and info[0] > 5:
                break
            time.sleep(0.05)
        info = watched._c.pool_info("w")
        assert info is not None
        high = info[0]
        assert high > 5, f"the counter only reached {high}"
    finally:
        _stop(bumper)

    time.sleep(0.5)
    registry.stop()
    registry.start()

    newcomer = subprocess.Popen(
        [sys.executable, "-c", NEWCOMER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert newcomer.stdout.readline().strip() == "READY"
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if any(h.state.get("who") == "newcomer" for h in watched.all()):
                break
            time.sleep(0.1)
        else:
            with urllib.request.urlopen(
                f"http://{registry.endpoint}/v1/pools", timeout=5
            ) as r:
                server_side = json.loads(r.read()).get("w")
            raise AssertionError(
                f"never saw the newcomer; registry has {server_side}, client is "
                f"stuck at version {watched._c.pool_info('w')}"
            )
        assert watched._c.pool_info("w")[0] < high, "the client kept the old numbering"
    finally:
        _stop(newcomer)
        me.leave()


def test_membership_regrows_after_a_restart(registry):
    """The plain case: soft state means nothing has to be recovered."""
    peers = []
    for i in range(3):
        p = subprocess.Popen(
            [sys.executable, "-c", NEWCOMER], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            text=True,
        )
        assert p.stdout.readline().strip() == "READY"
        peers.append(p)

    me = tinyray.join("obs", "churn")
    me.ready()
    try:
        assert len(tinyray.pool("w").wait(count=3, timeout=20)) == 3
        registry.stop()
        time.sleep(registry.ttl_ms / 1000 * 1.5)
        # Still resolvable from cache while it is gone.
        assert len(tinyray.pool("w").all()) == 3
        registry.start()

        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if len(tinyray.pool("w").all()) == 3 and me.silence_ms < 2000:
                break
            time.sleep(0.1)
        assert len(tinyray.pool("w").all()) == 3
    finally:
        for p in peers:
            _stop(p)
        me.leave()
