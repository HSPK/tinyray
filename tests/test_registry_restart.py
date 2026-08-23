"""A restarted registry counts from zero again.

Losing the registry is meant to be a gap rather than an outage, and mostly it
is. But its change counter restarts with it, and a client holding a higher
number asks for changes since a version the new process has never reached --
gets told there are none, and keeps a stale roster forever with nothing
anywhere to say so.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
import urllib.request

import pytest
import tinyray

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

NAMED = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join("w", "churn") as m:
        m.ready(who=sys.argv[1])
        print("READY", flush=True)
        sys.stdin.readline()
    """
)

# Answers only when asked, so nothing accumulates in the pipe while the process
# is stopped.
OBSERVER = textwrap.dedent(
    """
    import json, sys, time, tinyray
    with tinyray.join("obs", "churn") as me:
        me.ready()
        w = tinyray.pool("w")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and len(w.all()) != 1:
            time.sleep(0.02)
        print(json.dumps({"synced": w._c.pool_info("w")[0], "n": len(w.all())}), flush=True)
        for line in sys.stdin:
            if line.strip() == "q":
                break
            info = w._c.pool_info("w")
            print(
                json.dumps(
                    {
                        "version": None if info is None else info[0],
                        "who": sorted(x for x in (h.state.get("who") for h in w.all()) if x),
                    }
                ),
                flush=True,
            )
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
            with urllib.request.urlopen(f"http://{registry.endpoint}/v1/pools", timeout=5) as r:
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
    for _ in range(3):
        p = subprocess.Popen(
            [sys.executable, "-c", NEWCOMER],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
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


def _spawn(script: str, *args: str) -> subprocess.Popen:
    p = subprocess.Popen(
        [sys.executable, "-c", script, *args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert p.stdout.readline().strip() == "READY", "member never came up"
    return p


def _ask(observer: subprocess.Popen) -> dict:
    observer.stdin.write("?\n")
    observer.stdin.flush()
    return json.loads(observer.stdout.readline())


def _server_version(endpoint: str, pool: str) -> int | None:
    with urllib.request.urlopen(f"http://{endpoint}/v1/pools", timeout=5) as r:
        got = json.loads(r.read()).get(pool)
    return None if got is None else got["version"]


def _await_version(endpoint: str, pool: str, at_least: int, timeout: float = 20) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = _server_version(endpoint, pool)
        if v is not None and v >= at_least:
            return v
        time.sleep(0.02)
    raise AssertionError(f"{pool} never reached version {at_least}")


@pytest.mark.skipif(not hasattr(signal, "SIGSTOP"), reason="needs SIGSTOP to remove the race")
def test_a_restart_whose_counter_climbs_past_ours_still_leaves_a_whole_roster(registry):
    """The other direction, and the dangerous one.

    When the fresh registry stays *below* the position we are holding it sends
    an empty delta, the pool is left out of the ack entirely, no cache entry is
    made, and the next beat -- asking with no position at all -- gets a full
    roster. That is the case the test above covers, and it recovers by itself.

    When the fresh registry climbs *past* that position, the delta is not empty.
    It lists what changed since a version number this process has never used,
    which is nobody's idea of a question: every member placed at or below that
    number is simply absent from the answer. Applying it on top of the cleared
    cache leaves a roster with a hole in it and a version that says we are up to
    date, so nothing ever asks again.

    Measured before the fix: two members present, the client holding one, and
    -- worst of it -- the client's roster fingerprint equal to the registry's,
    so an epoch would freeze on it and every rank would agree it was fine while
    holding different member lists.

    The observer is stopped outright rather than raced against: at ttl/4 it
    would otherwise usually beat once before the new members were placed, take
    the recovering path, and hide the bug about nine times in ten.
    """
    endpoint = registry.endpoint
    first = _spawn(NAMED, "m1")
    _await_version(endpoint, "w", 2)

    placed: list[subprocess.Popen] = []
    observer = subprocess.Popen(
        [sys.executable, "-c", OBSERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        synced = json.loads(observer.stdout.readline())
        assert synced["n"] == 1, f"the observer never synced to begin with: {synced}"
        held = synced["synced"]

        # Freeze it, so its next beat is the first one against the new process.
        os.kill(observer.pid, signal.SIGSTOP)

        _stop(first)
        registry.stop()
        registry.start()

        # Land one member at or below the position the observer still holds,
        # then keep going until the fresh counter is past it -- that overtaking
        # is the whole point, and how many members it takes depends on how far
        # the observer had got before the restart. Two was enough while a
        # watcher lagged a beat behind; it syncs to the latest now, so it can
        # be holding a higher number than two members will reach.
        placed.append(_spawn(NAMED, "mA"))
        _await_version(endpoint, "w", 1)
        extra = 0
        while _server_version(endpoint, "w") <= held:
            extra += 1
            assert extra <= 8, (
                f"eight members did not take the fresh counter past {held}; "
                f"it is at {_server_version(endpoint, 'w')}"
            )
            placed.append(_spawn(NAMED, f"m{extra}"))
        assert extra >= 1, "nothing was placed above the observer's position"
        expected = {"mA"} | {f"m{i}" for i in range(1, extra + 1)}

        os.kill(observer.pid, signal.SIGCONT)

        deadline = time.monotonic() + 30
        view = _ask(observer)
        while time.monotonic() < deadline:
            view = _ask(observer)
            if set(view["who"]) == expected:
                break
            time.sleep(0.1)

        assert set(view["who"]) == expected, (
            f"the observer holds {view['who']} at version {view['version']}, but the "
            f"registry has {_server_version(endpoint, 'w')} with both members; it was "
            f"holding version {held} from the previous registry process"
        )
    finally:
        try:
            os.kill(observer.pid, signal.SIGCONT)
        except ProcessLookupError:
            pass
        _stop(observer)
        for p in placed:
            _stop(p)
