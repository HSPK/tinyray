"""Seat semantics: last-writer-wins by default, first-wins on request."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time

import pytest
import tinyray

HOLDER = textwrap.dedent(
    """
    import sys, tinyray
    kw = {"exclusive": True} if sys.argv[1] == "exclusive" else {}
    me = tinyray.join("seat", "stateful", slot=0, **kw)
    me.ready(who=sys.argv[2])
    print("HELD", flush=True)
    sys.stdin.readline()
    """
)


def _hold(mode: str, who: str) -> subprocess.Popen:
    p = subprocess.Popen(
        [sys.executable, "-c", HOLDER, mode, who],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert p.stdout.readline().strip() == "HELD"
    return p


def _release(p: subprocess.Popen) -> None:
    try:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=5)
    except Exception:
        p.kill()


def test_a_restarting_member_reclaims_its_seat(registry):
    """The default has to be last-writer-wins: a rank that comes back must get
    its seat even though the dead one's lease is still running."""
    first = _hold("default", "first")
    me = tinyray.join("watch", "churn")
    me.ready()
    held = tinyray.pool("seat").wait(count=1, timeout=10)[0]
    assert held.state["who"] == "first"

    first.kill()  # lease still ticking
    second = _hold("default", "second")
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            seat = tinyray.pool("seat").all()
            if seat and seat[0].state.get("who") == "second":
                break
            time.sleep(0.05)
        assert tinyray.pool("seat").slot(0).state["who"] == "second"
    finally:
        _release(second)
        me.leave()


def test_exclusive_refuses_an_occupied_seat(registry):
    first = _hold("exclusive", "first")
    try:
        with pytest.raises(tinyray.SeatTaken, match="already held"):
            tinyray.join("seat", "stateful", slot=0, exclusive=True)
    finally:
        _release(first)


def test_exclusive_succeeds_once_the_lease_lapses(registry):
    first = _hold("exclusive", "first")
    first.kill()  # no farewell, so only expiry frees the seat

    deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 3
    took = None
    while time.monotonic() < deadline:
        try:
            took = tinyray.join("seat", "stateful", slot=0, exclusive=True)
            break
        except tinyray.SeatTaken:
            time.sleep(0.1)
    assert took is not None, "the seat never freed up"
    took.ready(who="second")
    took.leave()


def test_the_short_label_stays_readable(registry):
    me = tinyray.join("engine", "serving", slot=3)
    me.ready()
    h = tinyray.pool("engine").wait(count=1, timeout=10)[0]
    assert h.label.startswith("engine/3#")
    assert len(h.label) < 24, f"{h.label} is not a label a human reads"
    # identity stays exact: it is the fencing token, not a display string.
    assert h.identity == f"engine/3#{h.incarnation}"
    me.leave()


SURVIVOR = textwrap.dedent(
    """
    import sys, time, tinyray
    class W:
        def whoami(self) -> str:
            return sys.argv[1]
    me = tinyray.join("ghost", "stateful", slot=0, serves=W())
    me.ready(gen=sys.argv[1])
    print(f"HELD {tinyray.pool('ghost').slot(0).url}", flush=True)
    # Deliberately keeps running and listening after being replaced.
    sys.stdin.readline()
    """
)


def test_a_superseded_process_stops_answering(registry):
    """The registry refuses a ghost's heartbeat, but nothing stops the ghost
    itself: it keeps running with its port open, and a caller holding the old
    handle would get a cheerful reply from the wrong process. The identity
    header cannot catch this -- the ghost's identity is exactly what the stale
    handle asks for -- so the process has to notice it lost the seat.
    """
    first = subprocess.Popen(
        [sys.executable, "-c", SURVIVOR, "gen-1"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert first.stdout.readline().startswith("HELD")

    me = tinyray.join("watch", "churn")
    me.ready()
    stale = tinyray.pool("ghost").wait(count=1, timeout=10)[0]
    assert stale.whoami() == "gen-1"

    second = subprocess.Popen(
        [sys.executable, "-c", SURVIVOR, "gen-2"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert second.stdout.readline().startswith("HELD")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            live = tinyray.pool("ghost").all()
            if live and live[0].incarnation != stale.incarnation:
                break
            time.sleep(0.05)
        assert tinyray.pool("ghost").slot(0).whoami() == "gen-2"

        # The old process is alive and still listening on that address. It only
        # learns it is a ghost from the registry's answer to its own heartbeat,
        # so "stops answering" is within a beat, not instantly.
        refused = None
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                stale.whoami()
            except tinyray.Fenced as exc:
                refused = exc
                break
            time.sleep(0.05)
        assert refused is not None, "the ghost kept serving its old identity"
    finally:
        _release(second)
        _release(first)
        me.leave()
