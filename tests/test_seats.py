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
