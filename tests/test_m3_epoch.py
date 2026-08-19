"""M3 acceptance: freeze a round, and notice when it breaks."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time

import pytest

import tinyray

RANK = textwrap.dedent(
    """
    import sys, tinyray
    rank, world = int(sys.argv[1]), int(sys.argv[2])
    with tinyray.join("trainer", "collective", slot=rank, size=world) as me:
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


class Ranks:
    def __init__(self, n: int, world: int, first: int = 1):
        self.procs = []
        for r in range(first, first + n):
            p = subprocess.Popen(
                [sys.executable, "-c", RANK, str(r), str(world)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            assert p.stdout.readline().strip() == "READY"
            self.procs.append(p)

    def kill(self, i: int) -> None:
        self.procs[i].kill()
        self.procs[i].wait(timeout=5)

    def stop(self) -> None:
        for p in self.procs:
            try:
                p.stdin.write("\n")
                p.stdin.flush()
                p.wait(timeout=5)
            except Exception:
                p.kill()


def test_a_round_waits_for_everyone_then_freezes(registry):
    world = 4
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(world - 1, world)
    try:
        ep = tinyray.pool("trainer").epoch(timeout=20)
        assert len(ep) == world
        assert sorted(h.slot for h in ep) == [0, 1, 2, 3]
        assert ep.valid
        # Frozen: the list does not move under us the way all() does.
        before = list(ep.members)
        time.sleep(0.3)
        assert ep.members == before
        assert ep.slot(2).slot == 2
    finally:
        peers.stop()
        me.leave()


def test_an_incomplete_round_does_not_open(registry):
    me = tinyray.join("trainer", "collective", slot=0, size=4)
    me.ready()
    peers = Ranks(2, 4)  # only 3 of 4 seats filled
    try:
        with pytest.raises(TimeoutError, match="3 of 4"):
            tinyray.pool("trainer").epoch(timeout=2)
        # ...unless the caller says a smaller round is acceptable.
        assert len(tinyray.pool("trainer").epoch(min=3, timeout=10)) == 3
    finally:
        peers.stop()
        me.leave()


def test_losing_a_rank_breaks_the_round_within_the_lease(registry):
    """The acceptance number: detection in seconds, not the ten minutes a
    collective timeout has to allow for a legitimately slow operation."""
    world = 3
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(world - 1, world)
    try:
        ep = tinyray.pool("trainer").epoch(timeout=20)
        assert ep.valid

        broke = threading.Event()
        t0 = time.monotonic()

        def watchdog() -> None:
            # Checking inside a training loop is useless: a stuck rank never
            # reaches the check. A separate thread can, because NCCL releases
            # the GIL while it blocks.
            while ep.valid and time.monotonic() - t0 < 30:
                time.sleep(0.05)
            broke.set()

        w = threading.Thread(target=watchdog, daemon=True)
        w.start()
        peers.kill(0)  # SIGKILL: no farewell, only the lease can notice

        assert broke.wait(timeout=20), "the round never noticed a missing rank"
        elapsed = time.monotonic() - t0
        lease = registry.ttl_ms / 1000
        assert elapsed < lease * 2 + 1, f"took {elapsed:.1f}s against a {lease}s lease"
        assert not ep.valid
    finally:
        peers.stop()
        me.leave()


def test_publishing_state_does_not_break_a_round(registry):
    """Two numbers exist for exactly this: the same event has to count as
    changed for caches and unchanged for a frozen round."""
    world = 2
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready(step=0)
    peers = Ranks(1, world)
    try:
        ep = tinyray.pool("trainer").epoch(timeout=20)
        for step in range(1, 6):
            me.ready(step=step)
            time.sleep(0.05)
            assert ep.valid, f"publishing step={step} voided the round"
    finally:
        peers.stop()
        me.leave()


def test_a_replacement_breaks_the_round_even_in_the_same_seat(registry):
    world = 2
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(1, world)
    ep = tinyray.pool("trainer").epoch(timeout=20)
    assert ep.valid

    peers.kill(0)
    deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 2
    while ep.valid and time.monotonic() < deadline:
        time.sleep(0.05)
    replacement = Ranks(1, world)  # same seat, new tenure
    try:
        tinyray.pool("trainer").wait(count=2, timeout=10)
        assert not ep.valid, "a new occupant of seat 1 is not the old round"
        fresh = tinyray.pool("trainer").epoch(timeout=10)
        assert fresh.valid and fresh.roster != ep.roster
    finally:
        replacement.stop()
        peers.stop()
        me.leave()


def test_opening_a_round_is_refused_while_out_of_touch(registry):
    """Two rules that look contradictory but are not.

    A round already running keeps going when the registry dies -- the ranks are
    all alive and the group is intact, so killing it would contradict the
    promise that the registry can die without stopping training. Opening a
    *new* round is refused, because ranks could freeze different rosters.
    """
    world = 2
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(1, world)
    try:
        ep = tinyray.pool("trainer").epoch(timeout=20)
        assert ep.valid

        registry.stop()
        time.sleep(registry.ttl_ms / 1000 * 2)

        assert ep.valid, "an established group must survive losing the phone book"
        with pytest.raises(tinyray.Stale, match="no contact"):
            tinyray.pool("trainer").epoch(timeout=5)
    finally:
        registry.start()
        peers.stop()
        me.leave()
