"""The one reason any of this is Rust.

A Python thread is not starved by ordinary CPU-bound Python -- the interpreter
hands the GIL round every few milliseconds. It is starved by two things that
happen constantly in a training process, and both are measured below:

  * many busy threads at once, where a sleeper rejoins the queue at the back
  * a single C-level call that never releases the GIL at all

Either one stretches a 250 ms tick past a second, which is longer than a
collective member's lease. Declaring a healthy rank dead voids the round for
everyone, so the heartbeat runs on a native thread that never calls into
Python and does not care who holds the GIL.

    python examples/14_gil_heartbeat.py
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402

HOLD_SECONDS = 6.0
BUSY_THREADS = 16


def hog_the_gil(stop: threading.Event, until: float) -> list[threading.Thread]:
    """Two hogs, because they starve a sleeper in different ways.

    Each hog watches the clock as well as the flag. The thread that would set
    the flag is the one being starved, so making the hogs depend on it is
    asking the victim to free itself: measured on a two-core box, `stop.set()`
    landed 6 to 14 seconds after the hold was over, and on a CI runner that
    ran past the driver's whole 90s budget and failed a release.
    """

    def busy() -> None:
        x = 0
        while not stop.is_set() and time.monotonic() < until:
            for _ in range(50_000):
                x = (x * 31 + 7) % 1000003

    def c_level() -> None:
        # One C call, no GIL release and no interrupt check in the middle.
        # This is the shape of a long op inside a native extension. One
        # multiplication of this size costs 1666.8ms on an idle core here, and
        # nothing else in the process runs for the whole of it -- which is the
        # point of the example, and also why the hold has to end by the clock.
        big = 7**3_000_000
        while not stop.is_set() and time.monotonic() < until:
            big * big

    threads = [threading.Thread(target=busy, daemon=True) for _ in range(BUSY_THREADS)]
    threads.append(threading.Thread(target=c_level, daemon=True))
    for t in threads:
        t.start()
    return threads


def run_trainer(_: list[str]) -> None:
    with tinyray.join("trainer", "collective", slot=0, size=1) as me:
        me.ready(step=0)
        tinyray.pool("trainer").wait(count=1, timeout=20)

        # A Python heartbeat doing the same job, for contrast.
        lease_ms = me.stats()["interval_ms"] * 4
        stop = threading.Event()
        gaps: list[float] = []

        def python_heartbeat() -> None:
            last = time.monotonic()
            while not stop.is_set():
                time.sleep(0.25)
                now = time.monotonic()
                gaps.append((now - last) * 1000)
                last = now

        threading.Thread(target=python_heartbeat, daemon=True).start()
        time.sleep(0.4)
        beats_before = me.stats()["beats_ok"]
        gaps.clear()

        print(
            f"[trainer] lease {lease_ms} ms; hogging the GIL for {HOLD_SECONDS}s "
            f"with {BUSY_THREADS} busy threads and one long C call",
            flush=True,
        )
        hogs = hog_the_gil(stop, time.monotonic() + HOLD_SECONDS)
        time.sleep(HOLD_SECONDS)
        stop.set()
        # One budget for all of them, not one each: seventeen threads at five
        # seconds apiece is eighty-five seconds of joining, which on its own
        # is longer than the driver waits for the whole example.
        deadline = time.monotonic() + 20.0
        for t in hogs:
            t.join(timeout=max(0.0, deadline - time.monotonic()))

        native = me.stats()["beats_ok"] - beats_before
        expected = HOLD_SECONDS / (me.stats()["interval_ms"] / 1000)
        worst_python = max(gaps) if gaps else 0.0

        print(f"[trainer] native beats sent     : {native} of about {expected:.0f}", flush=True)
        print(f"[trainer] failed beats          : {me.stats()['beats_failed']}", flush=True)
        print(
            f"[trainer] worst python tick gap : {worst_python:.0f} ms (asked for 250)", flush=True
        )
        print(f"[trainer] still holds its seat  : {me.accepted}", flush=True)
        print(f"[trainer] still in the roster   : {len(tinyray.pool('trainer').all())}", flush=True)

        assert native >= expected * 0.5, "the native heartbeat stalled behind the GIL"
        assert me.stats()["beats_failed"] == 0
        assert me.accepted, "a healthy rank was declared dead"
        assert len(tinyray.pool("trainer").all()) == 1
        assert worst_python > lease_ms, (
            f"the python thread never slipped past the {lease_ms} ms lease, so this "
            f"machine did not reproduce the hazard"
        )
        print(
            f"[trainer] a python heartbeat slipped {worst_python:.0f} ms > {lease_ms} ms "
            f"lease and would have been declared dead",
            flush=True,
        )


def driver() -> int:
    with Fleet(ttl_ms=1200) as fleet:  # the hold is five leases long
        fleet.spawn(__file__, "trainer", label="trainer")
        return fleet.wait_all(timeout=90)


if __name__ == "__main__":
    raise SystemExit(role_main({"trainer": run_trainer}, driver))
