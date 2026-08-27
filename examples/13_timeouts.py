"""Timeouts belong to the call site, not to the library.

rl-bridge tunes twelve of them separately and writes down that every request
must pass one explicitly -- a rule you only make after being burnt. So the
default is a starting point, and the modifier hangs off the method rather than
being a keyword argument, where it would collide with a parameter of the same
name on the far side.

    python examples/13_timeouts.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402


def run_service(_: list[str]) -> None:
    class Service:
        def quick(self) -> str:
            return "ok"

        def takes(self, seconds: float) -> str:
            time.sleep(seconds)
            return f"slept {seconds}"

        # A parameter genuinely called `timeout`: the modifier must not clash.
        def with_own_timeout(self, timeout: int) -> str:
            return f"the callee's own timeout argument was {timeout}"

    with tinyray.join("svc", "stateful", slot=0, serves=Service()) as me:
        me.ready()
        print("READY", flush=True)
        time.sleep(10)


def run_client(_: list[str]) -> None:
    with tinyray.join("client", "churn") as me:
        me.ready()
        svc = tinyray.pool("svc").wait(count=1, timeout=20)[0]

        print(
            f"[client] default budget is {tinyray._rpc.DEFAULT_TIMEOUT}s "
            f"(the measured control-plane band is 2-30s)",
            flush=True,
        )

        t0 = time.monotonic()
        try:
            svc.takes.timeout(0.2)(2.0)
        except tinyray.Unreachable:
            print(
                f"[client] a 0.2s budget gave up after {(time.monotonic() - t0) * 1000:.0f} ms",
                flush=True,
            )

        # The modifier is per call, not sticky: the next one is back to default.
        assert svc.takes(0.05) == "slept 0.05"
        print("[client] the next call is back to the default budget", flush=True)

        # Different budgets for different paths, from one handle.
        fast = svc.quick.timeout(1.0)
        patient = svc.takes.timeout(5.0)
        assert fast() == "ok"
        assert patient(0.3) == "slept 0.3"
        print("[client] two budgets from the same handle, chosen per path", flush=True)

        # And it does not collide with the callee's own parameter.
        print(f"[client] {svc.with_own_timeout.timeout(3.0)(timeout=99)}", flush=True)

        # Giving up is not knowing: a timeout is OutcomeUnknown, never
        # RemoteError, because we cannot tell whether the work ran.
        try:
            svc.takes.timeout(0.15)(1.0)
        except tinyray.OutcomeUnknown as exc:
            assert not isinstance(exc, tinyray.RemoteError)
            print("[client] a timeout is OutcomeUnknown: it may have run in full", flush=True)


def driver() -> int:
    with Fleet() as fleet:
        fleet.spawn(__file__, "service", label="service")
        time.sleep(0.6)
        fleet.spawn(__file__, "client", label="client")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"service": run_service, "client": run_client}, driver))
