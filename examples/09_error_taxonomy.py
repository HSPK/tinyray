"""Six failures that need six different reactions.

Only NotDelivered is safe to retry blindly, because nothing ran. Every other
failure leaves "did it run?" open: a call that timed out may have been carried
out in full. Whether a business failure can be repeated is something only the
application knows, so tinyray never decides that for you.

    python examples/09_error_taxonomy.py
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
        def ok(self, x: int) -> int:
            return x * 2

        def business_failure(self) -> None:
            raise ValueError("the rubric rejected this rollout")

        def slow(self, seconds: float) -> str:
            time.sleep(seconds)
            return "done"

    with tinyray.join("svc", "stateful", slot=0, serves=Service()) as me:
        me.ready()
        print("READY", flush=True)
        time.sleep(8)


def run_client(_: list[str]) -> None:
    with tinyray.join("client", "churn") as me:
        me.ready()
        svc = tinyray.pool("svc").wait(count=1, timeout=20)[0]
        assert svc.ok(21) == 42

        results: dict[str, str] = {}

        # 1. It arrived and the method raised. Retrying is the application's
        #    call, so this is never retried for you.
        try:
            svc.business_failure()
        except tinyray.RemoteError as exc:
            results["RemoteError"] = f"{exc.type}: {exc.message}"
            assert "rubric" in exc.traceback

        # 2. It never arrived: the connection was refused, so nothing ran.
        #    This is the only one that can be sent again as it stands.
        gone = tinyray.Handle(
            "svc",
            {
                "id": 0,
                "slot": 0,
                "incarnation": svc.incarnation,
                "url": "http://127.0.0.1:1",
                "ready": True,
            },
            ("ok",),
        )
        try:
            gone.ok(1)
        except tinyray.NotDelivered as exc:
            results["NotDelivered"] = str(exc)[:48]

        # 3. It arrived, but a later tenure owns that seat. Look it up again.
        stale = tinyray.Handle(
            "svc",
            {"id": 0, "slot": 0, "incarnation": svc.incarnation - 1, "url": svc.url, "ready": True},
            ("ok",),
        )
        try:
            stale.ok(1)
        except tinyray.Fenced as exc:
            results["Fenced"] = str(exc)[:48]

        # 4. We stopped waiting. The call may have been carried out in full --
        #    this is the one case that needs a request id or an idempotent
        #    operation, and the one the old advice got wrong.
        try:
            svc.slow.timeout(0.2)(2.0)
        except tinyray.OutcomeUnknown:
            results["OutcomeUnknown"] = "we stopped waiting; it may have run"

        # 5. A method that is not there at all. Not a network error: the
        #    process answered, and the answer was "no such method".
        try:
            svc.nope()
        except AttributeError as exc:
            results["AttributeError"] = str(exc)[:48]

        # 6. Bad arguments are the caller's fault, not the network's. Every way
        #    of getting them wrong lands here -- too many, too few, a keyword
        #    the method has no parameter for, one value given twice, or the
        #    wrong type. None of them ran.
        try:
            svc.ok("not an int")
        except TypeError as exc:
            results["TypeError"] = str(exc)[:48]

        for kind in (
            "RemoteError",
            "NotDelivered",
            "Fenced",
            "OutcomeUnknown",
            "AttributeError",
            "TypeError",
        ):
            print(f"[client] {kind:<15} {results[kind]}", flush=True)

        print("[client] retry policy:", flush=True)
        print("           NotDelivered   -> send it again as it stands", flush=True)
        print("           OutcomeUnknown -> same request id, or be idempotent", flush=True)
        print("           Fenced         -> re-resolve, then retry", flush=True)
        print("           RemoteError    -> yours to decide, never automatic", flush=True)
        print("           AttributeError -> fix the name; retrying cannot help", flush=True)
        print("           TypeError      -> fix the call; retrying cannot help", flush=True)
        print("         (both of the first two are Unreachable, so an existing", flush=True)
        print("          `except Unreachable` still catches them -- but it", flush=True)
        print("          cannot tell you which of the two you have.)", flush=True)


def driver() -> int:
    with Fleet() as fleet:
        fleet.spawn(__file__, "service", label="service")
        time.sleep(0.6)
        fleet.spawn(__file__, "client", label="client")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"service": run_service, "client": run_client}, driver))
