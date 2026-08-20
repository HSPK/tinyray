"""Three failures that need three different reactions.

Only "never arrived" is safe to retry blindly. Whether a business failure can
be repeated is something only the application knows, so tinyray never decides
that for you.

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

        # 2. It never arrived. Safe to retry if the operation can be repeated.
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
        except tinyray.Unreachable as exc:
            results["Unreachable"] = str(exc)[:48]

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

        # A timeout is an Unreachable: we do not know whether it ran.
        try:
            svc.slow.timeout(0.2)(2.0)
        except tinyray.Unreachable:
            results["timeout"] = "Unreachable (we cannot know if it ran)"

        # Typos and bad arguments are the caller's fault, not the network's.
        try:
            svc.nope()
        except AttributeError as exc:
            results["AttributeError"] = str(exc)[:48]
        try:
            svc.ok("not an int")
        except TypeError as exc:
            results["TypeError"] = str(exc)[:48]

        for kind in (
            "RemoteError",
            "Unreachable",
            "Fenced",
            "timeout",
            "AttributeError",
            "TypeError",
        ):
            print(f"[client] {kind:<15} {results[kind]}", flush=True)

        print("[client] retry policy:", flush=True)
        print("           Unreachable -> retry if the operation is repeatable", flush=True)
        print("           Fenced      -> re-resolve, then retry", flush=True)
        print("           RemoteError -> never retried for you", flush=True)


def driver() -> int:
    with Fleet() as fleet:
        fleet.spawn(__file__, "service", label="service")
        time.sleep(0.6)
        fleet.spawn(__file__, "client", label="client")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"service": run_service, "client": run_client}, driver))
