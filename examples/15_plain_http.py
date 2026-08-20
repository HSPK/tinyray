"""It is still HTTP, so curl still works.

Calling a method is a convention over plain HTTP rather than a new protocol.
That is not a detail: replacing curl-able endpoints with an opaque RPC would
trade away most of a team's debugging habits, and this keeps them.

    python examples/15_plain_http.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402


def run_service(_: list[str]) -> None:
    class Dispatcher:
        def assign(self, task: str, retries: int = 0) -> dict:
            return {"took": task, "retries": retries}

        def depth(self) -> int:
            return 3

        def _private(self) -> str:
            return "not reachable"

    with tinyray.join("dispatcher", "stateful", slot=0, serves=Dispatcher()) as me:
        me.ready(dp_rank=0)
        print(f"URL {tinyray.pool('dispatcher').slot(0).url}", flush=True)
        time.sleep(8)


def curl(*args: str) -> str:
    return subprocess.run(["curl", "-s", *args], capture_output=True, text=True).stdout


def run_client(argv: list[str]) -> None:
    url, registry = argv[0], argv[1]
    with tinyray.join("client", "churn") as me:
        me.ready()
        tinyray.pool("dispatcher").wait(count=1, timeout=20)

        print("[curl] what does this process serve?", flush=True)
        print(f"       {curl(f'{url}/_methods')}", flush=True)

        print("[curl] call a method by hand:", flush=True)
        out = curl(
            "-X",
            "POST",
            f"{url}/call/assign",
            "-H",
            "content-type: application/json",
            "-d",
            '{"args": [], "kwargs": {"task": "t-7", "retries": 2}}',
        )
        print(f"       {out}", flush=True)
        assert json.loads(out)["result"]["took"] == "t-7"

        print("[curl] the shorthand a human would type also works:", flush=True)
        out = curl("-X", "POST", f"{url}/call/assign", "-d", '{"task": "t-8"}')
        print(f"       {out}", flush=True)

        print("[curl] the registry has a plain view too:", flush=True)
        print(f"       {curl(f'http://{registry}/v1/pools')}", flush=True)
        print(f"       {curl(f'http://{registry}/health')}", flush=True)

        # urllib works as well: nothing about this needs the tinyray client.
        req = urllib.request.Request(
            f"{url}/call/depth",
            data=b"{}",
            method="POST",
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            print(f"[urllib] depth -> {json.loads(r.read())['result']}", flush=True)

        # Private methods are not exposed, by curl or otherwise.
        out = curl("-X", "POST", f"{url}/call/_private", "-d", "{}")
        print(f"[curl] private method: {out}", flush=True)
        assert "no method" in out


def driver() -> int:
    with Fleet() as fleet:
        svc = subprocess.Popen(
            [sys.executable, __file__, "service"],
            env=fleet.env,
            stdout=subprocess.PIPE,
            text=True,
        )
        fleet.procs.append(("service", svc))
        url = svc.stdout.readline().split()[1]
        fleet.spawn(__file__, "client", url, fleet.endpoint, label="client")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"service": run_service, "client": run_client}, driver))
