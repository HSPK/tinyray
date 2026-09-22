"""Same-host zero-copy payloads with sealed Linux memfd BlobRef.

python examples/21_blobref.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import tinyray  # noqa: E402
from _harness import Fleet, role_main  # noqa: E402


def run_service(_: list[str]) -> None:
    class Store:
        retained: tinyray.BlobRef | None = None

        def retain(self, value: tinyray.BlobRef) -> int:
            self.retained = value
            return len(value)

        def first(self) -> int:
            assert self.retained is not None
            return self.retained.view()[0]

    with tinyray.join("blob-store", "stateful", slot=0, size=1, serves=Store()) as me:
        me.ready(host="same")
        print("READY", flush=True)
        time.sleep(8)


def run_client(_: list[str]) -> None:
    with tinyray.join("blob-client") as me:
        me.ready()
        store = tinyray.pool("blob-store").wait(count=1, timeout=20)[0]
        payload = bytes([73]) + b"x" * ((16 << 20) - 1)
        blob = tinyray.blob(payload)
        assert blob.view().readonly
        assert store.retain(blob) == len(payload)
        blob.close()
        assert store.first() == 73
        print("16 MiB mapped read-only and retained after sender close", flush=True)


def driver() -> int:
    if sys.platform != "linux":
        print("BlobRef requires Linux")
        return 0
    with Fleet() as fleet:
        service = subprocess.Popen(
            [sys.executable, __file__, "service"],
            env=fleet.env,
            stdout=subprocess.PIPE,
            text=True,
        )
        fleet.procs.append(("service", service))
        assert service.stdout.readline().strip() == "READY"
        fleet.spawn(__file__, "client", label="client")
        return fleet.wait_all(timeout=60)


if __name__ == "__main__":
    raise SystemExit(role_main({"service": run_service, "client": run_client}, driver))
