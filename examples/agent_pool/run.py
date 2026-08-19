"""Run the ported agent tier: one pool, N workers, one killed mid-flight."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "examples"))

CAPABILITY, FINGERPRINT, TASKS = "tmax", "sha256:abc", 40


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    if len(sys.argv) > 1:
        from agent_pool import pool, worker

        if sys.argv[1] == "pool":
            svc = pool.serve(64, pool.Catalog(CAPABILITY, FINGERPRINT, TASKS), TASKS)
            done = sum(1 for a in svc.attempts.values() if a.state == "completed")
            print(f"[pool] completed {done}/{TASKS}", flush=True)
        else:
            n = worker.run(sys.argv[2], 2, CAPABILITY, FINGERPRINT, TASKS)
            print(f"[{sys.argv[2]}] finished {n} attempts", flush=True)
        return 0

    port = free_port()
    reg = subprocess.Popen(
        [str(Path(sys.executable).parent / "tinyray"), "--listen", f"127.0.0.1:{port}",
         "--ttl-ms", "2000"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    env = dict(os.environ, TINYRAY_REGISTRY=f"127.0.0.1:{port}")
    time.sleep(0.8)
    procs = [subprocess.Popen([sys.executable, __file__, "pool"], env=env)]
    time.sleep(0.3)
    procs += [
        subprocess.Popen([sys.executable, __file__, "worker", f"w{i}"], env=env) for i in range(4)
    ]
    t0 = time.monotonic()
    time.sleep(1.0)
    procs[2].kill()  # one worker dies holding work
    print("[run] killed w1", flush=True)
    rc = 0
    for i, p in enumerate(procs):
        code = p.wait(timeout=90)
        if code != 0 and i != 2:
            print(f"!! process {i} exited {code}")
            rc = 1
    print(f"[run] done in {time.monotonic() - t0:.1f}s")
    reg.terminate()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
