"""What a big pool costs, and who pays.

Memory is not about how many members exist. It is about members multiplied by
the number of processes that look them up, because each new subscriber gets the
whole roster copied once. A thousand processes watching eight members is free;
four processes watching a hundred thousand is a gigabyte and a half.

    python examples/19_pool_size_guard.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, free_port, role_main  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
LOADGEN = ROOT / "target" / "release" / "loadgen"
TINYRAY = Path(sys.executable).parent / "tinyray"


def rss_mb(pid: int) -> float:
    with open(f"/proc/{pid}/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


def measure(members: int, watchers: int, seconds: int = 8) -> tuple[float, int]:
    port = free_port()
    reg = subprocess.Popen(
        [str(TINYRAY), "--listen", f"127.0.0.1:{port}", "--ttl-ms", "60000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.0)
    base = rss_mb(reg.pid)
    procs, per = 4, max(members // 4, 1)
    load = [
        subprocess.Popen(
            [str(LOADGEN), "--endpoint", f"127.0.0.1:{port}", "--members", str(per),
             "--seconds", str(seconds), "--interval-ms", "5000",
             "--watchers", str(max(watchers // procs, 0)), "--conns", "8",
             "--offset", str(i * 300_000)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for i in range(procs)
    ]
    time.sleep(seconds - 2)
    peak = rss_mb(reg.pid)
    for p in load:
        p.wait(timeout=60)
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/pools", timeout=5) as r:
        seen = json.loads(r.read()).get("load", {}).get("members", 0)
    reg.terminate()
    reg.wait(timeout=10)
    return peak - base, seen


def driver() -> int:
    if not LOADGEN.exists():
        print(f"build the load generator first: cargo build --release ({LOADGEN} missing)")
        return 1

    print(f"{'members':>9} {'watchers':>9} {'product':>10} {'registry MB':>12} {'B/member':>10}")
    rows = [(2000, 0), (2000, 4), (20000, 0), (20000, 4)]
    results = []
    for members, watchers in rows:
        mb, seen = measure(members, watchers)
        per = mb * 1024 * 1024 / max(seen, 1)
        results.append((members, watchers, mb, per))
        print(f"{seen:>9} {watchers:>9} {seen * max(watchers, 1):>10} "
              f"{mb:>12.0f} {per:>10.0f}", flush=True)

    unwatched = [r for r in results if r[1] == 0]
    watched = [r for r in results if r[1] > 0]
    print(flush=True)
    print(f"storage alone is about {sum(r[3] for r in unwatched) / len(unwatched):.0f} "
          f"bytes a member", flush=True)
    print(f"with four subscribers it is about "
          f"{sum(r[3] for r in watched) / len(watched):.0f}", flush=True)
    print("the difference is the full roster each new subscriber is sent once",
          flush=True)
    print(flush=True)
    print("rule of thumb: members x subscribers past ~100,000 deserves a look,",
          flush=True)
    print("and the fix is usually to reverse who subscribes rather than add memory.",
          flush=True)
    assert watched[-1][3] > unwatched[-1][3], "subscribers should cost something"
    return 0


if __name__ == "__main__":
    raise SystemExit(role_main({}, driver))
