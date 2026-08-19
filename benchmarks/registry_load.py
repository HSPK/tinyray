"""Registry load benchmark.

The earlier figure of 4,295 ops/s was wrong: it came from a **single sequential
client**, so the server was never saturated. It measured the client.

The first job of this harness is therefore to **prove the client is not the
bottleneck** -- sweep client concurrency and look for the plateau. A number
without a plateau is not a server measurement.

Three paths, measured separately:

* ``raw``   -- a bare HTTP GET /health. Pure transport ceiling, no registry logic.
* ``rpc``   -- the tinyray client path. Note this costs **two round trips**
  (submit then fetch).
* ``local`` -- the Registry object called directly, no network. Data-structure
  ceiling.

The three together are what makes any one of them meaningful: ``local`` says
whether the data structure is fast enough, ``raw`` whether the transport is, and
``rpc`` how far the real path sits from both.

Usage::

    # sweep first, to confirm the client is not the limit
    python benchmarks/registry_load.py sweep --members 10000

    # then a fixed point
    python benchmarks/registry_load.py run --members 10000 --op heartbeat \
        --procs 8 --threads 32 --duration 20
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------
# 被测服务端
# --------------------------------------------------------------------------

REGISTRY_SCRIPT = """
import sys
import tinyray
tinyray.serve_registry(bind=f"0.0.0.0:{sys.argv[1]}", ttl=float(sys.argv[2]))
"""


def start_registry(port: int, ttl: float = 3600.0) -> subprocess.Popen:
    source = Path(tempfile.mkdtemp()) / "registry.py"
    source.write_text(REGISTRY_SCRIPT)
    process = subprocess.Popen(
        [sys.executable, str(source), str(port), str(ttl)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                return process
        except Exception:
            time.sleep(0.2)
    process.kill()
    raise RuntimeError("registry did not come up")


def populate(endpoint: str, members: int, group: str = "env") -> None:
    """Load N members as the steady state the benchmark runs against."""
    from tinyray.cluster import RegistryClient

    client = RegistryClient(endpoint)
    batch = max(1, members // 20)
    for rank in range(members):
        client.call_any(
            "register",
            group,
            rank,
            f"10.{rank // 65536}.{rank // 256 % 256}.{rank % 256}:41234",
            f"{rank:032x}",
            members,
            {},
        )
        if rank and rank % batch == 0:
            print(f"    loaded {rank}/{members}", flush=True)


# --------------------------------------------------------------------------
# 客户端 worker
# --------------------------------------------------------------------------


def worker_body(args: dict, out) -> None:
    """One client process: T threads, each looping on the operation."""
    import threading

    op = args["op"]
    endpoint = args["endpoint"]
    members = args["members"]
    duration = args["duration"]
    threads = args["threads"]
    seed = args["seed"]

    latencies: list[float] = []
    errors = [0]
    lock = threading.Lock()
    stop = threading.Event()

    if op == "raw":
        url = f"http://{endpoint}/health"

        def call(_i: int) -> None:
            with urllib.request.urlopen(url, timeout=10) as response:
                response.read()
    else:
        from tinyray.cluster import RegistryClient

        client = RegistryClient(endpoint, cache_ttl=0.0)

        if op == "heartbeat":

            def call(i: int) -> None:
                client.call_any("heartbeat", f"env/{i % members}")

        elif op == "register":

            def call(i: int) -> None:
                rank = i % members
                client.call_any(
                    "register",
                    "env",
                    rank,
                    f"10.0.0.1:{40000 + rank % 1000}",
                    f"{rank:032x}",
                    members,
                    {},
                )

        elif op == "lookup":

            def call(i: int) -> None:
                base = (i * 8) % max(1, members - 8)
                client.call_any("lookup", "env", list(range(base, base + 8)))

        elif op == "lookup_unchanged":
            version = client.call_any("lookup", "env", [0])["version"]

            def call(i: int) -> None:
                client.call_any("lookup", "env", [0], version)

        else:
            raise SystemExit(f"unknown op {op}")

    def run(thread_index: int) -> None:
        local: list[float] = []
        i = seed * 1_000_003 + thread_index * 7919
        while not stop.is_set():
            start = time.perf_counter()
            try:
                call(i)
                local.append(time.perf_counter() - start)
            except Exception:
                with lock:
                    errors[0] += 1
            i += 1
        with lock:
            latencies.extend(local)

    pool = [threading.Thread(target=run, args=(t,), daemon=True) for t in range(threads)]
    started = time.perf_counter()
    for thread in pool:
        thread.start()
    time.sleep(duration)
    stop.set()
    for thread in pool:
        thread.join(timeout=30)
    elapsed = time.perf_counter() - started

    out.put({"latencies": latencies, "errors": errors[0], "elapsed": elapsed})


def measure(
    endpoint: str, op: str, members: int, procs: int, threads: int, duration: float
) -> dict:
    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    args = {
        "op": op,
        "endpoint": endpoint,
        "members": members,
        "duration": duration,
        "threads": threads,
    }
    workers = [
        ctx.Process(target=worker_body, args=({**args, "seed": p}, queue)) for p in range(procs)
    ]
    for worker in workers:
        worker.start()
    results = [queue.get(timeout=duration + 180) for _ in workers]
    for worker in workers:
        worker.join(timeout=60)

    latencies = [x for r in results for x in r["latencies"]]
    errors = sum(r["errors"] for r in results)
    elapsed = max(r["elapsed"] for r in results)
    if not latencies:
        return {"ops": 0, "throughput": 0.0, "errors": errors}
    latencies.sort()

    def pct(p: float) -> float:
        return latencies[min(len(latencies) - 1, int(len(latencies) * p))] * 1e3

    return {
        "ops": len(latencies),
        "errors": errors,
        "elapsed": elapsed,
        "throughput": len(latencies) / elapsed,
        "p50_ms": pct(0.50),
        "p99_ms": pct(0.99),
        "p999_ms": pct(0.999),
        "mean_ms": statistics.fmean(latencies) * 1e3,
    }


# --------------------------------------------------------------------------
# Data-structure ceiling: no network
# --------------------------------------------------------------------------


def measure_local(members: int, op: str, seconds: float = 3.0) -> dict:
    from tinyray.registry import Registry

    registry = Registry(ttl=3600.0)
    for rank in range(members):
        registry.register("env", rank, f"10.0.0.1:{rank}", f"{rank:032x}", members, {})

    if op == "heartbeat":

        def call(i):
            registry.heartbeat(f"env/{i % members}")
    elif op == "lookup":

        def call(i):
            base = (i * 8) % max(1, members - 8)
            registry.lookup("env", list(range(base, base + 8)))
    else:
        raise SystemExit(f"local mode does not support {op}")

    calls = 0
    start = time.perf_counter()
    while time.perf_counter() - start < seconds:
        for _ in range(1000):
            call(calls)
            calls += 1
    elapsed = time.perf_counter() - start
    return {"throughput": calls / elapsed, "ops": calls, "elapsed": elapsed}


# --------------------------------------------------------------------------
# 入口
# --------------------------------------------------------------------------


def cmd_sweep(args: argparse.Namespace) -> None:
    """Sweep client concurrency. No plateau means you measured the client."""
    registry = None
    endpoint = args.registry
    if endpoint is None:
        from tinyray.process import free_port

        port = free_port()
        registry = start_registry(port)
        endpoint = f"127.0.0.1:{port}"
        print(f"registry up at {endpoint}")

    try:
        print(f"loading {args.members} members ...")
        populate(endpoint, args.members)
        print()

        print(
            f"{'path':<18} {'procs':>6} {'thr':>5} {'conc':>7} "
            f"{'ops/s':>12} {'p50 ms':>9} {'p99 ms':>9} {'errors':>8}"
        )
        print("-" * 82)
        for op in args.ops:
            previous = 0.0
            for procs, threads in args.grid:
                result = measure(endpoint, op, args.members, procs, threads, args.duration)
                flag = ""
                if previous and result["throughput"] < previous * 1.10:
                    flag = "  <= plateau"
                print(
                    f"{op:<18} {procs:>5} {threads:>5} {procs * threads:>7} "
                    f"{result['throughput']:>13,.0f} {result.get('p50_ms', 0):>9.2f} "
                    f"{result.get('p99_ms', 0):>9.2f} {result['errors']:>7}{flag}"
                )
                previous = max(previous, result["throughput"])
            print()
    finally:
        if registry:
            registry.kill()


def cmd_run(args: argparse.Namespace) -> None:
    registry = None
    endpoint = args.registry
    if endpoint is None:
        from tinyray.process import free_port

        port = free_port()
        registry = start_registry(port)
        endpoint = f"127.0.0.1:{port}"
    try:
        populate(endpoint, args.members)
        result = measure(endpoint, args.op, args.members, args.procs, args.threads, args.duration)
        result.update(members=args.members, op=args.op, procs=args.procs, threads=args.threads)
        print(json.dumps(result, indent=2))
    finally:
        if registry:
            registry.kill()


def cmd_local(args: argparse.Namespace) -> None:
    for members in args.member_counts:
        for op in ("heartbeat", "lookup"):
            result = measure_local(members, op)
            print(f"local {op:<10} members={members:>7,}  {result['throughput']:>12,.0f} ops/s")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    sweep = sub.add_parser(
        "sweep", help="sweep concurrency; confirm the client is not the bottleneck"
    )
    sweep.add_argument("--members", type=int, default=10000)
    sweep.add_argument("--duration", type=float, default=8.0)
    sweep.add_argument("--registry", default=None)
    sweep.add_argument("--ops", nargs="+", default=["raw", "heartbeat", "lookup"])
    sweep.set_defaults(func=cmd_sweep)

    run = sub.add_parser("run", help="fixed-point measurement")
    run.add_argument("--members", type=int, default=10000)
    run.add_argument("--op", default="heartbeat")
    run.add_argument("--procs", type=int, default=8)
    run.add_argument("--threads", type=int, default=32)
    run.add_argument("--duration", type=float, default=20.0)
    run.add_argument("--registry", default=None)
    run.set_defaults(func=cmd_run)

    local = sub.add_parser("local", help="data-structure ceiling, no network")
    local.add_argument("--member-counts", type=int, nargs="+", default=[1000, 10000, 100000])
    local.set_defaults(func=cmd_local)

    args = parser.parse_args()
    if args.cmd == "sweep":
        cores = os.cpu_count() or 8
        args.grid = [(1, 1), (1, 8), (2, 16), (4, 16), (8, 16), (8, 32), (min(16, cores), 32)]
    args.func(args)


if __name__ == "__main__":
    main()
