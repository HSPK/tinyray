#!/usr/bin/env python3
"""Measure tinyray, one scenario at a time, and print numbers.

Not a test. Tests here assert bounds -- a rate must sit between 0.5 and 6, a
satisfied wait must return inside 100ms -- which catches a regression only once
it crosses the line. Something can get twice as slow and stay green, and did:
join(timeout=) handing its whole budget to the first beat doubled the lossy-link
test from 7:22 to 17:06 while passing throughout.

Run against one build:

    python bench.py                     # human table
    python bench.py --json out.json     # machine-readable

Every scenario is feature-detected, because this same file is meant to run
against old wheels: anything the build cannot do is reported n/a rather than
crashing, so one script can compare versions that do not share an API.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

import tinyray

TTL_MS = 2000


# --------------------------------------------------------------------------
# harness
# --------------------------------------------------------------------------
def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def registry_binary() -> str:
    """The `tinyray` next to the interpreter running this."""
    return str(Path(sys.executable).parent / "tinyray")


class Registry:
    def __init__(self) -> None:
        self.port = free_port()
        self.endpoint = f"127.0.0.1:{self.port}"
        self.proc: subprocess.Popen | None = None

    def __enter__(self) -> Registry:
        self.proc = subprocess.Popen(
            [registry_binary(), "--listen", self.endpoint, "--ttl-ms", str(TTL_MS)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"http://{self.endpoint}/health", timeout=0.5) as r:
                    if r.status == 200:
                        os.environ["TINYRAY_REGISTRY"] = self.endpoint
                        return self
            except (urllib.error.URLError, OSError):
                time.sleep(0.02)
        raise RuntimeError("registry did not come up")

    def __exit__(self, *exc: object) -> None:
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def leave_quietly() -> None:
    client = getattr(tinyray, "_client", None)
    if client is not None:
        try:
            client.leave()
        except Exception:
            pass
        tinyray._client = None


def settle(member: Any) -> None:
    """Get published state to the registry, whatever this build calls it."""
    flush = getattr(member, "flush", None)
    if callable(flush):
        try:
            flush()
            return
        except Exception:
            pass
    time.sleep(TTL_MS / 1000 / 2)


def percentiles(samples: list[float]) -> dict[str, float]:
    s = sorted(samples)

    def at(p: float) -> float:
        return s[min(len(s) - 1, int(len(s) * p))]

    return {
        "p50_ms": round(statistics.median(s) * 1000, 3),
        "p90_ms": round(at(0.90) * 1000, 3),
        "p99_ms": round(at(0.99) * 1000, 3),
        "max_ms": round(s[-1] * 1000, 3),
    }


def watching(pool: Any, **kw: Any) -> Any:
    """`changes()` as a context manager, across builds that return a bare
    generator instead. Old ones cannot be closed or given fields; say so by
    raising TypeError, which every caller here already handles."""
    watch = pool.changes(**kw)
    if hasattr(watch, "__enter__"):
        return watch
    raise TypeError("this build's changes() is a plain generator")


def publish(member: Any, **state: Any) -> None:
    """update() where there is one, ready() where there is not. Before 0.7
    there was only ready(), which also asserts readiness -- the split is the
    point of update(), but for publishing a value they are the same call."""
    fn = getattr(member, "update", None) or member.ready
    fn(**state)


# --------------------------------------------------------------------------
# scenarios
# --------------------------------------------------------------------------
class Service:
    def ping(self) -> str:
        return "pong"

    def echo(self, blob: str) -> str:
        return blob

    def nothing(self) -> None:
        return None


def serving_member(pool: str = "b"):
    """join() with a served object, across builds that spell it differently."""
    try:
        return tinyray.join(pool, "stateful", slot=0, size=1, serves=Service())
    except TypeError:
        return tinyray.join(pool, "stateful", slot=0, serves=Service())


def bench_rpc_latency() -> dict[str, Any]:
    """One caller, one callee, a method that does nothing. The floor."""
    with Registry():
        me = serving_member()
        try:
            me.ready()
            settle(me)
            handle = tinyray.pool("b").wait(count=1, timeout=20)[0]
            for _ in range(200):  # warm the connection and the shape cache
                handle.ping()
            samples = []
            for _ in range(2000):
                t0 = time.perf_counter()
                handle.ping()
                samples.append(time.perf_counter() - t0)
            return percentiles(samples) | {"calls": len(samples)}
        finally:
            leave_quietly()


def bench_rpc_throughput() -> dict[str, Any]:
    """Eight threads on one client. Measures the shared path, not the network."""
    with Registry():
        me = serving_member()
        try:
            me.ready()
            settle(me)
            handle = tinyray.pool("b").wait(count=1, timeout=20)[0]
            handle.ping()
            stop = threading.Event()
            counts = [0] * 8

            def worker(i: int) -> None:
                n = 0
                while not stop.is_set():
                    handle.ping()
                    n += 1
                counts[i] = n

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
            t0 = time.perf_counter()
            for t in threads:
                t.start()
            time.sleep(5.0)
            stop.set()
            for t in threads:
                t.join()
            elapsed = time.perf_counter() - t0
            return {"threads": 8, "calls_per_s": round(sum(counts) / elapsed)}
        finally:
            leave_quietly()


def bench_rpc_payload() -> dict[str, Any]:
    """64 KiB out and 64 KiB back, to separate the framing from the copying."""
    with Registry():
        me = serving_member()
        try:
            me.ready()
            settle(me)
            handle = tinyray.pool("b").wait(count=1, timeout=20)[0]
            blob = "x" * (64 << 10)
            for _ in range(20):
                handle.echo(blob)
            samples = []
            for _ in range(300):
                t0 = time.perf_counter()
                handle.echo(blob)
                samples.append(time.perf_counter() - t0)
            return percentiles(samples) | {"bytes": len(blob)}
        finally:
            leave_quietly()


def bench_discovery() -> dict[str, Any]:
    """Publish here, notice there. This is what long polling bought."""
    with Registry() as reg:
        peer = None
        try:
            peer_src = (
                "import os, sys, tinyray\n"
                f"os.environ['TINYRAY_REGISTRY'] = '{reg.endpoint}'\n"
                "m = tinyray.join('news', 'churn')\n"
                "m.ready(n=0)\n"
                "print('UP', flush=True)\n"
                "n = 0\n"
                "for line in sys.stdin:\n"
                "    n += 1\n"
                "    m.update(n=n) if hasattr(m, 'update') else m.ready(n=n)\n"
                "    print('SENT', flush=True)\n"
            )
            peer = subprocess.Popen(
                [sys.executable, "-c", peer_src],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            assert peer.stdout is not None and peer.stdin is not None
            assert peer.stdout.readline().strip() == "UP"

            me = tinyray.join("watch", "churn")
            me.ready()
            pool = tinyray.pool("news")
            pool.wait(count=1, timeout=20)

            samples = []
            for i in range(1, 11):
                t0 = time.perf_counter()
                peer.stdin.write("go\n")
                peer.stdin.flush()
                peer.stdout.readline()
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    got = pool.all()
                    if got and got[0].state.get("n") == i:
                        break
                    time.sleep(0.001)
                else:
                    raise RuntimeError("the change never arrived")
                samples.append(time.perf_counter() - t0)
            return percentiles(samples) | {"changes": len(samples)}
        finally:
            if peer is not None:
                try:
                    peer.stdin.close()
                    peer.wait(timeout=5)
                except Exception:
                    peer.kill()
            leave_quietly()


def bench_idle_beat_rate() -> dict[str, Any]:
    """Requests a quiet member costs the registry per second."""
    with Registry():
        try:
            me = tinyray.join("quiet", "churn")
            me.ready()
            tinyray.pool("quiet")
            time.sleep(1.0)
            before = me.stats()
            t0 = time.perf_counter()
            time.sleep(6.0)
            after = me.stats()
            elapsed = time.perf_counter() - t0
            sent = (after.get("beats_ok", 0) - before.get("beats_ok", 0)) + (
                after.get("beats_failed", 0) - before.get("beats_failed", 0)
            )
            return {
                "beats_per_s": round(sent / elapsed, 2),
                "interval_ms": before.get("interval_ms"),
            }
        finally:
            leave_quietly()


def bench_join() -> dict[str, Any]:
    """Cold start: a process from nothing to registered and able to look up.

    Bimodal, and the same way in every version measured -- a start either
    catches the registry's answer at once or waits for the next beat, so
    samples pile up near 1ms and near 42ms with nothing in between. The median
    therefore lands on whichever side happens to hold more, and at n=10 it
    swung between 1.0, 21.2 and 41.6ms across versions that turned out to be
    identical at n=40 (24/40, 25/40 fast). So report the split, not just the
    middle.
    """
    with Registry() as reg:
        src = (
            "import os, time, tinyray\n"
            f"os.environ['TINYRAY_REGISTRY'] = '{reg.endpoint}'\n"
            "t0 = time.perf_counter()\n"
            "m = tinyray.join('cold', 'churn')\n"
            "m.ready()\n"
            "tinyray.pool('cold').all()\n"
            "print(f'{(time.perf_counter() - t0) * 1000:.2f}')\n"
            "m.leave()\n"
        )
        samples = []
        for _ in range(40):
            out = subprocess.run(
                [sys.executable, "-c", src], capture_output=True, text=True, timeout=120
            )
            if out.returncode:
                raise RuntimeError(out.stderr[-400:])
            samples.append(float(out.stdout.strip()) / 1000)
        fast = sum(1 for s in samples if s < 0.005)
        return percentiles(samples) | {
            "starts": len(samples),
            "under_5ms": fast,
            "over_15ms": sum(1 for s in samples if s > 0.015),
        }


def loadgen_binary() -> str | None:
    """The crowd generator, if this checkout has built one.

    Not in the wheel -- it is a cargo bin -- so it comes from the working
    tree and is the same one for every version measured. It only speaks the
    wire protocol, which is versioned, so an old registry ignores what it does
    not know. Scenarios that need a crowd report n/a without it rather than
    spawning a thousand interpreters.
    """
    env = os.environ.get("TINYRAY_LOADGEN")
    if env:
        return env if Path(env).exists() else None
    here = Path(__file__).resolve().parent / "target/release/loadgen"
    return str(here) if here.exists() else None


class Crowd:
    """`members` churn members in pool "load", held up for the duration."""

    def __init__(self, endpoint: str, members: int, seconds: int = 60) -> None:
        self.binary = loadgen_binary()
        self.endpoint = endpoint
        self.members = members
        self.seconds = seconds
        self.proc: subprocess.Popen | None = None

    def __enter__(self) -> Crowd:
        if self.binary is None:
            raise RuntimeError("no loadgen built")
        self.proc = subprocess.Popen(
            [
                self.binary,
                "--endpoint",
                self.endpoint,
                "--members",
                str(self.members),
                "--seconds",
                str(self.seconds),
                "--interval-ms",
                "500",
                "--watchers",
                "0",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return self

    def __exit__(self, *exc: object) -> None:
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def timed(fn: Callable[[], Any], rounds: int = 200) -> float:
    for _ in range(5):
        fn()
    samples = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return round(statistics.median(samples) * 1000, 3)


def bench_lookup_scaling() -> dict[str, Any]:
    """The local-cache reads, and epoch's freeze, against pool size.

    A curve rather than a point: these are the calls whose cost is supposed to
    grow with the roster, so one measurement says nothing about the shape. The
    crowd comes from loadgen, because one interpreter per member caps out long
    before the sizes worth knowing about -- 200 of them took 35s to start.
    """
    if loadgen_binary() is None:
        return {"error": "no loadgen built; run cargo build --release --bin loadgen"}
    out: dict[str, Any] = {}
    for size in (10, 100, 1000):
        with Registry() as reg, Crowd(reg.endpoint, size):
            try:
                me = tinyray.join("watch", "churn")
                me.ready()
                pool = tinyray.pool("load")
                pool.wait(count=size, timeout=90)
                row: dict[str, Any] = {
                    "all_ms": timed(lambda p=pool: p.all()),
                    "pick_filtered_ms": timed(lambda p=pool: p.all(shard=3)),
                    "snapshot_ms": timed(lambda p=pool: p.snapshot()),
                }
                digest = getattr(tinyray._client, "field_digest", None)
                if callable(digest):

                    def one_digest(d: Any = digest) -> Any:
                        try:
                            return d("load", ["shard"])
                        except TypeError:
                            return d("load", ["shard"], False)

                    row["field_digest_ms"] = timed(one_digest)
                else:
                    row["field_digest_ms"] = None
                try:
                    row["epoch_ms"] = timed(
                        lambda p=pool, n=size: p.epoch(min=n, timeout=30), rounds=50
                    )
                except Exception as e:
                    row["epoch_ms"] = f"n/a ({type(e).__name__})"
                out[str(size)] = row
            finally:
                leave_quietly()
    return out


def bench_watch_wakeup() -> dict[str, Any]:
    """changes(): how long from a peer publishing to the iterator returning.

    The discovery scenario polls all() in a loop, so it measures the change
    reaching the cache. This measures the thing the library is actually built
    around -- being woken -- and, with fields=, how much of that waking a
    watcher can decline.
    """
    with Registry() as reg:
        peer = None
        try:
            peer_src = (
                "import os, sys, tinyray\n"
                f"os.environ['TINYRAY_REGISTRY'] = '{reg.endpoint}'\n"
                "m = tinyray.join('news', 'churn')\n"
                "m.ready(role='a', n=0)\n"
                "print('UP', flush=True)\n"
                "n = 0\n"
                "for line in sys.stdin:\n"
                "    key, _, _ = line.strip().partition(' ')\n"
                "    n += 1\n"
                "    upd = getattr(m, 'update', None) or m.ready\n"
                "    upd(**({'role': f'r{n}'} if key == 'role' else {'n': n}))\n"
                "    print('SENT', flush=True)\n"
            )
            peer = subprocess.Popen(
                [sys.executable, "-c", peer_src],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            assert peer.stdout is not None and peer.stdin is not None
            assert peer.stdout.readline().strip() == "UP"

            me = tinyray.join("watch", "churn")
            me.ready()
            pool = tinyray.pool("news")
            pool.wait(count=1, timeout=20)

            out: dict[str, Any] = {}
            with watching(pool, timeout=20.0) as w:
                samples = []
                for _ in range(10):
                    t0 = time.perf_counter()
                    peer.stdin.write("n\n")
                    peer.stdin.flush()
                    peer.stdout.readline()
                    next(w)
                    samples.append(time.perf_counter() - t0)
                out["wakeup"] = percentiles(samples)

            try:
                watch = watching(pool, timeout=3.0, fields=["role"])
            except TypeError:
                out["fields_supported"] = False
                return out
            out["fields_supported"] = True
            with watch as w:
                got = []
                thread = threading.Thread(target=lambda: got.extend(iter(w)), daemon=True)
                thread.start()
                for _ in range(10):  # ten changes to a key nobody subscribed to
                    peer.stdin.write("n\n")
                    peer.stdin.flush()
                    peer.stdout.readline()
                time.sleep(2.0)
                out["wakeups_for_10_unwatched_changes"] = len(got)
                before = len(got)
                peer.stdin.write("role\n")
                peer.stdin.flush()
                peer.stdout.readline()
                time.sleep(2.0)
                out["woken_by_the_watched_key"] = len(got) > before
            return out
        finally:
            if peer is not None:
                try:
                    assert peer.stdin is not None
                    peer.stdin.close()
                    peer.wait(timeout=5)
                except Exception:
                    peer.kill()
            leave_quietly()


def bench_async_call() -> dict[str, Any]:
    """The async twin of the RPC path, beside the synchronous one.

    Sync and async are two implementations of one promise here, and the pair
    has drifted before, so the number worth having is the difference.
    """
    import asyncio

    if not hasattr(tinyray, "apool"):
        return {"error": "no apool in this build"}
    with Registry():
        me = serving_member()
        try:
            me.ready()
            settle(me)
            tinyray.pool("b").wait(count=1, timeout=20)

            async def body() -> list[float]:
                handle = tinyray.apool("b").all()[0]
                for _ in range(50):
                    await handle.ping()
                out = []
                for _ in range(1000):
                    t0 = time.perf_counter()
                    await handle.ping()
                    out.append(time.perf_counter() - t0)
                return out

            return percentiles(asyncio.run(body())) | {"calls": 1000}
        finally:
            leave_quietly()


def bench_rpc_error() -> dict[str, Any]:
    """A method that raises, beside one that does not. The failure path is
    code too, and it is the one nobody times."""

    class Raiser:
        def ok(self) -> str:
            return "ok"

        def boom(self) -> str:
            raise ValueError("no")

    with Registry():
        try:
            try:
                me = tinyray.join("e", "stateful", slot=0, size=1, serves=Raiser())
            except TypeError:
                me = tinyray.join("e", "stateful", slot=0, serves=Raiser())
            me.ready()
            settle(me)
            handle = tinyray.pool("e").wait(count=1, timeout=20)[0]

            def call_ok() -> Any:
                return handle.ok()

            def call_boom() -> Any:
                try:
                    handle.boom()
                except Exception:
                    pass

            return {
                "ok_p50_ms": timed(call_ok, rounds=500),
                "raising_p50_ms": timed(call_boom, rounds=500),
            }
        finally:
            leave_quietly()


def bench_publish() -> dict[str, Any]:
    """ready/update local cost, the dedup that makes republishing free, and
    what flush() pays to know the registry has it."""
    with Registry():
        try:
            me = tinyray.join("p", "churn")
            me.ready(step=0)
            settle(me)
            n = [0]

            def changing() -> None:
                n[0] += 1
                publish(me, step=n[0])

            def unchanged() -> None:
                publish(me, step=n[0])

            out = {
                "update_changed_us": round(timed(changing, rounds=500) * 1000, 1),
                "update_same_us": round(timed(unchanged, rounds=500) * 1000, 1),
            }
            flush = getattr(me, "flush", None)
            if callable(flush):
                samples = []
                for i in range(20):
                    publish(me, step=1000 + i)
                    t0 = time.perf_counter()
                    flush()
                    samples.append(time.perf_counter() - t0)
                out["flush_p50_ms"] = round(statistics.median(samples) * 1000, 2)
            else:
                out["flush_p50_ms"] = None
            return out
        finally:
            leave_quietly()


SCENARIOS: dict[str, Callable[[], dict[str, Any]]] = {
    "rpc_latency": bench_rpc_latency,
    "rpc_throughput": bench_rpc_throughput,
    "rpc_payload_64k": bench_rpc_payload,
    "rpc_error": bench_rpc_error,
    "async_call": bench_async_call,
    "publish": bench_publish,
    "lookup_scaling": bench_lookup_scaling,
    "watch_wakeup": bench_watch_wakeup,
    "discovery": bench_discovery,
    "idle_beat_rate": bench_idle_beat_rate,
    "join_cold_start": bench_join,
}

# What --check compares. Deliberately a short list: a baseline that cries wolf
# gets ignored, and then it is worse than none. Chosen from the spread across
# five consecutive runs on an idle machine, worst deviation from the median:
#
#   watch_wakeup p50   0.2%     |  rpc_payload p50        4.4%
#   discovery p50      0.4%     |  rpc_throughput         4.3%
#   rpc_latency p50    0.7%     |  publish flush p50      2.9%
#   lookup @1000       0.6-1.1% |  async_call p50         2.6%
#
# and what is left out, with the number that disqualified it:
#
#   join_cold_start p50   2306%  bimodal; the median lands on whichever mode
#                                happens to hold more, so it says nothing
#   async_call p99         129%  a thousand calls, tail owned by scheduling
#   every max_ms         24-34%  one sample, by definition the worst one
#   update_changed_us       33%  3us against a 1us resolution
#   idle_beat_rate           9%  two beats a second, quantised by the interval
#   lookup @10               0%  quantised at 0.001ms; a 20% move is noise
#   rpc_throughput          22%  see below
#
# Higher is worse everywhere except calls_per_s.
WATCHED: dict[str, float] = {
    # key: the smallest absolute change worth reporting, in the key's own unit
    "rpc_latency.p50_ms": 0.05,
    "rpc_latency.p90_ms": 0.05,
    "rpc_payload_64k.p50_ms": 0.1,
    "rpc_error.raising_p50_ms": 0.05,
    "async_call.p50_ms": 0.1,
    "publish.flush_p50_ms": 50.0,
    "lookup_scaling.100.all_ms": 0.02,
    "lookup_scaling.100.snapshot_ms": 0.02,
    "lookup_scaling.1000.all_ms": 0.1,
    "lookup_scaling.1000.snapshot_ms": 0.1,
    "lookup_scaling.1000.epoch_ms": 0.1,
    "lookup_scaling.1000.field_digest_ms": 0.02,
    "lookup_scaling.1000.pick_filtered_ms": 0.02,
    "discovery.p50_ms": 5.0,
    "watch_wakeup.wakeup.p50_ms": 5.0,
    # rpc_throughput is measured and printed but not watched. Five runs of one
    # build put it at 4.3%, which was flattering: six later runs of the same
    # build spread 815-958, and a run of v0.10.0 came back 662 against 899 and
    # 901 either side of it -- 22% below its own median. Eight GIL-bound
    # threads sharing one client for five seconds is not a quantity that holds
    # still. A tolerance wide enough not to cry wolf here would be wide enough
    # to let a real regression through, and a baseline that cries wolf gets
    # ignored, so it is worse than none.
}
BIGGER_IS_BETTER: set[str] = set()
TOLERANCE = 0.20


def flatten(scenarios: dict[str, Any], prefix: str = "") -> dict[str, float]:
    out: dict[str, float] = {}
    for key, value in scenarios.items():
        if isinstance(value, dict):
            out.update(flatten(value, f"{prefix}{key}."))
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            out[f"{prefix}{key}"] = float(value)
    return out


def compare(baseline: dict[str, Any], now: dict[str, Any]) -> tuple[list[str], list[str], int]:
    """Regressions, improvements, and how many watched metrics were checked."""
    was = flatten(baseline["scenarios"])
    is_ = flatten(now["scenarios"])
    worse: list[str] = []
    better: list[str] = []
    checked = 0
    for key, floor in WATCHED.items():
        if key not in was or key not in is_:
            continue
        checked += 1
        a, b = was[key], is_[key]
        if key in BIGGER_IS_BETTER:
            a, b = -a, -b
        change = (b - a) / abs(a) if a else 0.0
        if abs(b - a) < floor:
            continue
        if change > TOLERANCE:
            worse.append(f"{key}: {was[key]} -> {is_[key]}  ({change * 100:+.0f}%)")
        elif change < -TOLERANCE:
            better.append(f"{key}: {was[key]} -> {is_[key]}  ({change * 100:+.0f}%)")
    return worse, better, checked


def main() -> int:
    ap = argparse.ArgumentParser(description="Measure tinyray.")
    ap.add_argument("--json", help="write results here as well")
    ap.add_argument("--only", nargs="*", help="run just these scenarios")
    ap.add_argument("--label", default="", help="what build this is, for the report")
    ap.add_argument(
        "--check",
        nargs="?",
        const="bench-baseline.json",
        help="compare against a baseline and fail on a regression",
    )
    args = ap.parse_args()

    wanted = args.only or list(SCENARIOS)
    results: dict[str, Any] = {
        "label": args.label or getattr(tinyray, "__version__", "unknown"),
        "python": sys.version.split()[0],
        "scenarios": {},
    }
    for name in wanted:
        fn = SCENARIOS.get(name)
        if fn is None:
            print(f"{name}: no such scenario", file=sys.stderr)
            continue
        t0 = time.monotonic()
        try:
            got: Any = fn()
        except Exception as e:  # a build that cannot do this at all
            got = {"error": f"{type(e).__name__}: {e}"[:200]}
        got["took_s"] = round(time.monotonic() - t0, 1)
        results["scenarios"][name] = got
        print(f"{name:20s} {json.dumps(got, sort_keys=True)}", flush=True)

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, sort_keys=True))

    if not args.check:
        return 0
    path = Path(args.check)
    if not path.exists():
        print(f"\nno baseline at {path}; write one with --json {path}", file=sys.stderr)
        return 1
    worse, better, checked = compare(json.loads(path.read_text()), results)
    for line in better:
        print(f"  faster  {line}")
    for line in worse:
        print(f"  SLOWER  {line}")
    print(f"\n{checked - len(worse)} of {checked} within {int(TOLERANCE * 100)}% of the baseline")
    return 1 if worse else 0


if __name__ == "__main__":
    sys.exit(main())
