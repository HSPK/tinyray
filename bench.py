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
import hashlib
import inspect
import json
import math
import os
import platform
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import msgspec
import tinyray

TTL_MS = 2000
FORMAT_VERSION = 2
COALESCE_MS: int | None = None
_registries: list[Registry] = []


class UnsupportedScenario(Exception):
    """A feature known to be absent, not an execution failure."""


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
    def __init__(self, ttl_ms: int = TTL_MS) -> None:
        self.port = free_port()
        self.endpoint = f"127.0.0.1:{self.port}"
        self.ttl_ms = ttl_ms
        self.proc: subprocess.Popen | None = None
        self.members = ExitStack()
        self.previous_endpoint: str | None = None

    def __enter__(self) -> Registry:
        self.proc = subprocess.Popen(
            [registry_binary(), "--listen", self.endpoint, "--ttl-ms", str(self.ttl_ms)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"http://{self.endpoint}/health", timeout=0.5) as r:
                    if r.status == 200:
                        self.previous_endpoint = os.environ.get("TINYRAY_REGISTRY")
                        os.environ["TINYRAY_REGISTRY"] = self.endpoint
                        _registries.append(self)
                        return self
            except (urllib.error.URLError, OSError):
                time.sleep(0.02)
        self.stop()
        raise RuntimeError("registry did not come up")

    def stop(self) -> None:
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)

    def __exit__(self, *exc: object) -> None:
        try:
            self.members.close()
        finally:
            try:
                self.stop()
            finally:
                if _registries.pop() is not self:
                    raise RuntimeError("benchmark registry contexts exited out of order")
                if self.previous_endpoint is None:
                    os.environ.pop("TINYRAY_REGISTRY", None)
                else:
                    os.environ["TINYRAY_REGISTRY"] = self.previous_endpoint


def joined(*args: Any, **kwargs: Any) -> Any:
    if not _registries:
        raise RuntimeError("benchmark members need a Registry context")
    parameters = inspect.signature(tinyray.join).parameters
    if "size" not in parameters:
        kwargs.pop("size", None)
    if "serves" in kwargs and "serves" not in parameters:
        raise UnsupportedScenario("this build cannot serve RPC methods")
    if COALESCE_MS is not None:
        if "coalesce_ms" not in parameters:
            raise UnsupportedScenario("this build has no coalesce_ms option")
        kwargs["coalesce_ms"] = COALESCE_MS
    member = tinyray.join(*args, **kwargs)
    _registries[-1].members.callback(member.leave)
    return member


def leave_members() -> None:
    if _registries:
        _registries[-1].members.close()


def coalesce_source() -> str:
    return "" if COALESCE_MS is None else f", coalesce_ms={COALESCE_MS}"


def settle(member: Any) -> None:
    """Get published state to the registry, whatever this build calls it."""
    flush = getattr(member, "flush", None)
    if callable(flush):
        flush()
        return
    time.sleep(TTL_MS / 1000 / 2)


def percentiles(samples: list[float]) -> dict[str, float]:
    s = sorted(samples)

    def at(p: float) -> float:
        return s[min(len(s) - 1, int(len(s) * p))]

    return {
        "p50_ms": round(statistics.median(s) * 1000, 6),
        "p90_ms": round(at(0.90) * 1000, 6),
        "p99_ms": round(at(0.99) * 1000, 6),
        "max_ms": round(s[-1] * 1000, 6),
    }


def watching(pool: Any, **kw: Any) -> Any:
    """`changes()` as a context manager, across builds that return a bare
    generator instead. Known missing features are reported as unsupported,
    rather than treating execution failures as version differences."""
    if not hasattr(pool, "changes"):
        raise UnsupportedScenario("this build has no changes()")
    if "fields" in kw and "fields" not in inspect.signature(pool.changes).parameters:
        raise UnsupportedScenario("this build cannot watch selected fields")
    watch = pool.changes(**kw)
    if hasattr(watch, "__enter__"):
        return watch
    raise UnsupportedScenario("this build's changes() is a plain generator")


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
    return joined(pool, "stateful", slot=0, size=1, serves=Service())


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
            return percentiles(samples) | {"calls": len(samples), "topology": "same_process"}
        finally:
            leave_members()


def bench_rpc_throughput() -> dict[str, Any]:
    """Eight threads with caller and callee sharing the same interpreter."""
    with Registry():
        me = serving_member()
        try:
            me.ready()
            settle(me)
            handle = tinyray.pool("b").wait(count=1, timeout=20)[0]
            handle.ping()
            return {
                "threads": 8,
                "calls_per_s": rpc_rate(handle),
                "topology": "same_process",
            }
        finally:
            leave_members()


def rpc_rate(handle: Any, threads: int = 8, duration: float = 5.0) -> int:
    stop = threading.Event()

    def worker() -> int:
        count = 0
        while not stop.is_set():
            if handle.ping() != "pong":
                raise RuntimeError("RPC benchmark returned an unexpected result")
            count += 1
        return count

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as workers:
        futures = [workers.submit(worker) for _ in range(threads)]
        try:
            time.sleep(duration)
        finally:
            stop.set()
        count = sum(future.result(timeout=35) for future in futures)
    return round(count / (time.perf_counter() - t0))


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
            return percentiles(samples) | {"bytes": len(blob), "topology": "same_process"}
        finally:
            leave_members()


def bench_discovery(pause: float = 0.0) -> dict[str, Any]:
    """Publish here, notice there. This is what long polling bought."""
    with Registry() as reg:
        peer = None
        try:
            peer_src = (
                "import os, sys, tinyray\n"
                f"os.environ['TINYRAY_REGISTRY'] = '{reg.endpoint}'\n"
                f"m = tinyray.join('news', 'churn'{coalesce_source()})\n"
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
            if peer.stdout is None or peer.stdin is None:
                raise RuntimeError("benchmark peer pipes are missing")
            if peer.stdout.readline().strip() != "UP":
                raise RuntimeError("benchmark peer did not start")

            me = joined("watch", "churn")
            me.ready()
            pool = tinyray.pool("news")
            pool.wait(count=1, timeout=20)

            samples = []
            for i in range(1, 11):
                time.sleep(pause)
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
            return percentiles(samples) | {
                "changes": len(samples),
                "publish_gap_ms": pause * 1000,
                "topology": "separate_processes",
            }
        finally:
            if peer is not None:
                try:
                    peer.stdin.close()
                    peer.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    peer.kill()
                    peer.wait(timeout=5)
            leave_members()


def bench_idle_beat_rate() -> dict[str, Any]:
    """Requests a quiet member costs the registry per second."""
    with Registry():
        try:
            me = joined("quiet", "churn")
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
            leave_members()


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
            f"m = tinyray.join('cold', 'churn'{coalesce_source()})\n"
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
                self.proc.wait(timeout=5)


def timed(fn: Callable[[], Any], rounds: int = 200) -> float:
    for _ in range(5):
        fn()
    samples = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return round(statistics.median(samples) * 1000, 6)


def digest_reader(digest: Any) -> Callable[[], Any]:
    legacy = "require_ready" in inspect.signature(digest).parameters

    def read() -> Any:
        return digest("load", ["shard"], False) if legacy else digest("load", ["shard"])

    return read


def bench_lookup_scaling() -> dict[str, Any]:
    """The local-cache reads, and epoch's freeze, against pool size.

    A curve rather than a point: these are the calls whose cost is supposed to
    grow with the roster, so one measurement says nothing about the shape. The
    crowd comes from loadgen, because one interpreter per member caps out long
    before the sizes worth knowing about -- 200 of them took 35s to start.
    """
    if loadgen_binary() is None:
        raise RuntimeError("no loadgen built; run cargo build --release --bin loadgen")
    out: dict[str, Any] = {}
    for size in (10, 100, 1000):
        with Registry() as reg, Crowd(reg.endpoint, size):
            try:
                me = joined("watch", "churn")
                me.ready()
                pool = tinyray.pool("load")
                pool.wait(count=size, timeout=90)
                row: dict[str, Any] = {
                    "all_ms": timed(lambda p=pool: p.all()),
                    "all_filtered_ms": timed(lambda p=pool: p.all(shard=3)),
                    "pick_ms": timed(lambda p=pool: p.pick()),
                    "pick_filtered_ms": timed(lambda p=pool: p.pick(shard=3)),
                    "snapshot_ms": timed(lambda p=pool: p.snapshot()),
                }
                digest = getattr(tinyray._client, "field_digest", None)
                if callable(digest):
                    row["field_digest_ms"] = timed(digest_reader(digest))
                else:
                    row["field_digest_ms"] = None
                if hasattr(pool, "epoch"):
                    row["epoch_ms"] = timed(
                        lambda p=pool, n=size: p.epoch(min=n, timeout=30), rounds=50
                    )
                else:
                    row["epoch_ms"] = None
                out[str(size)] = row
            finally:
                leave_members()
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
                f"m = tinyray.join('news', 'churn'{coalesce_source()})\n"
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
            if peer.stdout is None or peer.stdin is None:
                raise RuntimeError("benchmark peer pipes are missing")
            if peer.stdout.readline().strip() != "UP":
                raise RuntimeError("benchmark peer did not start")

            me = joined("watch", "churn")
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
            except UnsupportedScenario:
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
                    if peer.stdin is None:
                        raise RuntimeError("benchmark peer input pipe is missing")
                    peer.stdin.close()
                    peer.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    peer.kill()
                    peer.wait(timeout=5)
            leave_members()


def bench_async_call() -> dict[str, Any]:
    """The async twin of the RPC path, beside the synchronous one.

    Sync and async are two implementations of one promise here, and the pair
    has drifted before, so the number worth having is the difference.
    """
    import asyncio

    if not hasattr(tinyray, "apool"):
        raise UnsupportedScenario("no apool in this build")
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

            return percentiles(asyncio.run(body())) | {
                "calls": 1000,
                "topology": "same_process",
                "concurrency": 1,
            }
        finally:
            leave_members()


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
            me = joined("e", "stateful", slot=0, size=1, serves=Raiser())
            me.ready()
            settle(me)
            handle = tinyray.pool("e").wait(count=1, timeout=20)[0]

            def call_ok() -> Any:
                return handle.ok()

            def call_boom() -> Any:
                try:
                    handle.boom()
                except tinyray.RemoteError:
                    pass

            return {
                "ok_p50_ms": timed(call_ok, rounds=500),
                "raising_p50_ms": timed(call_boom, rounds=500),
            }
        finally:
            leave_members()


def bench_publish() -> dict[str, Any]:
    """ready/update local cost, the dedup that makes republishing free, and
    what flush() pays to know the registry has it."""
    with Registry():
        try:
            me = joined("p", "churn")
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
            leave_members()


class RemoteService:
    """A callee with its own GIL, rather than another thread in the caller."""

    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None

    def __enter__(self) -> Any:
        source = (
            "import inspect, sys, tinyray\n"
            + inspect.getsource(Service)
            + "\nkwargs = {'slot': 0, 'serves': Service()}\n"
            + "if 'size' in inspect.signature(tinyray.join).parameters: kwargs['size'] = 1\n"
            + f"with tinyray.join('remote', 'stateful'{coalesce_source()}, **kwargs) as me:\n"
            + " me.ready()\n print('READY', flush=True)\n sys.stdin.readline()\n"
        )
        with ExitStack() as cleanup:
            self.proc = subprocess.Popen(
                [sys.executable, "-c", source],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            cleanup.callback(self.stop)
            if self.proc.stdout is None:
                raise RuntimeError("RPC benchmark peer output pipe is missing")
            if self.proc.stdout.readline().strip() != "READY":
                raise RuntimeError("RPC benchmark callee did not start")
            me = joined("caller")
            me.ready()
            settle(me)
            handle = tinyray.pool("remote").wait(count=1, timeout=20)[0]
            cleanup.pop_all()
            return handle

    def stop(self) -> None:
        if self.proc is not None:
            try:
                self.proc.communicate("\n", timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.communicate(timeout=5)

    def __exit__(self, *exc: object) -> None:
        self.stop()


def bench_rpc_separate() -> dict[str, Any]:
    with Registry(), RemoteService() as handle:
        for _ in range(200):
            handle.ping()
        samples = []
        for _ in range(2000):
            start = time.perf_counter()
            if handle.ping() != "pong":
                raise RuntimeError("RPC benchmark returned an unexpected result")
            samples.append(time.perf_counter() - start)
        return percentiles(samples) | {
            "calls": len(samples),
            "topology": "separate_processes",
        }


def bench_rpc_concurrency() -> dict[str, Any]:
    with Registry(), RemoteService() as handle:
        handle.ping()
        return {
            "topology": "separate_processes",
            "calls_per_s": {str(n): rpc_rate(handle, n, duration=3) for n in (1, 4, 8)},
        }


def bench_rpc_batch() -> dict[str, Any]:
    if not hasattr(tinyray, "batch"):
        raise UnsupportedScenario("this build has no batch RPC")
    count = 32
    calls = [tinyray.Call("ping") for _ in range(count)]
    expected = ["pong"] * count
    with Registry(), RemoteService() as handle:

        def individual() -> None:
            if [handle.ping() for _ in range(count)] != expected:
                raise RuntimeError("individual RPC benchmark returned unexpected results")

        def batched() -> None:
            if tinyray.batch(handle, calls) != expected:
                raise RuntimeError("batch RPC benchmark returned unexpected results")

        one = timed(individual, rounds=40)
        batch = timed(batched, rounds=40)
        return {
            "topology": "separate_processes",
            "logical_calls": count,
            "individual_total_ms": one,
            "batch_total_ms": batch,
            "batch_per_call_ms": batch / count,
            "speedup": one / batch,
        }


def bench_point_lookup() -> dict[str, Any]:
    """Stable seated rosters: selecting one member must not cost a full list."""
    out = {}
    for size in (100, 1000, 5000):
        with Registry(ttl_ms=120_000) as reg:
            # Synthetic members do not renew: keep their lease outside the
            # measurement window without concurrent load-generator traffic.
            with httpx.Client(base_url=f"http://{reg.endpoint}", trust_env=False) as client:
                for index in range(size):
                    response = client.post(
                        "/v1/beat",
                        json={
                            "pool": "points",
                            "id": index,
                            "slot": index,
                            "size": size,
                            "incarnation": 1,
                            "policy": "stateful",
                            "ready": True,
                            "state": {"idx": index, "shard": index % 8},
                        },
                    )
                    response.raise_for_status()
                    if not response.json()["accepted"]:
                        raise RuntimeError("registry refused the benchmark roster")
            me = joined("point_observer")
            me.ready()
            settle(me)
            pool = tinyray.pool("points")
            pool.wait(count=size, timeout=20)
            out[str(size)] = {
                "slot_ms": timed(lambda p=pool, k=size // 2: p.slot(k)),
                "pick_ms": timed(pool.pick),
                "pick_filtered_ms": timed(lambda p=pool: p.pick(shard=3)),
                "all_ms": timed(pool.all, rounds=50),
                "snapshot_ms": timed(pool.snapshot, rounds=50),
            }
    return out


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
    "rpc_latency_separate": bench_rpc_separate,
    "rpc_concurrency": bench_rpc_concurrency,
    "rpc_batch": bench_rpc_batch,
    "point_lookup": bench_point_lookup,
    "discovery_spaced": lambda: bench_discovery(pause=0.15),
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
    "publish.flush_p50_ms": 0.1,
    "lookup_scaling.100.all_ms": 0.002,
    "lookup_scaling.100.snapshot_ms": 0.002,
    "lookup_scaling.1000.all_ms": 0.1,
    "lookup_scaling.1000.snapshot_ms": 0.1,
    "lookup_scaling.1000.epoch_ms": 0.1,
    "lookup_scaling.1000.field_digest_ms": 0.00005,
    "lookup_scaling.1000.pick_filtered_ms": 0.002,
    "lookup_scaling.1000.all_filtered_ms": 0.02,
    "lookup_scaling.1000.pick_ms": 0.0002,
    "point_lookup.5000.slot_ms": 0.0001,
    "point_lookup.5000.pick_ms": 0.0001,
    "point_lookup.5000.all_ms": 0.1,
    "rpc_latency_separate.p50_ms": 0.05,
    "rpc_batch.batch_total_ms": 0.1,
    "discovery_spaced.p50_ms": 0.1,
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
    """Check every required metric in the selected scenarios, including omissions."""
    was = flatten(baseline["scenarios"])
    is_ = flatten(now["scenarios"])
    worse: list[str] = []
    better: list[str] = []
    checked = 0
    for key, floor in WATCHED.items():
        if key.split(".", 1)[0] not in now["scenarios"]:
            continue
        checked += 1
        if key not in was:
            worse.append(f"{key}: missing from baseline; record a complete baseline")
            continue
        if key not in is_:
            worse.append(f"{key}: missing from current results")
            continue
        a, b = was[key], is_[key]
        if not math.isfinite(a) or not math.isfinite(b) or a < 0 or b < 0:
            worse.append(f"{key}: non-finite or negative metric")
            continue
        if key in BIGGER_IS_BETTER:
            a, b = -a, -b
        change = (b - a) / abs(a) if a else (0.0 if b == a else math.copysign(math.inf, b - a))
        if abs(b - a) < floor:
            continue
        if change > TOLERANCE:
            worse.append(f"{key}: {was[key]} -> {is_[key]}  ({change * 100:+.0f}%)")
        elif change < -TOLERANCE:
            better.append(f"{key}: {was[key]} -> {is_[key]}  ({change * 100:+.0f}%)")
    if checked == 0:
        worse.append("no watched metrics selected; this comparison cannot establish performance")
    return worse, better, checked


def provenance() -> dict[str, Any]:
    root = Path(__file__).resolve().parent
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=5,
    )
    dirty = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=5,
    )
    package = Path(tinyray.__file__).resolve().parent
    core = getattr(tinyray, "_tinyray", None)
    fingerprint = hashlib.sha256()
    paths = sorted(package.glob("*.py"))
    if core is not None and getattr(core, "__file__", None):
        paths.append(Path(core.__file__))
    for path in paths:
        fingerprint.update(path.name.encode())
        fingerprint.update(path.read_bytes())
    cpu = platform.processor() or platform.machine()
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        cpu = next(
            (
                line.split(":", 1)[1].strip()
                for line in cpuinfo.read_text().splitlines()
                if line.startswith("model name")
            ),
            cpu,
        )
    return {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "benchmark_revision": revision.stdout.strip() if revision.returncode == 0 else None,
        "worktree_dirty": bool(dirty.stdout) if dirty.returncode == 0 else None,
        "package_version": getattr(tinyray, "__version__", "unknown"),
        "native_version": getattr(core, "version", None),
        "library_fingerprint": fingerprint.hexdigest(),
        "package_path": (
            package.relative_to(root).as_posix()
            if package.is_relative_to(root)
            else "<external>/tinyray"
        ),
        "environment": {
            "platform": platform.platform(),
            "cpu": cpu,
            "logical_cpus": os.cpu_count(),
            "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
            "httpx": httpx.__version__,
            "msgspec": msgspec.__version__,
            "python_optimization": sys.flags.optimize,
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Measure tinyray.")
    ap.add_argument("--json", help="write results here as well")
    ap.add_argument("--only", nargs="+", choices=list(SCENARIOS), help="run just these scenarios")
    ap.add_argument("--label", default="", help="what build this is, for the report")
    ap.add_argument("--coalesce-ms", type=int, help="explicit client coalescing policy")
    ap.add_argument(
        "--check",
        nargs="?",
        const="bench-baseline.json",
        help="compare against a baseline and fail on a regression",
    )
    args = ap.parse_args(argv)
    global COALESCE_MS
    COALESCE_MS = args.coalesce_ms
    if COALESCE_MS is not None:
        if COALESCE_MS < 0:
            ap.error("--coalesce-ms must be nonnegative")
        if "coalesce_ms" not in inspect.signature(tinyray.join).parameters:
            ap.error("this build does not support --coalesce-ms")
    baseline = None
    if args.check:
        path = Path(args.check)
        if not path.is_file():
            ap.error(f"no baseline at {path}; write one with --json {path}")
        try:
            baseline = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            ap.error(f"invalid baseline JSON: {exc}")
        if not isinstance(baseline, dict) or baseline.get("format_version") != FORMAT_VERSION:
            ap.error("baseline format is obsolete; record a new baseline with --json")
        if not isinstance(baseline.get("scenarios"), dict):
            ap.error("baseline scenarios must be an object")
        if not isinstance(baseline.get("settings"), dict):
            ap.error("baseline settings must be an object")
        if baseline["settings"] != {"ttl_ms": TTL_MS, "coalesce_ms": COALESCE_MS}:
            ap.error("baseline uses different workload settings")

    wanted = args.only or list(SCENARIOS)
    results: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "label": args.label or getattr(tinyray, "__version__", "unknown"),
        "python": sys.version.split()[0],
        "settings": {"ttl_ms": TTL_MS, "coalesce_ms": COALESCE_MS},
        "provenance": provenance(),
        "scenarios": {},
    }
    failed = False
    unsupported = False
    for name in wanted:
        fn = SCENARIOS[name]
        t0 = time.monotonic()
        try:
            got: Any = fn()
            if not isinstance(got, dict):
                raise TypeError("a scenario must return a result object")
            if any(not math.isfinite(value) or value < 0 for value in flatten(got).values()):
                raise ValueError("scenario returned a non-finite or negative measurement")
            if got.get("error"):
                failed = True
                got["status"] = "error"
        except UnsupportedScenario as e:
            unsupported = True
            got = {"status": "unsupported", "reason": str(e)}
        except Exception as e:
            # Keep the remaining observations, but never turn an execution
            # failure into a successful benchmark or a skipped metric.
            failed = True
            got = {"status": "error", "error": f"{type(e).__name__}: {e}"[:200]}
        got["took_s"] = round(time.monotonic() - t0, 1)
        results["scenarios"][name] = got
        print(f"{name:20s} {json.dumps(got, sort_keys=True)}", flush=True)

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, sort_keys=True, allow_nan=False))

    if not args.check:
        return int(failed)
    if baseline is None:
        raise RuntimeError("benchmark comparison has no baseline")
    worse, better, checked = compare(baseline, results)
    for line in better:
        print(f"  faster  {line}")
    for line in worse:
        print(f"  SLOWER  {line}")
    if checked:
        print(
            f"\n{checked - len(worse)} of {checked} within tolerance "
            f"({int(TOLERANCE * 100)}% plus absolute noise floors)"
        )
    return int(bool(worse) or failed or unsupported)


if __name__ == "__main__":
    sys.exit(main())
