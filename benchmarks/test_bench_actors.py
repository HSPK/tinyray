"""Benchmarks for the actor path.

Run with::

    pytest benchmarks/ -q -s -m bench

These measure the numbers the design commits to: call latency against a real
HTTP listener, 10 MB result throughput, and actor creation cost.
"""

from __future__ import annotations

import statistics
import time

import pytest

import tinyray

pytestmark = pytest.mark.bench

MB = 1024 * 1024


def _report(label: str, seconds: float, nbytes: int | None = None) -> None:
    if nbytes:
        print(f"\n  {label:<52} {seconds * 1e3:8.3f} ms   {nbytes / seconds / 1e9:6.2f} GB/s")
    else:
        print(f"\n  {label:<52} {seconds * 1e6:8.1f} us")


@tinyray.remote
class Bench:
    def __init__(self):
        self.calls = 0

    def noop(self):
        self.calls += 1
        return None

    def produce(self, nbytes):
        import numpy as np

        return np.zeros(nbytes, dtype=np.uint8)

    def consume(self, payload):
        return int(payload.nbytes)

    def busy(self, seconds):
        # Pure Python: holds the GIL, standing in for a training step.
        deadline = time.perf_counter() + seconds
        total = 0
        while time.perf_counter() < deadline:
            for _ in range(1000):
                total += 1
        return total


@pytest.fixture(scope="module")
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


def test_round_trip_latency(ray):
    """Submit plus fetch against a real listener."""
    actor = Bench.remote()
    ray.get(actor.noop.remote())  # warm the connection pool

    samples = []
    for _ in range(200):
        started = time.perf_counter()
        ray.get(actor.noop.remote())
        samples.append(time.perf_counter() - started)
    samples.sort()

    median = statistics.median(samples)
    print(
        f"\n  {'actor call round trip (submit + fetch)':<52} "
        f"p50 {median * 1e6:7.1f} us   p99 {samples[int(len(samples) * 0.99)] * 1e6:7.1f} us"
    )
    # Against a 200 ms task this is noise; the design explicitly declines to
    # optimise it further.
    assert median < 5e-3


def test_submission_is_non_blocking(ray):
    actor = Bench.remote()
    samples = []
    for _ in range(200):
        started = time.perf_counter()
        actor.noop.remote()
        samples.append(time.perf_counter() - started)
    median = statistics.median(samples)
    _report("submit only (no wait for the method)", median)
    assert median < 2e-3


def test_ten_megabyte_result(ray):
    actor = Bench.remote()
    ray.get(actor.produce.remote(1024))

    samples = []
    for _ in range(10):
        started = time.perf_counter()
        ray.get(actor.produce.remote(10 * MB))
        samples.append(time.perf_counter() - started)
    median = statistics.median(samples)
    _report("produce + fetch 10 MB result", median, 10 * MB)


def test_actor_creation_cost(ray):
    samples = []
    for _ in range(5):
        started = time.perf_counter()
        handle = Bench.remote()
        samples.append(time.perf_counter() - started)
        tinyray.kill(handle)
    median = statistics.median(samples)
    print(f"\n  {'cold actor creation (process start + __init__)':<52} {median * 1e3:8.1f} ms")


def test_serving_is_not_blocked_by_a_busy_actor(ray):
    """The architectural claim, measured through the full stack.

    One actor is pinned at 100% GIL usage while another serves results. If the
    data path were driven from Python, the second actor's throughput would
    collapse; because it is served by tokio threads that never enter the
    interpreter, it should not.
    """
    busy = Bench.remote()
    server = Bench.remote()
    ray.get(server.produce.remote(1024))

    def measure():
        samples = []
        for _ in range(5):
            started = time.perf_counter()
            ray.get(server.produce.remote(MB))
            samples.append(time.perf_counter() - started)
        return statistics.median(samples)

    quiet = measure()
    busy.busy.remote(3.0)  # occupies the other actor's interpreter
    time.sleep(0.3)
    contended = measure()

    print(
        f"\n  {'1 MB fetch while another actor pins the GIL':<52} "
        f"{quiet * 1e3:8.3f} ms vs {contended * 1e3:.3f} ms  ({contended / quiet:.2f}x)"
    )
    # Separate processes, so this is really testing that nothing in the path
    # serialises behind a single interpreter.
    assert contended < quiet * 3
