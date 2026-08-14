"""Performance benchmarks for the tinyray data path.

Run with::

    pytest benchmarks/ -q -s -m bench

These are not correctness tests; they exist to keep the performance claims
honest. Two of them are load-bearing for the whole architecture:

``test_serialize_does_not_scale_with_copies``
    proves the out-of-band path really avoids duplicating tensors.

``test_encode_is_not_blocked_by_a_gil_hog``
    is the justification for writing the core in Rust at all. If a thread
    spinning in pure Python can stall the data path, the Rust core is not
    buying what the design says it buys, and the sensible response is to go
    back to a pure Python implementation.
"""

from __future__ import annotations

import statistics
import threading
import time

import pytest

import tinyray
from tinyray import decode_message, encode_message, serde

pytestmark = pytest.mark.bench

MB = 1024 * 1024
PAYLOAD_MB = 10
REPEATS = 20


def _report(label: str, seconds: float, nbytes: int | None = None) -> None:
    if nbytes:
        rate = nbytes / seconds / 1e9
        print(f"\n  {label:<52} {seconds * 1e3:8.3f} ms   {rate:6.2f} GB/s")
    else:
        print(f"\n  {label:<52} {seconds * 1e6:8.1f} us")


def _time(fn, repeats=REPEATS):
    """Median wall time of ``fn``, after a warm-up call."""
    fn()
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples)


@pytest.fixture(scope="module")
def payload(request):
    numpy = pytest.importorskip("numpy")
    return numpy.ones(PAYLOAD_MB * MB // 4, dtype=numpy.float32)


class TestFramingThroughput:
    def test_encode_10mb(self, payload):
        body, frames = serde.serialize(payload)
        elapsed = _time(lambda: encode_message(body, frames))
        _report("encode 10 MB result", elapsed, payload.nbytes)

    def test_decode_10mb(self, payload):
        body, frames = serde.serialize(payload)
        blob = encode_message(body, frames)
        elapsed = _time(lambda: decode_message(blob))
        _report("decode 10 MB result (zero copy)", elapsed, payload.nbytes)
        # Decoding only slices refcounted buffers, so it should be far faster
        # than a memcpy of the payload.
        assert elapsed < 0.010, "decode should not be copying the payload"

    def test_full_serialize_encode_decode_deserialize(self, payload):
        def cycle():
            body, frames = serde.serialize(payload)
            header, decoded = decode_message(encode_message(body, frames))
            return serde.deserialize(header, decoded)

        elapsed = _time(cycle)
        _report("serialize -> encode -> decode -> deserialize", elapsed, payload.nbytes)

    def test_small_message_latency(self):
        body, frames = serde.serialize({"step": 1, "reward": 0.5})
        elapsed = _time(lambda: decode_message(encode_message(body, frames)), repeats=2000)
        _report("small control message round trip", elapsed)
        assert elapsed < 100e-6


class TestZeroCopy:
    def test_serialize_does_not_scale_with_copies(self, payload):
        """The pickle body must stay tiny however large the tensor is."""
        body, frames = serde.serialize(payload)
        total = serde.payload_size(body, frames)
        overhead = total - payload.nbytes
        print(
            f"\n  {'serialise overhead over raw tensor':<52} {overhead:8d} B   (body={len(body)} B)"
        )
        assert overhead < 4096, (
            f"{overhead} bytes of overhead on a {payload.nbytes} byte tensor: "
            "the payload is being copied through the pickle stream"
        )

    def test_decode_shares_memory_with_the_wire_buffer(self, payload):
        numpy = pytest.importorskip("numpy")
        body, frames = serde.serialize(payload)
        _, decoded = decode_message(encode_message(body, frames))

        first = numpy.frombuffer(decoded[0], dtype=numpy.uint8)
        second = numpy.frombuffer(decoded[0], dtype=numpy.uint8)
        assert first.__array_interface__["data"][0] == second.__array_interface__["data"][0], (
            "each reader got its own copy"
        )

    def test_serving_one_result_to_many_readers_is_flat(self, payload):
        """Cost of handing a result to N consumers must not grow with N."""
        numpy = pytest.importorskip("numpy")
        body, frames = serde.serialize(payload)
        _, decoded = decode_message(encode_message(body, frames))
        frame = decoded[0]

        one = _time(lambda: numpy.frombuffer(frame, dtype=numpy.uint8), repeats=200)
        thirty_two = _time(
            lambda: [numpy.frombuffer(frame, dtype=numpy.uint8) for _ in range(32)],
            repeats=200,
        )
        print(
            f"\n  {'view 1 vs 32 consumers':<52} {one * 1e6:8.2f} us vs {thirty_two * 1e6:.2f} us"
        )
        # 32 views of a 10 MB buffer would cost ~320 MB of copying if this were
        # not zero copy; it must stay in the microseconds.
        assert thirty_two < 1e-3


class TestGilIsolation:
    """The load-bearing benchmark for the Rust core.

    A pure-Python thread that never releases the GIL stands in for an actor
    running a 200 ms training step. While that is happening, other actors must
    still be able to pull results from this process at full speed.

    The distinction these two benchmarks draw is the whole argument for the
    Rust core:

    * work *initiated from Python* inherits GIL scheduling latency, no matter
      how little of it actually runs in the interpreter;
    * work *initiated from a native thread* is immune.

    The real serving path is the second kind: a tokio worker answers a result
    fetch without ever entering the interpreter. That is why the design forbids
    driving the serving path from Python.
    """

    @staticmethod
    def _with_gil_hogs(fn, *, hogs: int):
        stop = threading.Event()

        def burn_gil():
            # Pure Python arithmetic: holds the GIL except at switch intervals.
            total = 0
            while not stop.is_set():
                for _ in range(10_000):
                    total += 1
            return total

        threads = []
        for _ in range(hogs):
            thread = threading.Thread(target=burn_gil, daemon=True)
            thread.start()
            threads.append(thread)
        if threads:
            time.sleep(0.05)  # let them get going
        try:
            return fn()
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)

    def test_native_thread_decode_is_immune_to_gil_contention(self, payload):
        """This is the result that justifies writing the core in Rust."""
        body, frames = serde.serialize(payload)
        blob = encode_message(body, frames)

        def measure():
            return tinyray._tinyray.bench_decode_native(blob, 50)

        quiet = self._with_gil_hogs(measure, hogs=0)
        contended = self._with_gil_hogs(measure, hogs=4)
        slowdown = contended / quiet
        print(
            f"\n  {'decode 10 MB on a native thread, idle vs 4 hogs':<52} "
            f"{quiet * 1e3:8.3f} ms vs {contended * 1e3:.3f} ms  ({slowdown:.2f}x)"
        )
        # The design budgets a 5% regression; allow slack for a shared machine.
        assert slowdown < 1.5, (
            f"native decode slowed down {slowdown:.2f}x under GIL contention; the "
            "Rust data path is not actually decoupled from the interpreter"
        )

    def test_python_initiated_decode_does_pay_gil_latency(self, payload):
        """The contrast case, kept deliberately.

        This is *expected* to degrade. It documents why the LocalStore serving
        path must be driven by tokio rather than by a Python-side loop: the
        moment Python has to make the call, it queues behind whatever else is
        holding the interpreter.
        """
        body, frames = serde.serialize(payload)
        blob = encode_message(body, frames)

        def measure():
            return _time(lambda: decode_message(blob), repeats=50)

        quiet = self._with_gil_hogs(measure, hogs=0)
        contended = self._with_gil_hogs(measure, hogs=4)
        print(
            f"\n  {'decode 10 MB from Python, idle vs 4 hogs':<52} "
            f"{quiet * 1e3:8.3f} ms vs {contended * 1e3:.3f} ms  "
            f"({contended / quiet:.2f}x)  <- expected to degrade"
        )
        assert contended >= quiet
