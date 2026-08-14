"""Functional tests for the actor API.

These start real processes and talk to them over real HTTP. They are slower
than the unit tests on purpose: the interesting failures in a system like this
live in process startup, ordering under concurrency, and what happens when
something dies.
"""

from __future__ import annotations

import json
import time

import pytest

import tinyray


@pytest.fixture
def ray():
    """A fresh tinyray context, torn down afterwards."""
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


@tinyray.remote
class Counter:
    def __init__(self, start=0):
        self.n = start
        self.history = []

    def inc(self, by=1):
        self.n += by
        self.history.append(by)
        return self.n

    def value(self):
        return self.n

    def get_history(self):
        return list(self.history)

    def boom(self, message="deliberate"):
        raise ValueError(message)

    def slow(self, seconds=0.2):
        time.sleep(seconds)
        return "done"

    def echo(self, value):
        return value

    def unserialisable(self):
        import threading

        return threading.Lock()


class TestBasics:
    def test_construct_and_call(self, ray):
        counter = Counter.remote(10)
        assert ray.get(counter.inc.remote(5)) == 15
        assert ray.get(counter.value.remote()) == 15

    def test_constructor_arguments_reach_the_actor(self, ray):
        assert ray.get(Counter.remote(99).value.remote()) == 99

    def test_keyword_arguments(self, ray):
        counter = Counter.remote(start=7)
        assert ray.get(counter.inc.remote(by=3)) == 10

    def test_actors_are_independent(self, ray):
        first, second = Counter.remote(0), Counter.remote(100)
        ray.get(first.inc.remote(1))
        assert ray.get(second.value.remote()) == 100

    def test_state_persists_across_calls(self, ray):
        counter = Counter.remote()
        for _ in range(5):
            ray.get(counter.inc.remote())
        assert ray.get(counter.value.remote()) == 5

    def test_get_accepts_a_list(self, ray):
        counter = Counter.remote()
        refs = [counter.inc.remote() for _ in range(3)]
        assert ray.get(refs) == [1, 2, 3]

    def test_handle_reports_its_endpoint_and_pid(self, ray):
        counter = Counter.remote()
        assert ":" in counter.endpoint
        assert counter.pid > 0
        assert counter.is_alive()


class TestNonBlockingSubmission:
    def test_remote_returns_before_the_method_runs(self, ray):
        counter = Counter.remote()
        started = time.perf_counter()
        ref = counter.slow.remote(0.5)
        submit_time = time.perf_counter() - started
        assert submit_time < 0.2, (
            f".remote() took {submit_time:.3f}s: it must not wait for the method"
        )
        assert ray.get(ref) == "done"

    def test_many_calls_pipeline(self, ray):
        counter = Counter.remote()
        started = time.perf_counter()
        refs = [counter.slow.remote(0.05) for _ in range(10)]
        submit_time = time.perf_counter() - started
        assert submit_time < 0.3, "submission should not serialise behind execution"
        assert ray.get(refs) == ["done"] * 10


class TestOrdering:
    def test_calls_run_in_submission_order(self, ray):
        # The guarantee that makes `set_weights` then `step` safe.
        counter = Counter.remote()
        refs = [counter.inc.remote(i) for i in range(50)]
        ray.get(refs)
        assert ray.get(counter.get_history.remote()) == list(range(50))

    def test_ordering_holds_with_a_deep_pipeline(self, ray):
        counter = Counter.remote()
        refs = [counter.inc.remote(i) for i in range(200)]
        assert ray.get(refs[-1]) == sum(range(200))


class TestErrors:
    def test_user_exception_surfaces_at_get(self, ray):
        counter = Counter.remote()
        ref = counter.boom.remote("kaboom")  # must not raise here
        with pytest.raises(tinyray.UserCodeError) as excinfo:
            ray.get(ref)
        assert "kaboom" in str(excinfo.value)

    def test_remote_traceback_is_attached(self, ray):
        counter = Counter.remote()
        with pytest.raises(tinyray.UserCodeError) as excinfo:
            ray.get(counter.boom.remote())
        # Without the remote traceback, a distributed failure is unactionable.
        assert "ValueError" in excinfo.value.remote_traceback
        assert "raise ValueError" in excinfo.value.remote_traceback
        assert excinfo.value.kind == "UserException"

    def test_actor_survives_a_failed_call(self, ray):
        counter = Counter.remote(5)
        with pytest.raises(tinyray.UserCodeError):
            ray.get(counter.boom.remote())
        assert ray.get(counter.value.remote()) == 5

    def test_missing_method_lists_the_alternatives(self, ray):
        counter = Counter.remote()
        with pytest.raises(tinyray.UserCodeError) as excinfo:
            ray.get(counter.no_such_method.remote())
        message = str(excinfo.value)
        assert "no method 'no_such_method'" in message
        assert "available:" in message
        assert "inc" in message

    def test_private_methods_are_not_remotely_callable(self, ray):
        counter = Counter.remote()
        with pytest.raises(AttributeError):
            counter._secret.remote()

    def test_unserialisable_result_is_reported_clearly(self, ray):
        counter = Counter.remote()
        with pytest.raises(tinyray.UserCodeError, match="cannot be serialised"):
            ray.get(counter.unserialisable.remote())

    def test_constructor_failure_raises_at_creation(self, ray):
        @tinyray.remote
        class Broken:
            def __init__(self):
                raise RuntimeError("bad config")

        # Failing at `.remote()` is where a user expects a constructor error,
        # rather than at the first method call.
        with pytest.raises(tinyray.UserCodeError, match="bad config"):
            Broken.remote()

    def test_calling_a_remote_class_directly_is_refused(self):
        with pytest.raises(TypeError, match=r"\.remote\(\.\.\.\)"):
            Counter(1)

    def test_calling_a_method_without_remote_is_refused(self, ray):
        counter = Counter.remote()
        with pytest.raises(TypeError, match=r"\.remote\(\.\.\.\)"):
            counter.inc(1)

    def test_remote_rejects_functions(self):
        with pytest.raises(TypeError, match="tinyray has no tasks"):

            @tinyray.remote
            def not_a_class():
                pass


class TestPayloads:
    def test_numpy_round_trip(self, ray, numpy):
        counter = Counter.remote()
        array = numpy.arange(100_000, dtype=numpy.float32)
        restored = ray.get(counter.echo.remote(array))
        assert numpy.array_equal(restored, array)

    def test_ten_megabyte_payload(self, ray, numpy):
        counter = Counter.remote()
        array = numpy.ones(10 * 1024 * 1024 // 4, dtype=numpy.float32)
        restored = ray.get(counter.echo.remote(array))
        assert restored.nbytes == array.nbytes
        assert float(restored[0]) == 1.0

    def test_results_are_zero_copy_views(self, ray, numpy):
        counter = Counter.remote()
        restored = ray.get(counter.echo.remote(numpy.arange(200_000, dtype=numpy.int64)))
        # A view of the Rust buffer, and read-only because several consumers
        # may share it.
        assert not restored.flags.owndata
        assert not restored.flags.writeable

    def test_nested_structures(self, ray, numpy):
        counter = Counter.remote()
        payload = {
            "weights": [numpy.zeros(50_000, dtype=numpy.float32) for _ in range(3)],
            "step": 42,
            "tag": "iteration",
        }
        restored = ray.get(counter.echo.remote(payload))
        assert restored["step"] == 42
        assert len(restored["weights"]) == 3
        assert restored["weights"][0].shape == (50_000,)


class TestWait:
    def test_wait_returns_the_ready_ones(self, ray):
        counter = Counter.remote()
        fast = [counter.inc.remote() for _ in range(3)]
        ray.get(fast)
        slow = counter.slow.remote(2.0)

        ready, pending = ray.wait([*fast, slow], num_returns=3, timeout=5.0)
        assert len(ready) == 3
        assert len(pending) == 1
        assert pending[0] is slow

    def test_wait_preserves_identity(self, ray):
        counter = Counter.remote()
        refs = [counter.inc.remote() for _ in range(4)]
        ready, pending = ray.wait(refs, num_returns=4, timeout=10.0)
        assert {id(r) for r in ready} <= {id(r) for r in refs}
        assert pending == []

    def test_wait_times_out_without_raising(self, ray):
        counter = Counter.remote()
        slow = counter.slow.remote(5.0)
        ready, pending = ray.wait([slow], num_returns=1, timeout=0.3)
        assert ready == []
        assert pending == [slow]


class TestRefsBetweenActors:
    """References passed between actors are what make the design work.

    With no object store, the only way a 10 MB rollout reaches the learner
    without going through the driver is for the learner to fetch it from the
    producer itself.
    """

    def test_a_top_level_ref_is_resolved_automatically(self, ray, numpy):
        @tinyray.remote
        class Producer:
            def produce(self, n):
                return numpy.arange(n, dtype=numpy.int64)

        @tinyray.remote
        class Consumer:
            def total(self, value):
                # Arrives as the array itself: top-level references are
                # resolved before the method runs, as in Ray.
                return int(value.sum())

        producer = Producer.remote()
        consumer = Consumer.remote()
        ref = producer.produce.remote(1000)
        assert ray.get(consumer.total.remote(ref)) == sum(range(1000))

    def test_nested_refs_are_passed_through_for_the_actor_to_fetch(self, ray, numpy):
        # Matching Ray: only top-level arguments are resolved, so an actor can
        # take a batch of references and decide when to fetch each one.
        @tinyray.remote
        class Producer:
            def produce(self, n):
                return numpy.arange(n, dtype=numpy.int64)

        @tinyray.remote
        class Consumer:
            def total(self, refs):
                import tinyray as tr

                return sum(int(value.sum()) for value in tr.get(refs))

        producer = Producer.remote()
        consumer = Consumer.remote()
        refs = [producer.produce.remote(100) for _ in range(3)]
        assert ray.get(consumer.total.remote(refs)) == sum(range(100)) * 3

    def test_the_payload_does_not_travel_through_the_driver(self, ray, numpy):
        @tinyray.remote
        class Producer:
            def produce(self):
                return numpy.zeros(500_000, dtype=numpy.uint8)

        @tinyray.remote
        class Consumer:
            def size(self, refs):
                import tinyray as tr

                return sum(value.nbytes for value in tr.get(refs))

        producer = Producer.remote()
        consumer = Consumer.remote()
        ref = producer.produce.remote()

        # The reference names the producer, so the consumer fetches from there.
        assert ref.owner_endpoint == producer.endpoint
        assert ray.get(consumer.size.remote([ref])) == 500_000

        import json

        # One stored result, served twice, never copied through the driver.
        assert json.loads(producer.introspect())["store"]["ready"] >= 1


class TestLifecycle:
    def test_kill_stops_the_actor(self, ray):
        counter = Counter.remote()
        assert ray.get(counter.value.remote()) == 0
        pid = counter.pid
        ray.kill(counter)

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                import os

                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.05)
        else:
            pytest.fail("actor process outlived kill()")

    def test_release_frees_the_result(self, ray, numpy):
        counter = Counter.remote()
        ref = counter.echo.remote(numpy.zeros(100_000, dtype=numpy.uint8))
        ray.get(ref)
        ray.release(ref)
        # A released result reports ObjectLost, not a bare "not found": the
        # difference tells the user whether they hit a bug or a policy.
        with pytest.raises(tinyray.ObjectLost):
            ray.get(ref, timeout=5.0)

    def test_introspect_reports_progress(self, ray):
        import json

        counter = Counter.remote()
        ray.get(counter.inc.remote())
        report = json.loads(counter.introspect())
        assert report["accepted"] >= 2  # __init__ plus inc
        assert report["completed"] >= 2
        assert report["stuck_callers"] == []


class TestBackpressure:
    def test_actor_refuses_once_its_queue_is_full(self, ray):
        # Backpressure only engages when the actor is slower than its caller,
        # which is exactly the rollout case: cheap to submit, expensive to run.
        # With a fast method the queue simply never fills, so the test has to
        # make execution the bottleneck for the limit to mean anything.
        counter = Counter.options(max_pending_calls=4).remote()
        refs = [counter.slow.remote(0.02) for _ in range(30)]

        # The client retries backpressure by itself, so the visible outcome is
        # that everything still completes -- with the actor's memory bounded.
        assert ray.get(refs[-1]) == "done"

        import json

        report = json.loads(counter.introspect())
        assert report["rejected_backpressure"] > 0, "the limit was never exercised"
        assert report["queued"] <= 4

    def test_queue_limit_does_not_reorder_calls(self, ray):
        # Retrying after a 429 must not let a later call overtake an earlier
        # one; the sequence numbers are what prevent that.
        counter = Counter.options(max_pending_calls=2).remote()
        refs = [counter.inc.remote(i) for i in range(40)]
        ray.get(refs)
        assert ray.get(counter.get_history.remote()) == list(range(40))


class TestOptionsChangeBehaviour:
    """Every accepted option must demonstrably do something.

    An option that is parsed, stored and then ignored looks supported and is
    not; `lifetime="detached"` was exactly that until it was made to fail loudly.
    """

    def test_store_ttl_seconds_expires_results(self, ray, monkeypatch):
        # The sweeper runs on its own interval, so shorten it too; otherwise the
        # TTL is correct in theory and untested in practice.
        monkeypatch.setenv("TINYRAY_SWEEP_INTERVAL", "0.5")

        @tinyray.remote(store_ttl_seconds=1.0)
        class Producer:
            def make(self):
                return b"payload" * 100

        producer = Producer.remote()
        ref = producer.make.remote()
        assert len(ray.get(ref)) == 700

        time.sleep(4.0)
        with pytest.raises(tinyray.ObjectLost):
            ray.get(ref, timeout=5.0)

    def test_store_max_bytes_bounds_the_store(self, ray, numpy):
        @tinyray.remote(store_max_bytes=8192)
        class Producer:
            def make(self, size):
                return numpy.zeros(size, dtype=numpy.uint8)

        producer = Producer.remote()
        first = producer.make.remote(64 * 1024)
        ray.get(first)
        for _ in range(3):
            ray.get(producer.make.remote(64 * 1024))

        report = json.loads(producer.introspect())
        assert report["store"]["evictions"] > 0
        with pytest.raises(tinyray.ObjectLost):
            ray.get(first, timeout=5.0)

    def test_memory_bytes_is_accounted_for_in_placement(self, ray):
        # Requesting more memory than the node has must be refused, otherwise
        # the option is decoration.
        @tinyray.remote(memory_bytes=1 << 60)
        class Greedy:
            def ping(self):
                return "pong"

        with pytest.raises(tinyray.PlacementFailed):
            Greedy.remote()

    def test_num_cpus_is_accounted_for(self, ray):
        @tinyray.remote(num_cpus=1e9)
        class Greedy:
            def ping(self):
                return "pong"

        with pytest.raises(tinyray.PlacementFailed):
            Greedy.remote()


class TestWaitDoesNotRelayPayloads:
    """`wait` reports which references settled; it must not fetch them.

    Regression test. `wait` used to answer the readiness question by issuing a
    full fetch and discarding the body, so a driver waiting on 32 rollouts of
    10 MB pulled 320 MB it never looked at -- the star-shaped relay that the
    whole no-object-store design exists to avoid. It was invisible in every
    functional test because the results were correct and small.
    """

    def test_cost_does_not_scale_with_the_payload(self, ray, numpy):
        @tinyray.remote(num_cpus=0.1)
        class Producer:
            def make(self, nbytes):
                return numpy.zeros(nbytes, dtype=numpy.uint8)

        producer = Producer.remote()

        def wait_cost(megabytes):
            ref = producer.make.remote(megabytes * 1024 * 1024)
            ray.get(ref)  # settle it first, so we time readiness alone
            best = float("inf")
            for _ in range(3):
                started = time.perf_counter()
                ready, _pending = ray.wait([ref], num_returns=1, timeout=60.0)
                best = min(best, time.perf_counter() - started)
                assert len(ready) == 1
            return best

        small = wait_cost(1)
        large = wait_cost(64)

        # Transferring 64 MB takes tens of milliseconds; a status probe is a
        # single small round trip. The bound is loose because the difference
        # being guarded against is roughly sixtyfold.
        assert large < max(small * 10, 0.010), (
            f"wait() took {large * 1e3:.1f} ms for 64 MB against "
            f"{small * 1e3:.1f} ms for 1 MB: it is fetching payloads, not "
            "probing readiness"
        )

    def test_wait_still_reports_failures_as_settled(self, ray):
        # A probe must not mistake a failed task for an unfinished one, or a
        # driver would wait out the timeout on a result that will never come.
        @tinyray.remote(num_cpus=0.1)
        class Fragile:
            def boom(self):
                raise ValueError("nope")

        actor = Fragile.remote()
        ref = actor.boom.remote()
        ready, pending = ray.wait([ref], num_returns=1, timeout=30.0)
        assert ready == [ref]
        assert pending == []
        with pytest.raises(tinyray.UserCodeError, match="nope"):
            ray.get(ref)

    def test_get_after_wait_still_returns_the_value(self, ray, numpy):
        @tinyray.remote(num_cpus=0.1)
        class Producer:
            def make(self):
                return numpy.arange(100_000, dtype=numpy.int64)

        ref = Producer.remote().make.remote()
        ready, _ = ray.wait([ref], num_returns=1, timeout=30.0)
        assert numpy.array_equal(ray.get(ready[0]), numpy.arange(100_000))
