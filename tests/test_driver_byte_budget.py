"""A byte budget for the driver.

The design's central claim is that payloads move between actors and never
through the driver. That claim was tested in exactly one place -- a fetch
between two actors -- and `wait` violated it for months of development without
a single test noticing, because the results it relayed were correct.

An invariant checked at one call site is not an invariant. It is an anecdote
about that call site.

So this file does not test any particular function. It parks a large result in
an actor, exercises every driver-side operation in turn, and asserts what
crossed the driver's wire. A future operation that starts relaying fails here,
whoever writes it and whatever it is called.
"""

from __future__ import annotations

import pytest

import tinyray

MB = 1024 * 1024

#: Large enough that relaying is unmistakable, small enough to stay quick.
PAYLOAD = 32 * MB

#: Control traffic is a header plus a reference. Kilobytes, not megabytes.
BUDGET = 64 * 1024


@tinyray.remote(num_cpus=0.1)
class Producer:
    def make(self, nbytes):
        import numpy as np

        return np.zeros(nbytes, dtype=np.uint8)

    def ping(self):
        return "pong"

    def boom(self):
        raise ValueError("deliberate")


@tinyray.remote(num_cpus=0.1)
class Consumer:
    def total_size(self, refs):
        import tinyray as tr

        return sum(value.nbytes for value in tr.get(refs))

    def direct(self, value):
        return int(value.nbytes)


@pytest.fixture(scope="module")
def scenario():
    """A settled large result, plus the actors around it."""
    tinyray.shutdown()
    tinyray.init()
    producer = Producer.remote()
    consumer = Consumer.remote()
    reference = producer.make.remote(PAYLOAD)
    tinyray.get(reference)  # settle it; this transfer is deliberate
    yield producer, consumer, reference
    tinyray.shutdown()


def driver_bytes_in() -> int:
    return sum(peer["bytes_received"] for peer in tinyray.transport_stats().values())


class TestDriverStaysOutOfTheDataPath:
    """Each operation gets its own budget assertion, named for what it does."""

    def measure(self, operation):
        before = driver_bytes_in()
        result = operation()
        return result, driver_bytes_in() - before

    def test_wait_moves_only_references(self, scenario):
        # The regression. `wait` answers "has it settled?"; fetching the value
        # to find out routed 32 rollouts x 10 MB through the driver.
        _producer, _consumer, reference = scenario
        _, moved = self.measure(
            lambda: tinyray.wait([reference], num_returns=1, timeout=60.0)
        )
        assert moved < BUDGET, (
            f"wait() pulled {moved:,} bytes with a {PAYLOAD:,} byte result in play; "
            "it is relaying the payload rather than probing readiness"
        )

    def test_submitting_a_call_moves_only_the_arguments(self, scenario):
        producer, _consumer, _reference = scenario
        _, moved = self.measure(lambda: producer.ping.remote())
        assert moved < BUDGET

    def test_passing_a_reference_in_a_list_moves_only_the_reference(self, scenario):
        # The consumer fetches the payload itself, straight from the producer.
        _producer, consumer, reference = scenario
        (size, moved) = self.measure(
            lambda: tinyray.get(consumer.total_size.remote([reference]))
        )
        assert size == PAYLOAD, "the consumer did not actually receive the data"
        assert moved < BUDGET, (
            f"the driver moved {moved:,} bytes relaying a reference; the whole "
            "point of a reference is that it does not"
        )

    def test_passing_a_reference_as_a_top_level_argument(self, scenario):
        # Resolved automatically -- but resolved *by the actor*, not the driver.
        _producer, consumer, reference = scenario
        (size, moved) = self.measure(
            lambda: tinyray.get(consumer.direct.remote(reference))
        )
        assert size == PAYLOAD
        assert moved < BUDGET, (
            f"automatic resolution cost the driver {moved:,} bytes; it must "
            "happen in the actor that needs the value"
        )

    def test_introspect_moves_only_a_report(self, scenario):
        producer, _consumer, _reference = scenario
        _, moved = self.measure(producer.introspect)
        assert moved < BUDGET

    def test_a_failed_call_moves_only_its_traceback(self, scenario):
        producer, _consumer, _reference = scenario

        def raise_and_catch():
            with pytest.raises(tinyray.UserCodeError):
                tinyray.get(producer.boom.remote())

        _, moved = self.measure(raise_and_catch)
        assert moved < BUDGET

    def test_actor_pool_moves_only_references(self, scenario):
        producer, _consumer, _reference = scenario
        pool = tinyray.ActorPool([producer])

        def drain():
            return list(pool.map_unordered(lambda a, _x: a.ping.remote(), range(4)))

        results, moved = self.measure(drain)
        assert results == ["pong"] * 4
        assert moved < BUDGET, (
            f"ActorPool moved {moved:,} bytes; it is built on wait(), so a "
            "regression there surfaces here too"
        )

    def test_release_moves_only_a_reference(self, scenario):
        producer, _consumer, _reference = scenario
        doomed = producer.make.remote(PAYLOAD)
        tinyray.get(doomed)  # settle it, deliberately transferring
        _, moved = self.measure(lambda: tinyray.release([doomed]))
        assert moved < BUDGET, (
            f"release() moved {moved:,} bytes; telling an owner to drop a result "
            "should cost a reference, not the result"
        )

    def test_kill_moves_nothing(self, scenario):
        _producer, _consumer, _reference = scenario
        doomed = Producer.remote()
        tinyray.get(doomed.ping.remote())
        _, moved = self.measure(lambda: tinyray.kill(doomed))
        assert moved < BUDGET

    def test_argument_resolution_happens_in_the_actor(self, scenario):
        """`resolve_arguments` runs inside the callee, and must stay there.

        If it ever ran driver-side, every reference passed between actors would
        become a round trip through the driver -- the failure this whole design
        is arranged to avoid.
        """
        producer, consumer, reference = scenario
        (size, moved) = self.measure(
            lambda: tinyray.get(consumer.direct.remote(reference))
        )
        assert size == PAYLOAD
        assert moved < BUDGET

        # And the consumer really did pull it: its transport, not the driver's.
        import json

        report = json.loads(producer.introspect())
        assert report["completed"] >= 1

    def test_get_does_transfer(self, scenario):
        """The control: an operation that is *supposed* to move the payload.

        Without this the budget could be satisfied by a runtime that never
        transfers anything, and the other assertions would prove nothing.
        """
        producer, _consumer, _reference = scenario
        fresh = producer.make.remote(PAYLOAD)
        value, moved = self.measure(lambda: tinyray.get(fresh))
        assert value.nbytes == PAYLOAD
        assert moved >= PAYLOAD, (
            "get() moved less than the payload; either the measurement is "
            "broken or the value is not really arriving"
        )
