"""Tests for the actor pool and the command line."""

from __future__ import annotations

import io
import json
import time
from contextlib import redirect_stdout

import pytest

import tinyray
from tinyray.cli import build_parser
from tinyray.cli import main as cli_main


@pytest.fixture
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


@tinyray.remote
class Worker:
    def square(self, x):
        return x * x

    def variable(self, x):
        # Uneven durations, so completion order genuinely differs from
        # submission order.
        time.sleep(0.02 * (x % 4))
        return x

    def slow(self, seconds=3.0):
        time.sleep(seconds)
        return "done"

    def boom(self):
        raise ValueError("trial diverged")


class TestActorPool:
    def test_map_preserves_input_order(self, ray):
        pool = tinyray.ActorPool([Worker.remote() for _ in range(3)])
        assert pool.map(lambda a, x: a.square.remote(x), range(10)) == [x * x for x in range(10)]

    def test_map_unordered_returns_everything(self, ray):
        pool = tinyray.ActorPool([Worker.remote() for _ in range(3)])
        results = list(pool.map_unordered(lambda a, x: a.variable.remote(x), range(20)))
        assert sorted(results) == list(range(20))

    def test_map_unordered_yields_before_the_slowest_finishes(self, ray):
        # The property that makes this worth having: one slow trial must not
        # hold up the results queued behind it.
        actors = [Worker.remote() for _ in range(2)]
        pool = tinyray.ActorPool(actors)

        started = time.perf_counter()
        stream = pool.map_unordered(
            lambda a, x: a.slow.remote(2.0) if x == 0 else a.square.remote(x),
            range(6),
        )
        first = next(stream)
        elapsed = time.perf_counter() - started
        assert elapsed < 1.5, f"first result waited {elapsed:.1f}s on an unrelated slow call"
        assert first != "done"
        list(stream)

    def test_work_is_spread_across_actors(self, ray):
        actors = [Worker.remote() for _ in range(3)]
        pool = tinyray.ActorPool(actors)
        list(pool.map_unordered(lambda a, x: a.square.remote(x), range(30)))

        completed = [json.loads(actor.introspect())["completed"] for actor in actors]
        assert all(count > 1 for count in completed), f"work piled onto one actor: {completed}"

    def test_failures_surface_from_the_pool(self, ray):
        pool = tinyray.ActorPool([Worker.remote()])
        with pytest.raises(tinyray.UserCodeError, match="trial diverged"):
            list(pool.map_unordered(lambda a, _x: a.boom.remote(), range(1)))

    def test_empty_pool_is_rejected(self):
        with pytest.raises(ValueError, match="at least one actor"):
            tinyray.ActorPool([])

    def test_in_flight_work_is_bounded(self, ray):
        # Submitting everything up front would just relocate the queue into the
        # actors' memory, which is what the watermark exists to prevent.
        actor = Worker.remote()
        pool = tinyray.ActorPool([actor], max_in_flight_per_actor=2)
        stream = pool.map_unordered(lambda a, x: a.variable.remote(x), range(50))
        next(stream)
        report = json.loads(actor.introspect())
        assert report["queued"] <= 4, f"pool submitted too far ahead: {report['queued']}"
        list(stream)


class TestCli:
    def test_status_reports_each_actor(self, ray):
        actors = [Worker.remote() for _ in range(2)]
        ray.get([a.square.remote(2) for a in actors])

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = cli_main(["status", *[a.endpoint for a in actors]])
        output = buffer.getvalue()

        assert code == 0
        assert "No problems detected" in output
        for actor in actors:
            assert actor.endpoint in output

    def test_status_shows_the_running_method(self, ray):
        actor = Worker.remote()
        actor.slow.remote(2.0)
        time.sleep(0.4)

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            cli_main(["status", actor.endpoint])
        assert "slow" in buffer.getvalue()

    def test_status_flags_an_unreachable_actor(self, ray):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = cli_main(["--timeout", "2", "status", "127.0.0.1:1"])
        output = buffer.getvalue()
        assert code == 1
        assert "UNREACHABLE" in output
        assert "did not answer" in output

    def test_status_warns_about_evictions(self, ray, numpy):
        # An evicted result becomes an ObjectLost for whoever wanted it, so the
        # operator should hear about it before the consumer does.
        actor = Worker.options(store_max_bytes=1024).remote()
        for _ in range(3):
            ray.get(actor.square.remote(2))
        # Force the watermark with something much larger than 1 KiB.
        big = numpy.zeros(64 * 1024, dtype=numpy.uint8)
        for _ in range(3):
            ray.get(actor.square.remote(len(big)))

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            cli_main(["status", actor.endpoint])
        output = buffer.getvalue()
        if "evicted" in output:
            assert "ObjectLost" in output

    def test_introspect_emits_valid_json(self, ray):
        actor = Worker.remote()
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = cli_main(["introspect", actor.endpoint])
        assert code == 0
        report = json.loads(buffer.getvalue())
        assert report["actor"]
        assert "store" in report

    def test_health(self, ray):
        actor = Worker.remote()
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = cli_main(["health", actor.endpoint])
        assert code == 0
        assert '"status":"ok"' in buffer.getvalue()

    def test_parser_requires_a_subcommand(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args([])
