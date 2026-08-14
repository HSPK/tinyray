"""Tests for process lifecycle: clean shutdown, prewarming, and signals.

The shutdown tests exist because of a bug that is easy to reintroduce and
invisible without measurement. Python only runs signal handlers while the main
thread is executing bytecode. An executor thread parked inside Rust with the
GIL released therefore never sees SIGTERM, and every "clean" shutdown silently
degrades into a SIGKILL after the timeout expires -- correct-looking, ten
seconds slow, and it skips the code that fails pending calls properly.
"""

from __future__ import annotations

import os
import time

import pytest

import tinyray
from tinyray.prewarm import DEFAULT_PREIMPORTS, PrewarmPool, preimport_from_env


@pytest.fixture
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


@tinyray.remote
class Idle:
    def ping(self):
        return "pong"

    def slow(self, seconds=1.0):
        time.sleep(seconds)
        return "done"


class TestCleanShutdown:
    def test_idle_actors_stop_promptly(self):
        # A SIGKILL fallback would put this at ~10s per actor.
        tinyray.init()
        [Idle.remote() for _ in range(3)]
        started = time.perf_counter()
        tinyray.shutdown()
        elapsed = time.perf_counter() - started
        assert elapsed < 5.0, (
            f"shutdown took {elapsed:.1f}s: SIGTERM is being ignored and the "
            "launcher is falling back to SIGKILL"
        )

    def test_kill_terminates_a_single_actor(self, ray):
        actor = Idle.remote()
        assert ray.get(actor.ping.remote()) == "pong"
        pid = actor.pid
        tinyray.kill(actor)

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.05)
        else:
            pytest.fail("actor process outlived kill()")

    def test_queued_calls_fail_rather_than_hang_on_shutdown(self, ray):
        actor = Idle.remote()
        # Fill the queue, then pull the actor out from under it. Every pending
        # call must resolve to an error; a hang here would strand the driver.
        refs = [actor.slow.remote(0.5) for _ in range(5)]
        time.sleep(0.1)
        tinyray.kill(actor)

        for ref in refs:
            with pytest.raises(tinyray.TinyrayError):
                ray.get(ref, timeout=15.0)


class TestPreimport:
    def test_preimport_loads_what_it_can(self, monkeypatch):
        monkeypatch.setenv("TINYRAY_PREIMPORT", "numpy,definitely_not_a_module")
        # A missing optional dependency must not stop the actor: warming is an
        # optimisation, not a requirement.
        assert preimport_from_env() == ["numpy"]

    def test_empty_preimport_is_harmless(self, monkeypatch):
        monkeypatch.delenv("TINYRAY_PREIMPORT", raising=False)
        assert preimport_from_env() == []

    def test_defaults_cover_the_expensive_imports(self):
        # torch is the whole reason this exists: three to eight seconds per
        # process, paid on every trial in a sweep.
        assert "torch" in DEFAULT_PREIMPORTS
        assert "numpy" in DEFAULT_PREIMPORTS


class TestPrewarmPool:
    def test_pool_keys_separate_device_assignments(self):
        # A process that has selected its GPUs cannot be repurposed for another
        # assignment, so the pools must not be shared.
        assert PrewarmPool.key_for(None) == ""
        assert PrewarmPool.key_for([]) == ""
        assert PrewarmPool.key_for([1, 0]) == "0,1"
        assert PrewarmPool.key_for([0]) != PrewarmPool.key_for([1])

    def test_first_acquire_misses_and_later_ones_hit(self, ray):
        context = tinyray.api._require_context()
        pool = PrewarmPool(context.launcher, size=1)
        try:
            assert pool.acquire() is None, "an empty pool must not block"
            assert pool.stats()["misses"] == 1

            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if pool.stats()["spawned"] >= 1:
                    break
                time.sleep(0.1)
            else:
                pytest.fail("background refill never produced a warm process")

            warm = pool.acquire()
            assert warm is not None
            assert warm.is_alive()
            assert pool.stats()["hits"] == 1
        finally:
            pool.shutdown()

    def test_disabled_pool_never_spawns(self, ray):
        context = tinyray.api._require_context()
        pool = PrewarmPool(context.launcher, size=2, enabled=False)
        assert pool.acquire() is None
        time.sleep(0.5)
        assert pool.stats()["spawned"] == 0

    def test_acquire_never_blocks_the_caller(self, ray):
        # Refilling happens in the background precisely so actor creation never
        # waits on an import.
        context = tinyray.api._require_context()
        pool = PrewarmPool(context.launcher, size=2)
        try:
            started = time.perf_counter()
            pool.acquire()
            assert time.perf_counter() - started < 0.5
        finally:
            pool.shutdown()


class TestIntrospection:
    def test_introspect_reports_the_running_method(self, ray):
        import json

        actor = Idle.remote()
        actor.slow.remote(1.0)
        time.sleep(0.3)

        report = json.loads(actor.introspect())
        # The answer to "what is this actor stuck on?", which is the question
        # people actually have at 3am.
        assert report["inflight"] == "slow"
        assert report["inflight_seconds"] > 0

    def test_introspect_shows_no_stuck_callers_in_a_healthy_actor(self, ray):
        import json

        actor = Idle.remote()
        ray.get([actor.ping.remote() for _ in range(5)])
        report = json.loads(actor.introspect())
        assert report["stuck_callers"] == []
        assert report["reordered"] >= 0
        assert report["completed"] >= 6  # __init__ plus five pings


class TestPrewarmIsActuallyUsed:
    """The pool has to be wired into actor creation, not merely exist.

    It was written, tested in isolation, and then never called -- which is the
    easiest kind of feature to ship by accident.
    """

    def test_warm_actors_start_much_faster(self):
        tinyray.shutdown()
        tinyray.init(prewarm=2)
        try:
            context = tinyray.api._require_context()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if context.agent.pool.stats()["spawned"] >= 1:
                    break
                time.sleep(0.1)
            else:
                pytest.fail("the pool never primed itself at startup")

            started = time.perf_counter()
            actor = Idle.remote()
            warm = time.perf_counter() - started
            assert tinyray.get(actor.ping.remote()) == "pong"

            assert context.agent.pool.stats()["hits"] >= 1, "the pool was bypassed"
            # A cold start pays for a process plus every import; a warm one is
            # just the constructor call.
            assert warm < 0.030, f"warm creation took {warm * 1e3:.0f} ms"
        finally:
            tinyray.shutdown()

    def test_prewarm_is_off_unless_requested(self):
        tinyray.shutdown()
        tinyray.init()
        try:
            context = tinyray.api._require_context()
            time.sleep(0.5)
            assert context.agent.pool.stats()["spawned"] == 0
            Idle.remote()
            assert context.agent.pool.stats()["hits"] == 0
        finally:
            tinyray.shutdown()

    def test_custom_process_options_bypass_the_pool(self):
        # Queue and store limits are fixed when the process starts, so a warm
        # process cannot serve an actor that asked for different ones.
        tinyray.shutdown()
        tinyray.init(prewarm=2)
        try:
            context = tinyray.api._require_context()
            time.sleep(3)
            before = context.agent.pool.stats()["hits"]
            Idle.options(max_pending_calls=7).remote()
            assert context.agent.pool.stats()["hits"] == before
        finally:
            tinyray.shutdown()

    def test_a_restart_does_not_adopt_a_warm_process(self):
        # A restarted actor must keep its id, and a warm process brings its own.
        tinyray.shutdown()
        tinyray.init(prewarm=1)
        try:

            @tinyray.remote(max_restarts=1)
            class Crashy:
                def ping(self):
                    return "pong"

                def crash(self):
                    os._exit(5)

            actor = Crashy.remote()
            actor_id = actor.actor_id
            with pytest.raises(tinyray.TinyrayError):
                tinyray.get(actor.crash.remote(), timeout=5.0)

            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                try:
                    if tinyray.get(actor.ping.remote(), timeout=3.0) == "pong":
                        break
                except tinyray.TinyrayError:
                    time.sleep(0.2)
            else:
                pytest.fail("actor did not come back")

            assert actor.actor_id == actor_id, "the restart changed the actor's identity"
        finally:
            tinyray.shutdown()
