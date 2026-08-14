"""End-to-end tests shaped like the workloads tinyray was designed for.

Two scenarios, both from the design document:

* an **RL rollout loop** -- 8 rollout actors, a learner, weights broadcast each
  iteration, results passed by reference from rollouts straight to the learner;
* a **hyperparameter sweep** -- many short-lived trials, early stopping, results
  consumed as they arrive.

These are slower than the unit tests and that is fine. They are the tests that
would catch a regression in how the pieces fit together, which is exactly the
class of bug the unit tests cannot see.
"""

from __future__ import annotations

import json
import time

import pytest

import tinyray

pytestmark = pytest.mark.slow


@pytest.fixture
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


# These tests exercise the *shape* of a workload, not resource accounting, so
# the actors ask for a slice of a CPU rather than a whole one. With the default
# of one CPU each, a four-core CI runner cannot host the group and gang
# placement correctly refuses -- a failure about the runner, not about tinyray.
LIGHT = {"num_cpus": 0.1}


@tinyray.remote(**LIGHT)
class Rollout:
    """Stands in for an environment sampler."""

    def __init__(self, seed, obs_size=200_000):
        import numpy as np

        self.rng = np.random.default_rng(seed)
        self.obs_size = obs_size
        self.weights_version = -1
        self.steps = 0

    def set_weights(self, weights):
        self.weights_version = int(weights["version"])
        return self.weights_version

    def step(self):
        import numpy as np

        self.steps += 1
        return {
            "obs": np.full(self.obs_size, self.steps, dtype=np.float32),
            "reward": float(self.rng.random()),
            "weights_version": self.weights_version,
        }

    def stats(self):
        return {"steps": self.steps, "weights_version": self.weights_version}


@tinyray.remote(**LIGHT)
class Learner:
    """Consumes rollouts and produces the next weights."""

    def __init__(self, size=100_000):
        import numpy as np

        self.weights = np.zeros(size, dtype=np.float32)
        self.version = 0
        self.samples = 0

    def get_weights(self):
        return {"version": self.version, "values": self.weights}

    def update(self, batches):
        """Take rollout results by reference and fetch them directly.

        This is the property that makes the whole design work without an object
        store: the payloads travel rollout -> learner, and the driver only ever
        moved the references.
        """
        import tinyray as tr

        total = 0.0
        for batch in tr.get(batches):
            total += float(batch["reward"])
            self.samples += int(batch["obs"].shape[0])
        self.version += 1
        return {"version": self.version, "reward": total, "samples": self.samples}


class TestRolloutLoop:
    def test_a_full_rl_iteration(self, ray):
        learner = Learner.remote()
        rollouts = tinyray.create_actors(Rollout, 0, count=4)

        for iteration in range(3):
            weights = ray.get(learner.get_weights.remote())
            # Broadcast. Without GPUs this is a fan-out of HTTP calls; with
            # them it would be one NCCL broadcast (see tinyray.collective).
            versions = ray.get([r.set_weights.remote(weights) for r in rollouts])
            assert versions == [iteration] * len(rollouts)

            refs = [r.step.remote() for r in rollouts]
            summary = ray.get(learner.update.remote(refs))
            assert summary["version"] == iteration + 1

        stats = ray.get([r.stats.remote() for r in rollouts])
        assert all(s["steps"] == 3 for s in stats)

    def test_stragglers_can_be_dropped(self, ray):
        """`wait` lets a loop train on the fastest N rollouts.

        Note the caveat from the design: dropping a straggler's *result* is
        fine, but if the group runs NCCL collectives that same actor must still
        attend the next barrier.
        """
        rollouts = tinyray.create_actors(Rollout, 0, count=5)
        refs = [r.step.remote() for r in rollouts]
        ready, pending = ray.wait(refs, num_returns=3, timeout=60.0)
        assert len(ready) == 3
        assert len(ready) + len(pending) == 5

        batch = ray.get(ready)
        assert all("obs" in item for item in batch)

    def test_results_go_straight_from_producer_to_consumer(self, ray):
        """The driver must not become a relay for 10 MB payloads."""
        learner = Learner.remote()
        rollouts = tinyray.create_actors(Rollout, 0, count=3)

        refs = [r.step.remote() for r in rollouts]
        # Each reference names the rollout that produced it, not the driver.
        rollout_endpoints = {r.endpoint for r in rollouts}
        assert {ref.owner_endpoint for ref in refs} <= rollout_endpoints

        ray.get(learner.update.remote(refs))
        # The learner really did fetch from the rollouts.
        report = json.loads(rollouts[0].introspect())
        assert report["completed"] >= 2


class TestHyperparameterSweep:
    def test_sweep_with_early_stopping(self, ray):
        @tinyray.remote(**LIGHT)
        class Trial:
            def __init__(self, learning_rate):
                self.learning_rate = learning_rate

            def evaluate(self):
                # Pretend a lower learning rate scores better.
                return {"lr": self.learning_rate, "score": 1.0 / (1.0 + self.learning_rate)}

            def train_forever(self):
                time.sleep(60)

        grid = [0.1, 0.01, 0.001, 0.0001]
        trials = [Trial.remote(lr) for lr in grid]
        results = ray.get([t.evaluate.remote() for t in trials])
        assert len(results) == 4

        best = max(results, key=lambda r: r["score"])
        assert best["lr"] == 0.0001

        # Early stopping: kill the losers rather than wait them out.
        doomed = trials[0]
        doomed.train_forever.remote()
        tinyray.kill(doomed)
        assert not doomed.is_alive()

        # The survivors are unaffected.
        assert ray.get(trials[1].evaluate.remote())["lr"] == 0.01

    def test_pool_streams_results_as_they_finish(self, ray):
        @tinyray.remote(**LIGHT)
        class Trial:
            def run(self, config):
                time.sleep(0.05 * (config % 3))
                return config * 10

        pool = tinyray.ActorPool([Trial.remote() for _ in range(3)])
        results = list(pool.map_unordered(lambda a, c: a.run.remote(c), range(15)))
        assert sorted(results) == [c * 10 for c in range(15)]


class TestFailureModes:
    def test_one_failing_trial_does_not_take_down_the_sweep(self, ray):
        @tinyray.remote(**LIGHT)
        class Trial:
            def __init__(self, should_fail):
                self.should_fail = should_fail

            def run(self):
                if self.should_fail:
                    raise RuntimeError("diverged")
                return "ok"

        trials = [Trial.remote(i == 2) for i in range(4)]
        refs = [t.run.remote() for t in trials]

        outcomes = []
        for ref in refs:
            try:
                outcomes.append(ray.get(ref))
            except tinyray.UserCodeError as exc:
                assert "diverged" in str(exc)
                outcomes.append("failed")
        assert outcomes == ["ok", "ok", "failed", "ok"]

    def test_evicted_results_are_reported_not_silently_wrong(self, ray):
        # A tiny store forces eviction. The consumer must be told, rather than
        # receiving stale or partial data.
        @tinyray.remote(store_max_bytes=4096)
        class Producer:
            def make(self, size):
                import numpy as np

                return np.zeros(size, dtype=np.uint8)

        producer = Producer.remote()
        first = producer.make.remote(64 * 1024)
        ray.get(first)
        for _ in range(3):
            ray.get(producer.make.remote(64 * 1024))

        with pytest.raises(tinyray.ObjectLost, match="evicted or expired"):
            ray.get(first, timeout=10.0)
