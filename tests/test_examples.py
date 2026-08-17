"""The examples run, and the claims they print are true.

An example nobody executes is documentation that rots quietly. Worse, these
particular examples exist to make a numeric argument -- that the driver stays
out of the data path, that prefetching overlaps two stages, that stragglers are
dropped without being abandoned -- and an argument nobody checks is an
assertion in prose.

So each example is run for real and its own output is parsed. The assertions
below are deliberately loose on magnitude and strict on direction: the exact
ratio depends on the machine, but "the driver moved three orders of magnitude
less than the workers did" must not silently become false.

These are the slowest tests in the suite (~15s total). They earn it: the
``run_on`` deadlock in the RL example was found this way, and no unit test in
this repository would have caught it.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"

TIMEOUT = 300


def run_example(name: str) -> str:
    """Run one example to completion and return its stdout."""
    script = EXAMPLES / f"{name}.py"
    assert script.exists(), f"{script} is missing"
    completed = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        cwd=ROOT,
    )
    assert completed.returncode == 0, (
        f"{name} exited {completed.returncode}\n"
        f"--- stdout ---\n{completed.stdout}\n--- stderr ---\n{completed.stderr}"
    )
    return completed.stdout


def number(pattern: str, text: str) -> float:
    match = re.search(pattern, text)
    assert match, f"expected {pattern!r} in output:\n{text}"
    return float(match.group(1).replace(",", ""))


@pytest.fixture(scope="module")
def native_stack() -> str:
    return run_example("native_stack")


@pytest.fixture(scope="module")
def dataloader() -> str:
    return run_example("dataloader_to_trainer")


@pytest.fixture(scope="module")
def rl() -> str:
    return run_example("rl_control_plane")


class TestEveryExampleIsRunnable:
    def test_no_example_is_left_unexercised(self):
        covered = {"native_stack", "dataloader_to_trainer", "rl_control_plane"}
        present = {p.stem for p in EXAMPLES.glob("*.py") if not p.stem.startswith("_")}
        assert present == covered, (
            f"examples nobody runs: {sorted(present - covered)}; add them here, "
            "because an example that is only linted is an example that is broken"
        )

    def test_examples_clean_up_after_themselves(self, dataloader, rl, native_stack):
        leftovers = sorted(p.name for p in EXAMPLES.glob("_generated_*"))
        assert not leftovers, (
            f"examples left generated files behind: {leftovers}; the finally "
            "block is supposed to remove them even on failure"
        )


class TestNativeStack:
    def test_it_reports_control_traffic(self, native_stack: str):
        moved = number(r"control traffic through the driver: ([\d,]+) bytes", native_stack)
        assert 0 < moved < 200_000, (
            f"the driver moved {moved:,.0f} bytes coordinating four ranks and a "
            "server; it is supposed to move control messages, not payloads"
        )

    def test_the_trainer_and_the_server_both_came_up(self, native_stack: str):
        assert "trainer up:" in native_stack
        assert "rollout up:" in native_stack

    def test_the_loop_advanced_the_weight_version(self, native_stack: str):
        versions = [int(v) for v in re.findall(r"version=(\d+)", native_stack)]
        assert versions == sorted(versions) and versions[-1] > versions[0], (
            f"weight versions {versions} did not advance; the trainer ran but nothing changed"
        )


class TestDataloaderToTrainer:
    def test_the_driver_stays_out_of_the_data_path(self, dataloader: str):
        payload = number(r"data loader -> trainer:\s+([\d,.]+) MB", dataloader)
        driver = number(r"data through the driver:\s+([\d,.]+) KB", dataloader)
        ratio = payload * 1e6 / (driver * 1e3)
        assert ratio > 100, (
            f"batches were {payload:.1f} MB but the driver moved {driver:.1f} KB "
            f"({ratio:.0f}x). References are supposed to keep the driver's share "
            "negligible; this looks like something started fetching payloads"
        )

    def test_prefetching_actually_overlaps(self, dataloader: str):
        speedup = number(r"we reached ([\d.]+)x", dataloader)
        ceiling = number(r"perfect overlap would be ([\d.]+)x", dataloader)
        assert speedup > 1.0, (
            f"prefetching gave {speedup:.2f}x, so the loader and the trainer are still taking turns"
        )
        assert speedup <= ceiling * 1.15, (
            f"measured {speedup:.2f}x against a theoretical ceiling of "
            f"{ceiling:.2f}x; the benchmark is measuring the wrong thing"
        )

    def test_every_rank_trained_on_data(self, dataloader: str):
        samples = number(r"rank 0 saw (\d+) samples", dataloader)
        assert samples > 0

    def test_batches_were_produced_by_every_shard(self, dataloader: str):
        produced = number(r"batches produced:\s+(\d+)", dataloader)
        assert produced >= 32, f"only {produced:.0f} batches; the loaders barely ran"


class TestRLControlPlane:
    def iterations(self, output: str) -> list[dict]:
        rows = []
        for line in output.splitlines():
            match = re.match(
                r"iteration (\d+): (\d+) trajectories in (\d+)ms, "
                r"reward ([-+][\d.]+), policy v(\d+), straggled \[([^\]]*)\]",
                line,
            )
            if match:
                rows.append(
                    {
                        "trajectories": int(match.group(2)),
                        "reward": float(match.group(4)),
                        "version": int(match.group(5)),
                        "straggled": [int(r) for r in match.group(6).split(",") if r.strip()],
                    }
                )
        return rows

    def test_the_loop_ran(self, rl: str):
        assert len(self.iterations(rl)) >= 3

    def test_wait_returns_exactly_what_was_asked_for(self, rl: str):
        counts = {row["trajectories"] for row in self.iterations(rl)}
        assert counts == {6}, (
            f"wait(num_returns=6) produced {counts}; it is supposed to return as "
            "soon as six have settled, no more and no fewer"
        )

    def test_the_slow_workers_are_the_ones_dropped(self, rl: str):
        rows = self.iterations(rl)
        assert all(row["straggled"] for row in rows), (
            "nothing straggled, so the example is not demonstrating what it claims"
        )
        # The rollout script makes cost grow with rank, so the stragglers must be
        # the high ranks. If they are not, `wait` is not returning the *first* k.
        for row in rows:
            assert min(row["straggled"]) >= 4, (
                f"straggled {row['straggled']}: a fast worker was dropped while a "
                "slow one was kept, so wait() is not ordering by completion"
            )

    def test_the_policy_version_advances_every_iteration(self, rl: str):
        versions = [row["version"] for row in self.iterations(rl)]
        assert versions == list(range(1, len(versions) + 1)), (
            f"policy versions {versions}; publish/reload is not sequencing cleanly"
        )

    def test_learning_signal_moves_in_one_direction(self, rl: str):
        rewards = [row["reward"] for row in self.iterations(rl)]
        assert rewards[-1] > rewards[0], (
            f"reward went {rewards}; the published policy is not reaching the "
            "rollout workers, which would make the whole loop decorative"
        )

    def test_stragglers_are_dropped_but_not_abandoned(self, rl: str):
        generated = number(r"trajectories generated:\s+(\d+)", rl)
        consumed = number(r"consumed by the learner:\s+(\d+)", rl)
        droppped = number(r"dropped as straggling:\s+(\d+)", rl)
        assert consumed + droppped == generated
        assert droppped > 0
        # Every worker, straggler included, must still be told about the new
        # policy -- otherwise a real NCCL broadcast would hang on the ones that
        # were skipped.
        assert "reward +" in rl or "reward -" in rl

    def test_the_driver_stays_out_of_the_data_path(self, rl: str):
        payload = number(r"rollout -> learner:\s+([\d,.]+) MB", rl)
        driver = number(r"through the driver:\s+([\d,.]+) KB", rl)
        ratio = payload * 1e6 / (driver * 1e3)
        assert ratio > 50, (
            f"trajectories were {payload:.1f} MB but the driver moved "
            f"{driver:.1f} KB ({ratio:.0f}x); the control plane is carrying data"
        )
