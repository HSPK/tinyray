"""Performance gates must fail on broken measurements, not merely slow ones."""

from __future__ import annotations

import json
import socket
import subprocess
import sys

import pytest
import tinyray

import bench


def results(**scenarios):
    return {
        "format_version": bench.FORMAT_VERSION,
        "settings": {"ttl_ms": bench.TTL_MS, "coalesce_ms": None},
        "scenarios": scenarios,
    }


@pytest.fixture(autouse=True)
def restore_benchmark_options(monkeypatch):
    monkeypatch.setattr(bench, "COALESCE_MS", None)
    monkeypatch.setattr(bench, "provenance", lambda: {"test": True})


def test_unknown_or_empty_scenario_selection_is_an_error():
    for argv in (["--only", "nonexistent"], ["--only"]):
        with pytest.raises(SystemExit) as caught:
            bench.main(argv)
        assert caught.value.code == 2


@pytest.mark.parametrize("checking", [False, True])
def test_scenario_failure_cannot_be_a_success(monkeypatch, tmp_path, checking):
    def failed():
        raise RuntimeError("measurement failed")

    monkeypatch.setattr(bench, "SCENARIOS", {"rpc_latency": failed})
    path = tmp_path / "result.json"
    argv = ["--only", "rpc_latency", "--json", str(path)]
    if checking:
        baseline = tmp_path / "baseline.json"
        baseline.write_text(json.dumps(results(rpc_latency={"p50_ms": 1.0, "p90_ms": 1.0})))
        argv += ["--check", str(baseline)]
    assert bench.main(argv) == 1
    outcome = json.loads(path.read_text())["scenarios"]["rpc_latency"]
    assert outcome["status"] == "error"
    assert "measurement failed" in outcome["error"]


def test_missing_current_metrics_fail_instead_of_disappearing():
    before = results(rpc_latency={"p50_ms": 1.0, "p90_ms": 1.0})
    after = results(rpc_latency={"p50_ms": 1.0})
    worse, better, checked = bench.compare(before, after)
    assert checked == 2 and not better
    assert len(worse) == 1 and "p90_ms: missing from current" in worse[0]


def test_incomplete_baselines_are_not_silently_accepted():
    before = results(rpc_latency={"p50_ms": 1.0})
    after = results(rpc_latency={"p50_ms": 1.0, "p90_ms": 1.0})
    worse, _, checked = bench.compare(before, after)
    assert checked == 2
    assert len(worse) == 1 and "missing from baseline" in worse[0]


def test_selecting_a_subset_does_not_require_unselected_scenarios():
    before = results(
        rpc_latency={"p50_ms": 1.0, "p90_ms": 1.0},
        publish={"flush_p50_ms": 0.2},
    )
    assert bench.compare(before, results(rpc_latency={"p50_ms": 1.0, "p90_ms": 1.0})) == (
        [],
        [],
        2,
    )


def test_zero_comparable_metrics_cannot_pass():
    worse, better, checked = bench.compare(results(), results())
    assert worse and "no watched metrics" in worse[0]
    assert not better and checked == 0


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf"), -1.0])
def test_nonfinite_metrics_are_errors(bad):
    before = results(rpc_latency={"p50_ms": 1.0, "p90_ms": 1.0})
    after = results(rpc_latency={"p50_ms": bad, "p90_ms": 1.0})
    assert "non-finite" in bench.compare(before, after)[0][0]
    assert "non-finite" in bench.compare(after, before)[0][0]


def test_flush_gate_detects_sub_millisecond_regressions():
    before = results(publish={"flush_p50_ms": 0.2})
    after = results(publish={"flush_p50_ms": 0.6})
    assert bench.compare(before, after)[0]
    assert not bench.compare(before, results(publish={"flush_p50_ms": 0.22}))[0]


def test_obsolete_baseline_is_refused_before_measurement(monkeypatch, tmp_path):
    called = []
    monkeypatch.setattr(bench, "SCENARIOS", {"rpc_latency": lambda: called.append(True)})
    path = tmp_path / "old.json"
    path.write_text(json.dumps({"scenarios": {}}))
    with pytest.raises(SystemExit) as caught:
        bench.main(["--check", str(path)])
    assert caught.value.code == 2 and not called


def test_unsupported_features_are_distinct_from_execution_errors(monkeypatch, tmp_path):
    def unsupported():
        raise bench.UnsupportedScenario("old wheel")

    monkeypatch.setattr(bench, "SCENARIOS", {"rpc_latency": unsupported})
    path = tmp_path / "out.json"
    assert bench.main(["--json", str(path)]) == 0
    assert json.loads(path.read_text())["scenarios"]["rpc_latency"]["status"] == "unsupported"
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps(results(rpc_latency={"p50_ms": 1.0, "p90_ms": 1.0})))
    assert bench.main(["--check", str(baseline)]) == 1


def test_settle_does_not_hide_a_failed_publication():
    class Member:
        def flush(self):
            raise TimeoutError("registry unavailable")

    with pytest.raises(TimeoutError, match="registry unavailable"):
        bench.settle(Member())


@pytest.mark.parametrize("legacy", [False, True])
def test_digest_signature_detection_stays_outside_timed_calls(monkeypatch, legacy):
    inspected = []
    original = bench.inspect.signature

    def signature(fn):
        inspected.append(fn)
        return original(fn)

    monkeypatch.setattr(bench.inspect, "signature", signature)

    def current(pool, fields):
        assert pool == "load" and fields == ["shard"]
        return 7

    def old(pool, fields, require_ready):
        assert require_ready is False
        return current(pool, fields)

    reader = bench.digest_reader(old if legacy else current)
    assert len(inspected) == 1
    for _ in range(20):
        assert reader() == 7
    assert len(inspected) == 1


def test_benchmark_teardown_closes_members_and_restores_environment(monkeypatch):
    monkeypatch.setenv("TINYRAY_REGISTRY", "original:123")
    with bench.Registry():
        member = bench.serving_member()
        member.ready()
        bench.settle(member)
        server = member._server
        assert server is not None
        assert tinyray.pool("b").pick().ping() == "pong"
        bench.leave_members()
        assert not server._thread.is_alive()
        with socket.socket() as client:
            assert client.connect_ex(("127.0.0.1", server.port)) != 0
    import os

    assert os.environ["TINYRAY_REGISTRY"] == "original:123"
    assert not bench._registries and tinyray._client is None


def test_python_optimization_cannot_remove_workloads_or_cleanup():
    script = """
import contextlib, json
import bench

registry = bench.Registry()
bench._registries.append(registry)
registry.__exit__(None, None, None)
if bench._registries:
    raise RuntimeError('cleanup was optimized away')

class Handle:
    calls = 0
    def ping(self):
        self.calls += 1
        return 'pong'

handle = Handle()
bench.Registry = lambda: contextlib.nullcontext()
bench.RemoteService = lambda: contextlib.nullcontext(handle)
bench.bench_rpc_separate()
if handle.calls != 2200:
    raise RuntimeError(f'only {handle.calls} latency calls ran')

handle.calls = 0
batches = []
def batch(target, calls):
    batches.append(len(calls))
    return ['pong'] * len(calls)
def once(fn, **kwargs):
    fn()
    return 1.0
bench.tinyray.batch = batch
bench.timed = once
bench.bench_rpc_batch()
if handle.calls != 32 or batches != [32]:
    raise RuntimeError('batch workloads were optimized away')
before = handle.calls
bench.rpc_rate(handle, threads=1, duration=0.01)
if handle.calls <= before:
    raise RuntimeError('throughput workload was optimized away')
print('all workloads executed')
"""
    completed = subprocess.run(
        [sys.executable, "-O", "-c", script],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "all workloads executed"
