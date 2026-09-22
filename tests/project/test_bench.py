"""Performance gates must fail on broken measurements, not merely slow ones."""

from __future__ import annotations

import json
import pathlib
import socket
import subprocess
import sys

import pytest
import tinyray

import bench

ROOT = pathlib.Path(__file__).resolve().parents[2]


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
        assert server._closed
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


def test_native_bytes_boundaries_keep_zero_copy_pybacked_extraction():
    source = (ROOT / "crates/tinyray-client/src/rpc.rs").read_text()
    assert source.count("payload: PyBackedBytes") == 2
    python_service = source.split("struct PythonService", 1)[1]
    assert "value.extract::<(" in python_service
    assert "u8,\n                            PyBackedBytes," in python_service
    assert "payload.to_vec()" in python_service


def test_native_rpc_binding_is_a_thin_public_transport_adapter():
    source = (ROOT / "crates/tinyray-client/src/rpc.rs").read_text()
    assert len(source.splitlines()) <= 900
    assert "tinyray::Client::from_current()" in source
    assert "request_with_blob_owners_cancellable_async" in source
    assert "cancellation: tinyray::ClientRequestCancellation" in source
    assert "self.cancellation.cancel();" in source
    assert "task.abort();" in source
    assert "struct SharedRpcServer" in source
    assert "module.add_class::<SharedRpcServer>()?;" in source
    for duplicate in (
        "struct ConnectionPool",
        "struct ClientConnection",
        "struct FrameBudgets",
        "struct PendingConnectSocket",
        "async fn client_writer",
        "async fn client_reader",
        "async fn client_idle",
        "async fn exchange",
        "pub struct RpcServer",
        "module.add_class::<RpcServer>()?;",
        "async fn accept_loop",
        "async fn serve_connection",
    ):
        assert duplicate not in source


def test_rust_server_borrows_rpc_payload_before_its_single_owned_copy():
    source = (ROOT / "crates/tinyray/src/transport/server.rs").read_text()
    borrowed = source.split("struct BorrowedRpcRequest", 1)[1].split("struct ServerRpcRequest", 1)[
        0
    ]
    decoder = source.split("fn decode_server_request", 1)[1].split("fn validate_request", 1)[0]
    assert "payload: &'a [u8]" in borrowed
    assert "payload: Arc::from(request.payload)" in decoder
    assert "request.payload.to_vec()" not in decoder


def test_snapshot_phase_baseline_keeps_creation_and_materialization_separate():
    baseline = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["point_lookup"]
    required = {
        "snapshot_ms",
        "snapshot_materialize_ms",
        "snapshot_state_ms",
        "snapshot_len_ms",
        "snapshot_slot_ms",
        "snapshot_get_ms",
        "wait_count_ms",
        "epoch_ms",
        "epoch_materialize_ms",
        "all_state_ms",
        "cached_all_state_ms",
        "cached_snapshot_state_ms",
    }
    for size in ("100", "1000", "5000"):
        assert required <= baseline[size].keys()
    assert baseline["5000"]["all_ms"] < 1.5
    assert baseline["5000"]["snapshot_materialize_ms"] < 1.5
    assert {
        "point_lookup.5000.snapshot_ms",
        "point_lookup.5000.snapshot_len_ms",
        "point_lookup.5000.snapshot_slot_ms",
        "point_lookup.5000.snapshot_get_ms",
        "point_lookup.5000.wait_count_ms",
        "point_lookup.5000.epoch_ms",
        "point_lookup.5000.all_state_ms",
        "point_lookup.5000.cached_all_state_ms",
        "point_lookup.5000.snapshot_state_ms",
        "point_lookup.5000.cached_snapshot_state_ms",
    } <= bench.WATCHED.keys()


def test_filter_index_baseline_separates_cold_builds_from_warm_hits():
    baseline = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["filter_index"]
    required = {
        "count_cold_ms",
        "count_warm_ms",
        "pick_cold_ms",
        "pick_warm_ms",
        "all_cold_ms",
        "all_warm_ms",
        "wait_cold_ms",
        "wait_warm_ms",
        "index_entries",
        "index_bytes",
        "index_hits",
        "index_builds",
        "index_evictions",
        "index_invalidations",
        "index_fallbacks",
        "index_uncached",
        "index_max_entries",
        "index_max_fields",
        "index_max_key_bytes",
        "index_max_ids_per_entry",
        "index_max_bytes",
    }
    for size in ("100", "1000", "5000"):
        assert required <= baseline[size].keys()
        assert baseline[size]["index_entries"] == 1
        assert baseline[size]["index_bytes"] <= baseline[size]["index_max_bytes"]
        assert baseline[size]["index_max_entries"] == 32
        assert baseline[size]["index_max_fields"] == 8
        assert baseline[size]["index_max_key_bytes"] == 4096
        assert baseline[size]["index_max_ids_per_entry"] == 8192
        assert baseline[size]["index_max_bytes"] == 1024 * 1024
    assert baseline["5000"]["count_warm_ms"] < 0.02
    assert baseline["5000"]["pick_warm_ms"] < 0.02
    assert {
        "filter_index.5000.count_warm_ms",
        "filter_index.5000.pick_warm_ms",
        "filter_index.5000.all_warm_ms",
        "filter_index.5000.wait_warm_ms",
        "filter_index.5000.index_bytes",
    } <= bench.WATCHED.keys()


def test_model_codec_fixed_costs_are_watched():
    assert {
        "rpc_models.codec.encode_plain_us",
        "rpc_models.codec.encode_dataclass_us",
        "rpc_models.codec.encode_typed_container_us",
        "rpc_models.codec.restore_dataclass_us",
        "rpc_models.codec.restore_typed_container_us",
    } <= bench.WATCHED.keys()


def test_model_baseline_contains_only_dataclasses_and_typed_containers():
    models = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["rpc_models"]
    assert "pydantic" not in json.dumps(models).lower()
    assert {
        "encode_dataclass_us",
        "encode_typed_container_us",
        "restore_dataclass_us",
        "restore_typed_container_us",
        "wire_typed_container_bytes",
    } <= models["codec"].keys()
    assert {"plain", "dataclass", "dataclass_manual", "typed_container"} <= models[
        "round_trip"
    ].keys()


def test_rpc_concurrency_baseline_measures_multiplexing_not_only_rate():
    concurrency = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"][
        "rpc_concurrency"
    ]
    assert concurrency["connections"] == concurrency["connections_opened"] == 1
    assert set(concurrency["callers"]) == {"1", "4", "8", "32", "128"}
    expected_connections = {"1": 1, "4": 2, "8": 4, "32": 4, "128": 4}
    for callers, result in concurrency["callers"].items():
        assert result["calls"] > 0
        assert result["calls_per_s"] > 0
        assert result["connections"] == expected_connections[callers]
        assert result["connection_reduction"] == 1 - result["connections"] / int(callers)
        assert result["in_flight_after"] == 0
        assert result["p50_ms"] <= result["p99_ms"] <= result["max_ms"]
    assert concurrency["callers"]["8"]["calls_per_s"] > 7248
    assert concurrency["callers"]["8"]["p99_ms"] < 2.77
    assert concurrency["callers"]["128"]["connections"] <= 4
    assert {
        "rpc_concurrency.callers.8.calls_per_s",
        "rpc_concurrency.callers.8.p50_ms",
        "rpc_concurrency.callers.8.p99_ms",
        "rpc_concurrency.callers.128.connection_reduction",
        "rpc_concurrency.connections",
    } <= bench.WATCHED.keys()


def test_rust_service_baseline_compares_the_same_transport_and_meets_target():
    result = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["rust_service"]
    assert result["rust"]["no_op"]["p50_ms"] <= 0.10
    assert (
        result["rust"]["concurrency"]["8"]["calls_per_s"]
        > result["python"]["concurrency"]["8"]["calls_per_s"]
    )
    assert result["throughput_8_speedup"] > 3
    for implementation in ("python", "rust"):
        assert set(result[implementation]["concurrency"]) == {"1", "8", "32", "128"}
        assert {
            "no_op",
            "payload_64k",
            "typed",
            "batch_32",
            "concurrency",
        } <= result[implementation].keys()
    assert result["rust"]["concurrency"]["128"]["connections"] <= 4
    assert {
        "rust_service.python.no_op.p50_ms",
        "rust_service.rust.no_op.p50_ms",
        "rust_service.rust.concurrency.8.calls_per_s",
        "rust_service.rust.concurrency.128.connections",
        "rust_service.throughput_8_speedup",
    } <= bench.WATCHED.keys()


def test_rust_discovery_baseline_keeps_arc_views_cheaper_than_owned_clones():
    result = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["rust_discovery"]
    for size in ("100", "1000", "5000"):
        assert result[size]["snapshot_ms"] < 0.001
        assert result[size]["refs_ms"] < result[size]["owned_ms"]
        assert result[size]["clone_speedup"] > 5
    assert result["5000"]["refs_ms"] < 0.1
    assert result["5000"]["clone_speedup"] > 25
    assert {
        "rust_discovery.5000.refs_ms",
        "rust_discovery.5000.owned_ms",
        "rust_discovery.5000.clone_speedup",
    } <= bench.WATCHED.keys()


def test_rust_member_runtime_shares_client_and_server_workers():
    result = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["rust_runtime"]
    assert result["membership_workers"] == 2
    assert result["rpc_workers"] == 4
    assert result["total_native_workers"] == 6
    assert {
        "rust_runtime.rpc_workers",
        "rust_runtime.total_native_workers",
    } <= bench.WATCHED.keys()


def test_rpc_copy_profile_records_the_borrowed_decode_boundary():
    result = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["rpc_copy_profile"]
    assert result["65536"]["borrowed_decode_us"] < 5
    assert result["65536"]["decode_speedup"] > 20
    assert result["1048576"]["borrowed_decode_us"] < result["1048576"]["decode_us"]
    assert {
        "rpc_copy_profile.65536.borrowed_decode_us",
        "rpc_copy_profile.65536.decode_speedup",
        "rpc_copy_profile.1048576.borrowed_decode_us",
    } <= bench.WATCHED.keys()


def test_blobref_baseline_separates_creation_calls_access_and_wire_size():
    result = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["blobref"]
    for topology in ("python_same", "python_separate", "rust_same", "rust_separate"):
        assert set(result[topology]) == {"65536", "1048576", "16777216"}
        for size, row in result[topology].items():
            assert {
                "creation_ms",
                "ordinary_call_ms",
                "blob_call_ms",
                "access_ms",
                "ordinary_wire_bytes",
                "blob_wire_bytes",
            } <= row.keys()
            assert row["ordinary_wire_bytes"] > int(size)
            assert row["blob_wire_bytes"] < 256
        assert (
            result[topology]["16777216"]["blob_call_ms"]
            < result[topology]["65536"]["blob_call_ms"] * 2
        )
    assert {
        "blobref.python_same.65536.ordinary_call_ms",
        "blobref.python_same.16777216.blob_call_ms",
        "blobref.python_separate.16777216.blob_call_ms",
        "blobref.rust_same.16777216.blob_call_ms",
        "blobref.rust_separate.16777216.blob_call_ms",
        "blobref.rust_same.16777216.blob_wire_bytes",
    } <= bench.WATCHED.keys()


def test_registry_connection_baseline_measures_reuse_not_only_beat_rate():
    scenarios = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]
    idle = scenarios["idle_beat_rate"]
    scaled = scenarios["registry_connections"]
    assert idle["connections_opened"] == 0
    assert idle["reuses"] > 0
    assert scaled["members"] == scaled["persistent_connections"] == 1000
    assert scaled["persistent_server_accepts"] == 1000
    assert scaled["persistent_server_frames"] == 6000
    assert scaled["persistent_reuses"] == 5000
    assert scaled["persistent_failures"] == scaled["reconnecting_failures"] == 0
    assert scaled["persistent_live_members"] == scaled["reconnecting_live_members"] == 1000
    assert 0 < scaled["persistent_p50_ratio"] < 1
    assert scaled["persistent_p99_ratio"] <= 1.2
    assert scaled["reconnecting_connections"] == scaled["reconnecting_beats"] == 6000
    assert scaled["reconnecting_server_accepts"] == 6000
    assert scaled["accept_reduction"] > 0.8


def test_idle_waiter_baseline_rejects_heartbeat_fanout():
    idle = json.loads((ROOT / "bench-baseline.json").read_text())["scenarios"]["idle_waiters"]
    assert idle["waiters"] == 1000
    assert idle["beats"] >= 4
    assert idle["rechecks"] == 0
    assert idle["watch_wakeups"] == 0
    assert {
        "idle_waiters.rechecks",
        "idle_waiters.watch_wakeups",
    } <= bench.WATCHED.keys()
