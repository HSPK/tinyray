"""Indexed scalar filters must be indistinguishable from the scan path."""

from __future__ import annotations

import random
import time

import msgspec
import pytest
import tinyray

from tests.support.registry_wire import beat


def _publish(
    endpoint: str,
    ident: int,
    state: dict,
    *,
    ready: bool,
    publication: int = 0,
    leaving: bool = False,
) -> None:
    response = beat(
        endpoint,
        {
            "pool": "indexed",
            "id": ident,
            "incarnation": 1,
            "publication": publication,
            "policy": "churn",
            "state": state,
            "ready": ready,
            "leaving": leaving,
        },
    )
    assert response["accepted"]


def _states() -> list[tuple[dict, bool]]:
    states = []
    for ident in range(96):
        state = {
            "bucket": ("hot", "warm", "cold")[ident % 3],
            "rank": ident % 11 if ident % 2 else float(ident % 11),
            "flag": ident % 5 == 0,
            "nullable": None if ident % 7 == 0 else ident % 4,
            "big": 2**63 + ident,
            "nested": {"values": [ident % 4]},
            "tags": [ident % 3, "x"],
        }
        if ident % 6 == 0:
            state.pop("nullable")
        states.append((state, ident % 4 != 0))
    return states


def _wait_for_ids(pool, expected: set[int], timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while True:
        got = {member.id for member in pool.snapshot().members}
        if got == expected:
            return
        revision = pool._c.cache_revision()
        assert time.monotonic() < deadline, (got, expected)
        pool._c.wait_revision(revision, 200)


def _scan_ids(pool, filt: dict, require_ready: bool = True) -> list[int]:
    raw = None if not filt else msgspec.msgpack.encode(filt)
    return pool._c.debug_filter_scan_ids(pool._name, raw, require_ready)


def test_randomized_indexed_filters_match_forced_scans(long_lease):
    states = _states()
    for ident, (state, ready) in enumerate(states):
        _publish(long_lease.endpoint, ident, state, ready=ready)

    with tinyray.join("filter-observer", registry_url=long_lease.endpoint, coalesce_ms=0):
        pool = tinyray.pool("indexed")
        ready_ids = {ident for ident, (_, ready) in enumerate(states) if ready}
        pool.wait(count=len(ready_ids), timeout=10)

        rng = random.Random(0xF17E2)
        scalar_values = {
            "bucket": ["hot", "warm", "cold", "missing", None],
            "rank": [0, 0.0, 3, 3.0, 7.5, True],
            "flag": [True, False, 0, 1, 1.0],
            "nullable": [None, 0, 0.0, 3, 3.0, False],
            "big": [2**63, 2**63 + 1, float(2**63), 2**64 - 1],
            "missing": [None, "x", 1],
        }
        filters = [
            {"flag": True},
            {"flag": 1},
            {"rank": 3},
            {"rank": 3.0},
            {"nullable": None},
            {"missing": None},
            {"nested": {"values": [3.0]}},
            {"tags": [1.0, "x"]},
        ]
        fields = list(scalar_values)
        for _ in range(120):
            chosen = rng.sample(fields, rng.randint(1, 3))
            filters.append({field: rng.choice(scalar_values[field]) for field in chosen})

        pool._c.debug_filter_index_clear(pool._name)
        for index, filt in enumerate(filters):
            expected = _scan_ids(pool, filt)
            raw = msgspec.msgpack.encode(filt)
            assert pool._c.count(pool._name, raw, True) == len(expected), filt
            assert [member.id for member in pool.all(**filt)] == expected
            waited = pool.wait(count=len(expected), timeout=1, **filt)
            assert [member.id for member in waited] == expected
            if expected:
                picked = {pool.pick(**filt).id for _ in range(20)}
                assert picked <= set(expected)
            else:
                with pytest.raises(tinyray.NotFound):
                    pool.pick(**filt)

            if index == 0:
                assert pool._c.debug_filter_index_stats(pool._name)["entries"] == 1

        stats = pool._c.debug_filter_index_stats(pool._name)
        assert stats["entries"] <= stats["max_entries"] == 32
        assert stats["bytes"] <= stats["max_bytes"] == 1024 * 1024
        assert stats["hits"] > 0
        assert stats["builds"] > stats["max_entries"]
        assert stats["evictions"] > 0
        assert stats["fallbacks"] >= 2


def test_updates_readiness_removals_and_restart_never_reuse_stale_filter_ids(long_lease):
    states = _states()[:8]
    for ident, (state, ready) in enumerate(states):
        _publish(long_lease.endpoint, ident, state, ready=ready)

    with tinyray.join("filter-updates", registry_url=long_lease.endpoint, coalesce_ms=0):
        pool = tinyray.pool("indexed")
        pool.wait(count=sum(ready for _, ready in states), timeout=10)
        hot = {"bucket": "hot"}
        before = _scan_ids(pool, hot)
        assert [member.id for member in pool.all(**hot)] == before
        assert pool._c.debug_filter_index_stats(pool._name)["entries"] == 1

        changed = dict(states[3][0], bucket="cold")
        _publish(long_lease.endpoint, 3, changed, ready=False, publication=1)
        expected_members = set(range(8))
        _wait_for_ids(pool, expected_members)
        deadline = time.monotonic() + 5
        while 3 in _scan_ids(pool, hot):
            revision = pool._c.cache_revision()
            assert time.monotonic() < deadline
            pool._c.wait_revision(revision, 200)
        assert [member.id for member in pool.all(**hot)] == _scan_ids(pool, hot)

        _publish(
            long_lease.endpoint,
            6,
            states[6][0],
            ready=states[6][1],
            publication=1,
            leaving=True,
        )
        _wait_for_ids(pool, expected_members - {6})
        assert [member.id for member in pool.all(**hot)] == _scan_ids(pool, hot)

        long_lease.stop()
        long_lease.start()
        for ident, (state, ready) in enumerate(states[:3]):
            _publish(long_lease.endpoint, ident, dict(state, bucket="restarted"), ready=ready)
        expected = {ident for ident, (_, ready) in enumerate(states[:3]) if ready}
        pool.wait(count=len(expected), timeout=15, bucket="restarted")
        assert [member.id for member in pool.all(bucket="hot")] == []
        assert [member.id for member in pool.all(bucket="restarted")] == sorted(expected)
