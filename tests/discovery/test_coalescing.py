"""Configuration and lease safety; exact pacing uses Rust's paused clock tests."""

from __future__ import annotations

import inspect
import time

import pytest
import tinyray
from tinyray._tinyray import Client

from tests.support.registry import RegistryProc


@pytest.mark.parametrize(
    "bad", [-1, -100, 0.0, 1.5, float("nan"), float("inf"), -float("inf"), True, False, "50", None]
)
def test_coalescing_rejects_invalid_values_before_starting_a_client(monkeypatch, bad):
    def unexpected_client(*args, **kwargs):
        pytest.fail("invalid coalescing configuration reached the runtime")

    monkeypatch.setattr(tinyray, "_Client", unexpected_client)
    with pytest.raises(ValueError, match="coalesce_ms.*nonnegative integer"):
        tinyray.join("invalid", coalesce_ms=bad)
    assert tinyray._client is None


def test_python_and_native_coalescing_defaults_remain_fifty_milliseconds():
    assert inspect.signature(tinyray.join).parameters["coalesce_ms"].default == 50
    client = Client("http://127.0.0.1:1", "default", 1, 1, "churn")
    assert client.stats()["coalesce_ms"] == 50
    assert client.stats()["effective_coalesce_ms"] == 50


@pytest.mark.parametrize("requested", [0, 7, 50, 10**100])
def test_join_forwards_coalescing_and_bounds_it_to_the_lease(registry, requested):
    with tinyray.join("configured", coalesce_ms=requested) as me:
        me.ready(value=1).flush(timeout=3)
        stats = me.stats()
        assert stats["coalesce_ms"] == min(requested, 2**64 - 1)
        assert stats["effective_coalesce_ms"] == min(requested, registry.ttl_ms // 4)
        for value in range(2, 5):
            me.ready(value=value).flush(timeout=3)
            assert tinyray.pool("configured").pick().state["value"] == value


@pytest.mark.parametrize("requested", [0, 50, 10**9])
def test_large_coalescing_cannot_expire_a_short_healthy_lease(requested):
    registry = RegistryProc(ttl_ms=200)
    registry.start()
    try:
        with tinyray.join("short", registry_url=registry.endpoint, coalesce_ms=requested) as me:
            me.ready(step=0).flush(timeout=3)
            pool = tinyray.pool("short")
            identity = pool.pick().identity
            for step in range(1, 7):
                me.ready(step=step).flush(timeout=3)
                assert pool.pick().identity == identity
                assert pool.pick().state["step"] == step
                assert me.stats()["effective_coalesce_ms"] <= registry.ttl_ms // 4

            native = pool._c
            target = native.stats()["beats_ok"] + 4
            deadline = time.monotonic() + 3
            while native.stats()["beats_ok"] < target:
                revision = native.cache_revision()
                assert time.monotonic() < deadline, "idle renewal stopped behind coalescing"
                native.wait_revision(revision, 100)
            assert native.accepted
            assert pool.pick().identity == identity
    finally:
        registry.stop()
