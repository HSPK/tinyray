"""Shared fixtures for every Python subsystem suite."""

from __future__ import annotations

import os

import pytest

from tests.support.registry import RegistryProc


@pytest.fixture(autouse=True)
def _no_leaked_membership():
    """One process is one member, so a test that fails before leave() would
    make every later test fail with "already joined". Reset it centrally
    rather than relying on each test to clean up after itself."""
    yield
    import tinyray

    if tinyray._client is not None:
        try:
            tinyray._client.leave()
        except Exception:
            pass
        tinyray._client = None


@pytest.fixture
def registry():
    r = RegistryProc(ttl_ms=2000)
    r.start()
    os.environ["TINYRAY_REGISTRY"] = r.endpoint
    try:
        yield r
    finally:
        r.stop()


@pytest.fixture
def long_lease():
    """A registry whose lease is long enough that one heartbeat interval is a
    visible amount of time. Anything measuring the cost of *not* being woken
    needs it: at the default 2s lease the bell rings every 500ms, which papers
    over mistakes that would cost seconds in a real deployment."""
    r = RegistryProc(ttl_ms=20000)
    r.start()
    os.environ["TINYRAY_REGISTRY"] = r.endpoint
    try:
        yield r
    finally:
        r.stop()
