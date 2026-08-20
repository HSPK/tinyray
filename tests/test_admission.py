"""Beats that would damage state, rather than merely being wrong, are refused.

Everything here is something a buggy client can send by accident. The registry
has no authentication and is not trying to have any; the point is that one bad
peer must not be able to make a seat unusable or grow memory without bound.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

import pytest


def beat(endpoint: str, **kw) -> dict:
    body = {
        "pool": "t",
        "id": 0,
        "slot": 0,
        "incarnation": 1,
        "policy": "stateful",
        "url": None,
        "state": {},
        "ready": True,
        "leaving": False,
        "exclusive": False,
        "methods": [],
        "watch": [],
        "seen": {},
    }
    body.update(kw)
    req = urllib.request.Request(
        f"http://{endpoint}/v1/beat",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def pools(endpoint: str) -> dict:
    with urllib.request.urlopen(f"http://{endpoint}/v1/pools", timeout=5) as r:
        return json.loads(r.read())


def test_a_runaway_clock_cannot_lock_a_seat_forever(registry):
    """The high-water mark is permanent, so accepting a tenure from the year
    50,000 would mean nothing healthy could ever take that seat again. For a
    trainer rank that is a job which can never be restarted."""
    ep = registry.endpoint
    assert beat(ep, incarnation=2**64 - 1)["accepted"] is False
    assert "t" not in pools(ep), "a refused beat still created the pool"

    now = int(time.time() * 1000) << 20
    assert beat(ep, incarnation=now)["accepted"] is True

    # And a restart still takes the seat back, which is the whole reason the
    # default is last-writer-wins.
    assert beat(ep, incarnation=now + 1)["accepted"] is True


def test_a_plausible_future_tenure_is_still_accepted(registry):
    """The bound has to be loose enough that clock skew is not an outage."""
    ep = registry.endpoint
    an_hour_ahead = (int(time.time() * 1000) + 3_600_000) << 20
    assert beat(ep, incarnation=an_hour_ahead)["accepted"] is True


def test_absurd_names_are_refused_rather_than_stored(registry):
    ep = registry.endpoint
    before = len(pools(ep))
    assert beat(ep, pool="x" * 100_000, id=1)["accepted"] is False
    assert beat(ep, pool="ok", id=1, watch=["y" * 100_000])["accepted"] is False
    assert len(pools(ep)) == before, "a refused beat created a pool anyway"


def test_watching_everything_is_refused(registry):
    """A subscriber is sent the whole roster of each pool it watches, so an
    unbounded watch list is an unbounded response."""
    ep = registry.endpoint
    assert beat(ep, pool="w", id=1, watch=[f"p{i}" for i in range(10_000)])["accepted"] is False
    assert beat(ep, pool="w", id=1, watch=[f"p{i}" for i in range(8)])["accepted"] is True


def test_malformed_bodies_are_rejected_without_killing_anything(registry):
    ep = registry.endpoint
    for raw in (b"", b"not json", b"[1,2,3]", b'{"pool":"p"}', b'{"pool":"p","id":"one"}'):
        req = urllib.request.Request(
            f"http://{ep}/v1/beat",
            data=raw,
            headers={"content-type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(req, timeout=5)
        assert e.value.code == 400
    # Still serving afterwards.
    assert beat(ep, pool="after", id=1)["accepted"] is True
