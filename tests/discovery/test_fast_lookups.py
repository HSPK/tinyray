"""Single-member paths must preserve the full-roster lookup rules."""

from __future__ import annotations

import json
import multiprocessing
import os
import time
from contextlib import ExitStack

import httpx
import pytest
import tinyray
from tinyray._tinyray import Client


@pytest.fixture
def fleet(registry):
    with ExitStack() as cleanup:
        me = cleanup.enter_context(tinyray.join("observer", coalesce_ms=0))
        peers = {}
        for ident, slot, ready, state in [
            (902, 2, True, {"tag": "two", "n": 3, "flag": True, "big": 2**63}),
            (104, 0, False, {"tag": "zero", "n": 3, "flag": True}),
            (703, 1, True, {"tag": "one", "n": 4, "flag": False}),
        ]:
            state["nested"] = {"items": [3], "meta": {"value": "original"}}
            peer = Client(
                endpoint=f"http://{registry.endpoint}",
                pool="fast",
                policy="stateful",
                id=ident,
                incarnation=1,
                slot=slot,
                size=3,
                methods=["ping"],
                coalesce_ms=0,
            )
            cleanup.callback(peer.leave)
            peer.set_state(json.dumps(state), ready)
            peer.watch(["fast"])
            assert peer.start()
            assert peer.accepted
            peers[slot] = peer
        pool = tinyray.pool("fast")
        pool.wait(count=2, timeout=5)
        assert [h.id for h in pool.snapshot()] == [104, 703, 902]
        yield me, pool, peers


@pytest.mark.parametrize("pool_type", [tinyray.Pool, tinyray.AsyncPool])
def test_single_member_lookups_never_materialize_the_roster(fleet, monkeypatch, pool_type):
    _, existing, _ = fleet
    pool = pool_type("fast", existing._c)

    def no_roster(*args, **kwargs):
        pytest.fail("single-member lookup materialized the full roster")

    monkeypatch.setattr(pool, "_members", no_roster)
    decoded = []
    original_decode = tinyray._json_decode

    def decode_one(raw):
        value = original_decode(raw)
        assert isinstance(value, dict), "only one member should cross the boundary"
        decoded.append(value["id"])
        return value

    monkeypatch.setattr(tinyray, "_json_decode", decode_one)
    picked = pool.pick(tag="two")
    seated = pool.slot(0)
    expected_type = tinyray.AsyncHandle if pool_type is tinyray.AsyncPool else tinyray.Handle
    assert type(picked) is expected_type
    assert type(seated) is expected_type
    assert picked.id == 902 and picked.slot == 2
    assert seated.id == 104 and not seated.ready
    assert picked._methods == ("ping",)
    assert picked.ping is not None
    assert decoded == [902, 104]
    with pytest.raises(tinyray.NotFound):
        pool.slot(0, require_ready=True)


@pytest.mark.parametrize(
    "filt,ids",
    [
        ({"n": 3.0}, {902}),
        ({"flag": True}, {902}),
        ({"flag": 1}, set()),
        ({"n": True}, set()),
        ({"big": 2**63}, {902}),
        ({"big": 2**63 + 1}, set()),
        ({"nested": {"items": [3.0], "meta": {"value": "original"}}}, {703, 902}),
        ({"nested": {"items": [True], "meta": {"value": "original"}}}, set()),
        ({"missing": None}, set()),
        ({}, {703, 902}),
    ],
)
def test_pick_uses_the_same_exact_filter_semantics_as_all(fleet, filt, ids):
    _, pool, _ = fleet
    assert {h.id for h in pool.all(**filt)} == ids
    if ids:
        assert {pool.pick(**filt).id for _ in range(100)} == ids
    else:
        with pytest.raises(tinyray.NotFound):
            pool.pick(**filt)


@pytest.mark.parametrize("pool_type", [tinyray.Pool, tinyray.AsyncPool])
def test_slots_are_not_wire_ids_and_missing_slots_never_fall_back(fleet, pool_type):
    _, existing, _ = fleet
    pool = pool_type("fast", existing._c)
    for slot, ident in [(0, 104), (1, 703), (2, 902)]:
        assert pool.slot(slot).id == ident
        assert pool.slot(float(slot)).id == ident
    for missing in [-1, 3, 104, 703, 902, 2**64, 1 << 100, 2**200, 1.5, float("nan"), float("inf")]:
        with pytest.raises(tinyray.NotFound):
            pool.slot(missing)


def test_duplicate_slots_choose_the_lowest_eligible_wire_id(fleet, registry):
    _, pool, _ = fleet
    duplicate = Client(
        endpoint=f"http://{registry.endpoint}",
        pool="fast",
        policy="stateful",
        id=5,
        incarnation=1,
        slot=2,
        size=3,
        methods=["ping"],
        coalesce_ms=0,
    )
    try:
        duplicate.set_state('{"tag":"duplicate"}', False)
        assert duplicate.start() and duplicate.accepted
        pool.until(lambda snap: len(snap) == 4, timeout=5)
        for view in [pool, tinyray.apool("fast")]:
            assert view.slot(2).id == 5
            assert view.slot(2, require_ready=True).id == 902
        duplicate.set_state('{"tag":"duplicate"}', True)
        pool.wait(count=3, timeout=5)
        assert pool.slot(2, require_ready=True).id == 5
        duplicate.leave()
        pool.until(lambda snap: len(snap) == 3, timeout=5)
        assert pool.slot(2).id == 902
    finally:
        duplicate.leave()


def _same_native_value(actual, expected):
    assert type(actual) is type(expected)
    if isinstance(expected, float):
        assert actual.hex() == expected.hex(), (actual.hex(), expected.hex())
    elif isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            _same_native_value(actual[key], expected[key])
    elif isinstance(expected, list):
        assert len(actual) == len(expected)
        for got, want in zip(actual, expected, strict=True):
            _same_native_value(got, want)
    else:
        assert actual == expected


def test_native_json_readers_match_stdlib_on_actual_native_output(registry):
    state = {
        "floats": [
            -0.0,
            0.0,
            5e-324,
            -5e-324,
            1e-323,
            1e-320,
            float.fromhex("0x0.fffffffffffffp-1022"),
            float.fromhex("0x1.0000000000000p-1022"),
            float.fromhex("0x1.0000000000001p-1022"),
            2.2250738585072012e-308,
            1.2345678901234567e-300,
            1e-100,
            0.12345678901234568,
            1.0000000000000002,
            1e100,
            1e300,
            1e308,
            float.fromhex("0x1.fffffffffffffp+1023"),
            -float.fromhex("0x1.fffffffffffffp+1023"),
        ],
        "integers": [
            -(1 << 63),
            -(1 << 63) + 1,
            -1,
            0,
            1,
            (1 << 53) - 1,
            1 << 53,
            (1 << 53) + 1,
            (1 << 63) - 1,
            1 << 63,
            (1 << 64) - 2,
            (1 << 64) - 1,
        ],
        "unicode": ["你好", "é", "e\u0301", "🙂", "\u2028", "\u0000"],
        "nested": {"键🙂": [{"float": -0.0, "flags": [True, False, None]}, [], {}]},
    }
    with tinyray.join("native-json", "stateful", slot=0, size=1, coalesce_ms=0) as me:
        pool = tinyray.pool("native-json")
        for publication in [1, 2]:
            me.ready(**state, publication=publication).flush(timeout=5)
            native = pool._c
            for require_ready in [False, True]:
                for _ in range(2):  # cold and cached serialization
                    raws = [
                        native.lookup(pool._name, "{}", require_ready),
                        native.lookup(
                            pool._name, json.dumps({"publication": publication}), require_ready
                        ),
                        native.frozen(pool._name, require_ready)[0],
                        native.choose(pool._name, "{}", require_ready),
                        native.lookup_slot(pool._name, 0, require_ready),
                    ]
                    for raw in raws:
                        _same_native_value(tinyray._json_decode(raw), json.loads(raw))
            expected = json.loads(native.lookup_slot(pool._name, 0))["state"]
            assert expected["publication"] == publication
            for view in [pool, tinyray.apool(pool._name)]:
                for handle in [view.all()[0], view.snapshot().slot(0), view.pick(), view.slot(0)]:
                    _same_native_value(handle.state, expected)


def test_cached_native_json_never_shares_mutable_python_state(fleet):
    _, pool, _ = fleet
    readers = [
        lambda: pool.all(tag="one")[0],
        lambda: pool.snapshot().slot(1),
        lambda: pool.slot(1),
        lambda: pool.pick(tag="one"),
        lambda: tinyray.apool("fast").slot(1),
    ]
    for read in readers:
        changed = read()
        changed.state["nested"]["items"].append(99)
        changed.state["nested"]["meta"]["value"] = "mutated"
        changed.state["local"] = True
        for fresh in readers:
            state = fresh().state
            assert state["nested"] == {"items": [3], "meta": {"value": "original"}}
            assert "local" not in state


def test_all_and_frozen_share_order_but_readiness_has_its_own_fingerprint(fleet):
    _, pool, peers = fleet
    for _ in range(3):
        all_raw, all_hash, roster, version = pool._c.frozen("fast", False)
        ready_raw, ready_hash, ready_roster, ready_version = pool._c.frozen("fast", True)
        assert [m["id"] for m in json.loads(all_raw)] == [104, 703, 902]
        assert [m["id"] for m in json.loads(ready_raw)] == [703, 902]
        assert all_hash == roster == ready_roster
        assert ready_hash != roster
        assert version == ready_version
    peers[0].set_state('{"tag":"zero","changed":true}', True)
    pool.wait(count=3, timeout=5)
    assert pool.pick(tag="zero").state["changed"] is True
    assert pool.slot(0, require_ready=True).ready
    raw, mine, whole, new_version = pool._c.frozen("fast", True)
    assert mine == whole == roster
    assert new_version > version
    assert len(json.loads(raw)) == 3


def test_state_digests_replacement_and_removal_invalidate_warm_paths(fleet, registry):
    _, pool, peers = fleet
    old = pool.slot(1)
    before = pool._c.field_digest("fast", ["tag", "ready", "url"])
    pool.snapshot()
    pool.all()
    peers[1].set_state('{"tag":"updated"}', False)
    pool.until(lambda snap: snap.slot(1).state.get("tag") == "updated", timeout=5)
    assert pool._c.field_digest("fast", ["tag", "ready", "url"]) != before
    assert not pool.slot(1).ready
    with pytest.raises(tinyray.NotFound):
        pool.pick(tag="updated")
    assert old.state["tag"] == "one"

    replacement = Client(
        endpoint=f"http://{registry.endpoint}",
        pool="fast",
        policy="stateful",
        id=703,
        incarnation=2,
        slot=1,
        size=3,
        methods=["ping"],
        coalesce_ms=0,
    )
    try:
        replacement.set_state('{"tag":"replacement"}', True)
        assert replacement.start()
        got = pool.wait_replacement(identity=old.identity, timeout=5)
        assert got is not None and got.incarnation == 2
        assert pool.slot(1).identity == got.identity
        assert pool.pick(tag="replacement").identity == got.identity
        assert pool.snapshot().slot(1).identity == got.identity
        replacement.leave()
        assert pool.wait_departure(got.identity, timeout=5)
        with pytest.raises(tinyray.NotFound):
            pool.slot(1)
        with pytest.raises(tinyray.NotFound):
            pool.pick(tag="replacement")
        assert [h.id for h in pool.all()] == [902]
    finally:
        replacement.leave()


def test_unknown_and_restarted_pools_drop_every_warm_index(registry):
    with tinyray.join("observer", coalesce_ms=0):
        empty = tinyray.pool("unknown")
        assert empty.all() == []
        for lookup in [empty.pick, lambda: empty.slot(0)]:
            with pytest.raises(tinyray.NotFound):
                lookup()
        with httpx.Client(trust_env=False) as wire:
            response = wire.post(
                f"http://{registry.endpoint}/v1/beat",
                json={
                    "pool": "fast",
                    "id": 997,
                    "slot": 2,
                    "incarnation": 1,
                    "policy": "stateful",
                    "size": 3,
                    "state": {"tag": "old"},
                    "ready": True,
                    "watch": [],
                    "seen": {},
                },
            )
            response.raise_for_status()
            assert response.json()["accepted"]
        pool = tinyray.pool("fast")
        assert pool.wait(timeout=5)[0].id == 997
        assert pool.slot(2).id == 997
        assert pool.pick().id == 997
        pool.snapshot()
        pool._c.field_digest("fast", ["tag"])
        registry.stop()
        registry.start()
        pool.until(lambda snap: len(snap) == 0, timeout=10)
        assert pool.all() == []
        assert pool._c.frozen("fast", False)[1:3] == (0, 0)
        with pytest.raises(tinyray.NotFound):
            pool.slot(2)
        with pytest.raises(tinyray.NotFound):
            pool.pick()


def test_many_draws_choose_only_live_eligible_members(fleet):
    _, pool, peers = fleet
    counts = {703: 0, 902: 0}
    for _ in range(2000):
        counts[pool.pick().id] += 1
    assert all(800 < count < 1200 for count in counts.values()), counts
    peers[2].leave()
    pool.until(lambda snap: snap.slot(2) is None, timeout=5)
    assert {pool.pick().id for _ in range(100)} == {703}


def _forked_choices(sender):
    try:
        with tinyray.join(f"picker-child-{os.getpid()}", coalesce_ms=0, timeout=5):
            pool = tinyray.pool("fast")
            pool.wait(count=2, timeout=5)
            sender.send([pool.pick().id for _ in range(64)])
    except BaseException as exc:
        sender.send({"error": repr(exc)})
    finally:
        sender.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
def test_new_clients_after_fork_do_not_copy_the_parents_choice_stream(fleet):
    _, pool, _ = fleet
    pool.pick()  # Initialize any thread-local RNG before both forks.
    context = multiprocessing.get_context("fork")
    children = []
    try:
        for _ in range(2):
            receiver, sender = context.Pipe(duplex=False)
            child = context.Process(target=_forked_choices, args=(sender,))
            child.start()
            sender.close()
            children.append((child, receiver))
        deadline = time.monotonic() + 15
        streams = []
        for child, receiver in children:
            assert receiver.poll(max(0, deadline - time.monotonic())), "forked client stalled"
            stream = receiver.recv()
            assert isinstance(stream, list), stream
            assert len(stream) == 64 and set(stream) <= {703, 902}
            streams.append(stream)
            child.join(timeout=max(0, deadline - time.monotonic()))
            assert child.exitcode == 0
        assert streams[0] != streams[1], "both children inherited the same native choice stream"
    finally:
        for child, receiver in children:
            if child.is_alive():
                child.kill()
            child.join(timeout=5)
            receiver.close()
