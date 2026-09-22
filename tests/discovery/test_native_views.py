"""Native immutable roster views keep Python compatibility without eager Handles."""

from __future__ import annotations

import asyncio
import os
import time

import pytest
import tinyray
from tinyray._tinyray import Client

from tests.support.faulty_net import FaultyProxy


class CountingHandle(tinyray.Handle):
    built = 0
    state_reads = 0

    @classmethod
    def _from_native(cls, *args):
        cls.built += 1
        return super()._from_native(*args)

    def __getattr__(self, name):
        if name == "state":
            type(self).state_reads += 1
        return super().__getattr__(name)


def test_native_handles_keep_fields_and_state_lazy_while_proxying_methods(registry):
    class Service:
        def echo(self, value):
            return value

    with tinyray.join(
        "lazy-handle",
        "stateful",
        slot=0,
        size=1,
        serves=Service(),
    ) as me:
        me.ready(step=1, nested={"values": [1]}).flush()
        pool = tinyray.pool(me.pool)
        handle = pool.slot(0)

        assert handle._native is not None
        assert getattr(handle, "_state", tinyray._STATE_UNMATERIALIZED) is (
            tinyray._STATE_UNMATERIALIZED
        )
        assert (handle.pool, handle.id, handle.slot, handle.incarnation, handle.ready) == (
            me.pool,
            0,
            0,
            me.incarnation,
            True,
        )
        assert handle.identity == me.identity
        assert handle.label.startswith(f"{me.pool}/0#")
        assert repr(handle).startswith("<Handle ")
        assert hash(handle) == hash(pool.slot(0))
        assert handle == pool.slot(0)
        assert handle.echo("ok") == "ok"
        assert getattr(handle, "_state", tinyray._STATE_UNMATERIALIZED) is (
            tinyray._STATE_UNMATERIALIZED
        )

        state = handle.state
        assert state == {"step": 1, "nested": {"values": [1]}}
        assert handle.state is state
        state["nested"]["values"].append(99)
        assert pool.slot(0).state == {"step": 1, "nested": {"values": [1]}}

        original_url = handle.url
        handle.url = "127.0.0.1:1"
        assert handle.url == "127.0.0.1:1"
        handle.url = original_url
        assert handle.echo("again") == "again"

        async def async_check():
            async_handle = tinyray.apool(me.pool).slot(0)
            assert isinstance(async_handle, tinyray.AsyncHandle)
            assert async_handle._native is not None
            assert getattr(async_handle, "_state", tinyray._STATE_UNMATERIALIZED) is (
                tinyray._STATE_UNMATERIALIZED
            )
            assert await async_handle.echo("async") == "async"
            assert getattr(async_handle, "_state", tinyray._STATE_UNMATERIALIZED) is (
                tinyray._STATE_UNMATERIALIZED
            )

        asyncio.run(async_check())


def test_snapshot_and_epoch_materialize_handles_only_when_requested(registry):
    with tinyray.join("native-view", "collective", slot=0, size=1) as me:
        me.ready(step=1).flush()
        pool = tinyray.Pool(me.pool, me._c)
        pool._handle_cls = CountingHandle
        CountingHandle.built = 0
        CountingHandle.state_reads = 0

        snapshot = pool.snapshot()
        assert CountingHandle.built == 0
        assert len(snapshot) == 1
        assert "members=1" in repr(snapshot)
        assert len(pool) == 1
        assert CountingHandle.built == 0

        selected = snapshot.slot(0)
        state = selected.state
        assert state == {"step": 1}
        assert selected.state is state
        assert CountingHandle.state_reads == 1
        assert snapshot.get(me.identity).identity == me.identity
        assert [handle.identity for handle in snapshot.ready()] == [me.identity]
        assert CountingHandle.built == 3

        members = snapshot.members
        assert isinstance(members, tuple)
        assert snapshot.members is members
        assert tuple(snapshot) == members
        assert getattr(members[0], "_state", tinyray._STATE_UNMATERIALIZED) is (
            tinyray._STATE_UNMATERIALIZED
        )
        assert CountingHandle.built == 4

        epoch = pool.epoch(timeout=5)
        assert CountingHandle.built == 4
        assert len(epoch) == 1 and epoch.valid
        assert "members=1" in repr(epoch)
        assert CountingHandle.built == 4
        assert epoch.slot(0).identity == me.identity
        assert CountingHandle.built == 5
        epoch_members = epoch.members
        assert isinstance(epoch_members, tuple)
        assert epoch.members is epoch_members
        assert tuple(epoch) == epoch_members
        assert getattr(epoch_members[0], "_state", tinyray._STATE_UNMATERIALIZED) is (
            tinyray._STATE_UNMATERIALIZED
        )
        assert CountingHandle.built == 6


def test_roster_paths_use_native_refs_without_eager_state(registry):
    with tinyray.join("no-roster-codec", "collective", slot=0, size=1) as me:
        me.ready(role="trainer").flush()
        pool = tinyray.pool(me.pool)
        handles = [
            pool.all(role="trainer")[0],
            pool.snapshot().members[0],
            pool.wait(count=1, timeout=1, role="trainer")[0],
            pool.epoch(timeout=1).members[0],
        ]
        assert all(handle.identity == me.identity for handle in handles)
        assert all(handle._native is not None for handle in handles)
        assert all(
            getattr(handle, "_state", tinyray._STATE_UNMATERIALIZED)
            is tinyray._STATE_UNMATERIALIZED
            for handle in handles
        )
        assert all(handle.state == {"role": "trainer"} for handle in handles)


def test_native_views_remain_frozen_and_state_copies_stay_isolated(registry):
    with tinyray.join("frozen-native", "stateful", slot=0, size=1) as me:
        me.ready(step=1, nested={"values": [1]}).flush()
        pool = tinyray.pool(me.pool)
        before = pool.snapshot()
        selected = before.slot(0)
        selected.state["nested"]["values"].append(99)

        me.ready(step=2, nested={"values": [2]}).flush()
        after = pool.until(lambda snap: snap.slot(0).state["step"] == 2, timeout=5)

        assert len(before) == len(after) == 1
        assert after.revision > before.revision
        assert before.members[0].state == {"step": 1, "nested": {"values": [1]}}
        assert after.members[0].state == {"step": 2, "nested": {"values": [2]}}
        assert pool.snapshot().slot(0).state == {"step": 2, "nested": {"values": [2]}}


def test_native_views_remain_frozen_across_a_registry_restart(registry):
    with tinyray.join("restart-view", "stateful", slot=0, size=1) as me:
        me.ready(step=1).flush()
        pool = tinyray.pool(me.pool)
        before = pool.snapshot()

        registry.stop()
        me.update(step=2)
        registry.start()
        me.flush(timeout=10)
        after = pool.until(lambda snap: snap.slot(0).state["step"] == 2, timeout=10)

        assert before.slot(0).state == {"step": 1}
        assert before.members[0].state == {"step": 1}
        assert after.slot(0).state == {"step": 2}


def test_built_in_waits_bypass_python_predicate_and_roster_loops(registry, monkeypatch):
    with tinyray.join("native-wait", "churn") as me:
        me.ready(role="ready").flush()
        pool = tinyray.pool(me.pool)
        async_pool = tinyray.apool(me.pool)

        def no_until(*_args, **_kwargs):
            pytest.fail("a built-in wait fell back to the Python predicate loop")

        monkeypatch.setattr(tinyray.Pool, "until", no_until)
        monkeypatch.setattr(tinyray.AsyncPool, "auntil", no_until)
        monkeypatch.setattr(tinyray.Pool, "_members", no_until)

        assert pool.wait(count=1, timeout=1, role="ready")[0].identity == me.identity
        with pytest.raises(TimeoutError):
            pool.wait(count=2, timeout=0.05, role="ready")

        async def ready():
            found = await async_pool.await_ready(count=1, timeout=1, role="ready")
            with pytest.raises(TimeoutError):
                await async_pool.await_ready(count=2, timeout=0.05, role="ready")
            return found

        assert asyncio.run(ready())[0].identity == me.identity


def test_native_wait_thresholds_preserve_arbitrary_python_integers(registry):
    with tinyray.join("large-threshold", "collective", slot=0, size=1) as me:
        me.ready().flush()
        pool = tinyray.pool(me.pool)
        assert len(pool.wait(count=-(10**100), timeout=1)) == 1
        with pytest.raises(TimeoutError):
            pool.wait(count=10**100, timeout=0.05)
        assert len(pool.epoch(min=-(10**100), timeout=1)) == 1
        with pytest.raises(TimeoutError):
            pool.epoch(min=10**100, timeout=0.05)


def test_departure_wait_does_not_treat_an_unseen_pool_as_empty(registry):
    with tinyray.join("departure-observer", "churn", coalesce_ms=0):
        peer = Client(
            endpoint=registry.endpoint,
            pool="cold-departure",
            id=0,
            incarnation=1,
            policy="stateful",
            slot=0,
            size=1,
            coalesce_ms=0,
        )
        try:
            assert peer.start() and peer.accepted
            pool = tinyray.pool("cold-departure")
            assert not pool.wait_departure("cold-departure/0#1", timeout=0.2)
            peer.leave()
            assert pool.wait_departure("cold-departure/0#1", timeout=5)
        finally:
            peer.leave()


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_replacement_timeout_includes_the_first_pool_answer(registry, flavour):
    proxy = FaultyProxy(registry.endpoint)
    try:
        with tinyray.join(
            f"replacement-budget-{flavour}",
            "churn",
            registry_url=proxy.endpoint,
            coalesce_ms=0,
        ):
            proxy.reset_rate = 1.0
            started = time.monotonic()
            if flavour == "sync":
                result = tinyray.pool("unseen-replacement").wait_replacement(slot=0, timeout=0.1)
            else:

                async def wait():
                    return await tinyray.apool("unseen-replacement").await_replacement(
                        slot=0, timeout=0.1
                    )

                result = asyncio.run(wait())
            elapsed = time.monotonic() - started
            assert result is None
            assert elapsed < 0.5, f"the first pool answer escaped the budget: {elapsed:.2f}s"
    finally:
        proxy.close()


def test_snapshot_survives_leave_without_retaining_a_live_client(registry):
    me = tinyray.join("detached-view", "stateful", slot=0, size=1)
    me.ready(step=1).flush()
    snapshot = tinyray.pool(me.pool).snapshot()
    me.leave()
    assert len(snapshot) == 1
    assert snapshot.slot(0).state == {"step": 1}
    assert snapshot.members[0].state == {"step": 1}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
def test_forked_children_can_read_an_already_frozen_native_view(registry):
    with tinyray.join("forked-view", "stateful", slot=0, size=1) as me:
        me.ready(step=1).flush()
        snapshot = tinyray.pool(me.pool).snapshot()
        handle = snapshot.members[0]
        assert getattr(handle, "_state", tinyray._STATE_UNMATERIALIZED) is (
            tinyray._STATE_UNMATERIALIZED
        )
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            try:
                result = (len(snapshot), handle.state, handle.identity)
                os.write(write_fd, repr(result).encode())
            finally:
                os._exit(0)
        os.close(write_fd)
        result = os.read(read_fd, 4096).decode()
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert result == repr((1, {"step": 1}, me.identity))
