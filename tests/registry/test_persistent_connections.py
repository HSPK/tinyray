"""Registry heartbeats reuse clean serial connections and discard dirty ones."""

from __future__ import annotations

import time

import tinyray

from tests.support.ordering_proxy import OrderingProxy
from tests.support.registry_wire import health


def _wait_for_beats(client, target: int, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while client.stats()["beats_ok"] < target:
        revision = client.debug_beat_revision()
        left = deadline - time.monotonic()
        assert left > 0, f"only saw {client.stats()['beats_ok']} of {target} beats"
        client.debug_wait_beat_revision(revision, int(left * 1000) + 1)


def test_idle_heartbeats_reuse_one_clean_connection(registry):
    with tinyray.join("persistent", coalesce_ms=0) as me:
        me.ready().flush(timeout=5)
        client = me._c
        deadline = time.monotonic() + 5
        while client.debug_registry_transport()["reuses"] < 1:
            revision = client.debug_beat_revision()
            assert time.monotonic() < deadline
            client.debug_wait_beat_revision(revision, 5000)

        # A publication made during startup can leave one Notify permit that
        # deliberately cancels the first held request. Let that handoff finish
        # before measuring genuinely idle traffic.
        _wait_for_beats(client, client.stats()["beats_ok"] + 2)
        before = client.debug_registry_transport()
        target = client.stats()["beats_ok"] + 4
        _wait_for_beats(client, target)
        after = client.debug_registry_transport()
        assert after["connections"] == before["connections"]
        assert after["reuses"] >= before["reuses"] + 4

        registry_stats = health(registry.endpoint)
        assert registry_stats["frames_received"] > registry_stats["connections_accepted"]


def test_publication_cancels_and_discards_the_held_connection(long_lease):
    proxy = OrderingProxy(long_lease.endpoint)
    try:
        with tinyray.join(
            "cancel-held",
            "stateful",
            slot=0,
            size=1,
            registry_url=proxy.endpoint,
            coalesce_ms=0,
        ) as me:
            me.ready(step=0).flush(timeout=5)
            deadline = time.monotonic() + 5
            while True:
                with proxy.lock:
                    held = any(
                        isinstance(beat, dict) and beat.get("hold_ms", 0) > 0
                        for _, beat in proxy.requests
                    )
                    opened = proxy.opened
                if held:
                    break
                assert time.monotonic() < deadline, "heartbeat never entered a held request"
                revision = me._c.debug_beat_revision()
                me._c.debug_wait_beat_revision(revision, 5000)

            me.update(step=1).flush(timeout=5)
            with proxy.lock:
                assert proxy.opened > opened, "cancelled held request reused its dirty socket"
            assert tinyray.pool(me.pool).slot(0).state["step"] == 1
    finally:
        proxy.close()


def test_registry_restart_reconnects_and_cleanup_releases_the_idle_stream(registry):
    me = tinyray.join("persistent-restart", "stateful", slot=0, size=1, coalesce_ms=0)
    try:
        me.ready(step=0).flush(timeout=5)
        client = me._c
        before = client.debug_registry_transport()["connections"]
        failures = client.stats()["beats_failed"]
        registry.stop()
        deadline = time.monotonic() + 5
        while client.stats()["beats_failed"] == failures:
            revision = client.debug_beat_revision()
            assert time.monotonic() < deadline
            client.debug_wait_beat_revision(revision, 5000)
        registry.start()
        me.update(step=1).flush(timeout=15)
        assert client.debug_registry_transport()["connections"] > before
        assert tinyray.pool(me.pool).slot(0).state["step"] == 1
    finally:
        me.leave()

    deadline = time.monotonic() + 2
    while True:
        stats = health(registry.endpoint)
        # The health request itself is the one active connection represented
        # in its response.
        if stats["connections_active"] == 1:
            break
        assert time.monotonic() < deadline, stats
        time.sleep(0.02)
