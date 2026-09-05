"""Publication ordering across cancellation and metadata changes."""

from __future__ import annotations

import json
import time
import urllib.request

import pytest
import tinyray

from tests.support.ordering_proxy import OrderingProxy
from tests.support.registry import RegistryProc


def _member(registry: RegistryProc, pool: str) -> dict:
    request = urllib.request.Request(
        f"http://{registry.endpoint}/v1/beat",
        data=json.dumps(
            {
                "pool": "__native_review_observer",
                "id": 999,
                "incarnation": 1,
                "policy": "churn",
                "watch": [pool],
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=2) as response:
        ack = json.load(response)
    return ack["pools"][pool]["changed"][0]


def test_canceled_startup_cannot_undo_a_flushed_publication(registry):
    for attempt in range(12):
        proxy = OrderingProxy(registry.endpoint, hold_startup=True)
        me = None
        try:
            me = tinyray.join(f"ordered_{attempt}", registry_url=proxy.endpoint, timeout=2)
            if not proxy.canceled.wait(0.15):
                # The loop won the delayed connection this time, rather than
                # the one-shot that join cancels when the loop registers.
                continue
            assert proxy.startup is not None
            assert proxy.startup["state"] == {} and not proxy.startup["ready"]
            me.ready(value="new").flush(timeout=2)
            versions = me._c.publish_versions()
            assert versions == (1, 1)
            before = _member(registry, me.pool)
            assert before["state"] == {"value": "new"} and before["ready"]

            proxy.release.set()
            assert proxy.forwarded.wait(1), "the old body never reached the registry"
            deadline = time.monotonic() + 0.2
            samples = 0
            while time.monotonic() < deadline:
                observed = _member(registry, me.pool)
                assert observed["state"] == {"value": "new"} and observed["ready"], (
                    f"canceled startup rolled back a flushed publication: {observed}"
                )
                samples += 1
                time.sleep(0.002)
            assert samples > 1
            assert proxy.reset_forwarded.wait(1)
            assert me._c.publish_versions() == versions
            assert proxy.startup["publication"] == 0
            return
        finally:
            proxy.release.set()
            if me is not None:
                me.leave()
            proxy.close()
    pytest.fail("could not exercise a canceled one-shot startup request")


def test_url_changes_advance_the_publication_sequence(registry):
    with tinyray.join("url_sequence") as me:
        before = me._c.publish_versions()[0]
        me._c.set_url("http://127.0.0.1:12345")
        me.flush(timeout=2)
        assert me._c.publish_versions() == (before + 1, before + 1)
        assert _member(registry, me.pool)["url"] == "http://127.0.0.1:12345"
        me._c.set_url("http://127.0.0.1:12345")
        assert me._c.publish_versions() == (before + 1, before + 1)
