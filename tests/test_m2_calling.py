"""M2 acceptance: a plain object becomes callable across processes."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import time
import urllib.request

import pytest
import tinyray

SERVER = textwrap.dedent(
    """
    import sys, time, tinyray

    class Collector:
        def assign(self, task, priority=0):
            return {"took": task, "priority": priority}
        def add(self, a, b):
            return a + b
        def boom(self):
            raise ValueError("rubric failed")
        def slow(self, seconds):
            time.sleep(seconds)
            return "done"
        def echo_big(self, blob):
            return len(blob)
        def _private(self):
            return "should not be reachable"

    me = tinyray.join("collector", "stateful", slot=int(sys.argv[1]), serves=Collector())
    me.ready()
    print("READY", flush=True)
    sys.stdin.readline()
    """
)


class Peer:
    def __init__(self, slot: int):
        self.proc = subprocess.Popen(
            [sys.executable, "-c", SERVER, str(slot)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert self.proc.stdout.readline().strip() == "READY"

    def stop(self):
        try:
            self.proc.stdin.write("\n")
            self.proc.stdin.flush()
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()


@pytest.fixture
def peer(registry):
    me = tinyray.join("driver", "churn")
    me.ready()
    p = Peer(0)
    tinyray.pool("collector").wait(count=1, timeout=10)
    try:
        yield tinyray.pool("collector").slot(0)
    finally:
        p.stop()
        me.leave()


def test_a_method_call_crosses_the_process_boundary(peer):
    assert peer.assign("task-7") == {"took": "task-7", "priority": 0}
    assert peer.assign(task="task-8", priority=3) == {"took": "task-8", "priority": 3}
    assert peer.add(2, 3) == 5


def test_typos_fail_at_the_handle_not_at_runtime(peer):
    # The pool publishes its method list, so hasattr tells the truth. An
    # earlier design proxied every attribute and hasattr was always True.
    assert hasattr(peer, "assign")
    assert not hasattr(peer, "assgin")
    assert not hasattr(peer, "_private")
    with pytest.raises(AttributeError, match="assgin"):
        peer.assgin("x")


def test_business_failures_stay_business_failures(peer):
    with pytest.raises(tinyray.RemoteError) as e:
        peer.boom()
    assert e.value.type == "ValueError"
    assert e.value.message == "rubric failed"
    assert "rubric failed" in e.value.traceback
    # Not Unreachable: it arrived. Retrying it is the application's call.
    assert not isinstance(e.value, tinyray.Unreachable)


def test_a_dead_callee_is_unreachable_not_a_remote_error(peer, registry):
    peer_url = peer.url
    dead = tinyray.Handle(
        "collector",
        {"id": 0, "slot": 0, "incarnation": peer.incarnation, "url": peer_url, "ready": True},
        ("assign",),
    )
    dead.url = peer_url.rsplit(":", 1)[0] + ":1"  # nothing listens there
    with pytest.raises(tinyray.Unreachable):
        dead.assign("x")


def test_stale_tenure_is_fenced_not_silently_accepted(peer):
    stale = tinyray.Handle(
        "collector",
        {
            "id": 0,
            "slot": 0,
            "incarnation": peer.incarnation - 1,  # the previous occupant
            "url": peer.url,
            "ready": True,
        },
        ("assign",),
    )
    with pytest.raises(tinyray.Fenced):
        stale.assign("x")


def test_the_byte_budget_is_enforced_not_documented(peer):
    with pytest.raises(ValueError, match="over the"):
        peer.echo_big("x" * (1 << 21))
    assert peer.echo_big("x" * 1000) == 1000


def test_timeout_is_a_modifier_so_it_cannot_collide(peer):
    assert peer.slow(0.05) == "done"
    t0 = time.monotonic()
    with pytest.raises(tinyray.Unreachable):
        peer.slow.timeout(0.2)(3.0)
    assert time.monotonic() - t0 < 2.0
    # The modifier is per-call, not sticky.
    assert peer.slow(0.05) == "done"


def test_it_is_still_plain_http(peer):
    req = urllib.request.Request(
        f"{peer.url}/call/add",
        data=json.dumps({"a": 4, "b": 5}).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        assert json.loads(r.read())["result"] == 9

    with urllib.request.urlopen(f"{peer.url}/_methods", timeout=5) as r:
        body = json.loads(r.read())
    assert "assign" in body["methods"]
    assert "_private" not in body["methods"]


def test_a_member_without_serves_advertises_nothing(registry):
    me = tinyray.join("quiet", "churn")
    me.ready()
    h = tinyray.pool("quiet").wait(count=1, timeout=10)[0]
    assert h.url is None
    with pytest.raises(AttributeError, match="no methods"):
        h.anything()
    me.leave()
