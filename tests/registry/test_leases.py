"""TTL 太短时，租约在两次心跳之间就过期了。"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import time
import urllib.request

import pytest
import tinyray

from tests.support.ordering_proxy import OrderingProxy
from tests.support.registry import BIN, RegistryProc, free_port

MEMBER = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join("a", "churn") as m:
        m.ready(); print("READY", flush=True); sys.stdin.readline()
    """
)


@pytest.mark.parametrize("ttl", [0, 40, 199])
def test_a_ttl_clients_cannot_meet_is_refused(ttl):
    """静默地接受一个没人能满足的租约，比拒绝启动坏得多：
    实测 40ms 时成员只有 20% 时间可见，最长消失 690ms，而心跳全部成功。"""
    p = subprocess.run(
        [str(BIN), "--listen", f"127.0.0.1:{free_port()}", "--ttl-ms", str(ttl)],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert p.returncode != 0, f"--ttl-ms {ttl} 被接受了"
    assert "floor" in (p.stderr + p.stdout), p.stderr
    assert "Traceback" not in p.stderr, "操作员打错参数不该看到 traceback"


def test_the_shortest_allowed_ttl_keeps_a_member_continuously_visible():
    """下限必须真的够用，否则它只是换了个地方骗人。"""
    reg = RegistryProc(ttl_ms=200)
    reg.start()
    import os

    env_before = os.environ.get("TINYRAY_REGISTRY")
    os.environ["TINYRAY_REGISTRY"] = reg.endpoint
    p = subprocess.Popen(
        [sys.executable, "-c", MEMBER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        assert p.stdout.readline().strip() == "READY"
        with tinyray.join("b", "churn") as me:
            me.ready()
            pool = tinyray.pool("a")
            pool.wait(count=1, timeout=10)
            gone = 0
            checks = 0
            t0 = time.monotonic()
            while time.monotonic() - t0 < 4.0:
                checks += 1
                if not pool.all():
                    gone += 1
                time.sleep(0.01)
            assert gone == 0, f"{checks} 次采样里成员消失了 {gone} 次"
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
        reg.stop()
        if env_before is None:
            os.environ.pop("TINYRAY_REGISTRY", None)
        else:
            os.environ["TINYRAY_REGISTRY"] = env_before


@pytest.mark.parametrize("ttl", ["-5", "abc", "12.5", ""])
def test_a_lease_that_is_not_a_length_of_time_is_refused_cleanly(ttl):
    """上面那条测试早就写着"操作员打错参数不该看到 traceback"，但它只试了
    合法的整数。**唯一**真的会 traceback 的那个写法没被试到。

    `--ttl-ms -5` 通过 argparse（`type=int` 收负数），一路送到 pyo3，那里拒绝
    把负数变成 u64，抛 OverflowError —— 它不是 OSError/ValueError/RuntimeError
    中的任何一个，于是从 except 之间漏了出去：

        OverflowError: can't convert negative int to unsigned
    """
    p = subprocess.run(
        [str(BIN), "--listen", f"127.0.0.1:{free_port()}", "--ttl-ms", ttl],
        capture_output=True,
        text=True,
        timeout=15,
    )
    out = p.stderr + p.stdout
    assert p.returncode != 0, f"--ttl-ms {ttl!r} 被接受了"
    assert "Traceback" not in out, out[-400:]
    assert "is not a length of time" in out, out[-400:]


@pytest.mark.parametrize("ttl_ms", [200, 1000])
def test_delayed_downlink_does_not_stop_renewing_a_reachable_registry(ttl_ms):
    reg = RegistryProc(ttl_ms)
    proxy = None
    me = None
    reg.start()
    try:
        proxy = OrderingProxy(reg.endpoint, header_delay=0.35)
        me = tinyray.join("renewal", registry_url=proxy.endpoint, timeout=2)
        me.ready().flush(timeout=2)
        time.sleep(0.3)
        before = me.stats()
        proxy.arm_reply.set()
        assert proxy.reply_held.wait(2)
        started = time.monotonic()
        deadline = started + 0.8
        samples = 0
        while time.monotonic() < deadline:
            with urllib.request.urlopen(f"http://{reg.endpoint}/v1/pools", timeout=2) as r:
                count = json.load(r)[me.pool]["members"]
            assert count == 1, "reply timeout stopped renewals beyond the lease"
            samples += 1
            time.sleep(0.003)
        assert samples > 1
        assert me.stats()["beats_failed"] > before["beats_failed"], "fault was not exercised"
        if ttl_ms == 200:
            with proxy.lock:
                during = [t for t, _ in proxy.requests if started < t < started + 0.3]
            assert during, "upstream must stay usable while replies are delayed"
    finally:
        if me is not None:
            me.leave()
        if proxy is not None:
            proxy.close()
        reg.stop()


def test_headers_and_body_share_one_heartbeat_deadline():
    reg = RegistryProc(1000)
    proxy = None
    me = None
    reg.start()
    try:
        # The 250ms hold plus jitter and 120ms delay fit the 500ms budget.
        # Another 250ms for the body does not; a fresh deadline would accept it.
        proxy = OrderingProxy(reg.endpoint, header_delay=0.12, body_delay=0.25)
        me = tinyray.join("whole_deadline", registry_url=proxy.endpoint, timeout=2)
        me.ready().flush(timeout=2)
        time.sleep(0.3)
        before = me.stats()["beats_failed"]
        proxy.arm_reply.set()
        assert proxy.reply_held.wait(2)
        deadline = time.monotonic() + 1
        while me.stats()["beats_failed"] == before and time.monotonic() < deadline:
            time.sleep(0.002)
        assert me.stats()["beats_failed"] > before, "body collection restarted the budget"
        assert "reply body stalled" in me.last_error
    finally:
        if me is not None:
            me.leave()
        if proxy is not None:
            proxy.close()
        reg.stop()
