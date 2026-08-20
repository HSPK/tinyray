"""TTL 太短时，租约在两次心跳之间就过期了。"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time

import pytest
import tinyray
from conftest import BIN, RegistryProc, free_port

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
