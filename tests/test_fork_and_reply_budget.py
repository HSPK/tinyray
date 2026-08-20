"""fork() 和响应体积：两个静默失败，都不会自己喊疼。"""

import os
import subprocess
import sys
import textwrap

import pytest
import tinyray


def test_fork_child_gets_a_clear_error_not_a_frozen_client(registry):
    """fork 只带走调用线程。子进程继承的 client 看着像注册着的，
    实际心跳早没了 —— 这正是 DataLoader(num_workers>0) 的形状。"""
    me = tinyray.join("f", "churn")
    me.ready()
    r, w = os.pipe()
    if os.fork() == 0:
        os.close(r)
        bad = []
        for name, fn in [
            ("pool.all", lambda: tinyray.pool("f").all()),
            ("member.ready", lambda: me.ready()),
            ("member.stats", me.stats),
            ("member.leave", me.leave),
        ]:
            try:
                fn()
                bad.append(name)  # 悄悄成功才是 bug
            except RuntimeError:
                pass
        # 子进程必须能重新开张
        try:
            tinyray.join("f", "churn", slot=1).ready()
        except Exception as e:
            bad.append(f"rejoin:{type(e).__name__}")
        os.write(w, ",".join(bad).encode())
        os._exit(0)
    os.close(w)
    leaked = os.read(r, 4096).decode()
    os.wait()
    assert leaked == "", f"fork 后这些调用悄悄成功了: {leaked}"
    me.leave()


FAT = textwrap.dedent(
    """
    import sys, tinyray
    class S:
        def small(self) -> str: return "x" * 1024
        def fat(self) -> str: return "x" * (4 << 20)
    with tinyray.join("s", "stateful", slot=0, serves=S()) as me:
        me.ready(); print("READY", flush=True); sys.stdin.readline()
    """
)


def test_a_reply_over_budget_is_refused(registry):
    """入口方向一直有 1 MB 预算，出口方向没有 —— 而'顺手多带一点'
    恰恰长在返回值上。"""
    p = subprocess.Popen(
        [sys.executable, "-c", FAT], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    try:
        with tinyray.join("c", "churn") as c:
            c.ready()
            h = tinyray.pool("s").wait(count=1, timeout=15)[0]
            assert len(h.small()) == 1024
            with pytest.raises(ValueError, match="over the .* limit"):
                h.fat()
            assert len(h.small()) == 1024, "拒绝之后连接必须还能用"
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
