"""fork() 和响应体积：两个静默失败，都不会自己喊疼。"""

import os
import signal
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
        def fat_error(self): raise ValueError("y" * (8 << 20))
    with tinyray.join("s", "stateful", slot=0, serves=S()) as me:
        me.ready(); print("READY", flush=True); sys.stdin.readline()
    """
)


def test_a_reply_over_budget_is_warned_about_and_still_delivered(registry):
    """1 MB 是提示线，不是闸门。

    出口方向一度完全不设限，后来改成 413 拒发 —— 但那让一个从 900 KB 长到
    1.1 MB 的返回值把好好的系统直接打断。调用是点对点的，超了只是两端慢，
    不波及第三方，所以这里给警告、照送。

    （注册中心的 state 预算仍然是硬限，那条不一样：state 会复制给每一个订阅者，
    实测 6 MB 到 20 个订阅者变成 120 MB，它保护的是别人。）"""
    p = subprocess.Popen(
        [sys.executable, "-c", FAT], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    try:
        with tinyray.join("c", "churn") as c:
            c.ready()
            h = tinyray.pool("s").wait(count=1, timeout=15)[0]
            assert len(h.small()) == 1024
            with pytest.warns(tinyray.OversizeWarning, match="past the"):
                fat = h.fat()
            assert len(fat) == (4 << 20), "警告归警告，东西还是要送到"
            assert len(h.small()) == 1024, "之后连接必须还能用"

            # 警告必须可以被静默，否则它自己又成了新的脆弱。
            import warnings as _w

            with _w.catch_warnings():
                _w.simplefilter("error", tinyray.OversizeWarning)
                _w.filterwarnings("ignore", category=tinyray.OversizeWarning)
                assert len(h.fat()) == (4 << 20)
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


FORK_THEN_EXIT = textwrap.dedent(
    """
    import os, sys, tinyray
    me = tinyray.join("f2", "churn")
    me.ready()
    pid = os.fork()
    if pid == 0:
        # 正常退出：跑 atexit，也跑解释器收尾。上面那条 fork 测试用的是
        # os._exit(0)，两段都跳过了。
        sys.exit(0)
    _, status = os.waitpid(pid, 0)
    print("CHILD_EXITED", os.waitstatus_to_exitcode(status), flush=True)
    """
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork()")
def test_a_forked_child_that_exits_normally_does_not_hang(registry):
    """fork 只带走调用线程，心跳那两个 tokio 工作线程在子进程里根本不存在。

    子进程正常退出时，解释器收尾会 drop 掉继承来的 runtime，而 drop 要等那些
    工作线程收摊 —— 它们永远不会。实测：子进程永久挂住，faulthandler 打出来的
    栈是 `<no Python frame>`，卡在原生代码里，没有任何东西说明为什么；父进程的
    waitpid 跟着一起挂。DataLoader(num_workers>0) 就是这个形状。

    上面那条 fork 测试用 os._exit(0) 结束子进程，跳过 atexit 和解释器收尾，
    也就正好跳过了会挂的那一段 —— 所以它一直是绿的。
    """
    # 自成进程组：挂住的是**孙**进程，只 kill 父进程的话它还攥着 stdout 管道，
    # 收尾的 communicate() 会跟着一起永久阻塞 —— 写这条测试时先踩了一次。
    p = subprocess.Popen(
        [sys.executable, "-c", FORK_THEN_EXIT],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        out, err = p.communicate(timeout=45)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        try:
            out, err = p.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            out, err = "", ""
        raise AssertionError(
            "fork 之后的子进程正常退出时挂住了：继承来的 runtime 在 drop 时"
            f"等着一批不存在的线程。stdout={out!r} stderr={err[-500:]!r}"
        ) from None
    assert "CHILD_EXITED 0" in out, f"stdout={out!r} stderr={err[-800:]!r}"


def test_an_error_over_budget_arrives_whole(registry):
    """异常和返回值走同一条规则：给警告，不裁剪。

    上一版这里是截断到 256 KB。那是在"出口方向拒发"的前提下为了不把失败原因
    弄丢；既然拒发本身取消了，截断就成了唯一还在动用户数据的地方，规则要一致。
    """
    p = subprocess.Popen(
        [sys.executable, "-c", FAT], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    try:
        with tinyray.join("c", "churn") as c:
            c.ready()
            h = tinyray.pool("s").wait(count=1, timeout=15)[0]
            with pytest.raises(tinyray.RemoteError) as caught:
                h.fat_error()
            exc = caught.value
            assert exc.type == "ValueError"
            assert len(exc.message) == (8 << 20), "异常正文不该被裁"
            assert len(h.small()) == 1024, "之后连接必须还能用"
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
