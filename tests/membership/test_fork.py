"""Membership and transport ownership across fork()."""

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


FORK_THEN_BOTH_CALL = textwrap.dedent(
    """
    import asyncio, os, sys, tinyray
    from tinyray import _rpc

    class S:
        def echo(self, x): return x

    srv = tinyray.join("forksock", "stateful", slot=0, size=1, serves=S())
    srv.ready()
    loop = asyncio.new_event_loop()

    async def hammer(tag, n):
        h = tinyray.apool("forksock").slot(0)
        bad = []
        for i in range(n):
            want = f"{tag}{i}"
            try:
                got = await h.echo(want)
                if got != want:
                    bad.append(f"串号 想要{want!r} 拿到{got!r}")
            except Exception as e:
                bad.append(f"{type(e).__name__}: {e}")
        return bad

    loop.run_until_complete(hammer("warm", 3))   # 让连接池装上父进程的 socket

    go_r, go_w = os.pipe()
    res_r, res_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(go_r); os.close(res_r)
        try:
            carried = len(_rpc._loops)    # fork 之后第一件事：手里还攥着几个？
            kid = tinyray.join("forkcli", "churn")
            kid.ready()
            os.write(go_w, b"x")          # 加入完了再一起开打，让重叠最大
            bad = loop.run_until_complete(hammer("C", 400))   # 沿用同一个 loop
            os.write(res_w, f"CHILD carried={carried} {len(bad)} {bad[:2]}".encode()[:400])
        except BaseException as e:
            os.write(res_w, f"CHILD-ERR {type(e).__name__}: {e}".encode()[:400])
        os._exit(0)
    os.close(go_w); os.close(res_w)
    os.read(go_r, 1)
    mine = loop.run_until_complete(hammer("P", 400))
    kid_says = os.read(res_r, 4000).decode()
    os.waitpid(pid, 0)
    print(f"PARENT {len(mine)} {mine[:2]} | {kid_says}", flush=True)
    """
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork()")
def test_a_forked_child_does_not_talk_down_the_parents_sockets(registry):
    """连接池是按事件循环存的，而 fork 把循环连同它的 socket 一起复制了。

    子进程如果继续用 fork 之前那个 loop，拿到的就是**父进程那一个** httpx
    client —— 同一批已经建立好的 TCP 连接。于是两个进程往同一条连接里写请求。

    实测能坏成什么样：请求被拼接串了、服务端回 `HTTP 400`（而且是
    `OutcomeUnknown`，这次调用可能已经执行了），或者事件循环拒绝这个 socket 报
    `FileExistsError: [Errno 17]` —— 两个进程往同一个 epoll 注册同一个 fd。

    但**断言的是机制不是损坏**：把修复撤掉，即使父子加了握手同时开打，400 次
    调用也只有 1/5 的轮次真的串起来 —— 拿这个当唯一信号就是条会飘的测试。
    子进程 fork 之后手里攥着几个连接池是确定的：0，撤掉修复就是 1。
    （同步那条路上串号是 5/5 稳定复现的，危害由它去证。）

    继承来的管道早就是这么处理的：丢掉但不关闭，描述符还是父进程的。
    """
    p = subprocess.Popen(
        [sys.executable, "-c", FORK_THEN_BOTH_CALL],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        out, err = p.communicate(timeout=90)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        out, err = p.communicate(timeout=10)
        raise AssertionError(f"fork 之后父子互相调用挂住了 stderr={err[-400:]!r}") from None
    assert "CHILD carried=0 0 []" in out and "PARENT 0 []" in out, (
        f"父子共用了一条连接：stdout={out!r} stderr={err[-600:]!r}"
    )


FORK_THEN_BOTH_CALL_SYNC = textwrap.dedent(
    """
    import os, sys, tinyray
    from tinyray import _rpc

    class S:
        def echo(self, x): return x

    srv = tinyray.join("forksync", "stateful", slot=0, size=1, serves=S())
    srv.ready()

    def hammer(tag, n):
        h = tinyray.pool("forksync").slot(0)
        bad = []
        for i in range(n):
            want = f"{tag}{i}"
            try:
                got = h.echo(want)
                if got != want:
                    bad.append(f"串号 想要{want!r} 拿到{got!r}")
            except Exception as e:
                bad.append(f"{type(e).__name__}: {e}")
        return bad

    hammer("warm", 3)   # 让共用的那个 client 装上父进程的 socket

    go_r, go_w = os.pipe()
    res_r, res_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(go_r); os.close(res_r)
        try:
            carried = _rpc._sync is not None   # fork 之后还攥着父进程那个吗？
            kid = tinyray.join("forksynccli", "churn")
            kid.ready()
            os.write(go_w, b"x")          # 加入完了再一起开打，让重叠最大
            bad = hammer("C", 400)
            os.write(res_w, f"CHILD carried={carried} {len(bad)} {bad[:2]}".encode()[:400])
        except BaseException as e:
            os.write(res_w, f"CHILD-ERR {type(e).__name__}: {e}".encode()[:400])
        os._exit(0)
    os.close(go_w); os.close(res_w)
    os.read(go_r, 1)
    mine = hammer("P", 400)
    kid_says = os.read(res_r, 4000).decode()
    os.waitpid(pid, 0)
    print(f"PARENT {len(mine)} {mine[:2]} | {kid_says}", flush=True)
    """
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork()")
def test_a_forked_child_does_not_share_the_synchronous_connection(registry):
    """同步这条路上，共用连接不是报错，是**拿回别人的答案**。

    同步调用共用一个 httpx.Client —— 一个连接池，几条长连接。fork 把它连同
    socket 一起复制过去，于是父子在同一条 keep-alive 连接上轮流发请求，应答就
    按到达顺序发错了人。

    实测父子各调 300 次，三轮里有两轮串号，而且是成对的：父进程 `want 'P118'`
    拿到 `'C0'`，同一时刻子进程 `want 'C0'` 拿到 `'P118'`。**没有异常、没有
    警告**，RPC 就是返回了另一个进程的结果 —— 比异步那条路上的 HTTP 400 更坏，
    那边至少还会抛出来。

    异步的连接池和继承来的管道都已经这么丢掉了。共用的这个是最后一个。

    串号本身是竞态：单独跑 8/8 都能复现，但放进整轮变异检查里会飘过一次。所以
    **确定性的主张是机制** —— 子进程 fork 之后手里不该还攥着那个 client；串号
    作为它为什么要紧的证据留在这里一起断言。
    """
    p = subprocess.Popen(
        [sys.executable, "-c", FORK_THEN_BOTH_CALL_SYNC],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        out, err = p.communicate(timeout=90)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        out, err = p.communicate(timeout=10)
        raise AssertionError(f"fork 之后父子同步互调挂住了 stderr={err[-400:]!r}") from None
    assert "CHILD carried=False" in out, (
        f"子进程带着父进程那个 client 过了 fork：stdout={out!r} stderr={err[-600:]!r}"
    )
    assert "PARENT 0 []" in out and "CHILD carried=False 0 []" in out, (
        f"父子共用了同一条同步连接：stdout={out!r} stderr={err[-600:]!r}"
    )


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork()")
def test_a_forked_child_exiting_normally_says_nothing(registry):
    """子进程什么都没做错的时候，就不该在 stderr 上留下东西。

    `join()` 把告别交给 atexit，好让正常退出的进程当场腾出座位（实测 0.06s，
    对比被 SIGKILL 的 3.15s 走租约）。可 fork 把退出钩子连同别的一切复制过去，
    于是子进程退出时也会去跑它 —— 而子进程**不该**替父进程说再见。

    `leave()` 拒绝得没错，但它是靠抛异常拒绝的，atexit 会把这个异常打出来：
    **7 行 traceback**，而子进程什么都没做错。

    比听起来窄，记在这里是因为我一开始说反了。它需要**手写的 `os.fork()`**，
    而且子进程走正常的解释器收尾：实测 `sys.exit(0)` 7 行，跑到脚本末尾也 7 行；
    `os._exit()` 跳过 atexit，0 行。而 multiprocessing 用的正是 `os._exit()`，
    所以 `Pool` 和 `DataLoader` 根本碰不到 —— 修复前实测 5 轮 × 4 个 worker，
    stderr 0 行。

    钩子只该对注册它的那个进程有效。
    """
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
        out, err = p.communicate(timeout=10)
        raise AssertionError("fork 之后的子进程退出时挂住了") from None

    assert "CHILD_EXITED 0" in out, f"stdout={out!r}"
    assert err == "", (
        f"子进程正常退出却在 stderr 上留下了 {len(err.splitlines())} 行：{err[:400]!r}"
    )
