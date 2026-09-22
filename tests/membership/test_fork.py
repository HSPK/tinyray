"""Membership and transport ownership across fork()."""

import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time

import msgspec
import pytest
import tinyray
from tinyray import _msgpack

from tests.support.ordering_proxy import OrderingProxy
from tests.support.rpc_wire import frame, recv_frame


def _proc_fd_links(fds=None) -> set[str]:
    links = set()
    names = os.listdir("/proc/self/fd") if fds is None else [str(fd) for fd in fds]
    for name in names:
        try:
            links.add(os.readlink(f"/proc/self/fd/{name}"))
        except FileNotFoundError:
            pass
    return links


@pytest.mark.skipif(
    not hasattr(os, "fork") or not os.path.isdir("/proc/self/fd"),
    reason="needs fork() and procfs descriptor inodes",
)
def test_fork_closes_a_connect_in_progress_rpc_socket_and_parent_continues():
    tinyray._tinyray.rpc_debug_clear_pools()
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(0)
    listener.settimeout(5)
    endpoint = f"127.0.0.1:{listener.getsockname()[1]}"
    filler = socket.create_connection(listener.getsockname(), timeout=2)
    handle = tinyray.Handle(
        "pending-connect",
        {
            "id": 0,
            "slot": 0,
            "incarnation": 1,
            "url": endpoint,
            "ready": True,
        },
        ("ping",),
    )
    result = []

    def call():
        try:
            result.append(handle.ping.timeout(10)())
        except BaseException as exc:
            result.append(exc)

    caller = threading.Thread(target=call)
    caller.start()
    deadline = time.monotonic() + 3
    inherited = set()
    while not inherited:
        fds = tinyray._tinyray.rpc_debug_fds()
        inherited = {link for link in _proc_fd_links(fds) if link.startswith("socket:[")}
        assert time.monotonic() < deadline, "pending connect fd was never registered"
        time.sleep(0.005)
    assert caller.is_alive(), "connect completed before the backlog was saturated"

    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        leaked = sorted(inherited & _proc_fd_links())
        os.write(write_fd, repr(leaked).encode())
        os._exit(0)
    os.close(write_fd)
    child_result = os.read(read_fd, 4096).decode()
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert child_result == "[]", f"child retained pending connect socket(s): {child_result}"
    assert inherited <= _proc_fd_links(), "child cleanup closed the parent's pending socket"

    first, _ = listener.accept()
    first.close()
    connection, _ = listener.accept()
    request = msgspec.msgpack.decode(recv_frame(connection))
    connection.sendall(
        frame(
            {
                "v": 1,
                "id": request["id"],
                "status": "success",
                "body": _msgpack.dumps("pong"),
            }
        )
    )
    caller.join(10)
    assert result == ["pong"]
    connection.close()
    filler.close()
    listener.close()


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
    from tinyray import _tinyray

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

    loop.run_until_complete(hammer("warm", 3))   # 让原生连接池装上父进程的 socket
    parent_generation = _tinyray.rpc_debug_state()["generation"]

    go_r, go_w = os.pipe()
    res_r, res_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(go_r); os.close(res_r)
        try:
            state = _tinyray.rpc_debug_state()
            carried = state["idle_connections"]
            reset = state["generation"] != parent_generation
            kid = tinyray.join("forkcli", "churn")
            kid.ready()
            os.write(go_w, b"x")          # 加入完了再一起开打，让重叠最大
            bad = loop.run_until_complete(hammer("C", 400))   # 沿用同一个 loop
            os.write(
                res_w,
                f"CHILD carried={carried} reset={reset} {len(bad)} {bad[:2]}".encode()[:400],
            )
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
    """The child gets a new native runtime generation and no inherited idle socket."""
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
    assert "CHILD carried=0 reset=True 0 []" in out and "PARENT 0 []" in out, (
        f"父子共用了一条连接：stdout={out!r} stderr={err[-600:]!r}"
    )


FORK_THEN_BOTH_CALL_SYNC = textwrap.dedent(
    """
    import os, sys, tinyray
    from tinyray import _tinyray

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

    hammer("warm", 3)   # 让原生连接池装上父进程的 socket
    parent_generation = _tinyray.rpc_debug_state()["generation"]

    go_r, go_w = os.pipe()
    res_r, res_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(go_r); os.close(res_r)
        try:
            state = _tinyray.rpc_debug_state()
            carried = state["idle_connections"]
            reset = state["generation"] != parent_generation
            kid = tinyray.join("forksynccli", "churn")
            kid.ready()
            os.write(go_w, b"x")          # 加入完了再一起开打，让重叠最大
            bad = hammer("C", 400)
            os.write(
                res_w,
                f"CHILD carried={carried} reset={reset} {len(bad)} {bad[:2]}".encode()[:400],
            )
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
    """The synchronous path also starts from an empty child-native pool."""
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
    assert "CHILD carried=0 reset=True" in out, (
        f"子进程带着父进程连接过了 fork：stdout={out!r} stderr={err[-600:]!r}"
    )
    assert "PARENT 0 []" in out and "CHILD carried=0 reset=True 0 []" in out, (
        f"父子共用了同一条同步连接：stdout={out!r} stderr={err[-600:]!r}"
    )


@pytest.mark.skipif(
    not hasattr(os, "fork") or not os.path.isdir("/proc/self/fd"),
    reason="needs fork() and procfs descriptor inodes",
)
def test_a_forked_child_closes_inherited_registry_sockets_only(registry):
    """The child drops the active beat fd before abandoning its inherited runtime."""

    class Service:
        def echo(self, value: str) -> str:
            return value

    reply_gate = threading.Event()
    proxy = OrderingProxy(registry.endpoint, reply_gate=reply_gate)
    me = tinyray.join(
        "forkbeat",
        "stateful",
        slot=0,
        size=1,
        serves=Service(),
        registry_url=proxy.endpoint,
    )
    try:
        me.ready().flush()
        proxy.arm_reply.set()
        me.ready(fork_probe=1)
        assert proxy.reply_held.wait(5), "no heartbeat was held across fork"

        fds = me._c.debug_registry_fds()
        inherited = _proc_fd_links(fds)
        inherited = {link for link in inherited if link.startswith("socket:[")}
        assert inherited, f"the active registry socket was not tracked: fds={fds}"

        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            child_links = _proc_fd_links()
            leaked = sorted(inherited & child_links)
            os.write(write_fd, repr(leaked).encode())
            os._exit(0)

        os.close(write_fd)
        child_result = os.read(read_fd, 4096).decode()
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert child_result == "[]", f"child retained registry socket inode(s): {child_result}"

        parent_links = _proc_fd_links()
        assert inherited <= parent_links, "closing child descriptors damaged the parent socket"
        reply_gate.set()
        me.flush(timeout=10)
        assert tinyray.pool("forkbeat").slot(0).echo("parent") == "parent"
    finally:
        reply_gate.set()
        me.leave()
        proxy.close()


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
