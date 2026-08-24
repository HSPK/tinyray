"""fork() 和响应体积：两个静默失败，都不会自己喊疼。"""

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
import warnings

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


def test_the_oversize_nudge_points_at_the_line_that_made_the_call(registry):
    """告警要指向调用它的那一行，同步异步都一样。

    这条以前没人守。`stacklevel` 是写死的 4：同步方向对（`_nudge`、`invoke`、
    `BoundMethod.__call__`、应用），异步方向错 —— 协程真正跑起来的时候
    `__call__` 早已返回，同样数到 4 就落进了 asyncio 内部。实测指向
    `asyncio/events.py:84`。

    落错地方不只是难看：`warnings` 按 (消息, 类别, 位置) 去重，所有异步的
    超大告警会因为位置相同被折叠成一条，然后被过滤器压掉。
    """
    p = subprocess.Popen(
        [sys.executable, "-c", FAT], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    here = os.path.abspath(__file__)
    try:
        with tinyray.join("c", "churn") as c:
            c.ready()
            tinyray.pool("s").wait(count=1, timeout=15)

            def blamed(caught) -> list[str]:
                return [
                    f"{os.path.basename(w.filename)}:{w.lineno}"
                    for w in caught
                    if issubclass(w.category, tinyray.OversizeWarning)
                ]

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                tinyray.pool("s").slot(0).fat()
            sync_at = [w for w in caught if issubclass(w.category, tinyray.OversizeWarning)]
            assert sync_at, "超大返回值没有发出告警"
            assert sync_at[0].filename == here, f"同步告警指向了 {blamed(caught)}"

            async def call_it() -> None:
                with warnings.catch_warnings(record=True) as inner:
                    warnings.simplefilter("always")
                    await tinyray.apool("s").slot(0).fat()
                found = [w for w in inner if issubclass(w.category, tinyray.OversizeWarning)]
                assert found, "异步方向没有发出告警"
                assert found[0].filename == here, f"异步告警指向了 {blamed(inner)}"

            asyncio.run(call_it())
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


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
            kid = tinyray.join("forksynccli", "churn")
            kid.ready()
            os.write(go_w, b"x")          # 加入完了再一起开打，让重叠最大
            bad = hammer("C", 400)
            os.write(res_w, f"CHILD {len(bad)} {bad[:2]}".encode()[:400])
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
    assert "PARENT 0 []" in out and "CHILD 0 []" in out, (
        f"父子共用了同一条同步连接：stdout={out!r} stderr={err[-600:]!r}"
    )
