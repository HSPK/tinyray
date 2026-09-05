"""RPC payload budgets and warning attribution."""

import asyncio
import os
import subprocess
import sys
import textwrap
import warnings

import pytest
import tinyray

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
