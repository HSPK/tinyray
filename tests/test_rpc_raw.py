"""方法服务端是手写的 HTTP：直接对它发原始报文，绕过 SDK。"""

from __future__ import annotations

import socket
import subprocess
import sys
import textwrap
import time

import pytest
import tinyray
from tinyray import _rpc

SERVER = textwrap.dedent(
    """
    import sys, threading, tinyray
    class S:
        def ping(self) -> str: return "pong"
        def threads(self) -> int: return threading.active_count()
        def echo(self, x: int) -> int: return x
        def boom(self): raise ValueError("expected")
        def unserializable(self): return object()
        def selfref(self):
            d = {}; d["me"] = d; return d
    m = tinyray.join("s", "stateful", slot=0, size=1, serves=S())
    m.ready()
    print(m._server.port, flush=True)
    sys.stdin.readline()
    """
)


@pytest.fixture
def served(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    port = int(p.stdout.readline().strip())
    try:
        yield port
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


def _raw(port: int, head: bytes, body: bytes = b"", timeout: float = 20.0) -> bytes:
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    try:
        s.sendall(head + body)
        return s.recv(8192)
    finally:
        s.close()


def _post(path: bytes, cl: bytes) -> bytes:
    return b"POST " + path + b" HTTP/1.1\r\nHost: x\r\ncontent-length: " + cl + b"\r\n\r\n"


@pytest.mark.parametrize(
    "cl,expect",
    [(b"abc", b"400"), (b"-5", b"400"), (b"2", b"200")],
)
def test_a_malformed_content_length_gets_an_answer(served, cl, expect):
    """处理线程死掉时调用方看到的是连接被重置 —— 那读起来像"对端挂了"。"""
    got = _raw(served, _post(b"/call/ping", cl), b"{}")
    assert got.split(b"\r\n")[0].split()[1] == expect, got[:80]


def test_a_body_that_never_arrives_releases_the_thread(served):
    """声明 body 却不发，实测能占住 200 个线程直到攻击方自己松手。"""
    before = tinyray_call(served, b"/call/threads")
    holders = [socket.create_connection(("127.0.0.1", served), timeout=30) for _ in range(20)]
    for s in holders:
        s.sendall(_post(b"/call/ping", b"999999"))
    time.sleep(0.5)
    during = tinyray_call(served, b"/call/threads")
    assert during >= before + 15, f"线程没有被占住，测试本身失效了: {before} -> {during}"
    holders[0].settimeout(40)
    got = holders[0].recv(4096)
    assert b"408" in got.split(b"\r\n")[0], got[:80]
    for s in holders:
        s.close()


def tinyray_call(port: int, path: bytes) -> int:
    import json

    raw = _raw(port, _post(path, b"2"), b"{}")
    return json.loads(raw.split(b"\r\n\r\n")[-1])["result"]


@pytest.mark.parametrize("method", ["unserializable", "selfref"])
def test_a_return_value_json_cannot_carry_is_reported_not_dropped(served, method):
    """否则调用方拿到的是 Unreachable，指向错误的方向。"""
    with tinyray.join("c", "churn") as me:
        me.ready()
        h = tinyray.pool("s").wait(count=1, timeout=15)[0]
        with pytest.raises(tinyray.RemoteError, match="cannot be sent as JSON"):
            getattr(h, method)()
        assert h.ping() == "pong", "报错之后连接必须还能用"


STALE_SRV = textwrap.dedent(
    """
    import sys, tinyray
    class S:
        def who(self) -> str: return "ok"
    with tinyray.join("s", "stateful", slot=0, size=1, serves=S()) as m:
        m.ready(); print(m._server.port, flush=True); sys.stdin.readline()
    """
)


def test_a_fenced_call_does_not_poison_the_next_one(registry):
    """提前回复而不读掉请求体，会让 keep-alive 的下一个请求从残字节开始解析。

    实测表现为完美交替：过期句柄第 1、3、5 轮拿到 Fenced，第 2、4、6 轮拿到
    Unreachable（空 body），因为每个被围栏拒绝的请求都毁掉了它后面那一个。
    """
    p = subprocess.Popen(
        [sys.executable, "-c", STALE_SRV], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        port = int(p.stdout.readline().strip())
        with tinyray.join("c", "churn") as me:
            me.ready()
            good = tinyray.pool("s").wait(count=1, timeout=15)[0]
            stale = tinyray.Handle(
                "s",
                {
                    "id": 0,
                    "slot": 0,
                    "incarnation": good.incarnation - 1,
                    "url": f"http://127.0.0.1:{port}",
                    "ready": True,
                    "state": {},
                },
                ("who",),
            )
            for i in range(6):
                with pytest.raises(tinyray.Fenced):
                    stale.who()
                assert good.who() == "ok", f"第 {i + 1} 轮：围栏拒绝毁掉了下一个请求"
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


def _call(path: bytes, body: bytes = b"{}", extra: bytes = b"") -> bytes:
    return (
        b"POST "
        + path
        + b" HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: "
        + str(len(body)).encode()
        + b"\r\n"
        + extra
        + b"\r\n"
        + body
    )


@pytest.mark.parametrize(
    "label,first",
    [
        ("wrong-path", _call(b"/nope")),
        ("no-such-method", _call(b"/call/nosuch")),
        ("bad-argument", _call(b"/call/echo", b'{"args":["x"],"kwargs":{}}')),
        ("raises", _call(b"/call/boom")),
        ("not-json", _call(b"/call/echo", b"not json at all")),
        ("fenced", _call(b"/call/echo", extra=b"x-tinyray-target: s/0#999\r\n")),
        ("get-methods", b"GET /_methods HTTP/1.1\r\nHost: x\r\n\r\n"),
        ("get-unknown", b"GET /zzz HTTP/1.1\r\nHost: x\r\n\r\n"),
    ],
)
def test_no_reply_leaves_the_connection_unusable(served, label, first):
    """每一种应答都要么读掉请求体，要么关掉连接。漏一条，keep-alive 上
    紧跟着的那个请求就会从残字节开始解析 —— 围栏那条就是这么漏的。"""
    good = _call(b"/call/echo", b'{"args":[7],"kwargs":{}}')
    s = socket.create_connection(("127.0.0.1", served), timeout=15)
    try:
        s.sendall(first)
        assert s.recv(65536), f"{label} 没有应答"
        s.sendall(good)
        after = s.recv(65536)
        if not after:
            return  # 关掉连接是可接受的处理方式
        assert after.split(b"\r\n")[0].split()[1] == b"200", f"{label} 之后连接坏了: {after[:80]}"
    finally:
        s.close()


def test_a_chunked_body_is_refused_rather_than_silently_dropped(served):
    """分块请求没有 content-length，而 body 的长度只从那个头取。

    结果是两重的，而且第一重更糟：body 整个留在 socket 里，方法拿默认参数跑完
    并"成功"返回 —— echo(x=7) 被当成 echo() 执行。第二重是分块的框架字节接着被
    当成下一个请求行解析，连接以一个 HTML 400 收场，同一条连接上的下一个请求
    根本没人应答。

    实测（修复前，一条连接上先发分块再发普通请求）：只回来 1 个状态行，内容是
    `{"result": {"took": "none"}}`，然后是 `Bad request syntax ('23')` 的 HTML
    错误页，第二个请求的答复从未出现。

    把分块 body 正确解码要连 trailer 和 chunk-extension 一起处理，对一个上限
    1MB 的控制平面来说不值这些框架代码；HTTP 对这种情况本来就有一个说法。
    """
    head = (
        b"POST /call/echo HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"content-type: application/json\r\n"
        b"transfer-encoding: chunked\r\n"
        b"\r\n"
    )
    payload = b'{"kwargs": {"x": 7}}'
    body = b"%x\r\n" % len(payload) + payload + b"\r\n0\r\n\r\n"

    got = _raw(served, head, body)
    assert b"411" in got.split(b"\r\n")[0], got[:200]
    assert b'"result"' not in got, f"the method ran on a body that was never read: {got[:200]!r}"


def test_a_chunked_request_cannot_desynchronise_the_next_one(served):
    """拒绝还不够 —— 拒绝之后连接必须关掉。

    否则那些框架字节还在流里，下一个请求依然会从残渣开始解析。
    """
    chunked = (
        b"POST /call/echo HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"transfer-encoding: chunked\r\n"
        b"\r\n"
        b"14\r\n"
        b'{"kwargs": {"x": 7}}'
        b"\r\n0\r\n\r\n"
    )
    plain_body = b'{"kwargs": {"x": 99}}'
    plain = (
        b"POST /call/echo HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"content-type: application/json\r\n"
        b"content-length: %d\r\n\r\n" % len(plain_body)
    ) + plain_body

    s = socket.create_connection(("127.0.0.1", served), timeout=20)
    try:
        s.sendall(chunked)
        time.sleep(0.3)
        try:
            s.sendall(plain)
        except OSError:
            pass  # already closed on us, which is the intended outcome
        time.sleep(0.5)
        got = b""
        s.settimeout(3)
        try:
            while True:
                part = s.recv(65536)
                if not part:
                    break
                got += part
        except TimeoutError:
            pass
    finally:
        s.close()

    # 关键是"绝不能把残渣当成请求来解析"：不许出现按分块框架回的第二个答复，
    # 更不许把 x=7 当成结果返回。
    assert b'"result": 7' not in got, f"the dropped body was answered anyway: {got[:300]!r}"
    assert b"Bad request syntax" not in got, f"the stream desynchronised: {got[:300]!r}"


COUNTING = textwrap.dedent(
    """
    import sys, tinyray
    from tinyray import _serve
    _serve.BODY_TIMEOUT = 1.0        # 免得为了看一次 408 等满 15 秒
    M = []
    class S:
        n = 0
        def work(self, x: int) -> int:
            S.n += 1
            return x
        def ran(self) -> int:
            return S.n
        def counts(self) -> dict:
            return M[0].stats()
    m = tinyray.join("count", "stateful", slot=0, size=1, serves=S())
    M.append(m)
    m.ready()
    print(m._server.port, flush=True)
    sys.stdin.readline()
    """
)


@pytest.fixture
def counting(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", COUNTING], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    port = int(p.stdout.readline().strip())
    try:
        yield port
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


@pytest.mark.parametrize(
    "label,cl,body,status",
    [
        ("body 只发一半", b"200", b'{"x":', 408),
        ("content-length 不是数字", b"abc", b'{"x":1}', 400),
        ("content-length 是负数", b"-5", b'{"x":1}', 400),
    ],
)
def test_a_request_the_callee_never_read_whole_is_safe_to_send_again(
    counting, label, cl, body, status
):
    """ "没送到"和"可能跑过了"差的是调用方敢不敢直接重发。

    服务端在读完请求之前就放弃的所有路子 —— 读不懂的长度、发到一半停住的
    body —— 方法都还没被调用过。下半段用计数器把这件事量出来，而不是从代码
    读出来。

    `400` 和 `408` 原来会掉进兜底的 `OutcomeUnknown`，那等于告诉调用方**反话**：
    可能跑过了，所以非幂等的调用不能原样重发。大 payload 撞上一条忙的链路，
    body 传到一半停住是很平常的事。
    """
    with tinyray.join("asker", "churn") as me:
        me.ready()
        tinyray.pool("count").wait(count=1, timeout=10)
        h = tinyray.pool("count").slot(0)
        before = h.ran()

        got = _raw(counting, _post(b"/call/work", cl), body)
        assert got.split(b"\r\n")[0].split()[1] == str(status).encode(), got[:120]
        assert h.ran() == before, f"{label}: 服务端回了 {status}，方法却跑了"

    # 上一句量到的是"什么都没跑"。这一句是客户端据此必须告诉调用方的话。
    with pytest.raises(tinyray.NotDelivered):
        _rpc._decode(status, b'{"error":"x"}', "count/0#1")


@pytest.mark.parametrize(
    "label,cl,sent,status,rest",
    [
        ("body 只发一半", b"200", b'{"args":', b"408", b'[7],"kwargs":{}}' + b" " * 176),
        ("长度不是数字", b"abc", b"", b"400", b'{"args":[7],"kwargs":{}}'),
        ("长度是负数", b"-5", b"", b"400", b'{"args":[7],"kwargs":{}}'),
    ],
)
def test_a_body_the_server_gave_up_on_takes_the_connection_with_it(
    counting, label, cl, sent, status, rest
):
    """没被读干净的 body，没法在同一条连接上接着往下走。

    服务端放弃之后就不再读了，可字节还在路上：慢客户端会把 body 剩下的部分发
    完，长度读不懂时整个 body 都还没被读过。这些字节留在连接里，服务端就会拿
    它们当**下一个请求的请求行**去解析。

    实测不关连接的样子：回过 408 之后，同一条连接上又被推下来一个 `HTTP 500`。
    客户端复用这条长连接时会把它当成自己**下一个**请求的答复 —— 一次超时就这样
    错位成后面每次调用都答非所问。关掉之后读到的是干净的 EOF。

    三个入口都是同一件事，而三个原来一条测试都没有。
    """
    with tinyray.join("asker", "churn") as me:
        me.ready()
        tinyray.pool("count").wait(count=1, timeout=10)

        c = socket.create_connection(("127.0.0.1", counting), timeout=10)
        try:
            c.sendall(_post(b"/call/work", cl) + sent)
            first = c.recv(200)
            assert first.split()[1] == status, f"{label}: {first[:80]!r}"

            try:
                c.sendall(rest)
                c.settimeout(3)
                left = c.recv(200)
            except (BrokenPipeError, ConnectionResetError):
                left = b""
            assert left == b"", f"{label}: 之后服务端又在同一条连接上推了一个回复 {left[:60]!r}"
        finally:
            c.close()


@pytest.mark.parametrize(
    "path,label",
    [
        (b"/abcd/work", "长度对得上、前缀不对"),
        (b"/health", "别的服务上常见的路径"),
        (b"/", "光秃秃一个斜杠"),
    ],
)
def test_only_the_call_path_reaches_a_method(counting, path, label):
    """方法名是从路径上**逐字截**下来的，所以前缀必须是精确匹配。

    截取的写法是 `self.path[len("/call/"):]` —— 它不检查前面那六个字符是什么。
    把前缀判断拿掉，`POST /abcd/work` 就会执行 `work()`：实测调用次数从 1 变 2。

    路由不精确本身不是越权（能 POST 到这个端口的人本来就能 POST `/call/work`），
    但"只有一条路能到达方法"是这一层唯一说得清的事，模糊了就没法再说清了。
    """
    with tinyray.join("asker", "churn") as me:
        me.ready()
        tinyray.pool("count").wait(count=1, timeout=10)
        h = tinyray.pool("count").slot(0)
        before = h.ran()

        got = _raw(counting, _post(path, b"2"), b"{}")
        assert got.split(b"\r\n")[0].split()[1] == b"404", f"{label}: {got[:80]!r}"
        assert h.ran() == before, f"{label}: 走 {path!r} 居然把方法执行了"


def test_a_body_the_parser_gives_up_on_still_counts_as_a_call(counting):
    """派发那一层自己散架的时候，这次调用也得算数。

    它算得出来的每一种失败都会自己回话并记账。散架是剩下的那种：解析器在
    `_dispatch` 里抛出一个它没打算接的异常，回话的活落到上一层，而记账**没有
    人接手**。

    够得到吗？够得到：`json.loads` 碰上嵌套四万层的 body 会抛 `RecursionError`
    —— 不是 `JSONDecodeError`，所以那个 except 接不住。实测 78 KiB 的这么一坨
    得到 500，`failed` 加一，服务端之后照常工作。

    不记账的后果不是崩，是**账本悄悄地不准**：`calls` 和 `busy_ms` 正是用来判断
    要不要给这些调用单开一条传输通道的，而最会散架的那批恰好被漏掉。
    """
    with tinyray.join("asker", "churn") as me:
        me.ready()
        tinyray.pool("count").wait(count=1, timeout=10)
        h = tinyray.pool("count").slot(0)
        before = h.counts()

        deep = b'{"args":[' + b"[" * 40_000 + b"]" * 40_000 + b'],"kwargs":{}}'
        got = _raw(counting, _post(b"/call/work", str(len(deep)).encode()), deep)
        assert got.split(b"\r\n")[0].split()[1] == b"500", got[:80]

        after = h.counts()
        # counts() 自己也是一次调用，所以 calls 至少 +2：坏的那次和这一次。
        assert after["calls"] - before["calls"] >= 2, (
            f"散架的那次调用没被算进去：calls {before['calls']} -> {after['calls']}"
        )
        assert after["failed"] - before["failed"] == 1, (
            f"散架的那次没被算成失败：failed {before['failed']} -> {after['failed']}"
        )
        assert h.work(5) == 5, "之后就不答话了"


SILENT = textwrap.dedent(
    """
    import sys, threading, tinyray
    from tinyray import _serve
    _serve.BODY_TIMEOUT = 3.0       # 沉默阶段的上限，免得等满 15 秒
    _serve.IDLE_TIMEOUT = 9.0       # keep-alive 的空闲上限
    class S:
        def ping(self) -> str: return "pong"
        def threads(self) -> int: return threading.active_count()
    m = tinyray.join("silent", "stateful", slot=0, size=1, serves=S())
    m.ready()
    print(m._server.port, flush=True)
    sys.stdin.readline()
    """
)


@pytest.fixture
def silent(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", SILENT], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    port = int(p.stdout.readline().strip())
    try:
        yield port
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


def test_a_connection_that_says_nothing_at_all_releases_its_thread(silent):
    """`BODY_TIMEOUT` 只管到 body。同一个攻击**早一个阶段** —— 连上来一个字节
    都不发，或者只发半个头部 —— 从前完全没人管：`BaseHTTPRequestHandler` 读
    请求行用的是 `self.timeout`，而它默认是 None。

    实测（真实常量下）：

        100 条"连上就沉默"        +100 线程
        再加 100 条"半个头部"     +200 线程
        等 16 秒（>BODY_TIMEOUT） 仍然 +200，永不释放

    对照文件里自己写的 body 那一档："500 条停滞连接稳定在 125 线程"，因为
    那一档有上限。修后同样的场景 16 秒后回到 +0。
    """
    before = tinyray_call(silent, b"/call/threads")
    holders = [socket.create_connection(("127.0.0.1", silent), timeout=30) for _ in range(15)]
    for i, s in enumerate(holders):
        if i % 2:
            s.sendall(b"POST /call/ping HTTP/1.1\r\nHost: x\r\n")  # 半个头部
    time.sleep(1.2)
    during = tinyray_call(silent, b"/call/threads")
    assert during >= before + 10, f"线程没被占住，测试本身失效了: {before} -> {during}"

    time.sleep(4.0)  # 超过上面设的 3.0s
    after = tinyray_call(silent, b"/call/threads")
    assert after <= before + 2, f"沉默的连接没有放开线程: {before} -> {during} -> {after}"
    for s in holders:
        s.close()


def test_the_silence_bound_does_not_cut_a_keep_alive_connection_short(silent):
    """给沉默连接设上限，不能顺手把 keep-alive 掐了：调用方就是靠握着连接
    等下一次调用的，而 httpx 自己要到 60 秒才丢弃闲置连接。服务端必须比它
    等得久，否则两边会抢着关同一个 socket。"""
    s = socket.create_connection(("127.0.0.1", silent), timeout=30)
    try:
        s.sendall(_post(b"/call/ping", b"2") + b"{}")
        assert b"200" in s.recv(8192).split(b"\r\n")[0]

        time.sleep(4.0)  # 超过头部上限 3.0s，但没到空闲上限 9.0s

        s.sendall(_post(b"/call/ping", b"2") + b"{}")
        again = s.recv(8192)
        assert b"200" in again.split(b"\r\n")[0], again[:120]
        assert b"pong" in again, again[:200]
    finally:
        s.close()


def test_the_server_outwaits_the_client_on_an_idle_connection():
    """服务端的空闲上限必须比调用方自己的丢弃期限长，否则两边会抢着关同一个
    socket：调用方以为连接还在、正要发请求，服务端刚好把它关掉 —— 一次本该
    成功的调用变成 NotDelivered。

    这是两个常量之间的约定，而约定没人守着就会烂。它们分处两个文件，改任何
    一个都不会惊动另一个。
    """
    from tinyray import _rpc, _serve

    expiry = _rpc._LIMITS.keepalive_expiry
    assert expiry is not None, "调用方不再声明丢弃期限了，这条约定要重新想"
    assert _serve.IDLE_TIMEOUT > expiry, (
        f"服务端 {_serve.IDLE_TIMEOUT}s 等不过调用方的 {expiry}s：闲置连接会由服务端先关"
    )
    assert _serve.BODY_TIMEOUT < _serve.IDLE_TIMEOUT, (
        "还没说过话的连接不该比 keep-alive 拿到更长的宽限"
    )
