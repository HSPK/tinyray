"""身份、fencing、状态发布确认 —— 三件调用方本来要自己拼的事。

它们的共同点是：需要的事实 TinyRay 都已经知道（谁在、任期号、心跳落没落地），
只是没有交出去，于是业务层只好用参数传、用轮询查、用反查自己来凑。
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time

import pytest
import tinyray

ECHO_CALLER = textwrap.dedent(
    """
    import sys, tinyray

    class S:
        def whoami(self, ctx: tinyray.CallContext) -> dict:
            return {
                "identity": ctx.identity,
                "pool": ctx.pool,
                "slot": ctx.slot,
                "incarnation": ctx.incarnation,
                "request_id": ctx.request_id,
            }
        def mixed(self, n: int, ctx: tinyray.CallContext) -> dict:
            return {"n": n, "caller": ctx.identity}
        def ctx_first(self, ctx: tinyray.CallContext, n: int) -> dict:
            return {"n": n, "caller": ctx.identity}
        def ctx_middle(self, a: int, ctx: tinyray.CallContext, b: int) -> dict:
            return {"a": a, "b": b, "caller": ctx.identity}
        def plain(self, n: int) -> int:
            return n

    with tinyray.join("svc", "stateful", slot=0, serves=S()) as me:
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)

SEAT_THIEF = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join("held", "stateful", slot=0) as m:
        m.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


def _stop(p: subprocess.Popen) -> None:
    try:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=10)
    except Exception:
        p.kill()


@pytest.fixture
def peer(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", ECHO_CALLER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert p.stdout.readline().strip() == "READY"
    me = tinyray.join("caller", "stateful", slot=3)
    me.ready()
    tinyray.pool("svc").wait(count=1, timeout=15)
    try:
        yield me
    finally:
        _stop(p)
        me.leave()


def test_the_callee_is_told_who_called_without_being_passed_it(peer):
    """业务接口过去要手工传 worker_id + incarnation —— 传错、忘传都没人拦。"""
    got = tinyray.pool("svc").slot(0).whoami()
    assert got["pool"] == "caller"
    assert got["slot"] == 3
    assert got["incarnation"] == peer.incarnation
    assert got["identity"] == f"caller/3#{peer.incarnation}"


def test_every_call_carries_a_request_id_that_names_that_attempt(peer):
    """两边要能指着同一次调用说话。

    只给名字，不做去重：被调方无从知道一次调用重放是否安全，只有调用方知道，
    所以那个决定留在 NotDelivered / OutcomeUnknown 那一侧。
    """
    h = tinyray.pool("svc").slot(0)
    first = h.whoami()["request_id"]
    second = h.whoami()["request_id"]
    assert first and second, "调用没有带上 request id"
    assert first != second, "两次不同的尝试用了同一个 id"
    assert first.startswith(f"caller/3#{peer.incarnation}"), first


def test_the_context_does_not_disturb_the_real_arguments(peer):
    """注入不能挤掉调用方自己的参数，也不能影响没有声明它的方法。"""
    h = tinyray.pool("svc").slot(0)
    assert h.mixed(7)["n"] == 7
    assert h.mixed(7)["caller"].startswith("caller/3#")
    assert h.plain(5) == 5


def test_set_ready_replaces_the_state_instead_of_layering_on_it(registry):
    """ready() 是合并语义，所以过去发出去的 key 拿不回来。

    权重切换要的是「整张图换掉」，不是在上一版之上再糊一层。
    """
    me = tinyray.join("pub", "churn")
    try:
        me.ready(version=1, stale=True)
        assert me.state == {"version": 1, "stale": True}

        me.ready(version=2)
        assert me.state["stale"] is True, "合并语义就是这样，这里先钉住它"

        me.set_ready({"version": 3})
        assert me.state == {"version": 3}, "整体替换之后不该还留着 stale"
    finally:
        me.leave()


def test_flush_waits_until_the_registry_has_it(registry):
    """发布之后再反查自己，是因为没有别的办法说「它看见了吗」。"""
    me = tinyray.join("pub", "churn")
    watcher = tinyray.pool("pub")
    try:
        me.set_ready({"weights": "v9"})
        me.flush(timeout=20)
        # flush 返回即代表注册中心已经收下，所以这里不需要再等。
        mine = watcher.snapshot().get(me.identity)
        assert mine is not None, "flush 之后自己必须在册"
        assert mine.state["weights"] == "v9", "flush 返回了，状态却还没到"
    finally:
        me.leave()


def test_a_member_learns_it_was_replaced_without_being_asked(registry):
    """RPC 被拒只挡住了走 TinyRay 的那部分。

    旧 Worker 手里还有 GPU、推理服务和自己的 socket，它得被告知才能停掉那些。
    """
    me = tinyray.join("held", "stateful", slot=0)
    me.ready()
    fenced = threading.Event()

    def watch() -> None:
        if me.wait_fenced(timeout=30):
            fenced.set()

    t = threading.Thread(target=watch, daemon=True)
    t.start()

    thief = subprocess.Popen(
        [sys.executable, "-c", SEAT_THIEF],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert thief.stdout.readline().strip() == "READY"
        assert fenced.wait(timeout=30), "座位被抢了，旧成员却没被告知"
        assert not me.accepted
    finally:
        _stop(thief)
        t.join(timeout=5)
        try:
            me.leave()
        except Exception:
            pass


def test_wait_fenced_says_no_rather_than_hanging(registry):
    """没被顶替就该有界地返回 False，不是永远等下去。"""
    me = tinyray.join("held", "stateful", slot=0)
    me.ready()
    try:
        t0 = time.monotonic()
        assert me.wait_fenced(timeout=1.0) is False
        assert time.monotonic() - t0 < 5
    finally:
        me.leave()


def test_a_snapshot_answers_about_one_exact_tenure(registry):
    """「那个 incarnation 还在吗」要在一份固定的快照里问。

    对着活池子问两次，可能问到的是两个时刻。
    """
    me = tinyray.join("held", "stateful", slot=0)
    me.ready()
    try:
        snap = tinyray.pool("held").snapshot()
        assert snap.get(me.identity) is not None
        assert snap.get(f"held/0#{me.incarnation - 1}") is None, "旧任期不该被认成还在"
        assert snap.slot(0).incarnation == me.incarnation
    finally:
        me.leave()


def test_the_context_can_sit_anywhere_in_the_signature(peer):
    """注入的参数放在哪一位，都不该影响调用方怎么传参。

    以前只有放在**所有位置参数之后**才工作。放第一位或中间，调用方传位置参数
    就报 `Expected CallContext, got int` —— 位置对不上了，因为调用方根本不为
    注入的那个参数发值。更糟的是这句话在怪调用方，而问题出在被调方的签名顺序。
    """
    h = tinyray.pool("svc").slot(0)
    assert h.mixed(7)["n"] == 7
    assert h.ctx_first(7)["n"] == 7
    assert h.ctx_middle(1, 2) == {
        "a": 1,
        "b": 2,
        "caller": h.ctx_middle(1, 2)["caller"],
    }
    # 关键字调用一直是好的，这里作为对照，确保修复没把它弄坏。
    assert h.ctx_first(n=7)["n"] == 7
    assert h.ctx_middle(a=1, b=2)["b"] == 2
    # 注入的参数不算在调用方能填的里面。多给一个要当场说不，而且必须走"调用方
    # 传错了、什么都没跑"那条通道 —— 不能是 OutcomeUnknown，那会让人以为可能
    # 已经执行过。
    with pytest.raises(TypeError):
        h.ctx_first(7, 8)


def test_a_caller_can_pin_one_name_across_retries(peer):
    """自动生成的 id 每次都变 —— 对追踪是对的，对幂等是错的。

    被调方要认出"这是同一次重试"，就得每次拿到同一个名字。没有这个的话，幂等
    key 只能当普通参数传，于是它成了又一件要一路穿下去、又一件在重试分支上会
    忘掉的东西。
    """
    h = tinyray.pool("svc").slot(0)
    assert h.whoami()["request_id"] != h.whoami()["request_id"], "自动 id 应该每次都不同"

    with tinyray.request_id("commit-42"):
        seen = [h.whoami()["request_id"] for _ in range(3)]
    assert seen == ["commit-42"] * 3, seen

    # 出了这个块就该恢复自动生成，否则一个名字会粘住整个进程。
    assert h.whoami()["request_id"] != "commit-42"


def test_a_pinned_name_does_not_leak_into_a_neighbouring_task(peer):
    """用 ContextVar 而不是全局变量：它跟着 await 走进这个块起的任务，
    但不会漏进旁边那个。"""
    import asyncio

    async def body() -> tuple[str, str]:
        ah = tinyray.apool("svc").slot(0)

        async def pinned() -> str:
            with tinyray.request_id("pinned-one"):
                return (await ah.whoami())["request_id"]

        async def loose() -> str:
            await asyncio.sleep(0.05)
            return (await ah.whoami())["request_id"]

        return await asyncio.gather(pinned(), loose())  # type: ignore[return-value]

    a, b = asyncio.run(body())
    assert a == "pinned-one"
    assert b != "pinned-one", f"名字漏进了旁边的任务: {b}"


def test_an_empty_request_id_is_refused(peer):
    with pytest.raises(ValueError, match="something"):
        with tinyray.request_id(""):
            pass


@pytest.mark.parametrize("name", ["训练组", "grüße", "a\nb", "tab\there"])
def test_a_pool_name_that_cannot_be_a_header_is_refused(registry, name):
    """名字会随每次调用进 HTTP 头，而头值必须是可打印 ASCII。

    不拦的话，`join("训练组")` 注册、被发现、被订阅全都正常 —— 然后**每一次
    调用**死在一个裸的 `UnicodeEncodeError` 上：httpx 内部抛的，抛在调用现场，
    离那个名字很远。半个能用的系统比一个明确的"不行"更糟。

    `pool()` 也要拦：对端的 pool 名同样会进"要谁来答"那个头。
    """
    with pytest.raises(ValueError, match="printable ASCII"):
        tinyray.join(name, "churn")
    with tinyray.join("ok", "churn") as me:
        me.ready()
        with pytest.raises(ValueError, match="printable ASCII"):
            tinyray.pool(name)


def test_an_empty_pool_name_is_refused(registry):
    with pytest.raises(ValueError, match="needs a name"):
        tinyray.join("", "churn")


def test_a_request_id_that_cannot_be_a_header_is_refused_where_it_is_set(peer):
    """不合法的 id 要在**设置它的那一行**报错。

    否则它死在 httpx 里，而调用方收到的是"对端联系不上" —— 于是跑去查网络。
    换行更糟：它在头里就是一行的结束。
    """
    with pytest.raises(ValueError, match="printable ASCII"):
        with tinyray.request_id("a\nb"):
            pass
    with pytest.raises(ValueError, match="printable ASCII"):
        with tinyray.request_id("a\rb"):
            pass
    with pytest.raises(ValueError, match="too long"):
        with tinyray.request_id("x" * 9000):
            pass


def test_a_non_ascii_request_id_is_refused_too(peer):
    """和 pool 名字同一条规则：进头的东西必须是可打印 ASCII。"""
    with pytest.raises(ValueError, match="printable ASCII"):
        with tinyray.request_id("批次-42"):
            pass


@pytest.mark.parametrize(
    "kw,seated",
    [
        ({"policy": "stateful", "slot": 0, "size": 1}, True),
        ({"policy": "stateful", "slot": 3, "size": 4}, True),
        ({"policy": "churn"}, False),
    ],
    ids=["seat-0", "seat-3", "no-seat"],
)
def test_one_spelling_of_the_fencing_token(registry, kw, seated):
    """围栏令牌从前在四个地方各拼了一遍：对端手里的 handle、成员对自己的说法、
    每次调用带出去的头、以及交给方法服务端做比对的那一份。四者必须逐字相同，
    否则一个已被顶替的成员会通过本该拦下它的检查。

    `frozen()` 的文档为名单指纹讲过同一个道理："第二份实现会悄悄跑偏"。这里
    跑偏的地方是安全检查。

    合成一份之后，这条测试守的不再是"四份写法一致"（那已经由构造保证），而是
    每个调用点确实用的是那一份、而且交出去的是这一任而不是别的一任。

    顺带纠一个我一开始写错的说法：seat 0 并不是这里的坑。有座位时 `ident` 本身
    就等于 `slot`（`ident = slot if slot is not None else random`），所以连
    `slot or ident` 都恰好是对的 —— 实测那个变异体三例全过。
    """
    from tinyray import _rpc

    class S:
        def whoami(self) -> str:
            return "served"

    me = tinyray.join("tok", serves=S(), **kw)
    try:
        me.ready()
        me.flush()
        pool = tinyray.pool("tok")
        pool.until(lambda s: bool(s.ready()), timeout=10)
        handle = pool.all()[0]

        assert me.identity == handle.identity, "成员自己的说法和对端手里的不一样"
        assert me.identity == _rpc._identity, "调用头里带的和成员自己的不一样"
        assert me.identity == me._server._srv.identity, "服务端用来比对的和成员自己的不一样"

        seat = me.identity.partition("/")[2].partition("#")[0]
        if seated:
            assert seat == str(kw["slot"]), f"座位号丢了: {me.identity}"
        else:
            assert seat.isdigit() and int(seat) > 0, f"无座位的该用自己的 id: {me.identity}"
    finally:
        me.leave()
