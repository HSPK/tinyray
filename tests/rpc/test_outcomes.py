"""「送到了吗」和「跑了吗」是两个问题，答错第二个会重复执行。

Unreachable 过去把两者混在一起，文档还写着 "Never arrived. Retry if the
operation can be repeated." —— 照做就会重试一个可能已经执行过的调用。

调用方要据此决定：确未送达可以原样重试；结果未知只能带着同一个 request id
重试，或者干脆保证幂等。所以这两件事必须是两个类。
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time
from types import SimpleNamespace

import pytest
import tinyray
from tinyray import _rpc
from tinyray._msgpack import dumps

SLOW = textwrap.dedent(
    """
    import sys, threading, time, tinyray

    class S:
        def __init__(self):
            self.n = 0
            self.lock = threading.Lock()
        def quick(self) -> str:
            return "ok"
        def slow(self, seconds: float) -> str:
            with self.lock:
                self.n += 1
            time.sleep(seconds)
            return "done"
        def ran(self) -> int:
            return self.n

    with tinyray.join("svc", "stateful", slot=0, serves=S(), max_concurrency=2) as me:
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


@pytest.fixture
def served(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", SLOW], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    me = tinyray.join("c", "churn")
    me.ready()
    tinyray.pool("svc").wait(count=1, timeout=15)
    try:
        yield me
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
        me.leave()


def test_both_kinds_are_still_one_except_clause(registry):
    """现存代码里到处是 `except Unreachable`，不能因为分类变细就全断。"""
    assert issubclass(tinyray.NotDelivered, tinyray.Unreachable)
    assert issubclass(tinyray.OutcomeUnknown, tinyray.Unreachable)
    assert not issubclass(tinyray.NotDelivered, tinyray.OutcomeUnknown)


def test_a_refused_connection_says_it_never_arrived(served):
    """连不上就是确实没跑，可以原样重试，不需要 request id。"""
    real = tinyray.pool("svc").slot(0)
    gone = tinyray.Handle(
        "svc",
        {
            "id": 0,
            "slot": 0,
            "incarnation": real.incarnation,
            "url": "127.0.0.1:1",
            "ready": True,
        },
        ("quick",),
    )
    with pytest.raises(tinyray.NotDelivered):
        gone.quick()


def test_a_timeout_refuses_to_claim_it_never_arrived(served):
    """超时只说明我们不再等了，不说明对端没做。

    这条实测过：方法确实跑了，调用方却在过去收到 Unreachable，而那个类的
    文档叫它重试。
    """
    h = tinyray.pool("svc").slot(0)
    before = h.ran()
    with pytest.raises(tinyray.OutcomeUnknown) as caught:
        h.slow.timeout(0.2)(1.5)
    assert not isinstance(caught.value, tinyray.NotDelivered), (
        "超时被说成『确未送达』，照着重试就会做两遍"
    )
    time.sleep(2.0)
    assert h.ran() == before + 1, "对端确实执行了，所以这次绝不能算作未送达"


def test_no_address_never_left_this_process(served):
    """没有地址的成员，报错要点名**为什么**没有。

    `join()` 时不给 `serves=` 就不会有地址，而这是个很常见的手误。错误必须
    点明这个原因，而不是让底层 host:port 解析失败来代替。
    """
    urlless = tinyray.Handle("svc", {"id": 9, "incarnation": 1, "ready": True}, ("quick",))
    with pytest.raises(tinyray.NotDelivered) as e:
        urlless.quick()
    assert "serves=" in str(e.value), f"报错没说清为什么没有地址：{e.value}"


def test_a_method_the_far_side_does_not_have_is_not_a_maybe(served):
    """对面说"没有这个方法"，那就是确定没跑过 —— 而且是调用方的问题。

    `Handle` 在本地就会挡掉不认识的名字，所以要走到这一步，得是手上这份方法表
    和对面实际提供的对不上：拿着旧 handle 调一个已经改名的方法就是。

    分类要对得上事实。`AttributeError` 说的是"没有这个东西"；掉进兜底会变成
    `OutcomeUnknown`，那句话的意思是"可能已经跑了、重试要小心"，而它根本没跑。
    """
    h = tinyray.pool("svc").slot(0)
    # 绕开本地那道拦截，直接照着线上协议问一个不存在的方法。
    call = _rpc.BoundMethod(h, "no_such_method", 5.0)
    with pytest.raises(AttributeError) as e:
        call()
    assert not isinstance(e.value, tinyray.Unreachable), (
        f"没有这个方法被说成了「可能跑过了」：{type(e.value).__name__}"
    )


def test_going_over_the_concurrency_limit_is_refused_not_queued(served):
    """无上限就是每连接一个线程。拒绝是有界的，排队不是。

    而且拒绝发生在方法之前，所以它属于「确未送达」—— 换一个 worker 重试是
    安全的，不需要 request id。
    """
    h = tinyray.pool("svc").slot(0)
    refused: list[BaseException] = []
    ok: list[str] = []
    lock = threading.Lock()

    def call() -> None:
        try:
            got = h.slow(1.0)
            with lock:
                ok.append(got)
        except BaseException as exc:  # noqa: BLE001
            with lock:
                refused.append(exc)

    threads = [threading.Thread(target=call) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(ok) == 2, f"限额是 2，却跑了 {len(ok)} 个"
    assert len(refused) == 4, f"其余 4 个应当被拒，实际 {len(refused)}"
    assert all(isinstance(e, tinyray.NotDelivered) for e in refused), (
        f"过载拒绝必须是『确未送达』，得到 {[type(e).__name__ for e in refused]}"
    )
    # 名额要还回去，否则第一波过后服务端就永久满了。
    assert h.quick() == "ok"


def test_the_limit_is_opt_in(registry):
    """不传 max_concurrency 就不该有任何限额 —— 默认行为不能变。"""
    src = SLOW.replace(", max_concurrency=2", "")
    p = subprocess.Popen(
        [sys.executable, "-c", src], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    me = tinyray.join("c", "churn")
    me.ready()
    try:
        h = tinyray.pool("svc").wait(count=1, timeout=15)[0]
        done: list[str] = []
        lock = threading.Lock()

        def call() -> None:
            got = h.slow(0.4)
            with lock:
                done.append(got)

        threads = [threading.Thread(target=call) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert len(done) == 6, f"没有设限额却拒了人：只有 {len(done)} 个跑完"
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
        me.leave()


@pytest.mark.parametrize(
    "status,expected",
    [
        (tinyray._tinyray.RPC_STATUS_SUCCESS, None),
        (tinyray._tinyray.RPC_STATUS_FENCED, tinyray.Fenced),
        (tinyray._tinyray.RPC_STATUS_CONCURRENCY_REFUSED, tinyray.NotDelivered),
        (tinyray._tinyray.RPC_STATUS_CALLER_FAULT, TypeError),
        (tinyray._tinyray.RPC_STATUS_METHOD_NOT_FOUND, AttributeError),
        (tinyray._tinyray.RPC_STATUS_REMOTE_ERROR, tinyray.RemoteError),
        (tinyray._tinyray.RPC_STATUS_MALFORMED_PROTOCOL, tinyray.NotDelivered),
        (tinyray._tinyray.RPC_STATUS_INTERNAL, tinyray.OutcomeUnknown),
        (255, tinyray.OutcomeUnknown),
    ],
)
def test_every_native_status_lands_in_the_right_class(status, expected):
    outcome = SimpleNamespace(
        kind=tinyray._tinyray.RPC_OUTCOME_REPLY,
        status=status,
        payload=dumps(7),
        message="x",
        error_type="ValueError",
        traceback="trace",
        batch_index=None,
        completed=None,
    )
    if expected is None:
        assert _rpc._decode(outcome, "p/0#1") == 7
        return
    with pytest.raises(expected) as e:
        _rpc._decode(outcome, "p/0#1")
    if status != tinyray._tinyray.RPC_STATUS_REMOTE_ERROR:
        assert not isinstance(e.value, tinyray.RemoteError)
