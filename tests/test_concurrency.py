"""Many threads on one client at once.

One process is one member, but nothing stops that process from having dozens
of threads looking peers up, publishing state and calling methods at the same
time -- which is what any asyncio or worker-pool application looks like.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time

import pytest
import tinyray

SERVER = textwrap.dedent(
    """
    import sys, threading, time, tinyray
    class S:
        def __init__(self): self.n = 0; self.lock = threading.Lock()
        def fast(self, x: int) -> int:
            with self.lock: self.n += 1
            return x
        def slow(self) -> str:
            time.sleep(0.02); return "ok"
        def boom(self): raise ValueError("expected")
        def count(self) -> int: return self.n
        def echo(self, tag: str) -> str: return tag
        def sized(self, tag: str, n: int) -> str: return tag + "." + "z" * n
    with tinyray.join("s", "stateful", slot=0, serves=S()) as me:
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


@pytest.fixture
def served(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", SERVER], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p.stdout.readline().strip() == "READY"
    me = tinyray.join("c", "churn")
    me.ready(v=0)
    tinyray.pool("s").wait(count=1, timeout=15)
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


def hammer(fn, threads: int, seconds: float) -> tuple[int, list[str], int]:
    errors: list[str] = []
    calls = [0]
    stop = time.monotonic() + seconds

    def worker() -> None:
        while time.monotonic() < stop:
            try:
                fn()
                calls[0] += 1
            except BaseException as exc:  # noqa: BLE001 - anything counts
                errors.append(f"{type(exc).__name__}: {exc}")

    ts = [threading.Thread(target=worker) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=seconds + 30)
    return calls[0], errors, sum(1 for t in ts if t.is_alive())


@pytest.mark.parametrize(
    "what",
    ["lookup", "publish", "introspect", "call", "mixed"],
)
def test_one_client_under_many_threads(served, what):
    h = tinyray.pool("s").slot(0)
    fns = {
        "lookup": lambda: tinyray.pool("s").all(),
        "publish": lambda: served.ready(v=int(time.time() * 1000) % 997),
        "introspect": lambda: (served.stats(), served.accepted, served.silence_ms),
        "call": lambda: h.fast(1),
        "mixed": lambda: (tinyray.pool("s").all(), served.ready(v=1), h.fast(2)),
    }
    calls, errors, stuck = hammer(fns[what], threads=16, seconds=2.0)
    assert calls > 0
    assert not errors, f"{what}: {sorted(set(errors))[:3]}"
    assert stuck == 0, f"{what}: {stuck} threads never finished"


def test_business_failures_do_not_poison_pooled_connections(served):
    """A method that raises leaves a live connection behind it; the next call
    on that connection has to still work."""
    h = tinyray.pool("s").slot(0)
    before = h.count()

    def mix() -> None:
        for i in range(12):
            if i % 3 == 0:
                with pytest.raises(tinyray.RemoteError):
                    h.boom()
            else:
                assert h.fast(1) == 1

    calls, errors, stuck = hammer(mix, threads=12, seconds=2.0)
    assert not errors and stuck == 0
    assert h.count() > before


def test_epoch_is_safe_to_call_from_many_threads(registry):
    me = tinyray.join("t", "collective", slot=0, size=1)
    me.ready()
    try:
        calls, errors, stuck = hammer(
            lambda: tinyray.pool("t").epoch(timeout=5).valid, threads=32, seconds=2.0
        )
        assert calls > 0 and not errors and stuck == 0
    finally:
        me.leave()


def test_leave_racing_other_calls_says_what_happened(registry):
    """Threads outliving leave() is normal, and the message they get has to
    tell them apart from a process that never joined -- the two need opposite
    reactions and used to look identical."""
    me = tinyray.join("a", "churn")
    me.ready()
    seen: list[str] = []
    stop = threading.Event()

    def spin() -> None:
        while not stop.is_set():
            try:
                tinyray.pool("a").all()
            except RuntimeError as exc:
                seen.append(str(exc))
            except Exception:
                pass

    ts = [threading.Thread(target=spin, daemon=True) for _ in range(8)]
    for t in ts:
        t.start()
    time.sleep(0.1)
    me.leave()
    time.sleep(0.1)
    stop.set()
    for t in ts:
        t.join(timeout=5)

    assert seen, "leaving did not stop lookups at all"
    assert all("has left" in m for m in seen), f"misleading message: {seen[0]}"


def test_rapid_join_and_leave_cycles(registry):
    for _ in range(60):
        me = tinyray.join("q", "churn")
        me.ready()
        me.leave()
    me = tinyray.join("q", "churn")
    me.ready()
    assert len(tinyray.pool("q").wait(count=1, timeout=10)) == 1
    me.leave()


def test_concurrent_calls_each_get_their_own_answer(served):
    """每个调用者拿回的必须是**自己**那次调用的结果。

    上面几条并发测试全都调 fast(1)，参数永远一样 —— 应答就算串了线（这个线程
    收到另一个线程的回复），返回值也还是 1，看不出来。而答复串线正是这套代码
    出过一次的那一类：分块请求把 body 留在 socket 里，keep-alive 从残渣继续
    解析，下一个请求拿到的就是上一个的答复。

    所以这里每次调用都带一个全局唯一的标签，并且长度不一 —— 长度不同才会让
    content-length 各不相同，framing 一旦错位就对不上。
    """
    h = tinyray.pool("s").slot(0)
    mismatches: list[str] = []
    lock = threading.Lock()
    counter = [0]

    def worker(worker_id: int) -> None:
        for i in range(60):
            with lock:
                counter[0] += 1
                seq = counter[0]
            tag = f"w{worker_id}-i{i}-s{seq}"
            # 长度在 0..4095 之间变化，让每次的 content-length 都不一样。
            pad = (seq * 37) % 4096
            got = h.sized(tag, pad)
            want = tag + "." + "z" * pad
            if got != want:
                with lock:
                    mismatches.append(f"{tag}: got {got[:60]!r} ({len(got)} bytes)")

    ts = [threading.Thread(target=worker, args=(w,)) for w in range(16)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=90)

    assert not any(t.is_alive() for t in ts), "有线程没跑完"
    assert not mismatches, f"{len(mismatches)} 次调用拿到了别人的答复: {mismatches[:3]}"


def test_concurrent_calls_do_not_cross_between_pools_of_connections(served):
    """同一条连接被复用时，上一次的答复不能溢到下一次。

    串行地在一条复用连接上交替长短回复：如果 framing 有残留，短回复之后紧跟的
    读取会拿到上一次的尾巴。
    """
    h = tinyray.pool("s").slot(0)
    for i in range(200):
        big = h.sized(f"big{i}", 8192)
        assert big == f"big{i}." + "z" * 8192, f"第 {i} 次长回复对不上（{len(big)} 字节）"
        small = h.echo(f"small{i}")
        assert small == f"small{i}", f"第 {i} 次短回复拿到了 {small[:60]!r}"
