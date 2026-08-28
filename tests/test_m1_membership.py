"""M1 acceptance: report in, find each other, survive the registry dying."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time

import pytest
import tinyray

DRIVER = textwrap.dedent(
    """
    import json, os, sys, time
    import tinyray
    pool_name, policy = sys.argv[1], sys.argv[2]
    count, hold_s = int(sys.argv[3]), float(sys.argv[4])
    me = tinyray.join(pool_name, policy)
    me.ready(worker=os.getpid())
    print("READY", flush=True)
    sys.stdin.readline()
    peers = tinyray.pool(pool_name).all()
    print(json.dumps({"seen": len(peers), "beats": me.stats()}), flush=True)
    sys.stdin.readline()
    """
)


def _spawn(n: int, pool_name: str, policy: str = "churn") -> list[subprocess.Popen]:
    procs = []
    for _ in range(n):
        p = subprocess.Popen(
            [sys.executable, "-c", DRIVER, pool_name, policy, "1", "0"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        procs.append(p)
    for p in procs:
        assert p.stdout.readline().strip() == "READY"
    return procs


def _shutdown(procs):
    for p in procs:
        try:
            p.stdin.write("\n\n")
            p.stdin.flush()
        except (BrokenPipeError, ValueError):
            pass
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def test_join_then_find_each_other(registry):
    me = tinyray.join("env", "churn")
    me.ready(role="driver")
    procs = _spawn(3, "env")
    try:
        peers = tinyray.pool("env").wait(count=4, timeout=10)
        assert len(peers) == 4
        assert all(h.ready for h in peers)
        assert {h.incarnation for h in peers}, "every member carries a tenure"
    finally:
        _shutdown(procs)
        me.leave()


def test_leaving_is_noticed_sooner_than_dying(registry):
    """A farewell beat frees the seat at once; a kill can only be noticed when
    the lease runs out.

    The two are measured against each other rather than against the clock. An
    absolute threshold looks fine on an idle machine and fails under load,
    while the gap between the two paths survives both.
    """

    def time_departure(how: str) -> float:
        procs = _spawn(1, "env")
        tinyray.pool("env").wait(count=2, timeout=15)
        t0 = time.monotonic()
        if how == "leave":
            _shutdown(procs)  # exits normally, so atexit sends the farewell
        else:
            procs[0].kill()
            procs[0].wait(timeout=5)
        deadline = t0 + registry.ttl_ms / 1000 * 4 + 5
        while len(tinyray.pool("env").all()) > 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(tinyray.pool("env").all()) == 1, f"{how} was never noticed"
        return time.monotonic() - t0

    me = tinyray.join("env", "churn")
    me.ready()
    try:
        graceful = time_departure("leave")
        killed = time_departure("kill")
        assert graceful < killed / 2, (
            f"a farewell took {graceful:.2f}s against {killed:.2f}s for a kill; "
            f"it should not be waiting out the lease"
        )
    finally:
        me.leave()


def test_dead_member_expires_without_a_supervisor(registry):
    me = tinyray.join("env", "churn")
    me.ready()
    procs = _spawn(2, "env")
    tinyray.pool("env").wait(count=3, timeout=10)
    for p in procs:  # SIGKILL: no farewell beat, only the lease can reap it
        p.kill()
        p.wait(timeout=5)

    deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 2
    while len(tinyray.pool("env").all()) > 1 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert len(tinyray.pool("env").all()) == 1
    me.leave()


def test_filter_by_state(registry):
    me = tinyray.join("engine", "serving")
    me.ready(model_version=17)
    tinyray.pool("engine").wait(count=1, timeout=10)

    assert len(tinyray.pool("engine").all(model_version=17)) == 1
    assert tinyray.pool("engine").all(model_version=18) == []
    with pytest.raises(tinyray.NotFound):
        tinyray.pool("engine").pick(model_version=18)

    me.ready(model_version=18)
    deadline = time.monotonic() + 5
    while not tinyray.pool("engine").all(model_version=18) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert len(tinyray.pool("engine").all(model_version=18)) == 1
    me.leave()


def test_unready_members_are_not_picked(registry):
    me = tinyray.join("engine", "serving")
    # Registered but never declared ready: present, but not eligible.
    deadline = time.monotonic() + 5
    while tinyray.pool("engine")._c.pool_info("engine") is None and time.monotonic() < deadline:
        time.sleep(0.02)
    with pytest.raises(tinyray.NotFound):
        tinyray.pool("engine").pick()
    me.ready()
    assert len(tinyray.pool("engine").wait(count=1, timeout=5)) == 1
    me.leave()


def test_missing_seat_raises_instead_of_substituting(registry):
    me = tinyray.join("dispatcher", "stateful", slot=0)
    me.ready()
    tinyray.pool("dispatcher").wait(count=1, timeout=10)
    assert tinyray.pool("dispatcher").slot(0).slot == 0
    with pytest.raises(tinyray.NotFound):
        tinyray.pool("dispatcher").slot(3)
    me.leave()


def test_slotted_policy_requires_a_seat(registry):
    with pytest.raises(tinyray.PolicyError):
        tinyray.join("trainer", "collective")


def test_lookup_before_join_is_explicit():
    """三种情形都抛 RuntimeError，而它们需要相反的反应：还没 join、已经 leave、
    以及 fork 出来的子进程。`_require_client` 特意分开写了三条文案，注释里说
    "从这里看它们长得一样"—— 那么测试就不能只认类型。

    实测：把"还没 join"那条换成"已经离开了"的文案，五个文件 51 条测试全绿。
    """
    import importlib

    mod = importlib.reload(tinyray)
    with pytest.raises(RuntimeError, match="before looking anyone up"):
        mod.pool("anything")


@pytest.mark.parametrize("called", [False, True])
def test_leaving_lets_go_of_what_it_was_serving(registry, called):
    """走了就该放手 —— 被服务的那个对象十有八九是个模型或一份数据。

    三样东西各自攥着它，都实测过：

    1. `join()` 把 `member.leave` 交给了 atexit，而 atexit 从不遗忘。留着，
       成员就被钉到进程结束：8 轮 join/leave 之后 8 个方法服务器全活着。
    2. 关掉监听 socket 并不会结束**已经停在 keep-alive 连接上的处理线程**，
       而它攥着服务器和派发表。什么时候放手取决于调用方什么时候关连接，那不归
       我们管：实测 20 个成员离场，20 个对象还在，一分钟后仍然在。
    3. 注解和签名的缓存原来拿**绑定方法**当键，而绑定方法带着实例。

    第 2 条不能靠结束线程来解决 —— 解决的是那张查找表：此刻再来的请求都会被
    上面的围栏判成 409，表空着不花任何代价。
    """
    import gc
    import weakref

    refs, members = [], []
    for i in range(6):

        class Served:
            def echo(self, x: int) -> int:
                return x

        obj = Served()
        refs.append(weakref.ref(obj))
        name = f"letgo{'c' if called else 'n'}{i}"
        m = tinyray.join(name, "stateful", slot=0, size=1, serves=obj)
        members.append(weakref.ref(m))
        m.ready()
        if called:
            assert tinyray.pool(name).slot(0).echo(1) == 1
        del obj
        m.leave()
        del m

    gc.collect()
    how = "发过调用" if called else "没发过调用"
    stuck = [r for r in refs if r() is not None]
    assert not stuck, f"{how}：leave() 之后还有 {len(stuck)}/6 个被服务的对象没被释放"
    # 成员自己也一样。第 1 条（atexit）单看对象已经看不出来了 —— 表被清空之后
    # 对象无论如何都会释放，可成员还钉在那儿。
    held = [r for r in members if r() is not None]
    assert not held, f"{how}：leave() 之后还有 {len(held)}/6 个成员没被释放"
