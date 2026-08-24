"""发布状态和声明就绪是两件事，API 以前把它们焊死了。

`ready()` 和 `set_ready()` 都硬编码 `ready=True`，而成员 API 里没有任何只更新
状态的路径。后果不需要竞态就能看到：一个只想上报进度的组件，除了 `ready(step=n)`
无路可走，而这一句会把别人刚下的暂停静默掀掉。

实测过 `unready()` 之后 `ready(step=1)`，peers 看到的 ready 从 False 变回 True。

这里钉的是：只报进度的代码碰不到 readiness。有了这条，"每个 Member 只有一个
readiness owner" 就不再是一条靠自觉遵守的约定，而是结构上做不到别的。
"""

from __future__ import annotations

import json
import threading
import time

import tinyray


def _published(m, pool):
    """peers 看到的样子，从 pool 读而不是从自己读。"""
    m.flush()
    h = pool.snapshot().slot(m.slot)
    assert h is not None
    return h.ready, h.state


def test_update_publishes_without_touching_readiness(registry):
    with tinyray.join("p", slot=0, size=1) as m:
        pool = tinyray.pool("p")
        m.set_ready({"role": "trainer"})
        assert _published(m, pool)[0] is True

        m.unready()
        assert _published(m, pool)[0] is False

        m.update(step=1)
        ready, state = _published(m, pool)
        assert ready is False, "只报进度的调用把暂停掀掉了"
        assert state["step"] == 1, "状态没发出去"
        assert state["role"] == "trainer", "update 应该合并而不是替换"


def test_ready_still_declares_readiness(registry):
    """对偶：readiness owner 用 ready() 时必须仍然生效。"""
    with tinyray.join("p", slot=0, size=1) as m:
        pool = tinyray.pool("p")
        m.set_ready({})
        m.unready()
        assert _published(m, pool)[0] is False
        m.ready(step=2)
        assert _published(m, pool)[0] is True


def test_replace_takes_keys_back_without_touching_readiness(registry):
    with tinyray.join("p", slot=0, size=1) as m:
        pool = tinyray.pool("p")
        m.set_ready({"stale": True, "keep": 1})
        m.unready()
        m.replace({"keep": 1})
        ready, state = _published(m, pool)
        assert ready is False
        assert "stale" not in state, "replace 应该整体替换"
        assert state == {"keep": 1}


def test_is_ready_reports_what_peers_would_see(registry):
    with tinyray.join("p", slot=0, size=1) as m:
        m.set_ready({})
        assert m.is_ready is True
        m.update(step=1)
        assert m.is_ready is True, "update 不该改动 readiness"
        m.unready()
        assert m.is_ready is False
        m.update(step=2)
        assert m.is_ready is False


def test_two_publishers_do_not_lose_each_others_keys(registry):
    """合并是读-改-写。MAX_STATE 把窗口卡得很窄，今天打不中，但一旦窗口里
    多出任何工作就会开始丢数据 —— 把窗口撑开 0.5ms 测过，丢了 420 个 key。"""
    with tinyray.join("p", slot=0, size=1) as m:
        m.set_ready({})
        writers = 8
        gate = threading.Barrier(writers)

        def w(i: int) -> None:
            gate.wait()
            m.update(**{f"w{i}": 1})

        ts = [threading.Thread(target=w, args=(i,)) for i in range(writers)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        missing = [f"w{i}" for i in range(writers) if f"w{i}" not in m.state]
        assert not missing, f"并发合并丢了这些 key: {missing}"


def test_going_ready_again_is_never_deduplicated_away(registry):
    """同值去重必须把就绪位算进去。

    只比 state 的话，`unready()` 之后用同一份 state 再 `ready()` 会被当成"没变"
    丢掉 —— 成员从此停在不可用状态，而调用方以为自己已经宣告可用了。这是去重
    能造成的最坏后果，比多花一次请求严重得多。
    """
    with tinyray.join("p", slot=0, size=1) as m:
        pool = tinyray.pool("p")
        m.set_ready({"a": 1, "b": 2})
        assert _published(m, pool)[0] is True
        m.unready()
        assert _published(m, pool)[0] is False
        m.ready()  # 一个字节都没改，只有就绪位变了
        assert _published(m, pool)[0] is True, "重新 ready 被当成同值丢掉了"


def test_key_order_is_not_a_change(registry):
    """比的是解析后的值，不是字节。

    `{"b": 2, "a": 1}` 和 `{"a": 1, "b": 2}` 是同一件事。按字节比会把它当成
    两次改动，于是 dict 的构造顺序一变就白跑一趟 —— 而那个顺序调用方通常都
    不知道自己在控制。
    """
    with tinyray.join("p", slot=0, size=1) as m:
        m.set_ready({"a": 1, "b": 2, "cfg": {"x": 1, "y": 2}})
        assert m._c.set_state(json.dumps({"a": 1, "b": 2, "cfg": {"x": 1, "y": 2}}), True) is False
        assert m._c.set_state(json.dumps({"b": 2, "a": 1, "cfg": {"x": 1, "y": 2}}), True) is False
        assert m._c.set_state(json.dumps({"a": 1, "b": 2, "cfg": {"y": 2, "x": 1}}), True) is False
        assert m._c.set_state(json.dumps({"a": 1, "b": 3, "cfg": {"x": 1, "y": 2}}), True) is True


def test_republishing_the_same_thing_costs_nothing(registry):
    """同值发布既不该抬 pool 版本，也不该花掉一次请求。

    以前每次 set_ready 都会敲醒心跳、取消挂起的请求、把注册表已经拿着的东西
    再送一遍 —— 而注册表连版本都不会抬。
    """
    with tinyray.join("p", slot=0, size=1) as m:
        pool = tinyray.pool("p")
        m.set_ready({"same": 1})
        m.flush()
        before_version = pool.snapshot().revision
        before_beats = m.stats()["beats_ok"]
        # 岔开来发：挤在一起的话它们只合成一次唤醒，这条测试就什么也证明不了。
        for _ in range(20):
            m.set_ready({"same": 1})
            time.sleep(0.05)
        m.flush()
        assert pool.snapshot().revision == before_version, "同值发布抬高了 pool 版本"
        # flush() 自己要等两拍，所以基线是 2，不是 0。
        # 这 1 秒里本来就会有约两拍（ttl/4），flush() 自己再要两拍。
        spent = m.stats()["beats_ok"] - before_beats
        assert spent <= 6, f"20 次同值发布花了 {spent} 次心跳"
