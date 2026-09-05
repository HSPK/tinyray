"""订阅列表跟着每一次心跳走，所以它有上限。越过它不能是静默的。"""

from __future__ import annotations

import time

import pytest
import tinyray

LIMIT = 64  # 与 proto 里的 MAX_WATCH 一致，自己的池子也算一个


def test_subscribing_up_to_the_limit_keeps_the_member_alive(registry):
    """修前越界会让注册中心否掉整条心跳，循环随即退出：实测 2.5 秒 0 拍、
    accepted=False、failed=0、last_error 为空，而缓存里它还看得见自己。"""
    with tinyray.join("c", "churn") as me:
        me.ready()
        for i in range(LIMIT - 1):  # 自己那个占掉一个名额
            tinyray.pool(f"p{i}").all()
        before = me.stats()["beats_ok"]
        time.sleep(2.0)
        after = me.stats()
        assert after["beats_ok"] > before, "订到上限就把心跳停了"
        assert me.accepted, "注册中心否掉了这个成员"
        assert len(tinyray.pool("c").all()) == 1


def test_crossing_the_limit_fails_at_the_lookup_that_did_it(registry):
    """报错要点名是哪个池子越的界 —— 否则只能靠数。"""
    with tinyray.join("c", "churn") as me:
        me.ready()
        for i in range(LIMIT - 1):
            tinyray.pool(f"p{i}")
        with pytest.raises(RuntimeError, match=r"cannot watch \"over\""):
            tinyray.pool("over")
        # 越界被拒之后，成员必须照常活着
        before = me.stats()["beats_ok"]
        time.sleep(1.5)
        assert me.stats()["beats_ok"] > before, "一次被拒的订阅把心跳带走了"
        assert me.accepted
