"""examples/agent_pool 是唯一一个把领域状态机写全的示例，所以它是唯一一个能自己
写错的示例。这里只钉一件事：终态是终态。

起因是 cancel() 里那句 `except Unreachable: pass  # gone already`。kill 失败并不
代表 worker 没了 —— OutcomeUnknown 的意思就是不知道它跑没跑。worker 自己那句
"被取消了就跳过" 依赖 kill 送达，所以 kill 一旦超时，worker 就会把一个已取消的
attempt 正常做完并上报。改之前 submit_result 会照单全收，把它翻回 completed。
"""

from __future__ import annotations

import sys

import pytest

from tests.support.registry import ROOT

sys.path.insert(0, str(ROOT / "examples"))

import tinyray  # noqa: E402
from agent_pool.pool import AgentPool, Catalog  # noqa: E402


def running_pool(monkeypatch, kill_raises: BaseException | None):
    pool = AgentPool(capacity=8, catalog=Catalog("c", "f", 1))
    pool.submit_attempt("a1", {})
    pool.pull_work("w1")

    class FakeHandle:
        def pick(self, **_):
            return self

        def kill(self, _attempt_id):
            if kill_raises is not None:
                raise kill_raises

    monkeypatch.setattr(tinyray, "pool", lambda _name: FakeHandle())
    return pool


@pytest.mark.parametrize(
    "kill_raises",
    [
        tinyray.OutcomeUnknown("我们没等到回音"),
        tinyray.NotDelivered("连接被拒"),
        tinyray.Fenced("换人了"),
        tinyray.NotFound("池子里没有"),
        None,
    ],
    ids=["outcome_unknown", "not_delivered", "fenced", "not_found", "kill_landed"],
)
def test_a_cancelled_attempt_cannot_be_finished_by_a_survivor(monkeypatch, kill_raises):
    pool = running_pool(monkeypatch, kill_raises)
    assert pool.cancel("a1", "用户喊停")["state"] == "cancelled"

    # worker 没死，也没收到 kill，把活干完了回来交差
    out = pool.submit_result("w1", "a1", {"status": "ok"})

    assert out["state"] == "cancelled"
    assert pool.attempts["a1"].state == "cancelled"
    assert pool.attempts["a1"].result == {"status": "cancelled", "detail": "用户喊停"}
    assert pool.outstanding() == 0


def test_a_completed_attempt_cannot_be_cancelled(monkeypatch):
    """反方向本来就是拦着的。两边都钉住，免得只修一半。"""
    pool = running_pool(monkeypatch, None)
    pool.submit_result("w1", "a1", {"status": "ok"})

    assert pool.cancel("a1", "太晚了")["state"] == "completed"
    assert pool.attempts["a1"].result == {"status": "ok"}


def test_a_live_attempt_still_completes(monkeypatch):
    """守卫不能顺手把正常路径也堵上。"""
    pool = running_pool(monkeypatch, None)
    assert pool.outstanding() == 1

    out = pool.submit_result("w1", "a1", {"status": "ok", "reward": 1.0})

    assert out["state"] == "completed"
    assert pool.attempts["a1"].result == {"status": "ok", "reward": 1.0}
    assert pool.outstanding() == 0


def test_a_stranger_cannot_report_for_someone_else(monkeypatch):
    pool = running_pool(monkeypatch, None)
    with pytest.raises(ValueError, match="belongs to w1"):
        pool.submit_result("w2", "a1", {"status": "ok"})
    assert pool.attempts["a1"].state == "running"
