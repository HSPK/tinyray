"""A plain object becomes callable across processes."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import textwrap
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Literal, NamedTuple, TypedDict
from uuid import UUID

import pytest
import tinyray

SERVER = textwrap.dedent(
    """
    import sys, time, tinyray

    class Collector:
        def assign(self, task, priority=0):
            return {"took": task, "priority": priority}
        def add(self, a, b):
            return a + b
        def boom(self):
            raise ValueError("rubric failed")
        def slow(self, seconds):
            time.sleep(seconds)
            return "done"
        def echo_big(self, blob):
            return len(blob)
        def echo(self, value):
            return value
        def _private(self):
            return "should not be reachable"

    me = tinyray.join("collector", "stateful", slot=int(sys.argv[1]), serves=Collector())
    me.ready()
    print("READY", flush=True)
    sys.stdin.readline()
    """
)


class TaskKey(NamedTuple):
    task_id: str


class RolloutKey(NamedTuple):
    task: TaskKey
    sample_index: int


class AttemptKey(NamedTuple):
    rollout: RolloutKey
    retry_index: int


class AgentJob(NamedTuple):
    attempt: AttemptKey
    proxy_url: str


class JobState(Enum):
    RUNNING = "running"


class JobMeta(TypedDict):
    owner: str
    labels: list[str]


class JobReply(TypedDict):
    primary: AgentJob
    alternatives: list[AgentJob]


@dataclass(frozen=True)
class JobBatch:
    jobs: list[AgentJob]
    by_attempt: dict[int, AgentJob]
    selected: AgentJob | None
    state: JobState
    shape: tuple[int, int]
    tags: set[str]
    meta: JobMeta
    created_at: datetime
    trace_id: UUID
    kind: Literal["batch"]


class Peer:
    def __init__(self, slot: int):
        self.proc = subprocess.Popen(
            [sys.executable, "-c", SERVER, str(slot)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert self.proc.stdout.readline().strip() == "READY"

    def stop(self):
        try:
            self.proc.stdin.write("\n")
            self.proc.stdin.flush()
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()


@pytest.fixture
def peer(registry):
    me = tinyray.join("driver", "churn")
    me.ready()
    p = Peer(0)
    tinyray.pool("collector").wait(count=1, timeout=10)
    try:
        yield tinyray.pool("collector").slot(0)
    finally:
        p.stop()
        me.leave()


def test_a_method_call_crosses_the_process_boundary(peer):
    assert peer.assign("task-7") == {"took": "task-7", "priority": 0}
    assert peer.assign(task="task-8", priority=3) == {"took": "task-8", "priority": 3}
    assert peer.add(2, 3) == 5


def test_typos_fail_at_the_handle_not_at_runtime(peer):
    # The pool publishes its method list, so hasattr tells the truth. An
    # earlier design proxied every attribute and hasattr was always True.
    assert hasattr(peer, "assign")
    assert not hasattr(peer, "assgin")
    assert not hasattr(peer, "_private")
    with pytest.raises(AttributeError, match="assgin"):
        peer.assgin("x")


def test_business_failures_stay_business_failures(peer):
    with pytest.raises(tinyray.RemoteError) as e:
        peer.boom()
    assert e.value.type == "ValueError"
    assert e.value.message == "rubric failed"
    assert "rubric failed" in e.value.traceback
    # Not Unreachable: it arrived. Retrying it is the application's call.
    assert not isinstance(e.value, tinyray.Unreachable)


def test_a_dead_callee_is_unreachable_not_a_remote_error(peer, registry):
    peer_url = peer.url
    dead = tinyray.Handle(
        "collector",
        {"id": 0, "slot": 0, "incarnation": peer.incarnation, "url": peer_url, "ready": True},
        ("assign",),
    )
    dead.url = peer_url.rsplit(":", 1)[0] + ":1"  # nothing listens there
    with pytest.raises(tinyray.Unreachable):
        dead.assign("x")


def test_stale_tenure_is_fenced_not_silently_accepted(peer):
    stale = tinyray.Handle(
        "collector",
        {
            "id": 0,
            "slot": 0,
            "incarnation": peer.incarnation - 1,  # the previous occupant
            "url": peer.url,
            "ready": True,
        },
        ("assign",),
    )
    with pytest.raises(tinyray.Fenced):
        stale.assign("x")


def test_the_byte_budget_warns_rather_than_refuses(peer):
    """1 MB 是提示线不是闸门：超了要出声，但不能把调用打断。

    还要指对地方。警告一开始是在 _prepare 里发的，那比 invoke 深一层，
    stacklevel 就差了一格 —— 指到 _rpc.py 自己身上，而不是应用写的那一行。
    "有没有警告"验不出这个，所以这里连位置一起钉住。
    """
    with pytest.warns(tinyray.OversizeWarning, match="past the") as seen:
        assert peer.echo_big("x" * (1 << 21)) == (1 << 21)
    assert seen[0].filename == __file__, (
        f"警告指向 {seen[0].filename}:{seen[0].lineno}，应该指向调用它的那一行"
    )
    assert peer.echo_big("x" * 1000) == 1000


def test_timeout_is_a_modifier_so_it_cannot_collide(peer):
    assert peer.slow(0.05) == "done"
    t0 = time.monotonic()
    with pytest.raises(tinyray.Unreachable):
        peer.slow.timeout(0.2)(3.0)
    assert time.monotonic() - t0 < 2.0
    # The modifier is per-call, not sticky.
    assert peer.slow(0.05) == "done"


def test_returns_restores_a_nested_named_tuple(peer):
    """NamedTuple 过 JSON 会变 list，调用端应当只声明目标类型，不手工逐层拆包。"""
    raw = [[[["task-7"], 3], 2], "http://proxy/7"]
    got = peer.echo.returns(AgentJob)(raw)

    assert got == AgentJob(
        AttemptKey(RolloutKey(TaskKey("task-7"), 3), 2),
        "http://proxy/7",
    )
    assert isinstance(got, AgentJob)
    assert isinstance(got.attempt, AttemptKey)
    assert isinstance(got.attempt.rollout, RolloutKey)
    assert isinstance(got.attempt.rollout.task, TaskKey)


def test_returns_restores_named_tuples_nested_in_a_typed_dict(peer):
    """TypedDict 本身仍是 dict，但其 NamedTuple 字段必须递归恢复。"""
    primary = [[[["task-7"], 3], 2], "http://proxy/7"]
    alternative = [[[["task-8"], 4], 0], "http://proxy/8"]

    got = peer.echo.returns(JobReply)({"primary": primary, "alternatives": [alternative]})

    assert isinstance(got, dict)
    assert got == {
        "primary": AgentJob(AttemptKey(RolloutKey(TaskKey("task-7"), 3), 2), primary[1]),
        "alternatives": [AgentJob(AttemptKey(RolloutKey(TaskKey("task-8"), 4), 0), alternative[1])],
    }
    assert isinstance(got["primary"], AgentJob)
    assert isinstance(got["alternatives"][0], AgentJob)


def test_returns_restores_nested_standard_data_structures(peer):
    """容器和结构类型可以任意嵌套，JSON object 的数字 key 也要还原。"""
    raw_job = [[[["task-9"], 4], 1], "http://proxy/9"]
    raw = {
        "jobs": [raw_job],
        "by_attempt": {"9": raw_job},
        "selected": raw_job,
        "state": "running",
        "shape": [2, 8],
        "tags": ["gpu", "ready"],
        "meta": {"owner": "trainer", "labels": ["a", "b"]},
        "created_at": "2026-09-03T16:00:00Z",
        "trace_id": "12345678-1234-5678-1234-567812345678",
        "kind": "batch",
    }

    got = peer.echo.returns(JobBatch)(raw)

    assert isinstance(got, JobBatch)
    assert got.jobs == [AgentJob(AttemptKey(RolloutKey(TaskKey("task-9"), 4), 1), raw_job[1])]
    assert got.by_attempt == {9: got.jobs[0]}
    assert got.selected == got.jobs[0]
    assert got.state is JobState.RUNNING
    assert got.shape == (2, 8)
    assert got.tags == {"gpu", "ready"}
    assert got.meta == {"owner": "trainer", "labels": ["a", "b"]}
    assert got.created_at == datetime(2026, 9, 3, 16, tzinfo=timezone.utc)
    assert got.trace_id == UUID("12345678-1234-5678-1234-567812345678")
    assert got.kind == "batch"


def test_returns_handles_unions_fixed_tuples_and_null(peer):
    assert peer.echo.returns(AgentJob | None)(None) is None
    assert peer.echo.returns(tuple[str, int])(["worker", 7]) == ("worker", 7)
    assert peer.echo.returns(list[dict[str, int]])([{"step": 3}]) == [{"step": 3}]
    assert peer.echo.returns(None)(None) is None


def test_returns_and_timeout_compose_in_either_order(peer):
    raw = [[[["task"], 0], 0], "http://proxy"]
    assert isinstance(peer.echo.returns(AgentJob).timeout(5)(raw), AgentJob)
    assert isinstance(peer.echo.timeout(5).returns(AgentJob)(raw), AgentJob)
    for call in (
        peer.slow.returns(str).timeout(0.05),
        peer.slow.timeout(0.05).returns(str),
    ):
        with pytest.raises(tinyray.Unreachable):
            call(0.4)
    # A modifier belongs to the BoundMethod it returned, not later calls.
    assert peer.echo(raw) == raw


def test_returns_names_the_call_and_json_path_when_the_shape_is_wrong(peer):
    raw = {
        "jobs": [[[[["task"], 0], 0], "http://proxy"]],
        "by_attempt": {"not-an-int": [[[["task"], 0], 0], "http://proxy"]},
        "selected": None,
        "state": "running",
        "shape": [2, 8],
        "tags": [],
        "meta": {"owner": "trainer", "labels": []},
        "created_at": "2026-09-03T16:00:00Z",
        "trace_id": "12345678-1234-5678-1234-567812345678",
        "kind": "batch",
    }
    with pytest.raises(TypeError) as e:
        peer.echo.returns(JobBatch)(raw)

    message = str(e.value)
    assert f"{peer.identity}.echo()" in message
    assert "JobBatch" in message
    assert "by_attempt" in message


def test_returns_restores_the_async_result_too(peer):
    async def body() -> AgentJob:
        handle = tinyray.apool("collector").slot(0)
        raw = [[[["async-task"], 6], 3], "http://proxy/async"]
        return await handle.echo.returns(AgentJob)(raw)

    got = asyncio.run(body())
    assert got == AgentJob(
        AttemptKey(RolloutKey(TaskKey("async-task"), 6), 3),
        "http://proxy/async",
    )


def test_it_is_still_plain_http(peer):
    req = urllib.request.Request(
        f"{peer.url}/call/add",
        data=json.dumps({"a": 4, "b": 5}).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        assert json.loads(r.read())["result"] == 9

    with urllib.request.urlopen(f"{peer.url}/_methods", timeout=5) as r:
        body = json.loads(r.read())
    assert "assign" in body["methods"]
    assert "_private" not in body["methods"]


def test_a_member_without_serves_advertises_nothing(registry):
    me = tinyray.join("quiet", "churn")
    me.ready()
    h = tinyray.pool("quiet").wait(count=1, timeout=10)[0]
    assert h.url is None
    with pytest.raises(AttributeError, match="no methods"):
        h.anything()
    me.leave()
