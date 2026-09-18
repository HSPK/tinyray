"""Application models cross RPC without hand-written transport dictionaries."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

import pytest
import tinyray
from pydantic import BaseModel, ConfigDict, Field, field_serializer


class Phase(Enum):
    QUEUED = "queued"


class Item(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    task_id: str = Field(alias="taskId")
    created_at: datetime
    payload: bytes
    priority: int

    @field_serializer("priority")
    def serialize_priority(self, value: int) -> str:
        return str(value)


@dataclass(frozen=True)
class Request:
    item: Item
    attempt: tuple[int, int]
    trace_id: UUID
    phase: Phase


@dataclass(frozen=True)
class Plain:
    name: str
    created_at: datetime
    trace_id: UUID


SERVER = textwrap.dedent(
    """
    import asyncio
    import sys
    from dataclasses import dataclass
    from datetime import datetime
    from enum import Enum
    from uuid import UUID

    import tinyray
    from pydantic import BaseModel, ConfigDict, Field, field_serializer

    class Phase(Enum):
        QUEUED = "queued"

    class Item(BaseModel):
        model_config = ConfigDict(populate_by_name=True)
        task_id: str = Field(alias="taskId")
        created_at: datetime
        payload: bytes
        priority: int

        @field_serializer("priority")
        def serialize_priority(self, value: int) -> str:
            return str(value)

    @dataclass(frozen=True)
    class Request:
        item: Item
        attempt: tuple[int, int]
        trace_id: UUID
        phase: Phase

    @dataclass(frozen=True)
    class Plain:
        name: str
        created_at: datetime
        trace_id: UUID

    class Models:
        def __init__(self):
            self.calls = 0

        def model(self, item: Item) -> Item:
            self.calls += 1
            assert isinstance(item, Item)
            assert isinstance(item.created_at, datetime)
            return item

        def record(self, request: Request) -> Request:
            self.calls += 1
            assert isinstance(request, Request)
            assert isinstance(request.item, Item)
            assert isinstance(request.attempt, tuple)
            assert isinstance(request.trace_id, UUID)
            assert request.phase is Phase.QUEUED
            return request

        def models(self, items: list[Item]) -> list[Item]:
            self.calls += 1
            assert all(isinstance(item, Item) for item in items)
            return items

        def plain(self, value: Plain) -> Plain:
            self.calls += 1
            assert isinstance(value, Plain)
            return value

        async def async_model(self, item: Item) -> Item:
            await asyncio.sleep(0)
            return self.model(item)

        def bad_model(self):
            return {
                "taskId": "task-7",
                "created_at": "not a datetime",
                "payload": "weights",
                "priority": "3",
            }

        def call_count(self) -> int:
            return self.calls

    with tinyray.join("models", "stateful", slot=0, serves=Models()) as me:
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


@pytest.fixture
def model_peer(registry):
    with tinyray.join("model-driver", "churn") as me:
        me.ready()
        proc = subprocess.Popen(
            [sys.executable, "-c", SERVER],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "READY"
        handle = tinyray.pool("models").wait(count=1, timeout=10)[0]
        try:
            yield handle
        finally:
            if proc.stdin is not None:
                proc.stdin.write("\n")
                proc.stdin.flush()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


@pytest.fixture
def values():
    item = Item(
        task_id="task-7",
        created_at=datetime(2026, 9, 17, 16, 0, tzinfo=timezone.utc),
        payload=b"weights",
        priority=3,
    )
    request = Request(
        item=item,
        attempt=(3, 2),
        trace_id=UUID("12345678-1234-5678-1234-567812345678"),
        phase=Phase.QUEUED,
    )
    raw_item = {
        "taskId": "task-7",
        "created_at": "2026-09-17T16:00:00Z",
        "payload": "weights",
        "priority": "3",
    }
    raw_request = {
        "item": raw_item,
        "attempt": [3, 2],
        "trace_id": "12345678-1234-5678-1234-567812345678",
        "phase": "queued",
    }
    return item, request, raw_item, raw_request


def test_pydantic_models_are_direct_rpc_inputs_and_outputs(model_peer, values):
    item, _, raw_item, _ = values

    assert model_peer.model(item) == raw_item
    restored = model_peer.model.returns(Item)(item=item)
    assert restored == item
    assert isinstance(restored, Item)
    assert isinstance(restored.created_at, datetime)


def test_plain_dataclasses_are_direct_rpc_inputs_and_outputs(model_peer):
    plain = Plain(
        "task-7",
        datetime(2026, 9, 17, 16, 0, tzinfo=timezone.utc),
        UUID("12345678-1234-5678-1234-567812345678"),
    )

    restored = model_peer.plain.returns(Plain)(plain)
    assert restored == plain
    assert isinstance(restored, Plain)
    assert isinstance(restored.created_at, datetime)
    assert isinstance(restored.trace_id, UUID)


def test_dataclasses_and_pydantic_models_can_be_nested(model_peer, values):
    _, request, _, raw_request = values

    assert model_peer.record(request) == raw_request
    restored = model_peer.record.returns(Request)(request)
    assert restored == request
    assert isinstance(restored, Request)
    assert isinstance(restored.item, Item)
    assert isinstance(restored.attempt, tuple)
    assert isinstance(restored.trace_id, UUID)
    assert restored.phase is Phase.QUEUED


def test_pydantic_models_work_in_typed_containers(model_peer, values):
    item, _, raw_item, _ = values

    restored = model_peer.models.returns(list[Item])([item, item])
    assert restored == [item, item]
    assert all(isinstance(value, Item) for value in restored)
    assert model_peer.models([item]) == [raw_item]


def test_model_conversion_covers_async_and_batch_calls(model_peer, values):
    item, request, raw_item, raw_request = values

    async def async_call():
        handle = tinyray.apool("models").slot(0)
        return await handle.async_model.returns(Item)(item)

    assert asyncio.run(async_call()) == item
    assert tinyray.batch(
        model_peer,
        [
            tinyray.Call("model", (item,)),
            tinyray.Call("record", (request,)),
        ],
    ) == [raw_item, raw_request]


def test_model_codecs_are_safe_to_reuse_across_threads(model_peer, values):
    item, request, _, _ = values

    def call(index: int):
        if index % 2:
            return model_peer.model.returns(Item)(item)
        return model_peer.record.returns(Request)(request)

    with ThreadPoolExecutor(max_workers=8) as workers:
        results = list(workers.map(call, range(32)))

    assert results[::2] == [request] * 16
    assert results[1::2] == [item] * 16


def test_invalid_pydantic_input_is_rejected_before_the_method_runs(model_peer, values):
    item, _, _, _ = values
    before = model_peer.call_count()

    with pytest.raises(TypeError) as caught:
        model_peer.model(
            {
                "taskId": "task-7",
                "created_at": "not a datetime",
                "payload": "weights",
                "priority": "3",
            }
        )

    assert "created_at" in str(caught.value)
    assert model_peer.call_count() == before
    assert model_peer.model.returns(Item)(item) == item


def test_invalid_pydantic_output_names_the_local_restoration_failure(model_peer):
    with pytest.raises(TypeError) as caught:
        model_peer.bad_model.returns(Item)()

    message = str(caught.value)
    assert f"{model_peer.identity}.bad_model()" in message
    assert "Item" in message
    assert "created_at" in message


def test_model_encoding_is_still_plain_json(values):
    _, request, _, raw_request = values
    from tinyray import _json

    raw = _json.dumps({"args": [request], "kwargs": {}})
    assert json.loads(raw) == {"args": [raw_request], "kwargs": {}}
    assert b"Request" not in raw and b"Item" not in raw


def test_importing_tinyray_does_not_import_optional_pydantic():
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, tinyray; assert 'pydantic' not in sys.modules",
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr
