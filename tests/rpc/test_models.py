"""Standard Python application models cross RPC without transport dictionaries."""

from __future__ import annotations

import asyncio
import math
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import NamedTuple, TypedDict
from uuid import UUID

import pytest
import tinyray
from tinyray import _msgpack, _rpc, _serve

from tests.support.rpc_wire import exchange
from tests.support.rpc_wire import request as wire_request


class Phase(Enum):
    QUEUED = "queued"


class Attempt(NamedTuple):
    number: int
    replica: int


class JobMeta(TypedDict):
    attempt: Attempt
    phase: Phase


@dataclass(frozen=True)
class Item:
    task_id: str
    created_at: datetime
    payload: bytes
    priority: int


@dataclass(frozen=True)
class Request:
    item: Item
    attempt: Attempt
    trace_id: UUID
    phase: Phase


@dataclass(frozen=True)
class Plain:
    name: str
    created_at: datetime
    trace_id: UUID


@dataclass(frozen=True)
class LegacyValue:
    number: float
    text: str


@dataclass(frozen=True)
class ById:
    items: dict[int, str]
    count: int


SERVER = textwrap.dedent(
    """
    import asyncio
    import math
    import sys
    from dataclasses import dataclass
    from datetime import datetime
    from enum import Enum
    from typing import NamedTuple, TypedDict
    from uuid import UUID

    import tinyray

    class Phase(Enum):
        QUEUED = "queued"

    class Attempt(NamedTuple):
        number: int
        replica: int

    class JobMeta(TypedDict):
        attempt: Attempt
        phase: Phase

    @dataclass(frozen=True)
    class Item:
        task_id: str
        created_at: datetime
        payload: bytes
        priority: int

    @dataclass(frozen=True)
    class Request:
        item: Item
        attempt: Attempt
        trace_id: UUID
        phase: Phase

    @dataclass(frozen=True)
    class Plain:
        name: str
        created_at: datetime
        trace_id: UUID

    @dataclass(frozen=True)
    class LegacyValue:
        number: float
        text: str

    class Models:
        def __init__(self):
            self.calls = 0

        def item(self, item: Item) -> Item:
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

        def items(self, items: list[Item]) -> list[Item]:
            self.calls += 1
            assert all(isinstance(item, Item) for item in items)
            return items

        def metadata(self, value: JobMeta) -> JobMeta:
            self.calls += 1
            assert isinstance(value["attempt"], Attempt)
            assert value["phase"] is Phase.QUEUED
            return value

        def plain(self, value: Plain) -> Plain:
            self.calls += 1
            assert isinstance(value, Plain)
            return value

        def mixed(self, item: Item, note, *scores: int, **extras: int):
            self.calls += 1
            assert isinstance(item, Item)
            assert isinstance(note, dict)
            return {
                "note": note,
                "scores": [type(value).__name__ for value in scores],
                "extras": {key: type(value).__name__ for key, value in extras.items()},
            }

        def legacy_value(self, value: LegacyValue) -> LegacyValue:
            self.calls += 1
            assert math.isnan(value.number)
            assert value.text == "你好"
            return value

        async def async_item(self, item: Item) -> Item:
            await asyncio.sleep(0)
            return self.item(item)

        def bad_item(self):
            return {
                "task_id": "task-7",
                "created_at": "not a datetime",
                "payload": "weights",
                "priority": 3,
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
        attempt=Attempt(3, 2),
        trace_id=UUID("12345678-1234-5678-1234-567812345678"),
        phase=Phase.QUEUED,
    )
    raw_item = {
        "task_id": "task-7",
        "created_at": item.created_at,
        "payload": b"weights",
        "priority": 3,
    }
    raw_request = {
        "item": raw_item,
        "attempt": [3, 2],
        "trace_id": "12345678-1234-5678-1234-567812345678",
        "phase": "queued",
    }
    return item, request, raw_item, raw_request


def test_dataclasses_are_direct_rpc_inputs_and_outputs(model_peer, values):
    item, _, raw_item, _ = values
    assert model_peer.item(item) == raw_item
    restored = model_peer.item.returns(Item)(item=item)
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
    assert isinstance(restored.created_at, datetime)
    assert isinstance(restored.trace_id, UUID)


def test_dataclasses_named_tuples_and_enums_can_be_nested(model_peer, values):
    _, request, _, raw_request = values
    assert model_peer.record(request) == raw_request
    restored = model_peer.record.returns(Request)(request)
    assert restored == request
    assert isinstance(restored.item, Item)
    assert isinstance(restored.attempt, Attempt)
    assert isinstance(restored.trace_id, UUID)
    assert restored.phase is Phase.QUEUED


def test_dataclasses_work_in_typed_containers(model_peer, values):
    item, _, raw_item, _ = values
    restored = model_peer.items.returns(list[Item])([item, item])
    assert restored == [item, item]
    assert all(isinstance(value, Item) for value in restored)
    assert model_peer.items([item]) == [raw_item]


def test_typed_dict_and_named_tuple_fields_restore_recursively(model_peer):
    value: JobMeta = {"attempt": Attempt(4, 1), "phase": Phase.QUEUED}
    restored = model_peer.metadata.returns(JobMeta)(value)
    assert restored == value
    assert isinstance(restored["attempt"], Attempt)
    assert restored["phase"] is Phase.QUEUED


def test_model_conversion_covers_async_and_batch_calls(model_peer, values):
    item, request, raw_item, raw_request = values

    async def async_call():
        handle = tinyray.apool("models").slot(0)
        return await handle.async_item.returns(Item)(item)

    assert asyncio.run(async_call()) == item
    assert tinyray.batch(
        model_peer,
        [
            tinyray.Call("item", (item,)),
            tinyray.Call("record", (request,)),
        ],
    ) == [raw_item, raw_request]


def test_model_codecs_are_safe_to_reuse_across_threads(model_peer, values):
    item, request, _, _ = values

    def call(index: int):
        if index % 2:
            return model_peer.item.returns(Item)(item)
        return model_peer.record.returns(Request)(request)

    with ThreadPoolExecutor(max_workers=8) as workers:
        results = list(workers.map(call, range(32)))

    assert results[::2] == [request] * 16
    assert results[1::2] == [item] * 16


def test_typed_dataclass_rpc_skips_generic_object_graphs(monkeypatch, values):
    _, request, _, _ = values

    class Models:
        def item(self, value: Item) -> Item:
            return value

        def record(self, value: Request) -> Request:
            return value

        def plain(self, value: Plain) -> Plain:
            return value

    server = _serve.MethodServer(Models(), "raw/0#1", host="127.0.0.1")
    handle = tinyray.Handle(
        "raw",
        {"id": 0, "incarnation": 1, "url": server.url("127.0.0.1"), "ready": True},
        server.methods,
    )

    def generic_decode_was_used(*args, **kwargs):
        pytest.fail("typed dataclass RPC built an intermediate dict/list object graph")

    monkeypatch.setattr(_serve, "loads", generic_decode_was_used)
    monkeypatch.setattr(_rpc, "loads", generic_decode_was_used)
    monkeypatch.setattr(_msgpack, "loads", generic_decode_was_used)
    plain = Plain(
        "task-7",
        datetime(2026, 9, 17, 16, 0, tzinfo=timezone.utc),
        UUID("12345678-1234-5678-1234-567812345678"),
    )
    try:
        assert handle.item.returns(Item)(request.item) == request.item
        assert handle.record.returns(Request)(request) == request
        assert handle.plain.returns(Plain)(plain) == plain
    finally:
        server.close()


def test_native_reply_payload_is_the_result_not_a_wrapper(model_peer, values):
    _, _, raw_item, _ = values
    response = exchange(
        model_peer.url,
        wire_request(
            request_id="raw-model",
            target=model_peer.identity,
            method="item",
            body=_msgpack.dumps({"args": [raw_item], "kwargs": {}}),
        ),
    )
    assert response["status"] == "success"
    assert _msgpack.loads(response["body"]) == raw_item
    assert set(_msgpack.loads(response["body"])) != {"result"}


def test_typed_calls_use_one_native_attempt_without_compatibility_fallback(
    model_peer, values, monkeypatch
):
    item, _, _, _ = values
    original = _rpc._native.rpc_call_sync
    attempts = []

    def counted(*args, **kwargs):
        attempts.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(_rpc._native, "rpc_call_sync", counted)
    assert model_peer.item.returns(Item)(item) == item
    assert len(attempts) == 1


def test_raw_model_envelope_materializes_every_other_argument(model_peer, values):
    item, _, _, _ = values
    assert model_peer.mixed(
        item,
        {"owner": "trainer"},
        "1",
        2,
        retries="3",
    ) == {
        "note": {"owner": "trainer"},
        "scores": ["int", "int"],
        "extras": {"retries": "int"},
    }


def test_direct_dataclass_messagepack_keeps_nonfinite_and_unicode_semantics(model_peer):
    value = LegacyValue(float("nan"), "你好")
    restored = model_peer.legacy_value.returns(LegacyValue)(value)
    assert math.isnan(restored.number)
    assert restored.text == "你好"


def test_invalid_dataclass_input_is_rejected_before_the_method_runs(model_peer):
    before = model_peer.call_count()
    with pytest.raises(TypeError) as caught:
        model_peer.item(
            {
                "task_id": "task-7",
                "created_at": "not a datetime",
                "payload": b"weights",
                "priority": 3,
            }
        )
    assert "created_at" in str(caught.value)
    assert model_peer.call_count() == before


def test_invalid_dataclass_output_names_the_local_restoration_failure(model_peer):
    with pytest.raises(TypeError) as caught:
        model_peer.bad_item.returns(Item)()
    message = str(caught.value)
    assert f"{model_peer.identity}.bad_item()" in message
    assert "Item" in message
    assert "created_at" in message


def test_model_encoding_is_type_agnostic_messagepack(values):
    _, request, _, raw_request = values
    raw = _msgpack.dumps({"args": [request], "kwargs": {}})
    assert _msgpack.loads(raw) == {"args": [raw_request], "kwargs": {}}
    assert b"Request" not in raw and b"Item" not in raw
