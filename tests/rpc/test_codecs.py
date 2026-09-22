"""MessagePack application values and Python-owned model conversion."""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from uuid import UUID

import msgspec
import pytest
import tinyray
from tinyray import _msgpack
from tinyray._serve import MethodServer


class Phase(Enum):
    READY = "ready"


@dataclass(frozen=True)
class Plain:
    when: datetime
    payload: bytes
    values: tuple[int, int]


class Service:
    def __init__(self) -> None:
        self.calls = 0

    def echo(self, value):
        self.calls += 1
        return value

    def integer(self, value: int) -> int:
        self.calls += 1
        return value

    def plain(self, value: Plain) -> Plain:
        self.calls += 1
        return value

    def bad(self):
        self.calls += 1
        return object()


@pytest.fixture
def echo():
    service = Service()
    server = MethodServer(service, "codec/0#1", host="127.0.0.1", max_concurrency=2)
    handle = tinyray.Handle(
        "codec",
        {
            "id": 0,
            "slot": 0,
            "incarnation": 1,
            "url": server.url("127.0.0.1"),
            "ready": True,
        },
        server.methods,
    )
    try:
        yield service, server, handle
    finally:
        server.close()


def test_ordinary_messagepack_values_have_explicit_native_semantics(echo):
    _, _, handle = echo
    when = datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc)
    trace = UUID("12345678-1234-5678-1234-567812345678")
    values = [
        None,
        True,
        7,
        -4,
        1.25,
        -0.0,
        float("nan"),
        float("inf"),
        float("-inf"),
        "text",
        b"\x00weights\xff",
        when,
        trace,
        Phase.READY,
        (1, 2),
        {3, 4},
        {1: "integer", (2, 3): "tuple-key"},
    ]
    for value in values:
        got = handle.echo(value)
        if isinstance(value, float) and math.isnan(value):
            assert math.isnan(got)
        elif isinstance(value, UUID):
            assert got == str(value)
        elif isinstance(value, Enum):
            assert got == value.value
        elif isinstance(value, (tuple, set)):
            assert sorted(got) == sorted(value)
            assert isinstance(got, list)
        else:
            assert got == value


@pytest.mark.parametrize("value", [1 << 80, -(1 << 150)])
def test_arbitrary_size_python_integers_round_trip_as_values_and_keys(echo, value):
    _, _, handle = echo
    assert handle.echo(value) == value
    assert handle.integer(value) == value
    assert handle.echo({value: "large"}) == {value: "large"}


def test_composite_large_integer_keys_preserve_hashable_shapes(echo):
    _, _, handle = echo
    value = {
        (1 << 80, 2): "tuple",
        frozenset({1 << 81, 3}): "frozenset",
        ((1 << 82, 4), frozenset({5, 6})): "nested",
    }
    assert _msgpack.loads(_msgpack.dumps(value)) == value
    assert handle.echo(value) == value


def test_ordinary_values_skip_blob_tracking_scopes(monkeypatch):
    value = Plain(
        datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc),
        b"payload",
        (3, 5),
    )

    def unexpected_scope(*args, **kwargs):
        pytest.fail("ordinary MessagePack opened a BlobRef tracking scope")

    monkeypatch.setattr(_msgpack, "_encode_with_blob_scope", unexpected_scope)
    monkeypatch.setattr(_msgpack, "_decode_with_scope", unexpected_scope)
    raw, owners = _msgpack.dumps_with_blob_refs(value)
    assert owners == ()
    assert _msgpack.loads(raw) == {
        "when": value.when,
        "payload": value.payload,
        "values": [3, 5],
    }
    assert _msgpack.convert_msgpack(raw, Plain) == value


def test_typed_containers_scalars_enum_and_dataclass_restoration(echo):
    _, _, handle = echo
    trace = UUID("12345678-1234-5678-1234-567812345678")
    assert handle.echo.returns(tuple[int, int])((2, 4)) == (2, 4)
    assert handle.echo.returns(set[str])({"a", "b"}) == {"a", "b"}
    assert handle.echo.returns(UUID)(trace) == trace
    assert handle.echo.returns(Phase)(Phase.READY) is Phase.READY
    assert handle.echo.returns(date)(date(2026, 9, 18)) == date(2026, 9, 18)
    assert handle.echo.returns(time)(time(8, 30, 15)) == time(8, 30, 15)
    assert handle.echo.returns(timedelta)(timedelta(days=2, seconds=3)) == timedelta(
        days=2, seconds=3
    )
    assert handle.echo.returns(Decimal)(Decimal("123.4500")) == Decimal("123.4500")

    value = Plain(
        datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc),
        b"payload",
        (3, 5),
    )
    assert handle.plain.returns(Plain)(value) == value


def test_map_keys_nan_and_infinity_remain_messagepack_values(echo):
    _, _, handle = echo
    value = {
        7: float("nan"),
        (2, 3): float("inf"),
        "negative": float("-inf"),
    }
    got = handle.echo(value)
    assert math.isnan(got[7])
    assert got[(2, 3)] == float("inf")
    assert got["negative"] == float("-inf")


def test_invalid_inputs_never_send_and_invalid_returns_are_remote_errors(echo):
    service, server, handle = echo
    with pytest.raises(TypeError, match="unsupported"):
        handle.echo(object())
    assert server.counters.snapshot()["calls"] == 0

    cycle = []
    cycle.append(cycle)
    with pytest.raises((RecursionError, ValueError)):
        handle.echo(cycle)
    assert server.counters.snapshot()["calls"] == 0

    with pytest.raises(tinyray.RemoteError, match="cannot be sent as MessagePack"):
        handle.bad()
    assert service.calls == 1
    assert handle.echo(2) == 2


def test_framework_model_objects_and_types_are_explicitly_unsupported(echo, monkeypatch):
    service, _, handle = echo
    monkeypatch.setitem(sys.modules, "pydantic", object())

    class FrameworkModel:
        __pydantic_validator__ = object()

        def __init__(self, value: int):
            self.value = value

    @dataclass
    class FrameworkDataclass:
        __pydantic_validator__ = object()
        value: int = 1

    for value in [FrameworkModel(1), FrameworkDataclass()]:
        with pytest.raises(TypeError, match="Pydantic models are unsupported"):
            _msgpack.dumps(value)
        with pytest.raises(TypeError, match="Pydantic models are unsupported"):
            handle.echo(value)
    assert service.calls == 0

    for want in [FrameworkModel, FrameworkDataclass, list[FrameworkModel]]:
        with pytest.raises(TypeError, match="Pydantic models are unsupported"):
            handle.echo.returns(want)({})
    assert service.calls == 0


def test_removed_model_extension_codes_have_no_special_meaning():
    for code in (127,):
        value = msgspec.msgpack.Ext(code, b"former-model-marker")
        assert _msgpack.loads(msgspec.msgpack.encode(value)) == value


def test_malformed_custom_extension_is_a_caller_fault_not_an_unknown_outcome(echo):
    service, _, handle = echo
    before = service.calls
    malformed = msgspec.msgpack.encode(msgspec.msgpack.Ext(121, b"not-an-integer"))
    outcome = tinyray._tinyray.rpc_call_sync(
        handle.url,
        "malformed-ext",
        "caller/0#1",
        handle.identity,
        malformed,
        1000,
        method="echo",
    )
    assert outcome.status == tinyray._tinyray.RPC_STATUS_CALLER_FAULT
    assert service.calls == before


def test_unhashable_extension_map_key_is_a_correlated_type_error(echo):
    service, _, handle = echo
    key = msgspec.msgpack.encode([1, 2])
    item = msgspec.msgpack.encode("x")
    malformed_map = msgspec.msgpack.Ext(122, msgspec.msgpack.encode([(key, item)]))
    before = service.calls
    cases = [
        (
            "malformed-map-key-call",
            msgspec.msgpack.encode({"args": [malformed_map], "kwargs": {}}),
            {"method": "echo"},
        ),
        (
            "malformed-map-key-batch",
            msgspec.msgpack.encode(malformed_map),
            {"batch_len": 1},
        ),
    ]
    for request_id, body, operation in cases:
        outcome = tinyray._tinyray.rpc_call_sync(
            handle.url,
            request_id,
            "caller/0#1",
            handle.identity,
            body,
            1000,
            **operation,
        )
        assert outcome.request_id == request_id
        assert outcome.status == tinyray._tinyray.RPC_STATUS_CALLER_FAULT
        assert outcome.error_type == "TypeError"
    assert service.calls == before


def test_codec_rejects_trailing_and_malformed_messagepack():
    with pytest.raises(msgspec.DecodeError):
        _msgpack.loads(b"\x01\x02")
    with pytest.raises(msgspec.DecodeError):
        _msgpack.loads(b"\xc1")
