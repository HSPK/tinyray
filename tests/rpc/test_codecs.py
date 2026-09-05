"""Fast JSON must not widen accepted Python values or alter legacy JSON semantics."""

from __future__ import annotations

import json
import math
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import IntEnum

import msgspec
import pytest
import tinyray
from tinyray import _json, _rpc
from tinyray._serve import MethodServer


class Number(IntEnum):
    ONE = 1


class Integer(int):
    pass


class Text(str):
    pass


@dataclass
class Record:
    value: int


class Struct(msgspec.Struct):
    value: int


VALUES = [
    None,
    True,
    False,
    0,
    -(1 << 63),
    (1 << 64) - 1,
    1 << 80,
    -(1 << 150),
    1.25,
    -0.0,
    float("nan"),
    float("inf"),
    float("-inf"),
    "plain ASCII",
    '\x00\x1f\x7f\n\t\\"/',
    "你好😀",
    "\ud800",
    "\udfff",
    "\ud800\udfff",
    [1, (2, 3), {"yes": True}],
    {False: "no", 2: "int", 3.5: "float", None: "null"},
    {float("inf"): 1, float("-inf"): 2, float("nan"): 3},
    {1: "integer", "1": "text"},
    {"nested": [float("nan"), 1 << 100, "\ud800"]},
    Number.ONE,
    Integer(5),
    Text("subclass"),
]


def _same(actual, expected):
    assert type(actual) is type(expected)
    if isinstance(expected, float):
        if math.isnan(expected):
            assert math.isnan(actual)
        else:
            assert actual == expected
            assert math.copysign(1.0, actual) == math.copysign(1.0, expected)
    elif isinstance(expected, list):
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected, strict=True):
            _same(left, right)
    elif isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            _same(actual[key], expected[key])
    else:
        assert actual == expected


@pytest.mark.parametrize("value", VALUES)
def test_encoder_preserves_stdlib_wire_meaning(value):
    expected = json.loads(json.dumps(value))
    _same(json.loads(_json.dumps(value)), expected)


@pytest.mark.parametrize("value", VALUES)
def test_decoder_preserves_stdlib_values(value):
    raw = json.dumps(value).encode()
    _same(_json.loads(raw), json.loads(raw))


@pytest.mark.parametrize(
    "value",
    [
        object(),
        b"binary",
        bytearray(b"binary"),
        {1, 2},
        Record(1),
        Struct(1),
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        Decimal("1.25"),
        {(1, 2): "tuple key"},
        {"nested": Record(1)},
    ],
)
def test_encoder_does_not_adopt_msgspec_specific_object_encodings(value):
    with pytest.raises(TypeError) as legacy:
        json.dumps(value)
    with pytest.raises(TypeError) as current:
        _json.dumps(value)
    assert str(current.value) == str(legacy.value)


def test_cycles_keep_the_stdlib_error():
    value = {"nested": []}
    value["nested"].append(value)
    with pytest.raises(ValueError) as legacy:
        json.dumps(value)
    with pytest.raises(ValueError) as current:
        _json.dumps(value)
    assert str(current.value) == str(legacy.value)


def test_custom_container_behavior_is_not_inspected_twice():
    class Mapping(dict):
        def __init__(self):
            super().__init__(x=1)
            self.visits = 0

        def items(self):
            self.visits += 1
            return [("from_items", self.visits)]

    value = Mapping()
    assert json.loads(_json.dumps(value)) == {"from_items": 1}
    assert value.visits == 1


@pytest.mark.parametrize(
    "raw",
    [
        b"18446744073709551616",
        b"-9223372036854775809",
        b"123456789012345678901234567890123456789012345678901234567890",
        b"1e999",
        b"-1e999",
        b"1e-999",
        b"-0e0",
        b"0.123456789012345678901234567890123456789",
        b"2.2250738585072012e-308",
        b'{"duplicate":1,"duplicate":2}',
        b'{"n":NaN,"p":Infinity,"m":-Infinity}',
        b'["\\ud800","\\udfff","\\ud800\\udfff"]',
        b'["\xed\xa0\x80"]',
        "\ufeff".encode() + b'{"bom":true}',
        '{"encoding":"你好"}'.encode("utf-16"),
        '{"encoding":"你好"}'.encode("utf-16-le"),
        '{"encoding":"你好"}'.encode("utf-32"),
        bytearray(b'{"bytearray":1}'),
        '{"string":1}',
    ],
)
def test_decoder_compatibility_cases(raw):
    _same(_json.loads(raw), json.loads(raw))


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"not json",
        b'{"unfinished":',
        b'{"trailing":1,}',
        b'"\\uZZZZ"',
        b'"\x01"',
        b'"\xff"',
        b"[01]",
        b"[1] trailing",
        "\ufeff{}",
        None,
        3,
        [],
        memoryview(b"{}"),
    ],
)
def test_decode_errors_are_the_legacy_exception_type_and_message(raw):
    with pytest.raises((ValueError, TypeError)) as legacy:
        json.loads(raw)
    with pytest.raises(type(legacy.value)) as current:
        _json.loads(raw)
    assert str(current.value) == str(legacy.value)
    if isinstance(legacy.value, json.JSONDecodeError):
        assert current.value.pos == legacy.value.pos
        assert current.value.doc == legacy.value.doc


def test_large_integer_digit_limit_is_not_bypassed():
    if not hasattr(sys, "set_int_max_str_digits"):
        pytest.skip("integer string digit limit is not available on this Python")
    previous = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(640)
        raw = b"9" * 700
        with pytest.raises(ValueError) as legacy:
            json.loads(raw)
        with pytest.raises(ValueError) as current:
            _json.loads(raw)
        assert str(current.value) == str(legacy.value)
    finally:
        sys.set_int_max_str_digits(previous)


def test_deep_values_use_the_legacy_compatibility_path():
    value = 1
    for _ in range(90):
        value = [value]
    raw = json.dumps(value).encode()
    assert _json.dumps(value) == raw
    _same(_json.loads(raw), json.loads(raw))


def test_large_ascii_payload_uses_both_fast_paths(monkeypatch):
    value = {"result": "x" * (64 << 10), "count": 1, "items": [True, None]}
    raw = json.dumps(value).encode()

    def legacy_was_used(*args, **kwargs):
        pytest.fail("the ordinary 64 KiB ASCII payload did not use the fast path")

    monkeypatch.setattr(_json.json, "dumps", legacy_was_used)
    monkeypatch.setattr(_json.json, "loads", legacy_was_used)
    assert _json.dumps(value) == msgspec.json.encode(value)
    assert _json.loads(raw) == value


@pytest.mark.parametrize("floating", [False, True], ids=["integers", "floats"])
@pytest.mark.parametrize("mapping", [False, True], ids=["array", "object"])
def test_ten_thousand_numbers_use_stdlib_without_a_second_codec_pass(
    monkeypatch, floating, mapping
):
    numbers = [index / 3 if floating else index for index in range(10_000)]
    value = {str(index): number for index, number in enumerate(numbers)} if mapping else numbers
    # The long leading string prevents a bounded-prefix-only heuristic from
    # accidentally hiding the expensive numeric container.
    envelope = {"leading": "x" * (64 << 10), "result": value}
    raw = json.dumps(envelope).encode()

    def fast_codec_was_used(*args, **kwargs):
        pytest.fail("large numeric containers must not pay for two codec passes")

    monkeypatch.setattr(msgspec.json, "encode", fast_codec_was_used)
    monkeypatch.setattr(_json, "_decode_fast", fast_codec_was_used)
    assert _json.dumps(envelope) == raw
    _same(_json.loads(raw), json.loads(raw))


def test_nested_small_containers_cannot_evade_the_encoder_work_bound(monkeypatch):
    value = {"leading": "x" * (64 << 10), "grid": [list(range(8)) for _ in range(8)]}
    raw = json.dumps(value).encode()

    def fast_codec_was_used(*args, **kwargs):
        pytest.fail("the guard must bound total nodes, not just container length")

    monkeypatch.setattr(msgspec.json, "encode", fast_codec_was_used)
    assert _json.dumps(value) == raw


def test_small_messages_retain_legacy_wire_formatting():
    value = {"result": 7}
    assert _json.dumps(value) == json.dumps(value).encode()


def test_quoted_commas_only_select_a_conservative_compatible_fallback():
    value = {"text": "," * 1000, "values": [1 << 100, -0.0, "\ud800", float("nan")]}
    raw = json.dumps(value).encode()
    _same(_json.loads(raw), json.loads(raw))


@pytest.mark.parametrize("raw_type", [str, bytes, bytearray])
def test_large_container_decode_preserves_supported_input_types(raw_type):
    raw = json.dumps(list(range(10_000)))
    if raw_type is not str:
        raw = raw_type(raw.encode())
    _same(_json.loads(raw), json.loads(raw))


@pytest.mark.parametrize("raw_type", [str, bytes, bytearray])
@pytest.mark.parametrize("escaped", [False, True], ids=["raw-unicode", "unicode-escapes"])
def test_unicode_uses_stdlib_without_first_decoding_with_msgspec(monkeypatch, raw_type, escaped):
    value = {"result": "中文" * 10_000}
    raw = json.dumps(value, ensure_ascii=escaped)
    if raw_type is not str:
        raw = raw_type(raw.encode())

    def fast_decoder_was_used(*args, **kwargs):
        pytest.fail("known Unicode fallbacks must not decode twice")

    monkeypatch.setattr(_json, "_decode_fast", fast_decoder_was_used)
    _same(_json.loads(raw), json.loads(raw))


@pytest.mark.parametrize("token", [b"NaN", b"Infinity", b"-Infinity"])
def test_nonfinite_literals_bypass_the_fast_decoder_in_mixed_packets(monkeypatch, token):
    raw = b'{"result":"' + b"x" * (64 << 10) + b'","number":' + token + b"}"

    def fast_decoder_was_used(*args, **kwargs):
        pytest.fail("known nonfinite literals must not decode twice")

    monkeypatch.setattr(_json, "_decode_fast", fast_decoder_was_used)
    _same(_json.loads(raw), json.loads(raw))


@pytest.mark.parametrize(
    "token",
    [
        "0.0",
        "-0.0",
        "-0e0",
        "1e999",
        "-1e999",
        "1e-999",
        "-1e-999",
        "0e999999999",
        "1.7976931348623157e308",
        "1.7976931348623159e308",
        "2.2250738585072012e-308",
        "2.2250738585072014e-308",
        "4.9406564584124654e-324",
        "2.4703282292062327e-324",
        "2.4703282292062328e-324",
        "0.123456789012345678901234567890123456789",
        "9007199254740993.0",
        "18446744073709551616",
        "-9223372036854775809",
        "9" * 300,
    ],
)
@pytest.mark.parametrize("raw_type", [str, bytes, bytearray])
def test_mixed_ascii_blob_and_numbers_need_only_one_decode(monkeypatch, token, raw_type):
    raw = '{"result":"' + "x" * (64 << 10) + '","number":' + token + "}"
    if raw_type is not str:
        raw = raw_type(raw.encode())
    expected = json.loads(raw)

    def legacy_was_used(*args, **kwargs):
        pytest.fail("mixed ASCII blob and compatible numbers must not decode twice")

    monkeypatch.setattr(_json.json, "loads", legacy_was_used)
    _same(_json.loads(raw), expected)


def test_float_hook_matches_python_for_deterministic_decimal_samples(monkeypatch):
    rng = random.Random(7319)
    samples = []
    for _ in range(1000):
        digits = "".join(str(rng.randrange(10)) for _ in range(rng.randrange(1, 80)))
        sign = "-" if rng.randrange(2) else ""
        token = f"{sign}{rng.randrange(10)}.{digits}e{rng.randrange(-1000, 1001)}"
        raw = ('{"number":' + token + "}").encode()
        samples.append((raw, json.loads(raw)))

    def legacy_was_used(*args, **kwargs):
        pytest.fail("ordinary float tokens must use the compatible fast decoder")

    monkeypatch.setattr(_json.json, "loads", legacy_was_used)
    for raw, expected in samples:
        _same(_json.loads(raw), expected)


def test_integer_digit_limit_errors_remain_legacy_inside_mixed_packets():
    if not hasattr(sys, "set_int_max_str_digits"):
        pytest.skip("integer string digit limit is not available on this Python")
    previous = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(640)
        raw = b'{"result":"' + b"x" * (64 << 10) + b'","number":' + b"9" * 700 + b"}"
        with pytest.raises(ValueError) as legacy:
            json.loads(raw)
        with pytest.raises(type(legacy.value)) as current:
            _json.loads(raw)
        assert str(current.value) == str(legacy.value)
    finally:
        sys.set_int_max_str_digits(previous)


def test_msgspec_integer_ceiling_does_not_override_a_disabled_python_digit_limit():
    if not hasattr(sys, "set_int_max_str_digits"):
        pytest.skip("integer string digit limit is not available on this Python")
    previous = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(0)
        raw = b'{"number":' + b"9" * 5000 + b"}"
        _same(_json.loads(raw), json.loads(raw))
    finally:
        sys.set_int_max_str_digits(previous)


def test_deep_containers_go_to_stdlib_without_a_first_decode(monkeypatch):
    raw = b"[" * 90 + b"1" + b"]" * 90

    def fast_decoder_was_used(*args, **kwargs):
        pytest.fail("deep containers must bypass the fast decoder")

    monkeypatch.setattr(_json, "_decode_fast", fast_decoder_was_used)
    _same(_json.loads(raw), json.loads(raw))


@pytest.mark.parametrize(
    "suffix",
    [
        b'"number":01}',
        b'"number":1e}',
        b'"number":+1}',
        b'"number":1.}',
        b'"number":NaN} trailing',
        b'"number":Infinity} trailing',
        b'"unicode":"\\uZZZZ"}',
    ],
)
def test_malformed_mixed_packets_keep_exact_legacy_errors(suffix):
    raw = b'{"result":"' + b"x" * (64 << 10) + b'",' + suffix
    with pytest.raises(json.JSONDecodeError) as legacy:
        json.loads(raw)
    with pytest.raises(type(legacy.value)) as current:
        _json.loads(raw)
    assert str(current.value) == str(legacy.value)
    assert current.value.pos == legacy.value.pos
    assert current.value.doc == legacy.value.doc


@pytest.fixture
def echo():
    class Echo:
        def echo(self, value):
            return value

        def bad(self):
            return Record(1)

    server = MethodServer(Echo(), "codec/0#1", host="127.0.0.1", max_concurrency=1)
    handle = tinyray.Handle(
        "codec",
        {"id": 0, "incarnation": 1, "url": server.url("127.0.0.1"), "ready": True},
        server.methods,
    )
    try:
        yield server, handle
    finally:
        server.close()


def test_single_call_round_trips_keep_legacy_values(echo):
    _, handle = echo
    for value in VALUES:
        _same(handle.echo(value), json.loads(json.dumps(value)))


def test_invalid_inputs_never_send_and_invalid_returns_stay_remote_errors(echo):
    server, handle = echo
    with pytest.raises(TypeError, match="not JSON serializable"):
        handle.echo(Record(1))
    assert server.counters.snapshot()["calls"] == 0
    with pytest.raises(tinyray.RemoteError, match="cannot be sent as JSON"):
        handle.bad()
    assert server.counters.snapshot()["failed"] == 1
    assert handle.echo(2) == 2


def test_preparing_a_single_call_preserves_its_route_and_envelope(echo):
    _, handle = echo
    url, raw, headers = _rpc._prepare(handle, "echo", {"args": [(1, 2)], "kwargs": {}})
    assert url == f"{handle.url}/call/echo"
    assert json.loads(raw) == {"args": [[1, 2]], "kwargs": {}}
    assert headers["x-tinyray-target"] == handle.identity
