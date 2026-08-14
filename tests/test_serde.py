"""Serialisation tests.

The property that matters most here is that large buffers travel *out-of-band*
exactly once. Getting the ``buffer_callback`` protocol backwards silently
doubles every payload while still producing correct results, so these tests
assert on sizes, not just on equality.
"""

from __future__ import annotations

import pickle

import pytest

from tinyray import decode_message, encode_message, serde


def wire_roundtrip(value):
    """Serialise, push through the framing, and rebuild."""
    body, frames = serde.serialize(value)
    header, decoded_frames = decode_message(encode_message(body, frames))
    return serde.deserialize(header, decoded_frames)


class TestBasicRoundTrips:
    @pytest.mark.parametrize(
        "value",
        [
            None,
            42,
            -1.5,
            "unicode ✓ 中文",
            b"raw bytes",
            [1, 2, 3],
            {"nested": {"tuple": (1, "two")}},
            {frozenset({1, 2})},
        ],
        ids=lambda v: type(v).__name__,
    )
    def test_plain_python_values(self, value):
        assert wire_roundtrip(value) == value

    def test_no_out_of_band_frames_for_small_values(self):
        body, frames = serde.serialize({"a": 1})
        assert frames == []
        assert len(body) < 100

    def test_locally_defined_function_survives(self):
        # cloudpickle earns its place here: plain pickle cannot do this, and ML
        # code passes lambdas and locally defined classes constantly.
        def double(x):
            return x * 2

        assert wire_roundtrip(double)(21) == 42


class TestOutOfBandBuffers:
    def test_large_array_travels_out_of_band_exactly_once(self, numpy):
        array = numpy.arange(250_000, dtype=numpy.float32)  # 1 MB
        body, frames = serde.serialize(array)

        assert len(frames) == 1, "array should produce exactly one frame"
        assert memoryview(frames[0]).nbytes == array.nbytes
        assert len(body) < 1024, (
            f"pickle body is {len(body)} bytes: the array was inlined as well as "
            "sent out-of-band, doubling the payload"
        )
        assert serde.payload_size(body, frames) < array.nbytes + 1024

    def test_small_array_stays_inline(self, numpy):
        array = numpy.arange(4, dtype=numpy.int8)
        body, frames = serde.serialize(array)
        assert frames == []
        assert numpy.array_equal(serde.deserialize(body), array)

    def test_threshold_is_configurable(self, numpy):
        array = numpy.zeros(1000, dtype=numpy.uint8)
        _, small_threshold = serde.serialize(array, min_oob_size=512)
        _, large_threshold = serde.serialize(array, min_oob_size=8192)
        assert len(small_threshold) == 1
        assert len(large_threshold) == 0

    def test_several_arrays_produce_several_frames(self, numpy):
        payload = {
            "weights": numpy.zeros(100_000, dtype=numpy.float32),
            "bias": numpy.ones(50_000, dtype=numpy.float64),
            "step": 7,
        }
        _body, frames = serde.serialize(payload)
        assert len(frames) == 2
        restored = wire_roundtrip(payload)
        assert restored["step"] == 7
        assert numpy.array_equal(restored["weights"], payload["weights"])
        assert numpy.array_equal(restored["bias"], payload["bias"])

    def test_frame_order_is_preserved(self, numpy):
        arrays = [numpy.full(10_000, i, dtype=numpy.int32) for i in range(5)]
        restored = wire_roundtrip(arrays)
        for index, array in enumerate(restored):
            assert array[0] == index


class TestZeroCopyOnDeserialize:
    def test_restored_array_views_rust_memory(self, numpy):
        array = numpy.arange(100_000, dtype=numpy.int64)
        body, frames = serde.serialize(array)
        header, decoded = decode_message(encode_message(body, frames))
        restored = serde.deserialize(header, decoded)

        assert numpy.array_equal(restored, array)
        assert not restored.flags.owndata, "must be a view onto the frame"
        assert not restored.flags.writeable, "shared buffers must be read-only"

    def test_read_only_result_is_documented_not_accidental(self, numpy):
        restored = wire_roundtrip(numpy.arange(100_000, dtype=numpy.int64))
        with pytest.raises(ValueError, match="read-only"):
            restored[0] = 1
        # The documented escape hatch.
        assert restored.copy().flags.writeable


class TestErrors:
    def test_unpicklable_value_raises_serialization_error(self):
        import threading

        with pytest.raises(serde.SerializationError, match="failed to serialise"):
            serde.serialize(threading.Lock())

    def test_corrupt_body_raises_serialization_error(self):
        with pytest.raises(serde.SerializationError, match="failed to deserialise"):
            serde.deserialize(b"not a pickle at all")

    def test_missing_frames_raise_serialization_error(self, numpy):
        body, frames = serde.serialize(numpy.zeros(100_000, dtype=numpy.uint8))
        assert frames, "precondition: value must use out-of-band buffers"
        with pytest.raises(serde.SerializationError):
            serde.deserialize(body, [])


def test_protocol_is_pinned():
    # Out-of-band buffers require protocol 5. Silently dropping to 4 would still
    # work but would memcpy every tensor through the pickle stream.
    assert serde.PROTOCOL == 5
    assert serde.PROTOCOL <= pickle.HIGHEST_PROTOCOL
