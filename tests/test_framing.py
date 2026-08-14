"""Framing tests at the Python boundary.

The exhaustive codec tests live in Rust (``cargo test -p tinyray-core``). What
matters here is that the binding layer preserves those guarantees instead of
quietly copying, truncating, or swallowing errors.
"""

from __future__ import annotations

import pytest

import tinyray
from tinyray import Decoder, Frame, Limits, decode_message, encode_message


def test_roundtrip_header_only():
    encoded = encode_message(b"just-a-header", [])
    header, frames = decode_message(encoded)
    assert header == b"just-a-header"
    assert list(frames) == []


def test_roundtrip_with_frames():
    payloads = [b"alpha", b"", b"gamma" * 100]
    encoded = encode_message(b"hdr", payloads)
    header, frames = decode_message(encoded)
    assert header == b"hdr"
    assert [bytes(f.to_bytes()) for f in frames] == payloads


def test_frames_are_returned_as_zero_copy_views():
    encoded = encode_message(b"h", [b"x" * 1024])
    _, frames = decode_message(encoded)
    assert isinstance(frames[0], Frame)
    view = memoryview(frames[0])
    assert view.readonly, "shared result buffers must not be writable"
    assert view.nbytes == 1024


def test_wire_layout_is_stable():
    # The header is part of the protocol contract; pin it down so an accidental
    # change to the Rust side cannot pass silently.
    encoded = encode_message(b"AB", [b"xyz"])
    assert encoded[0:4] == b"TRY1"
    assert encoded[4:8] == (2).to_bytes(4, "big")
    assert encoded[8:12] == (1).to_bytes(4, "big")
    assert encoded[12:16] == (3).to_bytes(4, "big")
    assert encoded[16:] == b"ABxyz"


def test_accepts_any_buffer_like_input(numpy):
    array = numpy.arange(64, dtype=numpy.int32)
    encoded = encode_message(memoryview(b"hdr"), [array, bytearray(b"raw")])
    header, frames = decode_message(encoded)
    assert header == b"hdr"
    assert frames[0].to_bytes() == array.tobytes()
    assert frames[1].to_bytes() == b"raw"


def test_frame_can_be_reencoded_without_copying():
    encoded = encode_message(b"h", [b"payload" * 100])
    _, frames = decode_message(encoded)
    # Feeding a Frame straight back in must not go through a Python copy.
    again = encode_message(b"h", frames)
    assert again == encoded


def test_incomplete_buffer_is_rejected():
    encoded = encode_message(b"header", [b"frame"])
    with pytest.raises(ValueError, match="incomplete"):
        decode_message(encoded[:-1])


def test_trailing_bytes_are_rejected():
    encoded = encode_message(b"header", [b"frame"])
    with pytest.raises(ValueError, match="trailing"):
        decode_message(encoded + b"extra")


def test_bad_magic_raises_protocol_error():
    encoded = bytearray(encode_message(b"header", []))
    encoded[0:4] = b"XXXX"
    with pytest.raises(tinyray.ProtocolError, match="magic"):
        decode_message(bytes(encoded))


def test_limits_are_enforced_and_reported():
    tiny = Limits(max_header_len=8)
    with pytest.raises(tinyray.MessageTooLarge, match="header too large"):
        encode_message(b"a much longer header than eight bytes", [], tiny)


def test_message_too_large_is_a_protocol_error():
    # The exception hierarchy is part of the API: callers should be able to
    # catch the broad category.
    assert issubclass(tinyray.MessageTooLarge, tinyray.ProtocolError)
    assert issubclass(tinyray.ProtocolError, tinyray.TinyrayError)


class TestStreamingDecoder:
    def test_byte_at_a_time(self):
        encoded = encode_message(b"header", [b"one", b"two"])
        decoder = Decoder()
        for index, byte in enumerate(encoded):
            decoder.feed(bytes([byte]))
            message = decoder.next_message()
            if index + 1 < len(encoded):
                assert message is None, f"completed early at byte {index}"
            else:
                header, frames = message
                assert header == b"header"
                assert [f.to_bytes() for f in frames] == [b"one", b"two"]

    def test_several_messages_in_one_chunk(self):
        blob = b"".join(encode_message(f"h{i}".encode(), [f"f{i}".encode()]) for i in range(3))
        decoder = Decoder()
        decoder.feed(blob)
        seen = []
        while (message := decoder.next_message()) is not None:
            seen.append(message[0])
        assert seen == [b"h0", b"h1", b"h2"]
        assert decoder.buffered == 0
        assert decoder.at_message_boundary

    def test_boundary_flag_tracks_partial_messages(self):
        encoded = encode_message(b"header", [b"frame"])
        decoder = Decoder()
        assert decoder.at_message_boundary
        decoder.feed(encoded[:14])
        assert decoder.next_message() is None
        assert not decoder.at_message_boundary
        decoder.feed(encoded[14:])
        assert decoder.next_message() is not None
        assert decoder.at_message_boundary

    def test_decoder_poisons_on_protocol_error(self):
        decoder = Decoder()
        decoder.feed(b"XXXX" + (0).to_bytes(4, "big") + (0).to_bytes(4, "big"))
        with pytest.raises(tinyray.ProtocolError):
            decoder.next_message()
        assert decoder.poisoned
        # A poisoned decoder must keep failing rather than resynchronising on
        # attacker-controlled bytes.
        with pytest.raises(tinyray.ProtocolError):
            decoder.next_message()


def test_ten_megabyte_payload_survives():
    payload = b"\xab" * (10 * 1024 * 1024)
    encoded = encode_message(b"result", [payload])
    header, frames = decode_message(encoded)
    assert header == b"result"
    assert memoryview(frames[0]).nbytes == len(payload)
    assert frames[0].to_bytes() == payload
