"""Tests for the Rust/Python memory boundary.

These are the tests that justify the Rust core. The design doc commits to two
specific properties, and both are easy to regress without noticing:

* results handed from Rust to Python are **shared, not copied**;
* arguments handed from Python to Rust are **copied**, so that a caller
  mutating its numpy array after a non-blocking ``.remote()`` cannot corrupt an
  in-flight message.
"""

from __future__ import annotations

import pytest

from tinyray import Frame, decode_message, encode_message

from .conftest import buffer_address


class TestFrameBufferProtocol:
    def test_supports_memoryview(self):
        frame = Frame(b"payload")
        view = memoryview(frame)
        assert view.nbytes == 7
        assert view.format == "B"
        assert view.ndim == 1
        assert bytes(view) == b"payload"

    def test_is_read_only(self):
        frame = Frame(b"payload")
        assert memoryview(frame).readonly
        with pytest.raises(TypeError):
            memoryview(frame)[0] = 1

    def test_writable_request_is_refused(self):
        # Exercise the PyBUF_WRITABLE branch directly: higher-level APIs like
        # numpy or ctypes pre-check the read-only flag and never reach it, so
        # the only way to cover our refusal is to call PyObject_GetBuffer.
        import ctypes

        class PyBuffer(ctypes.Structure):
            _fields_ = [
                ("buf", ctypes.c_void_p),
                ("obj", ctypes.c_void_p),
                ("len", ctypes.c_ssize_t),
                ("itemsize", ctypes.c_ssize_t),
                ("readonly", ctypes.c_int),
                ("ndim", ctypes.c_int),
                ("format", ctypes.c_char_p),
                ("shape", ctypes.POINTER(ctypes.c_ssize_t)),
                ("strides", ctypes.POINTER(ctypes.c_ssize_t)),
                ("suboffsets", ctypes.POINTER(ctypes.c_ssize_t)),
                ("internal", ctypes.c_void_p),
            ]

        PyBUF_WRITABLE = 0x0001
        get_buffer = ctypes.pythonapi.PyObject_GetBuffer
        get_buffer.restype = ctypes.c_int
        get_buffer.argtypes = [ctypes.py_object, ctypes.POINTER(PyBuffer), ctypes.c_int]

        frame = Frame(b"payload")
        view = PyBuffer()
        with pytest.raises(BufferError, match="read-only"):
            get_buffer(frame, ctypes.byref(view), PyBUF_WRITABLE)

    def test_read_only_contract_holds_for_normal_apis(self, numpy):
        frame = Frame(b"payload")
        assert memoryview(frame).readonly
        assert not numpy.frombuffer(frame, dtype=numpy.uint8).flags.writeable
        with pytest.raises(TypeError):
            import ctypes

            ctypes.c_char.from_buffer(frame)

    def test_len_and_repr(self):
        frame = Frame(b"abcd")
        assert len(frame) == 4
        assert repr(frame) == "Frame(len=4)"

    def test_empty_frame(self):
        frame = Frame(b"")
        assert len(frame) == 0
        assert memoryview(frame).nbytes == 0
        assert frame.to_bytes() == b""


class TestRustToPythonIsZeroCopy:
    def test_numpy_views_rust_memory(self, numpy):
        payload = numpy.arange(4096, dtype=numpy.int64)
        encoded = encode_message(b"h", [payload])
        _, frames = decode_message(encoded)

        restored = numpy.frombuffer(frames[0], dtype=numpy.int64)
        assert numpy.array_equal(restored, payload)
        assert not restored.flags.owndata, "array must be a view, not a copy"
        assert not restored.flags.writeable

    def test_two_readers_share_one_allocation(self, numpy):
        encoded = encode_message(b"h", [b"z" * 8192])
        _, frames = decode_message(encoded)
        frame = frames[0]
        # Serving the same result to several consumers must not duplicate it.
        assert buffer_address(frame) == buffer_address(frame)
        first = numpy.frombuffer(frame, dtype=numpy.uint8)
        second = numpy.frombuffer(frame, dtype=numpy.uint8)
        assert first.__array_interface__["data"][0] == second.__array_interface__["data"][0]

    def test_frame_outlives_the_buffer_it_was_decoded_from(self, numpy):
        # The decoded frame keeps its own reference to the Rust allocation, so
        # dropping the wire buffer must not invalidate it.
        payload = numpy.arange(2048, dtype=numpy.int32)
        encoded = encode_message(b"h", [payload])
        _, frames = decode_message(encoded)
        frame = frames[0]
        del encoded
        import gc

        gc.collect()
        restored = numpy.frombuffer(frame, dtype=numpy.int32)
        assert numpy.array_equal(restored, payload)

    def test_reencoding_a_frame_shares_the_allocation(self):
        _, frames = decode_message(encode_message(b"h", [b"q" * 4096]))
        original = frames[0]
        # Round-tripping a Frame back through the encoder must reuse the same
        # Rust buffer rather than copying it through Python.
        _, again = decode_message(encode_message(b"h", [original]))
        assert again[0].to_bytes() == original.to_bytes()


class TestPythonToRustCopies:
    def test_mutating_the_source_after_encoding_is_safe(self, numpy):
        # This is the exact hazard the copy policy exists for: `.remote()` does
        # not block, so user code is free to mutate its arguments immediately
        # afterwards.
        source = numpy.zeros(4096, dtype=numpy.uint8)
        encoded = encode_message(b"h", [source])
        source[:] = 0xFF

        _, frames = decode_message(encoded)
        restored = numpy.frombuffer(frames[0], dtype=numpy.uint8)
        assert restored.max() == 0, "message must not observe post-encode mutation"

    def test_frame_does_not_alias_its_python_source(self, numpy):
        source = numpy.arange(4096, dtype=numpy.uint8)
        frame = Frame(source)
        assert buffer_address(frame) != buffer_address(source)
        source[:] = 0
        assert memoryview(frame)[10] == 10

    def test_non_contiguous_input_is_refused(self, numpy):
        strided = numpy.arange(1024, dtype=numpy.int64)[::2]
        assert not strided.flags.c_contiguous
        with pytest.raises(BufferError, match="contiguous"):
            Frame(strided)

    def test_non_buffer_input_is_refused(self):
        with pytest.raises(BufferError):
            Frame(12345)
        with pytest.raises(BufferError):
            Frame(None)


class TestFrameAcceptsCommonBufferTypes:
    @pytest.mark.parametrize(
        "value",
        [b"bytes", bytearray(b"bytearray"), memoryview(b"memoryview")],
        ids=["bytes", "bytearray", "memoryview"],
    )
    def test_roundtrip(self, value):
        expected = bytes(value)
        assert Frame(value).to_bytes() == expected

    def test_frame_from_frame_shares_memory(self):
        original = Frame(b"payload" * 1000)
        clone = Frame(original)
        assert buffer_address(clone) == buffer_address(original)
