//! The one place in tinyray where Python memory and Rust memory meet.
//!
//! Everything `unsafe` in the Python bindings lives here, deliberately, so the
//! rest of the codebase can be reviewed as ordinary safe Rust.
//!
//! # Copy policy
//!
//! * **Python -> Rust copies once.** `.remote()` is non-blocking, so the actual
//!   send happens after the call returns and the caller is free to mutate the
//!   numpy array it just passed. Borrowing that memory would be a data race, so
//!   we take a copy at the boundary. One 10 MB memcpy costs well under a
//!   millisecond against a 200 ms task; correctness is worth far more.
//! * **Rust -> Python does not copy.** Result buffers are immutable [`Bytes`],
//!   so a [`Frame`] can expose them through the Python buffer protocol and
//!   `pickle.loads(..., buffers=...)` will build numpy arrays that view Rust
//!   memory directly. This is the direction that actually matters: one produced
//!   result is served to many consumers.
//!
//! Arrays reconstructed this way are read-only, the same guarantee Ray gives
//! for objects served out of its store.

use std::mem::MaybeUninit;
use std::os::raw::{c_int, c_void};

use bytes::Bytes;
use pyo3::exceptions::PyBufferError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{ffi, PyErr};

/// A read-only view of a Rust-owned buffer, exposed to Python with no copy.
///
/// Cloning a `Frame` (or handing it to several consumers) only bumps a
/// refcount; the underlying bytes are never duplicated.
#[pyclass(module = "tinyray._tinyray", frozen)]
#[derive(Clone)]
pub struct Frame {
    pub(crate) inner: Bytes,
}

impl Frame {
    pub fn new(inner: Bytes) -> Frame {
        Frame { inner }
    }

    pub fn bytes(&self) -> &Bytes {
        &self.inner
    }
}

#[pymethods]
impl Frame {
    #[new]
    fn py_new(data: &Bound<'_, PyAny>) -> PyResult<Frame> {
        Ok(Frame {
            inner: bytes_from_py(data)?,
        })
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        format!("Frame(len={})", self.inner.len())
    }

    /// Copy the contents out into a regular Python `bytes` object.
    ///
    /// Only for tests and small payloads; the whole point of `Frame` is to
    /// avoid this.
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner)
    }

    /// Expose the buffer protocol so `memoryview(frame)` and
    /// `pickle.loads(body, buffers=[frame, ...])` work without copying.
    unsafe fn __getbuffer__(
        slf: &Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("null buffer view"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err(
                "tinyray frames are read-only; copy the array if you need to mutate it",
            ));
        }
        let borrowed = slf.get();
        // SAFETY: `PyBuffer_FillInfo` takes a strong reference to `slf`, so the
        // `Bytes` (and therefore the pointer we hand out) stays alive for as
        // long as the view does. `Bytes` is immutable, so the read-only
        // promise below holds.
        let rc = unsafe {
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                borrowed.inner.as_ptr() as *mut c_void,
                borrowed.inner.len() as ffi::Py_ssize_t,
                1, // readonly
                flags,
            )
        };
        if rc == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }
}

/// Copy any Python buffer-like object (`bytes`, `bytearray`, `memoryview`,
/// numpy array of any dtype, `PickleBuffer`) into Rust-owned [`Bytes`].
///
/// See the module docs for why this copies rather than borrows.
pub fn bytes_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    // Fast path: a Frame is already Rust-owned and immutable, so we can share
    // the allocation instead of copying it.
    if let Ok(frame) = obj.cast::<Frame>() {
        return Ok(frame.get().inner.clone());
    }
    if let Ok(py_bytes) = obj.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(py_bytes.as_bytes()));
    }

    // `PyBUF_SIMPLE` asks for the raw bytes of a C-contiguous buffer and
    // ignores the element type, which is what we want: a float32 tensor and a
    // bytes object are both just bytes on the wire. Requesting a typed buffer
    // instead would reject every numpy dtype that is not uint8.
    let mut view = MaybeUninit::<ffi::Py_buffer>::uninit();
    // SAFETY: `view` is valid for writes; on success CPython fully initialises
    // it and we hand ownership straight to `BufferGuard`.
    let rc = unsafe { ffi::PyObject_GetBuffer(obj.as_ptr(), view.as_mut_ptr(), ffi::PyBUF_SIMPLE) };
    if rc != 0 {
        // Normalise TypeError/BufferError/ValueError from various exporters
        // into one exception type, keeping CPython's more specific message.
        let err = PyErr::fetch(obj.py());
        let detail = err.value(obj.py()).to_string();
        let type_name = obj
            .get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "object".to_string());
        return Err(PyBufferError::new_err(format!(
            "expected a contiguous bytes-like object, got {type_name}: {detail}"
        )));
    }
    // SAFETY: `PyObject_GetBuffer` returned success, so the view is initialised
    // and must be released exactly once, which the guard does on drop.
    let guard = BufferGuard(unsafe { view.assume_init() });

    let len = guard.0.len;
    if len < 0 {
        return Err(PyBufferError::new_err("buffer reported a negative length"));
    }
    if len == 0 {
        return Ok(Bytes::new());
    }
    if guard.0.buf.is_null() {
        return Err(PyBufferError::new_err("buffer reported a null pointer"));
    }
    // SAFETY: CPython guarantees `buf` points to `len` readable bytes for as
    // long as the view is held, and `PyBUF_SIMPLE` guarantees C-contiguity.
    let slice = unsafe { std::slice::from_raw_parts(guard.0.buf as *const u8, len as usize) };
    Ok(Bytes::copy_from_slice(slice))
}

/// Releases a `Py_buffer` on drop so every early return stays leak-free.
struct BufferGuard(ffi::Py_buffer);

impl Drop for BufferGuard {
    fn drop(&mut self) {
        // SAFETY: constructed only from a successful `PyObject_GetBuffer`, and
        // released exactly once because the guard owns the view. The GIL is
        // held: `bytes_from_py` is only ever called from a `#[pyfunction]`.
        unsafe { ffi::PyBuffer_Release(&mut self.0) };
    }
}

/// Convert a Python sequence of buffer-like objects into frames.
pub fn frames_from_py(seq: &Bound<'_, PyAny>) -> PyResult<Vec<Bytes>> {
    let mut frames = Vec::new();
    for item in seq.try_iter()? {
        frames.push(bytes_from_py(&item?)?);
    }
    Ok(frames)
}
