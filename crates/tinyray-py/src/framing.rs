//! Python bindings for the wire framing.

use bytes::{Bytes, BytesMut};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use tinyray_core::framing::{Decoder as CoreDecoder, Message};
use tinyray_core::Limits;

use crate::buffers::{bytes_from_py, frames_from_py, Frame};
use crate::errors::frame_error_to_py;

/// Limits applied by a [`Decoder`], exposed so tests can shrink them.
#[pyclass(module = "tinyray._tinyray", name = "Limits", frozen)]
#[derive(Clone, Copy)]
pub struct PyLimits {
    pub(crate) inner: Limits,
}

#[pymethods]
impl PyLimits {
    #[new]
    #[pyo3(signature = (max_header_len=None, max_frames=None, max_frame_len=None, max_message_len=None))]
    fn py_new(
        max_header_len: Option<u64>,
        max_frames: Option<u64>,
        max_frame_len: Option<u64>,
        max_message_len: Option<u64>,
    ) -> PyLimits {
        let d = Limits::DEFAULT;
        PyLimits {
            inner: Limits {
                max_header_len: max_header_len.unwrap_or(d.max_header_len),
                max_frames: max_frames.unwrap_or(d.max_frames),
                max_frame_len: max_frame_len.unwrap_or(d.max_frame_len),
                max_message_len: max_message_len.unwrap_or(d.max_message_len),
            },
        }
    }

    #[staticmethod]
    fn default() -> PyLimits {
        PyLimits {
            inner: Limits::DEFAULT,
        }
    }

    #[getter]
    fn max_header_len(&self) -> u64 {
        self.inner.max_header_len
    }
    #[getter]
    fn max_frames(&self) -> u64 {
        self.inner.max_frames
    }
    #[getter]
    fn max_frame_len(&self) -> u64 {
        self.inner.max_frame_len
    }
    #[getter]
    fn max_message_len(&self) -> u64 {
        self.inner.max_message_len
    }

    fn __repr__(&self) -> String {
        format!(
            "Limits(max_header_len={}, max_frames={}, max_frame_len={}, max_message_len={})",
            self.inner.max_header_len,
            self.inner.max_frames,
            self.inner.max_frame_len,
            self.inner.max_message_len
        )
    }
}

/// Encode a header plus frames into a single wire buffer.
#[pyfunction]
#[pyo3(signature = (header, frames, limits=None))]
pub fn encode_message<'py>(
    py: Python<'py>,
    header: &Bound<'py, PyAny>,
    frames: &Bound<'py, PyAny>,
    limits: Option<PyLimits>,
) -> PyResult<Bound<'py, PyBytes>> {
    let header = bytes_from_py(header)?;
    let frames = frames_from_py(frames)?;
    let limits = limits.map(|l| l.inner).unwrap_or(Limits::DEFAULT);
    let message = Message::new(header, frames);
    let encoded = py
        .detach(|| message.encode_to_vec(&limits))
        .map_err(frame_error_to_py)?;
    Ok(PyBytes::new(py, &encoded))
}

/// Decode exactly one message from a complete buffer.
///
/// Returns `(header_bytes, [Frame, ...])`. The frames are zero-copy views of
/// Rust memory; call `bytes(frame)` if you really need a copy.
#[pyfunction]
#[pyo3(signature = (data, limits=None))]
pub fn decode_message<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
    limits: Option<PyLimits>,
) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, PyList>)> {
    let raw = bytes_from_py(data)?;
    let limits = limits.map(|l| l.inner).unwrap_or(Limits::DEFAULT);

    let decoded = py.detach(|| {
        let mut buf = BytesMut::from(&raw[..]);
        let mut decoder = CoreDecoder::new(limits);
        decoder
            .decode(&mut buf)
            .map(|opt| opt.map(|m| (m, buf.len())))
    });

    match decoded.map_err(frame_error_to_py)? {
        None => Err(PyValueError::new_err(
            "incomplete message: buffer does not contain a whole frame",
        )),
        Some((message, leftover)) => {
            if leftover != 0 {
                return Err(PyValueError::new_err(format!(
                    "trailing garbage: {leftover} bytes left after the message"
                )));
            }
            Ok(message_to_py(py, message)?)
        }
    }
}

fn message_to_py<'py>(
    py: Python<'py>,
    message: Message,
) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, PyList>)> {
    let header = PyBytes::new(py, &message.header);
    let frames = PyList::new(
        py,
        message
            .frames
            .into_iter()
            .map(|f| Frame::new(f).into_pyobject(py))
            .collect::<Result<Vec<_>, _>>()?,
    )?;
    Ok((header, frames))
}

/// Incremental decoder mirroring the Rust one, for feeding partial reads.
#[pyclass(module = "tinyray._tinyray", name = "Decoder", unsendable)]
pub struct PyDecoder {
    inner: CoreDecoder,
    buf: BytesMut,
}

#[pymethods]
impl PyDecoder {
    #[new]
    #[pyo3(signature = (limits=None))]
    fn py_new(limits: Option<PyLimits>) -> PyDecoder {
        PyDecoder {
            inner: CoreDecoder::new(limits.map(|l| l.inner).unwrap_or(Limits::DEFAULT)),
            buf: BytesMut::new(),
        }
    }

    /// Append bytes to the internal buffer.
    fn feed(&mut self, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let chunk: Bytes = bytes_from_py(data)?;
        self.buf.extend_from_slice(&chunk);
        Ok(())
    }

    /// Pop the next complete message, or `None` if more bytes are needed.
    fn next_message<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Option<(Bound<'py, PyBytes>, Bound<'py, PyList>)>> {
        let decoded = self
            .inner
            .decode(&mut self.buf)
            .map_err(frame_error_to_py)?;
        match decoded {
            None => Ok(None),
            Some(message) => Ok(Some(message_to_py(py, message)?)),
        }
    }

    /// Bytes buffered but not yet consumed.
    #[getter]
    fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// True when no partial message is in flight.
    #[getter]
    fn at_message_boundary(&self) -> bool {
        self.inner.is_at_message_boundary()
    }

    #[getter]
    fn poisoned(&self) -> bool {
        self.inner.is_poisoned()
    }
}
