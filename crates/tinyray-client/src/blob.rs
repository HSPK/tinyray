use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyBufferError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyType};
use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

pyo3::create_exception!(
    _tinyray,
    BlobError,
    PyRuntimeError,
    "A BlobRef could not be created, validated, mapped, or accessed."
);

/// A read-only same-host shared-memory payload backed by a sealed Linux memfd.
#[pyclass(name = "BlobRef", module = "tinyray._tinyray", weakref)]
pub struct PyBlobRef {
    state: Arc<PyBlobState>,
}

struct PyBlobState {
    inner: Mutex<Option<tinyray::BlobRef>>,
    exports: AtomicUsize,
    forked: AtomicBool,
}

fn blob_states() -> &'static Mutex<Vec<Weak<PyBlobState>>> {
    static STATES: OnceLock<Mutex<Vec<Weak<PyBlobState>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

impl PyBlobRef {
    fn new(inner: tinyray::BlobRef) -> Self {
        let state = Arc::new(PyBlobState {
            inner: Mutex::new(Some(inner)),
            exports: AtomicUsize::new(0),
            forked: AtomicBool::new(false),
        });
        let mut states = blob_states().lock().unwrap();
        states.retain(|state| state.strong_count() != 0);
        states.push(Arc::downgrade(&state));
        drop(states);
        Self { state }
    }

    fn error(error: tinyray::BlobError) -> PyErr {
        BlobError::new_err(error.to_string())
    }

    pub(crate) fn clone_inner(&self) -> PyResult<tinyray::BlobRef> {
        self.state
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| BlobError::new_err("BlobRef is closed"))
    }
}

#[pymethods]
impl PyBlobRef {
    #[classmethod]
    #[pyo3(signature = (data, max_bytes=tinyray::DEFAULT_MAX_BLOB_BYTES))]
    fn create(
        _class: &Bound<'_, PyType>,
        data: &Bound<'_, PyAny>,
        max_bytes: usize,
    ) -> PyResult<Self> {
        let buffer = PyBuffer::<u8>::get_bound(data)?;
        if !buffer.is_c_contiguous() {
            return Err(PyBufferError::new_err(
                "BlobRef input must be a contiguous bytes-like object",
            ));
        }
        let bytes = unsafe {
            std::slice::from_raw_parts(buffer.buf_ptr().cast::<u8>(), buffer.len_bytes())
        };
        tinyray::BlobRef::from_bytes_with_limit(bytes, max_bytes)
            .map(Self::new)
            .map_err(Self::error)
    }

    #[classmethod]
    #[pyo3(signature = (descriptor, max_bytes=tinyray::DEFAULT_MAX_BLOB_BYTES))]
    fn from_descriptor(
        _class: &Bound<'_, PyType>,
        descriptor: PyBackedBytes,
        max_bytes: usize,
    ) -> PyResult<Self> {
        tinyray::BlobRef::open_descriptor_with_limit(&descriptor, max_bytes)
            .map(Self::new)
            .map_err(Self::error)
    }

    fn descriptor(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let guard = self.state.inner.lock().unwrap();
        let blob = guard
            .as_ref()
            .ok_or_else(|| BlobError::new_err("BlobRef is closed"))?;
        let descriptor = blob.descriptor_bytes().map_err(Self::error)?;
        Ok(PyBytes::new_bound(py, &descriptor).unbind())
    }

    fn close(&self) -> PyResult<()> {
        if self.state.exports.load(Ordering::Acquire) != 0 {
            return Err(PyBufferError::new_err(
                "cannot close BlobRef while a memoryview is exported",
            ));
        }
        self.state.inner.lock().unwrap().take();
        Ok(())
    }

    fn _after_fork_close(&self) {
        self.state.forked.store(true, Ordering::Release);
        if self.state.exports.load(Ordering::Acquire) == 0 {
            self.state.inner.lock().unwrap().take();
        }
    }

    #[getter]
    fn closed(&self) -> bool {
        self.state.inner.lock().unwrap().is_none()
    }

    fn __len__(&self) -> PyResult<usize> {
        self.state
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .map(tinyray::BlobRef::len)
            .ok_or_else(|| BlobError::new_err("BlobRef is closed"))
    }

    fn __bytes__(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let guard = self.state.inner.lock().unwrap();
        let blob = guard
            .as_ref()
            .ok_or_else(|| BlobError::new_err("BlobRef is closed"))?;
        Ok(PyBytes::new_bound(py, blob.as_slice().map_err(Self::error)?).unbind())
    }

    fn view(slf: Bound<'_, Self>) -> PyResult<PyObject> {
        let py = slf.py();
        let builtins = py.import_bound("builtins")?;
        Ok(builtins
            .getattr("memoryview")?
            .call1((slf.into_any(),))?
            .unbind())
    }

    fn _clone(&self) -> PyResult<Self> {
        self.clone_inner().map(Self::new)
    }

    fn __enter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    #[pyo3(signature = (_type=None, _value=None, _traceback=None))]
    fn __exit__(
        &self,
        _type: Option<&Bound<'_, PyAny>>,
        _value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        let guard = self.state.inner.lock().unwrap();
        format!(
            "<BlobRef len={}{}>",
            guard.as_ref().map_or(0, tinyray::BlobRef::len),
            if guard.is_none() { " closed" } else { "" }
        )
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("buffer view is null"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err("BlobRef is read-only"));
        }
        let borrowed = slf.borrow();
        let guard = borrowed.state.inner.lock().unwrap();
        let blob = guard
            .as_ref()
            .ok_or_else(|| BlobError::new_err("BlobRef is closed"))?;
        let data = blob.as_slice().map_err(Self::error)?;
        borrowed.state.exports.fetch_add(1, Ordering::AcqRel);

        (*view).obj = slf.into_any().into_ptr();
        (*view).buf = data.as_ptr() as *mut c_void;
        (*view).len = data.len() as isize;
        (*view).readonly = 1;
        (*view).itemsize = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            CString::new("B").unwrap().into_raw()
        } else {
            ptr::null_mut()
        };
        (*view).ndim = 1;
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            &mut (*view).len
        } else {
            ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            &mut (*view).itemsize
        } else {
            ptr::null_mut()
        };
        (*view).suboffsets = ptr::null_mut();
        (*view).internal = ptr::null_mut();
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        if !view.is_null() && !(*view).format.is_null() {
            drop(CString::from_raw((*view).format));
        }
        let previous = self.state.exports.fetch_sub(1, Ordering::AcqRel);
        if previous == 1 && self.state.forked.load(Ordering::Acquire) {
            self.state.inner.lock().unwrap().take();
        }
    }
}

#[pyfunction]
fn blob_close_after_fork() {
    let mut states = blob_states().lock().unwrap();
    states.retain(|state| {
        let Some(state) = state.upgrade() else {
            return false;
        };
        state.forked.store(true, Ordering::Release);
        if state.exports.load(Ordering::Acquire) == 0 {
            state.inner.lock().unwrap().take();
        }
        true
    });
}

pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("BlobError", module.py().get_type_bound::<BlobError>())?;
    module.add("BLOB_EXT_CODE", tinyray::BLOB_EXT_CODE)?;
    module.add("BLOB_MAX_BYTES", tinyray::DEFAULT_MAX_BLOB_BYTES)?;
    module.add(
        "BLOB_MAX_REFS_PER_MESSAGE",
        tinyray::MAX_BLOB_REFS_PER_MESSAGE,
    )?;
    module.add(
        "BLOB_MAX_MAPPED_BYTES_PER_MESSAGE",
        tinyray::MAX_BLOB_MAPPED_BYTES_PER_MESSAGE,
    )?;
    module.add(
        "BLOB_MAX_DECODED_HANDLES",
        tinyray::MAX_DECODED_BLOB_HANDLES,
    )?;
    module.add(
        "BLOB_MAX_DECODED_MAPPINGS",
        tinyray::MAX_DECODED_BLOB_MAPPINGS,
    )?;
    module.add("BLOB_MAX_DECODED_BYTES", tinyray::MAX_DECODED_BLOB_BYTES)?;
    module.add_class::<PyBlobRef>()?;
    module.add_function(wrap_pyfunction!(blob_close_after_fork, module)?)?;
    Ok(())
}
