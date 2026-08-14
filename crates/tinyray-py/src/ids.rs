//! Python bindings for tinyray identifiers.

use std::str::FromStr;

use pyo3::basic::CompareOp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tinyray_core::ids::Id;

/// A 128-bit tinyray identifier.
#[pyclass(module = "tinyray._tinyray", name = "Id", frozen)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyId {
    pub(crate) inner: Id,
}

impl PyId {
    pub(crate) fn from_inner(inner: Id) -> PyId {
        PyId { inner }
    }
}

#[pymethods]
impl PyId {
    #[new]
    fn py_new(value: &str) -> PyResult<PyId> {
        Id::from_str(value)
            .map(|inner| PyId { inner })
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    #[staticmethod]
    fn nil() -> PyId {
        PyId { inner: Id::NIL }
    }

    #[getter]
    fn hex(&self) -> String {
        self.inner.to_hex()
    }

    fn is_nil(&self) -> bool {
        self.inner.is_nil()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("Id('{}')", self.inner)
    }

    fn __hash__(&self) -> u64 {
        self.inner.hi ^ self.inner.lo.rotate_left(17)
    }

    fn __richcmp__(&self, other: &PyId, op: CompareOp) -> bool {
        op.matches(self.inner.cmp(&other.inner))
    }
}

/// Allocate a fresh, process-unique identifier.
#[pyfunction]
pub fn new_id() -> PyId {
    PyId {
        inner: Id::generate(),
    }
}
