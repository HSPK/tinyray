//! Python bindings for the driver-side client.
//!
//! `.remote()` lands here and returns immediately; `get()` blocks in Rust with
//! the GIL released, so a driver waiting on 32 rollouts is not sitting on the
//! interpreter.

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use tinyray_core::ActorId;
use tinyray_runtime::client::{ClientError, ClientRuntime, OwnerRef};
use tinyray_runtime::transport::client::ClientConfig;

use crate::buffers::{bytes_from_py, frames_from_py, Frame};
use crate::errors::remote_error_to_py;
use crate::runtime::tokio_handle;

/// A reference to a result that lives in the actor that produced it.
///
/// Small and cheap to pass around: handing one to another actor is what keeps
/// large results off the driver.
#[pyclass(module = "tinyray._tinyray", name = "OwnerRef", frozen)]
#[derive(Clone)]
pub struct PyOwnerRef {
    pub(crate) inner: OwnerRef,
}

#[pymethods]
impl PyOwnerRef {
    #[new]
    fn py_new(task_id: &str, actor_id: &str, endpoint: &str) -> PyResult<PyOwnerRef> {
        Ok(PyOwnerRef {
            inner: OwnerRef {
                task_id: task_id
                    .parse()
                    .map_err(|_| PyValueError::new_err(format!("invalid task id: {task_id}")))?,
                actor_id: actor_id
                    .parse()
                    .map_err(|_| PyValueError::new_err(format!("invalid actor id: {actor_id}")))?,
                endpoint: endpoint.to_string(),
            },
        })
    }

    #[getter]
    fn task_id(&self) -> String {
        self.inner.task_id.to_string()
    }

    #[getter]
    fn actor_id(&self) -> String {
        self.inner.actor_id.to_string()
    }

    #[getter]
    fn endpoint(&self) -> String {
        self.inner.endpoint.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "OwnerRef(task={}, owner={})",
            &self.inner.task_id.to_string()[..8],
            self.inner.endpoint
        )
    }

    fn __hash__(&self) -> u64 {
        self.inner.task_id.0.hi ^ self.inner.task_id.0.lo.rotate_left(17)
    }

    fn __eq__(&self, other: &PyOwnerRef) -> bool {
        self.inner == other.inner
    }

    /// Pickle support, so a reference can be passed to another actor as an
    /// ordinary argument.
    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let class = py.get_type::<PyOwnerRef>();
        let args = PyTuple::new(
            py,
            [
                self.inner.task_id.to_string(),
                self.inner.actor_id.to_string(),
                self.inner.endpoint.clone(),
            ],
        )?;
        PyTuple::new(py, [class.into_any(), args.into_any()])
    }
}

/// Driver-side client: submit calls, fetch results, wait on batches.
#[pyclass(module = "tinyray._tinyray", name = "ClientRuntime")]
pub struct PyClientRuntime {
    inner: Arc<ClientRuntime>,
}

#[pymethods]
impl PyClientRuntime {
    #[new]
    #[pyo3(signature = (connections_per_peer=4, request_timeout_seconds=300.0, max_retries=16))]
    fn py_new(
        connections_per_peer: usize,
        request_timeout_seconds: f64,
        max_retries: usize,
    ) -> PyClientRuntime {
        PyClientRuntime {
            inner: ClientRuntime::new(ClientConfig {
                connections_per_peer,
                request_timeout: Duration::from_secs_f64(request_timeout_seconds),
                max_retries,
                ..Default::default()
            }),
        }
    }

    #[getter]
    fn caller_id(&self) -> String {
        self.inner.caller_id().to_string()
    }

    /// Record where an actor lives.
    fn register_actor(&self, actor_id: &str, endpoint: &str) -> PyResult<()> {
        self.inner
            .register_actor(parse_actor(actor_id)?, endpoint.to_string());
        Ok(())
    }

    fn forget_actor(&self, actor_id: &str) -> PyResult<()> {
        self.inner.forget_actor(parse_actor(actor_id)?);
        Ok(())
    }

    fn endpoint_of(&self, actor_id: &str) -> PyResult<Option<String>> {
        Ok(self.inner.endpoint_of(parse_actor(actor_id)?))
    }

    /// Submit a call. Returns as soon as the actor acknowledges it, without
    /// waiting for the method to run.
    fn submit(
        &self,
        py: Python<'_>,
        actor_id: &str,
        method: &str,
        body: &Bound<'_, PyAny>,
        frames: &Bound<'_, PyAny>,
    ) -> PyResult<PyOwnerRef> {
        let actor_id = parse_actor(actor_id)?;
        let body = bytes_from_py(body)?;
        let frames = frames_from_py(frames)?;
        let method = method.to_string();
        let inner = self.inner.clone();
        let handle = tokio_handle()?;

        let result = py.detach(move || {
            handle.block_on(async move { inner.submit(actor_id, &method, body, frames).await })
        });
        result
            .map(|inner| PyOwnerRef { inner })
            .map_err(client_error_to_py)
    }

    /// Fetch a result, blocking with the GIL released.
    #[pyo3(signature = (reference, timeout_seconds=300.0))]
    fn fetch<'py>(
        &self,
        py: Python<'py>,
        reference: &PyOwnerRef,
        timeout_seconds: f64,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyList>)> {
        let inner = self.inner.clone();
        let reference = reference.inner.clone();
        let handle = tokio_handle()?;
        let timeout = Duration::from_secs_f64(timeout_seconds);

        let value = py
            .detach(move || handle.block_on(async move { inner.fetch(&reference, timeout).await }))
            .map_err(client_error_to_py)?;

        let body = Frame::new(value.body).into_pyobject(py)?.into_any();
        let frames = PyList::new(
            py,
            value
                .frames
                .into_iter()
                .map(|f| Frame::new(f).into_pyobject(py))
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        Ok((body, frames))
    }

    /// Wait for `num_returns` references to settle.
    #[pyo3(signature = (refs, num_returns, timeout_seconds=300.0))]
    fn wait(
        &self,
        py: Python<'_>,
        refs: Vec<PyRef<'_, PyOwnerRef>>,
        num_returns: usize,
        timeout_seconds: f64,
    ) -> PyResult<(Vec<PyOwnerRef>, Vec<PyOwnerRef>)> {
        let owned: Vec<OwnerRef> = refs.iter().map(|r| r.inner.clone()).collect();
        let inner = self.inner.clone();
        let handle = tokio_handle()?;
        let timeout = Duration::from_secs_f64(timeout_seconds);

        let (ready, pending) = py.detach(move || {
            handle.block_on(async move { inner.wait(&owned, num_returns, timeout).await })
        });
        Ok((
            ready
                .into_iter()
                .map(|inner| PyOwnerRef { inner })
                .collect(),
            pending
                .into_iter()
                .map(|inner| PyOwnerRef { inner })
                .collect(),
        ))
    }

    /// Tell the owner a result is no longer needed. Best effort.
    fn release(&self, py: Python<'_>, reference: &PyOwnerRef) -> PyResult<()> {
        let inner = self.inner.clone();
        let reference = reference.inner.clone();
        let handle = tokio_handle()?;
        py.detach(move || handle.block_on(async move { inner.release(&reference).await }));
        Ok(())
    }

    /// Read a plain-text endpoint such as `/health` or `/introspect`.
    fn get_text(&self, py: Python<'_>, endpoint: &str, path: &str) -> PyResult<String> {
        let inner = self.inner.clone();
        let handle = tokio_handle()?;
        let endpoint = endpoint.to_string();
        let path = path.to_string();
        py.detach(move || {
            handle.block_on(async move { inner.transport().get_text(&endpoint, &path).await })
        })
        .map_err(|err| crate::errors::TinyrayError::new_err(err.to_string()))
    }
}

fn parse_actor(value: &str) -> PyResult<ActorId> {
    value
        .parse()
        .map_err(|_| PyValueError::new_err(format!("invalid actor id: {value}")))
}

fn client_error_to_py(err: ClientError) -> PyErr {
    match err {
        ClientError::Remote(remote) => remote_error_to_py(&remote),
        other => crate::errors::TinyrayError::new_err(other.to_string()),
    }
}
