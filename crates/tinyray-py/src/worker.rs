//! Python bindings for the actor-side runtime.
//!
//! The Python half of an actor is a plain loop:
//!
//! ```python
//! while (task := runtime.next_task()) is not None:
//!     result = getattr(actor, task.method)(*args)
//!     runtime.complete(task.task_id, *serialize(result))
//! ```
//!
//! No callbacks from Rust into Python, no reentrancy, no async. The tokio
//! threads underneath serve result fetches the whole time, without ever asking
//! for the GIL.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};
use tinyray_core::proto::ErrorKind;
use tinyray_core::ActorId;
use tinyray_runtime::actor::{ActorConfig, ActorRuntime};
use tinyray_runtime::store::StoreConfig;
use tinyray_runtime::transport::server::{serve, RunningServer, ServerConfig};

use crate::buffers::{bytes_from_py, frames_from_py, Frame};
use crate::ids::PyId;
use crate::runtime::tokio_handle;

/// A call handed to the Python executor thread.
#[pyclass(module = "tinyray._tinyray", name = "Task", frozen)]
pub struct PyTask {
    #[pyo3(get)]
    task_id: PyId,
    #[pyo3(get)]
    method: String,
    body: Bytes,
    frames: Vec<Bytes>,
}

#[pymethods]
impl PyTask {
    /// The pickle body of the call arguments.
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(Frame::new(self.body.clone()).into_pyobject(py)?.into_any())
    }

    /// Out-of-band argument buffers, as zero-copy views.
    #[getter]
    fn frames<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self.frames
                .iter()
                .map(|f| Frame::new(f.clone()).into_pyobject(py))
                .collect::<Result<Vec<_>, _>>()?,
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "Task(task_id={}, method='{}', frames={})",
            self.task_id.inner.to_hex(),
            self.method,
            self.frames.len()
        )
    }
}

/// The actor-side runtime, as seen from Python.
#[pyclass(module = "tinyray._tinyray", name = "ActorRuntime")]
pub struct PyActorRuntime {
    runtime: Arc<ActorRuntime>,
    server: Option<RunningServer>,
    endpoint: String,
    actor_id: ActorId,
}

#[pymethods]
impl PyActorRuntime {
    /// Bind a server and start accepting calls.
    #[new]
    #[pyo3(signature = (
        actor_id,
        bind="127.0.0.1:0",
        max_pending_calls=1000,
        store_max_bytes=None,
        store_ttl_seconds=None,
        inline_threshold=256 * 1024,
    ))]
    fn py_new(
        py: Python<'_>,
        actor_id: &str,
        bind: &str,
        max_pending_calls: usize,
        store_max_bytes: Option<u64>,
        store_ttl_seconds: Option<f64>,
        inline_threshold: usize,
    ) -> PyResult<PyActorRuntime> {
        let actor_id: ActorId = actor_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid actor id: {actor_id}")))?;
        let bind = bind
            .parse()
            .map_err(|err| PyValueError::new_err(format!("invalid bind address {bind}: {err}")))?;

        let defaults = StoreConfig::default();
        let config = ActorConfig {
            actor_id,
            inline_threshold,
            max_pending_calls,
            store: StoreConfig {
                max_bytes: store_max_bytes.unwrap_or(defaults.max_bytes),
                ttl: store_ttl_seconds
                    .map(Duration::from_secs_f64)
                    .unwrap_or(defaults.ttl),
                ..defaults
            },
            server: ServerConfig {
                bind,
                ..Default::default()
            },
            ..Default::default()
        };

        let runtime = ActorRuntime::new(config.clone());
        let handle = tokio_handle()?;
        let server = py.detach(|| {
            handle.block_on(async { serve(config.server.clone(), runtime.clone()).await })
        })?;

        Ok(PyActorRuntime {
            endpoint: server.addr().to_string(),
            actor_id,
            runtime,
            server: Some(server),
        })
    }

    /// `host:port` this actor is reachable at. Reported back to the node agent.
    #[getter]
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[getter]
    fn actor_id(&self) -> String {
        self.actor_id.to_string()
    }

    /// Wait for the next call, giving up after `timeout_seconds`.
    ///
    /// Returns `None` on either a timeout or shutdown; check `shutting_down`
    /// to tell them apart. The timeout is not a nicety: Python only runs
    /// signal handlers while the main thread executes bytecode, so an executor
    /// parked in Rust indefinitely would ignore SIGTERM and every clean
    /// shutdown would fall back to SIGKILL.
    ///
    /// The GIL is released for the whole wait, which is what lets the tokio
    /// threads keep serving fetches while this actor is idle *or* busy.
    #[pyo3(signature = (timeout_seconds=0.2))]
    fn next_task(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<Option<PyTask>> {
        let runtime = self.runtime.clone();
        let handle = tokio_handle()?;
        let timeout = Duration::from_secs_f64(timeout_seconds.max(0.001));
        let dispatch = py.detach(move || {
            handle.block_on(async move { runtime.next_task_timeout(timeout).await })
        });
        Ok(dispatch.map(|d| PyTask {
            task_id: PyId::from_inner(d.task_id.0),
            method: d.method,
            body: d.body,
            frames: d.frames,
        }))
    }

    /// Publish a successful result.
    fn complete(
        &self,
        task_id: &str,
        body: &Bound<'_, PyAny>,
        frames: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let task_id = parse_task_id(task_id)?;
        let body = bytes_from_py(body)?;
        let frames = frames_from_py(frames)?;
        self.runtime.complete(task_id, body, frames);
        Ok(())
    }

    /// Publish a failure. `traceback` is the remote Python traceback, which is
    /// carried to the caller verbatim.
    #[pyo3(signature = (task_id, kind, message, traceback=None))]
    fn fail(
        &self,
        task_id: &str,
        kind: &str,
        message: &str,
        traceback: Option<&str>,
    ) -> PyResult<()> {
        let task_id = parse_task_id(task_id)?;
        self.runtime.fail(
            task_id,
            parse_error_kind(kind)?,
            message.to_string(),
            traceback.map(|t| t.to_string()),
        );
        Ok(())
    }

    /// Stop accepting work and fail anything still queued.
    fn begin_shutdown(&self) {
        self.runtime.begin_shutdown();
        if let Some(server) = &self.server {
            server.shutdown();
        }
    }

    #[getter]
    fn shutting_down(&self) -> bool {
        self.runtime.is_shutting_down()
    }

    /// The same JSON `/introspect` serves, for local debugging.
    fn introspect<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self.runtime.introspect_json())
    }

    /// Drop results past their TTL. Called periodically by the actor loop.
    fn sweep_expired(&self) -> usize {
        self.runtime.store().sweep_expired()
    }
}

pub(crate) fn parse_task_id(value: &str) -> PyResult<tinyray_core::TaskId> {
    value
        .parse()
        .map_err(|_| PyValueError::new_err(format!("invalid task id: {value}")))
}

fn parse_error_kind(value: &str) -> PyResult<ErrorKind> {
    match value {
        "UserException" => Ok(ErrorKind::UserException),
        "ObjectLost" => Ok(ErrorKind::ObjectLost),
        "ActorDied" => Ok(ErrorKind::ActorDied),
        "NotFound" => Ok(ErrorKind::NotFound),
        "Backpressure" => Ok(ErrorKind::Backpressure),
        "Internal" => Ok(ErrorKind::Internal),
        other => Err(PyRuntimeError::new_err(format!(
            "unknown error kind: {other}"
        ))),
    }
}
