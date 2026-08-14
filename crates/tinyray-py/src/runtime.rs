//! The one tokio runtime shared by everything in this process.
//!
//! Actors, the driver and the node agent all live in the same process at
//! different times; giving each its own runtime would multiply threads for no
//! benefit. Created lazily on first use so importing `tinyray` costs nothing.

use std::sync::OnceLock;

use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;
use tokio::runtime::{Handle, Runtime};

static RUNTIME: OnceLock<std::io::Result<Runtime>> = OnceLock::new();

/// Handle to the shared runtime, starting it if necessary.
pub fn tokio_handle() -> PyResult<Handle> {
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            // Enough to overlap several 10 MB transfers without spawning a
            // thread per core on a machine that is mostly running GPU work.
            .worker_threads(4)
            .thread_name("tinyray-io")
            .enable_all()
            .build()
    });
    match runtime {
        Ok(runtime) => Ok(runtime.handle().clone()),
        Err(err) => Err(PyRuntimeError::new_err(format!(
            "failed to start the tinyray runtime: {err}"
        ))),
    }
}
