use crate::blob::PyBlobRef;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyBytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tinyray_proto::rpc::{
    RpcError, RpcOperation, RpcRequest, RpcStatus, MAX_RPC_BATCH, MAX_RPC_FRAME_BYTES,
    MAX_RPC_REQUEST_ID_BYTES, RPC_PROTOCOL,
};

const OUTCOME_REPLY: u8 = 0;
const OUTCOME_NOT_DELIVERED: u8 = 1;
const OUTCOME_UNKNOWN: u8 = 2;
const OUTCOME_CANCELLED: u8 = 3;
const RUNTIME_WORKERS: usize = 4;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
static PYTHON_ALIVE: AtomicBool = AtomicBool::new(true);
static GLOBAL: OnceLock<Mutex<Option<Arc<RpcGlobal>>>> = OnceLock::new();

fn global_slot() -> &'static Mutex<Option<Arc<RpcGlobal>>> {
    GLOBAL.get_or_init(|| Mutex::new(None))
}

struct RpcGlobal {
    pid: u32,
    generation: u64,
    runtime: tokio::runtime::Runtime,
    client: tinyray::Client,
}

impl RpcGlobal {
    fn new() -> Result<Arc<Self>, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(RUNTIME_WORKERS)
            .enable_all()
            .thread_name("tinyray-rpc")
            .build()
            .map_err(|error| format!("cannot start the native RPC runtime: {error}"))?;
        let client = {
            let _entered = runtime.handle().enter();
            tinyray::Client::from_current().map_err(|error| error.to_string())?
        };
        Ok(Arc::new(Self {
            pid: std::process::id(),
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            runtime,
            client,
        }))
    }

    fn shutdown_python(&self) {
        self.client.clear_pools();
    }

    fn abandon_after_fork(&self) {
        self.client.abandon_after_fork();
    }
}

fn current_global() -> Result<Arc<RpcGlobal>, String> {
    let pid = std::process::id();
    let mut slot = global_slot().lock().unwrap();
    if slot.as_ref().is_some_and(|global| global.pid != pid) {
        let inherited = slot.take().unwrap();
        inherited.abandon_after_fork();
        std::mem::forget(inherited);
    }
    if slot.is_none() {
        *slot = Some(RpcGlobal::new()?);
    }
    Ok(slot.as_ref().unwrap().clone())
}

fn reset_after_fork_inner() {
    PYTHON_ALIVE.store(true, Ordering::Release);
    let mut slot = global_slot().lock().unwrap();
    if let Some(inherited) = slot.take() {
        inherited.abandon_after_fork();
        std::mem::forget(inherited);
    }
}

#[pyclass(frozen)]
pub struct RpcOutcome {
    #[pyo3(get)]
    kind: u8,
    reply: Option<tinyray::ReceivedRpcReply>,
    transport_message: String,
}

#[pymethods]
impl RpcOutcome {
    #[getter]
    fn request_id(&self) -> String {
        self.reply
            .as_ref()
            .map(|reply| reply.request_id.clone())
            .unwrap_or_default()
    }

    #[getter]
    fn status(&self) -> Option<u8> {
        self.reply.as_ref().map(|reply| status_code(reply.status))
    }

    #[getter]
    fn error_type(&self) -> String {
        self.reply
            .as_ref()
            .and_then(|reply| reply.error.as_ref())
            .map(|error| error.type_name.clone())
            .unwrap_or_default()
    }

    #[getter]
    fn message(&self) -> String {
        self.reply
            .as_ref()
            .and_then(|reply| reply.error.as_ref())
            .map(|error| error.message.clone())
            .unwrap_or_else(|| self.transport_message.clone())
    }

    #[getter]
    fn traceback(&self) -> String {
        self.reply
            .as_ref()
            .and_then(|reply| reply.error.as_ref())
            .map(|error| error.traceback.clone())
            .unwrap_or_default()
    }

    #[getter]
    fn batch_index(&self) -> Option<u16> {
        self.reply.as_ref().and_then(|reply| reply.batch_index)
    }

    #[getter]
    fn completed(&self) -> Option<u16> {
        self.reply.as_ref().and_then(|reply| reply.completed)
    }

    #[getter]
    fn payload(&self, py: Python<'_>) -> Py<PyBytes> {
        let payload = self
            .reply
            .as_ref()
            .map(|reply| reply.payload.as_slice())
            .unwrap_or_default();
        PyBytes::new_bound(py, payload).unbind()
    }
}

impl RpcOutcome {
    fn reply(reply: tinyray::ReceivedRpcReply) -> Self {
        Self {
            kind: OUTCOME_REPLY,
            reply: Some(reply),
            transport_message: String::new(),
        }
    }

    fn not_delivered(message: impl Into<String>) -> Self {
        Self::transport(OUTCOME_NOT_DELIVERED, message)
    }

    fn unknown(message: impl Into<String>) -> Self {
        Self::transport(OUTCOME_UNKNOWN, message)
    }

    fn transport(kind: u8, message: impl Into<String>) -> Self {
        Self {
            kind,
            reply: None,
            transport_message: message.into(),
        }
    }

    fn from_result(result: Result<tinyray::ReceivedRpcReply, tinyray::CallError>) -> Self {
        match result {
            Ok(reply) => Self::reply(reply),
            Err(tinyray::CallError::NotDelivered(message)) => Self::not_delivered(message),
            Err(tinyray::CallError::OutcomeUnknown(message)) => Self::unknown(message),
            Err(error) => Self::unknown(error.to_string()),
        }
    }
}

fn status_code(status: RpcStatus) -> u8 {
    match status {
        RpcStatus::Success => 0,
        RpcStatus::MethodNotFound => 1,
        RpcStatus::Fenced => 2,
        RpcStatus::CallerFault => 3,
        RpcStatus::ConcurrencyRefused => 4,
        RpcStatus::RemoteError => 5,
        RpcStatus::MalformedProtocol => 6,
        RpcStatus::Internal => 7,
    }
}

fn status_from_code(code: u8) -> Option<RpcStatus> {
    Some(match code {
        0 => RpcStatus::Success,
        1 => RpcStatus::MethodNotFound,
        2 => RpcStatus::Fenced,
        3 => RpcStatus::CallerFault,
        4 => RpcStatus::ConcurrencyRefused,
        5 => RpcStatus::RemoteError,
        6 => RpcStatus::MalformedProtocol,
        7 => RpcStatus::Internal,
        _ => return None,
    })
}

fn valid_endpoint(endpoint: &str) -> Result<(), String> {
    if endpoint.trim() != endpoint || endpoint.is_empty() {
        return Err("the native RPC endpoint must be a non-empty host:port".into());
    }
    if endpoint.contains("://") {
        return Err(format!(
            "the method endpoint {endpoint:?} is a URL; native method RPC requires host:port"
        ));
    }
    if !endpoint.contains(':') {
        return Err(format!(
            "the method endpoint {endpoint:?} has no port; native method RPC requires host:port"
        ));
    }
    Ok(())
}

fn valid_method(method: &str) -> bool {
    let mut chars = method.bytes();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic())
        && chars.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_request_id(request_id: &str) -> bool {
    !request_id.is_empty()
        && request_id.len() <= MAX_RPC_REQUEST_ID_BYTES
        && request_id
            .bytes()
            .all(|byte| byte.is_ascii() && (b' '..=b'~').contains(&byte))
}

fn validate_request(request: &RpcRequest) -> Result<(), String> {
    if request.protocol != RPC_PROTOCOL {
        return Err(format!(
            "method RPC protocol {} is unsupported; this build speaks {}",
            request.protocol, RPC_PROTOCOL
        ));
    }
    if !valid_request_id(&request.request_id) {
        return Err("request id must be 1-200 bytes of printable ASCII".into());
    }
    match request.operation {
        RpcOperation::Call => {
            if request.batch_len.is_some()
                || request
                    .method
                    .as_deref()
                    .is_none_or(|method| !valid_method(method))
            {
                return Err(
                    "call metadata needs one public ASCII method and no batch length".into(),
                );
            }
        }
        RpcOperation::Batch => {
            if request.method.is_some()
                || request.batch_len.is_none_or(|count| count > MAX_RPC_BATCH)
            {
                return Err(format!(
                    "batch metadata needs no method and a length from 0 through {MAX_RPC_BATCH}"
                ));
            }
        }
        RpcOperation::BlobAck => {
            if request.method.is_some()
                || request.batch_len.is_some()
                || !request.caller.is_empty()
                || !request.target.is_empty()
                || !request.payload.is_empty()
            {
                return Err("BlobRef acknowledgement carries only a request id".into());
            }
        }
    }
    Ok(())
}

fn make_request(
    request_id: String,
    caller: String,
    target: String,
    method: Option<String>,
    batch_len: Option<u16>,
    payload: Vec<u8>,
) -> Result<RpcRequest, String> {
    let request = match (method, batch_len) {
        (Some(method), None) => RpcRequest::call(request_id, caller, target, method, payload),
        (None, Some(batch_len)) => {
            RpcRequest::batch(request_id, caller, target, batch_len, payload)
        }
        _ => {
            return Err("native RPC needs either a method or a batch length, but never both".into())
        }
    };
    validate_request(&request)?;
    Ok(request)
}

fn clone_blob_owners(
    py: Python<'_>,
    owners: Option<Vec<Py<PyBlobRef>>>,
) -> PyResult<Vec<tinyray::BlobRef>> {
    owners
        .unwrap_or_default()
        .into_iter()
        .map(|owner| owner.bind(py).borrow().clone_inner())
        .collect()
}

#[pyclass]
pub struct RpcCompletion {
    inner: Mutex<Option<CompletionInner>>,
}

struct CompletionInner {
    outcome: RpcOutcome,
}

#[pymethods]
impl RpcCompletion {
    fn resolve(&self, _reusable: bool) -> PyResult<RpcOutcome> {
        let mut inner = self.inner.lock().unwrap();
        let inner = inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("native RPC completion was already resolved"))?;
        Ok(inner.outcome)
    }
}

#[pyclass]
pub struct RpcCallTicket {
    cancellation: tinyray::ClientRequestCancellation,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[pymethods]
impl RpcCallTicket {
    fn cancel(&self) {
        self.stop();
    }
}

impl RpcCallTicket {
    fn stop(&self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.lock().unwrap().take() {
            task.abort();
        }
    }
}

impl Drop for RpcCallTicket {
    fn drop(&mut self) {
        self.stop();
    }
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (endpoint, request_id, caller, target, payload, timeout_ms, method=None, batch_len=None, blob_owners=None))]
fn rpc_call_sync(
    py: Python<'_>,
    endpoint: String,
    request_id: String,
    caller: String,
    target: String,
    // PyBackedBytes avoids PyO3's eager Vec extraction copy at the FFI boundary.
    payload: PyBackedBytes,
    timeout_ms: u64,
    method: Option<String>,
    batch_len: Option<u16>,
    blob_owners: Option<Vec<Py<PyBlobRef>>>,
) -> PyResult<RpcOutcome> {
    let blob_owners = clone_blob_owners(py, blob_owners)?;
    let request = make_request(
        request_id,
        caller,
        target,
        method,
        batch_len,
        payload.to_vec(),
    )
    .map_err(PyValueError::new_err)?;
    if let Err(error) = valid_endpoint(&endpoint) {
        return Ok(RpcOutcome::not_delivered(error));
    }
    let global = current_global().map_err(PyRuntimeError::new_err)?;
    let client = global.client.clone();
    let outcome = py.allow_threads(move || {
        client.request_with_blob_owners(
            request,
            endpoint,
            Duration::from_millis(timeout_ms),
            blob_owners,
        )
    });
    Ok(RpcOutcome::from_result(outcome))
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (loop_, callback, endpoint, request_id, caller, target, payload, timeout_ms, method=None, batch_len=None, blob_owners=None))]
fn rpc_call_async(
    loop_: Py<PyAny>,
    callback: Py<PyAny>,
    endpoint: String,
    request_id: String,
    caller: String,
    target: String,
    // Keep the borrowed Python buffer until the one owned async request copy.
    payload: PyBackedBytes,
    timeout_ms: u64,
    method: Option<String>,
    batch_len: Option<u16>,
    blob_owners: Option<Vec<Py<PyBlobRef>>>,
) -> PyResult<RpcCallTicket> {
    let blob_owners = Python::with_gil(|py| clone_blob_owners(py, blob_owners))?;
    let request = make_request(
        request_id,
        caller,
        target,
        method,
        batch_len,
        payload.to_vec(),
    )
    .map_err(PyValueError::new_err)?;
    let global = current_global().map_err(PyRuntimeError::new_err)?;
    let handle = global.runtime.handle().clone();
    let client = global.client.clone();
    let cancellation = tinyray::ClientRequestCancellation::new();
    let task_cancellation = cancellation.clone();
    let endpoint_error = valid_endpoint(&endpoint).err();
    let task = handle.spawn(async move {
        let outcome = if let Some(error) = endpoint_error {
            RpcOutcome::not_delivered(error)
        } else {
            RpcOutcome::from_result(
                client
                    .request_with_blob_owners_cancellable_async(
                        request,
                        endpoint,
                        Duration::from_millis(timeout_ms),
                        blob_owners,
                        task_cancellation.clone(),
                    )
                    .await,
            )
        };
        if task_cancellation.is_cancelled() || !PYTHON_ALIVE.load(Ordering::Acquire) {
            return;
        }
        Python::with_gil(|py| {
            let completion = Py::new(
                py,
                RpcCompletion {
                    inner: Mutex::new(Some(CompletionInner { outcome })),
                },
            );
            let Ok(completion) = completion else {
                return;
            };
            let _ = loop_
                .bind(py)
                .call_method1("call_soon_threadsafe", (callback.bind(py), completion));
        });
    });
    Ok(RpcCallTicket {
        cancellation,
        task: Mutex::new(Some(task)),
    })
}

#[pyfunction]
fn rpc_reset_after_fork() -> PyResult<()> {
    reset_after_fork_inner();
    Ok(())
}

#[pyfunction]
fn rpc_shutdown() {
    PYTHON_ALIVE.store(false, Ordering::Release);
    if let Some(global) = global_slot().lock().unwrap().as_ref() {
        global.shutdown_python();
    }
}

#[pyfunction]
fn rpc_drop_endpoint(endpoint: &str) {
    let slot = global_slot().lock().unwrap();
    if let Some(global) = slot.as_ref() {
        global.client.drop_endpoint(endpoint);
    }
}

#[pyfunction]
fn rpc_debug_clear_pools() {
    let slot = global_slot().lock().unwrap();
    if let Some(global) = slot.as_ref() {
        global.client.clear_pools();
    }
}

#[pyfunction]
fn rpc_debug_state() -> PyResult<HashMap<String, u64>> {
    let global = current_global().map_err(PyRuntimeError::new_err)?;
    let stats = global.client.stats();
    Ok(HashMap::from([
        ("pid".into(), global.pid as u64),
        ("generation".into(), global.generation),
        ("pools".into(), stats.pools as u64),
        ("idle_connections".into(), stats.idle_connections as u64),
        ("connections".into(), stats.connections as u64),
        (
            "tracked_connections".into(),
            stats.tracked_connections as u64,
        ),
        ("in_flight".into(), stats.in_flight as u64),
        ("connections_opened".into(), stats.connections_opened),
        ("connection_limit".into(), stats.connection_limit as u64),
        (
            "endpoint_connection_limit".into(),
            stats.endpoint_connection_limit as u64,
        ),
        (
            "connection_in_flight_limit".into(),
            stats.connection_in_flight_limit as u64,
        ),
        (
            "endpoint_in_flight_limit".into(),
            stats.endpoint_in_flight_limit as u64,
        ),
        (
            "process_in_flight_limit".into(),
            stats.process_in_flight_limit as u64,
        ),
    ]))
}

#[pyfunction]
fn rpc_debug_fds() -> PyResult<Vec<i32>> {
    let global = current_global().map_err(PyRuntimeError::new_err)?;
    Ok(global.client.debug_fds())
}

struct PythonService {
    methods: Vec<String>,
    callback: Arc<Py<PyAny>>,
    owned: Arc<Py<PyAny>>,
}

#[async_trait::async_trait]
impl tinyray::Service for PythonService {
    fn methods(&self) -> &[String] {
        &self.methods
    }

    async fn dispatch(&self, request: tinyray::ServiceRequest) -> tinyray::ServiceResponse {
        let callback = self.callback.clone();
        let owned = self.owned.clone();
        let operation = match request.operation {
            RpcOperation::Call => "call",
            RpcOperation::Batch => "batch",
            RpcOperation::BlobAck => "blob_ack",
        };
        let method = request.method;
        let payload = request.payload;
        let caller = request.context.caller.to_string();
        let request_id = request.context.request_id.to_string();
        let batch_len = request.batch_len;
        let dispatched = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let ours = owned
                    .bind(py)
                    .call0()
                    .and_then(|value| value.extract::<bool>())
                    .map_err(|error| format!("the ownership check failed: {error}"))?;
                if !ours {
                    return Ok((
                        status_code(RpcStatus::Fenced),
                        Vec::new(),
                        "Fenced".into(),
                        "the member is held by a later tenure".into(),
                        String::new(),
                        None,
                        None,
                        Vec::new(),
                    ));
                }
                callback
                    .bind(py)
                    .call1((
                        operation,
                        method,
                        PyBytes::new_bound(py, &payload),
                        caller,
                        request_id,
                        batch_len,
                    ))
                    .and_then(|value| {
                        value.extract::<(
                            u8,
                            PyBackedBytes,
                            String,
                            String,
                            String,
                            Option<u16>,
                            Option<u16>,
                            Vec<Py<PyBlobRef>>,
                        )>()
                    })
                    .map(
                        |(
                            status,
                            payload,
                            error_type,
                            message,
                            traceback,
                            batch_index,
                            completed,
                            blob_owners,
                        )| {
                            let blob_owners = blob_owners
                                .into_iter()
                                .map(|owner| owner.bind(py).borrow().clone_inner())
                                .collect::<PyResult<Vec<_>>>()?;
                            Ok((
                                status,
                                payload.to_vec(),
                                error_type,
                                message,
                                traceback,
                                batch_index,
                                completed,
                                blob_owners,
                            ))
                        },
                    )
                    .and_then(|result| result)
                    .map_err(|error| error.to_string())
            })
        })
        .await;
        let result = match dispatched {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                return tinyray::ServiceResponse::error(tinyray::ServiceError::Internal(error))
            }
            Err(error) => {
                return tinyray::ServiceResponse::error(tinyray::ServiceError::Internal(format!(
                    "the Python dispatch worker failed: {error}"
                )))
            }
        };
        let (status, payload, error_type, message, traceback, batch_index, completed, blob_owners) =
            result;
        let Some(status) = status_from_code(status) else {
            return tinyray::ServiceResponse::error(tinyray::ServiceError::Internal(format!(
                "Python returned unknown RPC status {status}"
            )));
        };
        tinyray::ServiceResponse {
            status,
            payload,
            error: (status != RpcStatus::Success)
                .then(|| RpcError::new(error_type, message, traceback)),
            batch_index,
            completed,
            blob_owners,
        }
    }
}

#[pyclass(name = "RpcServer")]
struct SharedRpcServer {
    server: Mutex<Option<tinyray::Server>>,
    identity: String,
    #[pyo3(get)]
    port: u16,
}

#[pymethods]
impl SharedRpcServer {
    #[new]
    #[pyo3(signature = (identity, methods, callback, owned, host="0.0.0.0", max_concurrency=None))]
    fn new(
        identity: String,
        methods: Vec<String>,
        callback: Py<PyAny>,
        owned: Py<PyAny>,
        host: &str,
        max_concurrency: Option<usize>,
    ) -> PyResult<Self> {
        let listen = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]:0")
        } else {
            format!("{host}:0")
        };
        let service = Arc::new(PythonService {
            methods,
            callback: Arc::new(callback),
            owned: Arc::new(owned),
        });
        let mut config = tinyray::ServerConfig::new(listen, identity.clone());
        config.max_concurrency = max_concurrency;
        let global = current_global().map_err(PyRuntimeError::new_err)?;
        let server = tinyray::Server::start_on(global.runtime.handle(), config, service)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let port = server
            .endpoint()
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse().ok())
            .ok_or_else(|| PyRuntimeError::new_err("native RPC listener returned no port"))?;
        Ok(Self {
            server: Mutex::new(Some(server)),
            identity,
            port,
        })
    }

    fn close(&self, py: Python<'_>) {
        if let Some(mut server) = self.server.lock().unwrap().take() {
            py.allow_threads(|| server.close());
        }
    }

    fn abandon(&self) {
        if let Some(mut server) = self.server.lock().unwrap().take() {
            server.abandon();
        }
    }

    #[getter]
    fn identity(&self) -> &str {
        &self.identity
    }

    fn stats(&self) -> HashMap<String, u64> {
        let server = self.server.lock().unwrap();
        let Some(server) = server.as_ref() else {
            return HashMap::new();
        };
        let stats = server.stats();
        HashMap::from([
            ("calls".into(), stats.calls),
            ("refused".into(), stats.refused),
            ("connections_refused".into(), stats.connections_refused),
            ("failed".into(), stats.failed),
            ("in_flight".into(), stats.in_flight as u64),
            ("peak_in_flight".into(), stats.peak_in_flight as u64),
            ("busy_ms".into(), stats.busy_ms),
            ("connections".into(), stats.connections as u64),
            ("frames_in_flight".into(), stats.frames_in_flight as u64),
            ("small_frame_bytes".into(), stats.small_frame_bytes as u64),
            ("bulk_frame_bytes".into(), stats.bulk_frame_bytes as u64),
            ("unacked_blob_refs".into(), stats.unacked_blob_refs as u64),
            ("unacked_blob_bytes".into(), stats.unacked_blob_bytes as u64),
        ])
    }
}

impl Drop for SharedRpcServer {
    fn drop(&mut self) {
        if let Some(mut server) = self.server.get_mut().unwrap().take() {
            server.close();
        }
    }
}

pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("RPC_OUTCOME_REPLY", OUTCOME_REPLY)?;
    module.add("RPC_OUTCOME_NOT_DELIVERED", OUTCOME_NOT_DELIVERED)?;
    module.add("RPC_OUTCOME_UNKNOWN", OUTCOME_UNKNOWN)?;
    module.add("RPC_OUTCOME_CANCELLED", OUTCOME_CANCELLED)?;
    module.add("RPC_STATUS_SUCCESS", status_code(RpcStatus::Success))?;
    module.add(
        "RPC_STATUS_METHOD_NOT_FOUND",
        status_code(RpcStatus::MethodNotFound),
    )?;
    module.add("RPC_STATUS_FENCED", status_code(RpcStatus::Fenced))?;
    module.add(
        "RPC_STATUS_CALLER_FAULT",
        status_code(RpcStatus::CallerFault),
    )?;
    module.add(
        "RPC_STATUS_CONCURRENCY_REFUSED",
        status_code(RpcStatus::ConcurrencyRefused),
    )?;
    module.add(
        "RPC_STATUS_REMOTE_ERROR",
        status_code(RpcStatus::RemoteError),
    )?;
    module.add(
        "RPC_STATUS_MALFORMED_PROTOCOL",
        status_code(RpcStatus::MalformedProtocol),
    )?;
    module.add("RPC_STATUS_INTERNAL", status_code(RpcStatus::Internal))?;
    module.add("RPC_PROTOCOL", RPC_PROTOCOL)?;
    module.add("RPC_MAX_FRAME_BYTES", MAX_RPC_FRAME_BYTES)?;
    module.add_class::<RpcOutcome>()?;
    module.add_class::<RpcCompletion>()?;
    module.add_class::<RpcCallTicket>()?;
    module.add_class::<SharedRpcServer>()?;
    module.add_function(wrap_pyfunction!(rpc_call_sync, module)?)?;
    module.add_function(wrap_pyfunction!(rpc_call_async, module)?)?;
    module.add_function(wrap_pyfunction!(rpc_reset_after_fork, module)?)?;
    module.add_function(wrap_pyfunction!(rpc_shutdown, module)?)?;
    module.add_function(wrap_pyfunction!(rpc_drop_endpoint, module)?)?;
    module.add_function(wrap_pyfunction!(rpc_debug_clear_pools, module)?)?;
    module.add_function(wrap_pyfunction!(rpc_debug_state, module)?)?;
    module.add_function(wrap_pyfunction!(rpc_debug_fds, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_ticket_cancels_before_aborting_the_task() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let task = runtime.spawn(std::future::pending());
        let cancellation = tinyray::ClientRequestCancellation::new();
        let ticket = RpcCallTicket {
            cancellation: cancellation.clone(),
            task: Mutex::new(Some(task)),
        };

        ticket.stop();

        assert!(cancellation.is_cancelled());
        assert!(ticket.task.lock().unwrap().is_none());
    }
}
