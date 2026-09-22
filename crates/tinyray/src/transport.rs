use crate::blob::{
    clear_inherited_rpc_blob_owners, decode, encode_with_blob_owners, BlobRef, RpcBlobOwners,
};
use crate::fds::{FdTable, RawFd};
use crate::service::{CallContext, CancellationToken, Service, ServiceRequest};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tinyray_proto::rpc::{
    RpcError, RpcOperation, RpcReply, RpcRequest, RpcRequestHeader, RpcStatus, MAX_RPC_BATCH,
    MAX_RPC_FRAME_BYTES, MAX_RPC_REQUEST_ID_BYTES, RPC_PROTOCOL,
};
use tinyray_proto::wire::{
    bind_tcp_listener, decode_message, encode_message, read_frame, read_frame_body,
    write_frame_bytes, FrameError,
};
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{lookup_host, TcpListener, TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant as TokioInstant;

#[cfg(unix)]
use std::os::fd::AsRawFd;

mod client;
mod server;

use client::BlobReplyAck;
pub use client::{Client, ClientRequestCancellation, ReceivedRawReply, ReceivedRpcReply, Target};
pub use server::{Server, ServerConfig, ServerStats};

const MAX_IDLE_CONNECTIONS: usize = 64;
const MAX_ENDPOINT_POOLS: usize = 256;
const MAX_CLIENT_CONNECTIONS: usize = 256;
const MAX_CONNECTIONS_PER_ENDPOINT: usize = 4;
const MAX_INFLIGHT_PER_CONNECTION: usize = 128;
const PREFERRED_INFLIGHT_PER_CONNECTION: usize = 2;
const MAX_INFLIGHT_PER_ENDPOINT: usize = 256;
const MAX_INFLIGHT_PROCESS: usize = 512;
const MAX_ABANDONED_PER_CONNECTION: usize = 256;
pub const MAX_BLOB_REFS_PER_REPLY: usize = crate::MAX_BLOB_REFS_PER_MESSAGE;
pub const MAX_BLOB_BYTES_PER_REPLY: usize = crate::MAX_BLOB_MAPPED_BYTES_PER_MESSAGE;
pub const MAX_UNACKED_BLOB_REFS_PER_CONNECTION: usize = 128;
pub const MAX_UNACKED_BLOB_BYTES_PER_CONNECTION: usize = 512 << 20;
pub const MAX_UNACKED_BLOB_REFS_PER_SERVER: usize = 512;
pub const MAX_UNACKED_BLOB_BYTES_PER_SERVER: usize = 1 << 30;
pub const MAX_UNACKED_BLOB_REFS_PROCESS: usize = 2_048;
pub const MAX_UNACKED_BLOB_BYTES_PROCESS: usize = 2 << 30;
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const SERVER_FRAME_TIMEOUT: Duration = Duration::from_secs(15);
const SERVER_WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const SERVER_BLOB_ACK_TIMEOUT: Duration = Duration::from_secs(60);
const GLOBAL_CONNECTION_LIMIT: usize = 2_048;
const SERVER_CONNECTION_LIMIT: usize = 512;
const GLOBAL_FRAME_LIMIT: usize = 512;
const SERVER_FRAME_LIMIT: usize = 128;
const SMALL_FRAME_MAX_BYTES: usize = 1 << 20;
const GLOBAL_SMALL_FRAME_BUDGET_BYTES: usize = 32 << 20;
const SERVER_SMALL_FRAME_BUDGET_BYTES: usize = 8 << 20;
const GLOBAL_BULK_FRAME_BUDGET_BYTES: usize = 4 * MAX_RPC_FRAME_BYTES;
const SERVER_BULK_FRAME_BUDGET_BYTES: usize = 2 * MAX_RPC_FRAME_BYTES;

static SERVER_GLOBAL_CONNECTIONS: OnceLock<Arc<Semaphore>> = OnceLock::new();
static SERVER_GLOBAL_FRAMES: OnceLock<Arc<FrameBudgets>> = OnceLock::new();
static SERVER_GLOBAL_BLOB_REPLIES: OnceLock<Arc<BlobReplyBudget>> = OnceLock::new();

fn server_global_connections() -> Arc<Semaphore> {
    SERVER_GLOBAL_CONNECTIONS
        .get_or_init(|| Arc::new(Semaphore::new(GLOBAL_CONNECTION_LIMIT)))
        .clone()
}

fn server_global_frames() -> Arc<FrameBudgets> {
    SERVER_GLOBAL_FRAMES
        .get_or_init(|| {
            Arc::new(FrameBudgets::new(
                GLOBAL_FRAME_LIMIT,
                GLOBAL_SMALL_FRAME_BUDGET_BYTES,
                GLOBAL_BULK_FRAME_BUDGET_BYTES,
            ))
        })
        .clone()
}

fn server_global_blob_replies() -> Arc<BlobReplyBudget> {
    SERVER_GLOBAL_BLOB_REPLIES
        .get_or_init(|| {
            Arc::new(BlobReplyBudget::new(
                MAX_UNACKED_BLOB_REFS_PROCESS,
                MAX_UNACKED_BLOB_BYTES_PROCESS,
            ))
        })
        .clone()
}

pub fn reset_blob_reply_budgets_after_fork() {
    if let Some(budget) = SERVER_GLOBAL_BLOB_REPLIES.get() {
        *budget.state.lock().unwrap() = BlobReplyBudgetState::default();
    }
}

struct BlobReplyBudget {
    max_refs: usize,
    max_bytes: usize,
    state: Mutex<BlobReplyBudgetState>,
}

#[derive(Default)]
struct BlobReplyBudgetState {
    refs: usize,
    bytes: usize,
}

impl BlobReplyBudget {
    fn new(max_refs: usize, max_bytes: usize) -> Self {
        Self {
            max_refs,
            max_bytes,
            state: Mutex::new(BlobReplyBudgetState::default()),
        }
    }

    fn try_acquire(self: &Arc<Self>, refs: usize, bytes: usize) -> Option<BlobReplyPermit> {
        let mut state = self.state.lock().unwrap();
        if state.refs.checked_add(refs)? > self.max_refs
            || state.bytes.checked_add(bytes)? > self.max_bytes
        {
            return None;
        }
        state.refs += refs;
        state.bytes += bytes;
        Some(BlobReplyPermit {
            budget: self.clone(),
            refs,
            bytes,
        })
    }

    fn usage(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.refs, state.bytes)
    }
}

struct BlobReplyPermit {
    budget: Arc<BlobReplyBudget>,
    refs: usize,
    bytes: usize,
}

impl Drop for BlobReplyPermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().unwrap();
        state.refs = state.refs.saturating_sub(self.refs);
        state.bytes = state.bytes.saturating_sub(self.bytes);
    }
}

struct BlobReplyAdmission {
    _process: BlobReplyPermit,
    _server: BlobReplyPermit,
    _connection: BlobReplyPermit,
}

struct BlobResponseLease {
    _owners: RpcBlobOwners,
    _admission: BlobReplyAdmission,
    expires_at: Instant,
}

struct RuntimeOwner {
    runtime: Option<tokio::runtime::Runtime>,
}

impl RuntimeOwner {
    fn new(worker_threads: usize, name: &'static str) -> Result<Self, CallError> {
        Ok(Self {
            runtime: Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(worker_threads.max(1))
                    .enable_all()
                    .thread_name(name)
                    .build()
                    .map_err(|error| CallError::NotDelivered(error.to_string()))?,
            ),
        })
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        self.runtime.as_ref().unwrap().handle()
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle().block_on(future)
    }
}

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            drop(std::thread::spawn(move || drop(runtime)));
        } else {
            drop(runtime);
        }
    }
}

struct FrameBudgets {
    frame_limit: usize,
    small_limit: usize,
    bulk_limit: usize,
    frames: Arc<Semaphore>,
    small_bytes: Arc<Semaphore>,
    bulk_bytes: Arc<Semaphore>,
}

impl FrameBudgets {
    fn new(frame_limit: usize, small_limit: usize, bulk_limit: usize) -> Self {
        Self {
            frame_limit,
            small_limit,
            bulk_limit,
            frames: Arc::new(Semaphore::new(frame_limit)),
            small_bytes: Arc::new(Semaphore::new(small_limit)),
            bulk_bytes: Arc::new(Semaphore::new(bulk_limit)),
        }
    }

    fn in_flight(&self) -> usize {
        self.frame_limit - self.frames.available_permits()
    }

    fn small_bytes(&self) -> usize {
        self.small_limit - self.small_bytes.available_permits()
    }

    fn bulk_bytes(&self) -> usize {
        self.bulk_limit - self.bulk_bytes.available_permits()
    }
}

struct FrameAdmission {
    _global_frame: OwnedSemaphorePermit,
    _endpoint_frame: OwnedSemaphorePermit,
    _global_bytes: OwnedSemaphorePermit,
    _endpoint_bytes: OwnedSemaphorePermit,
}

fn try_admit_frame(
    global: &Arc<FrameBudgets>,
    endpoint: &Arc<FrameBudgets>,
    length: usize,
) -> Option<FrameAdmission> {
    let permits = u32::try_from(length).ok()?;
    let global_frame = global.frames.clone().try_acquire_owned().ok()?;
    let endpoint_frame = endpoint.frames.clone().try_acquire_owned().ok()?;
    let (global_bytes, endpoint_bytes) = if length <= SMALL_FRAME_MAX_BYTES {
        (
            global
                .small_bytes
                .clone()
                .try_acquire_many_owned(permits)
                .ok()?,
            endpoint
                .small_bytes
                .clone()
                .try_acquire_many_owned(permits)
                .ok()?,
        )
    } else {
        (
            global
                .bulk_bytes
                .clone()
                .try_acquire_many_owned(permits)
                .ok()?,
            endpoint
                .bulk_bytes
                .clone()
                .try_acquire_many_owned(permits)
                .ok()?,
        )
    };
    Some(FrameAdmission {
        _global_frame: global_frame,
        _endpoint_frame: endpoint_frame,
        _global_bytes: global_bytes,
        _endpoint_bytes: endpoint_bytes,
    })
}

#[derive(Clone, Debug)]
pub struct RemoteFailure {
    pub status: RpcStatus,
    pub type_name: String,
    pub message: String,
    pub traceback: String,
    pub payload: Vec<u8>,
    pub batch_index: Option<u16>,
    pub completed: Option<u16>,
    _blob_ack: Option<BlobReplyAck>,
}

impl RemoteFailure {
    pub fn decode_payload<T: DeserializeOwned>(&self) -> Result<T, CallError> {
        decode(&self.payload).map_err(|error| {
            CallError::OutcomeUnknown(format!("error payload is malformed: {error}"))
        })
    }
}

#[derive(Clone, Debug)]
pub enum CallError {
    NotDelivered(String),
    OutcomeUnknown(String),
    MethodNotFound(Box<RemoteFailure>),
    Fenced(Box<RemoteFailure>),
    CallerFault(Box<RemoteFailure>),
    ConcurrencyRefused(Box<RemoteFailure>),
    Remote(Box<RemoteFailure>),
    Internal(Box<RemoteFailure>),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotDelivered(message) => write!(f, "not delivered: {message}"),
            Self::OutcomeUnknown(message) => write!(f, "outcome unknown: {message}"),
            Self::MethodNotFound(error)
            | Self::Fenced(error)
            | Self::CallerFault(error)
            | Self::ConcurrencyRefused(error)
            | Self::Remote(error)
            | Self::Internal(error) => {
                write!(f, "{}: {}", error.type_name, error.message)
            }
        }
    }
}

impl std::error::Error for CallError {}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub worker_threads: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self { worker_threads: 4 }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClientStats {
    pub pools: usize,
    pub connections: usize,
    pub idle_connections: usize,
    pub in_flight: usize,
    pub tracked_connections: usize,
    pub connections_opened: u64,
    pub connection_limit: usize,
    pub endpoint_connection_limit: usize,
    pub connection_in_flight_limit: usize,
    pub endpoint_in_flight_limit: usize,
    pub process_in_flight_limit: usize,
}

#[derive(Clone)]
pub struct RpcRuntime {
    owner: Arc<RuntimeOwner>,
}

impl RpcRuntime {
    pub fn new(worker_threads: usize) -> Result<Self, CallError> {
        Ok(Self {
            owner: Arc::new(RuntimeOwner::new(worker_threads, "tinyray-rpc")?),
        })
    }

    pub fn client(&self) -> Client {
        Client::from_runtime(self.owner.clone())
    }

    pub fn start_server(
        &self,
        config: ServerConfig,
        service: Arc<dyn Service>,
    ) -> Result<Server, CallError> {
        Server::start_inner(
            config,
            service,
            self.owner.handle().clone(),
            Some(self.owner.clone()),
        )
    }
}
