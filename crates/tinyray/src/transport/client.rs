use super::*;

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[doc(hidden)]
#[derive(Clone, Default)]
pub struct ClientRequestCancellation {
    inner: Arc<CallCancellation>,
}

#[derive(Clone)]
pub struct Target {
    client: Client,
    endpoint: String,
    identity: String,
    caller: String,
}

struct ClientInner {
    pid: u32,
    handle: tokio::runtime::Handle,
    runtime: Option<Arc<RuntimeOwner>>,
    inline_async: bool,
    pools: Mutex<HashMap<String, Arc<ConnectionPool>>>,
    pool_clock: AtomicU64,
    idle_connections: AtomicUsize,
    connections: AtomicUsize,
    connections_opened: AtomicU64,
    connection_admission: Arc<Semaphore>,
    inflight: Arc<Semaphore>,
    frames: Arc<FrameBudgets>,
    blocking: Mutex<()>,
    fds: FdTable,
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self, CallError> {
        Ok(RpcRuntime::new(config.worker_threads)?.client())
    }

    pub(super) fn from_runtime(runtime: Arc<RuntimeOwner>) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                pid: std::process::id(),
                handle: runtime.handle().clone(),
                runtime: Some(runtime),
                inline_async: false,
                pools: Mutex::new(HashMap::new()),
                pool_clock: AtomicU64::new(1),
                idle_connections: AtomicUsize::new(0),
                connections: AtomicUsize::new(0),
                connections_opened: AtomicU64::new(0),
                connection_admission: Arc::new(Semaphore::new(MAX_CLIENT_CONNECTIONS)),
                inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_PROCESS)),
                frames: Arc::new(FrameBudgets::new(
                    MAX_INFLIGHT_PROCESS,
                    GLOBAL_SMALL_FRAME_BUDGET_BYTES,
                    GLOBAL_BULK_FRAME_BUDGET_BYTES,
                )),
                blocking: Mutex::new(()),
                fds: FdTable::new(),
            }),
        }
    }

    #[cfg(test)]
    pub(super) fn runtime_owner(&self) -> Option<&Arc<RuntimeOwner>> {
        self.inner.runtime.as_ref()
    }

    pub fn from_current() -> Result<Self, CallError> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            CallError::NotDelivered(
                "Client::from_current() must be called from a Tokio runtime".into(),
            )
        })?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                pid: std::process::id(),
                handle,
                runtime: None,
                inline_async: true,
                pools: Mutex::new(HashMap::new()),
                pool_clock: AtomicU64::new(1),
                idle_connections: AtomicUsize::new(0),
                connections: AtomicUsize::new(0),
                connections_opened: AtomicU64::new(0),
                connection_admission: Arc::new(Semaphore::new(MAX_CLIENT_CONNECTIONS)),
                inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_PROCESS)),
                frames: Arc::new(FrameBudgets::new(
                    MAX_INFLIGHT_PROCESS,
                    GLOBAL_SMALL_FRAME_BUDGET_BYTES,
                    GLOBAL_BULK_FRAME_BUDGET_BYTES,
                )),
                blocking: Mutex::new(()),
                fds: FdTable::new(),
            }),
        })
    }

    pub fn target(&self, endpoint: impl Into<String>, identity: impl Into<String>) -> Target {
        Target {
            client: self.clone(),
            endpoint: endpoint.into(),
            identity: identity.into(),
            caller: "rust/0#1".into(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn call_raw(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<ReceivedRawReply, CallError> {
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let received = require_success(self.request_delivery(
            request,
            endpoint.into(),
            timeout,
            Vec::new(),
        )?)?;
        Ok(received.into_raw())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn call_raw_with_blob_owners(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        payload: Vec<u8>,
        blob_owners: Vec<BlobRef>,
        timeout: Duration,
    ) -> Result<ReceivedRawReply, CallError> {
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let received = require_success(self.request_delivery(
            request,
            endpoint.into(),
            timeout,
            blob_owners,
        )?)?;
        Ok(received.into_raw())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn call_raw_async(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<ReceivedRawReply, CallError> {
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let received = require_success(
            self.request_delivery_async(request, endpoint.into(), timeout, Vec::new())
                .await?,
        )?;
        Ok(received.into_raw())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn call_raw_with_blob_owners_async(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        payload: Vec<u8>,
        blob_owners: Vec<BlobRef>,
        timeout: Duration,
    ) -> Result<ReceivedRawReply, CallError> {
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let received = require_success(
            self.request_delivery_async(request, endpoint.into(), timeout, blob_owners)
                .await?,
        )?;
        Ok(received.into_raw())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn call_typed<Req: Serialize, Res: DeserializeOwned>(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        request: &Req,
        timeout: Duration,
    ) -> Result<Res, CallError> {
        let (payload, blob_owners) = encode_with_blob_owners(request)
            .map_err(|error| CallError::NotDelivered(error.to_string()))?;
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let response = require_success(self.request_delivery(
            request,
            endpoint.into(),
            timeout,
            blob_owners,
        )?)?;
        decode(&response.reply.payload).map_err(|error| {
            CallError::OutcomeUnknown(format!("typed response is malformed: {error}"))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn call_typed_async<Req, Res>(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        request: &Req,
        timeout: Duration,
    ) -> Result<Res, CallError>
    where
        Req: Serialize + Sync,
        Res: DeserializeOwned,
    {
        let (payload, blob_owners) = encode_with_blob_owners(request)
            .map_err(|error| CallError::NotDelivered(error.to_string()))?;
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let response = require_success(
            self.request_delivery_async(request, endpoint.into(), timeout, blob_owners)
                .await?,
        )?;
        decode(&response.reply.payload).map_err(|error| {
            CallError::OutcomeUnknown(format!("typed response is malformed: {error}"))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn call_arg<Req: Serialize, Res: DeserializeOwned>(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        request: &Req,
        timeout: Duration,
    ) -> Result<Res, CallError> {
        let (payload, blob_owners) = encode_with_blob_owners(&OneArgRef {
            args: [request],
            kwargs: HashMap::<String, ()>::new(),
        })
        .map_err(|error| CallError::NotDelivered(error.to_string()))?;
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let response = require_success(self.request_delivery(
            request,
            endpoint.into(),
            timeout,
            blob_owners,
        )?)?;
        decode(&response.reply.payload).map_err(|error| {
            CallError::OutcomeUnknown(format!("typed response is malformed: {error}"))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn call_arg_async<Req, Res>(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        request: &Req,
        timeout: Duration,
    ) -> Result<Res, CallError>
    where
        Req: Serialize + Sync,
        Res: DeserializeOwned,
    {
        let (payload, blob_owners) = encode_with_blob_owners(&OneArgRef {
            args: [request],
            kwargs: HashMap::<String, ()>::new(),
        })
        .map_err(|error| CallError::NotDelivered(error.to_string()))?;
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let response = require_success(
            self.request_delivery_async(request, endpoint.into(), timeout, blob_owners)
                .await?,
        )?;
        decode(&response.reply.payload).map_err(|error| {
            CallError::OutcomeUnknown(format!("typed response is malformed: {error}"))
        })
    }

    pub fn call_no_args<Res: DeserializeOwned>(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        timeout: Duration,
    ) -> Result<Res, CallError> {
        let payload = rmp_serde::to_vec_named(&NoArgsRef {
            args: Vec::<()>::new(),
            kwargs: HashMap::<String, ()>::new(),
        })
        .map_err(|error| CallError::NotDelivered(error.to_string()))?;
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let response = require_success(self.request_delivery(
            request,
            endpoint.into(),
            timeout,
            Vec::new(),
        )?)?;
        decode(&response.reply.payload).map_err(|error| {
            CallError::OutcomeUnknown(format!("typed response is malformed: {error}"))
        })
    }

    pub async fn call_no_args_async<Res: DeserializeOwned>(
        &self,
        endpoint: impl Into<String>,
        target: impl Into<String>,
        method: impl Into<String>,
        caller: impl Into<String>,
        request_id: impl Into<String>,
        timeout: Duration,
    ) -> Result<Res, CallError> {
        let payload = rmp_serde::to_vec_named(&NoArgsRef {
            args: Vec::<()>::new(),
            kwargs: HashMap::<String, ()>::new(),
        })
        .map_err(|error| CallError::NotDelivered(error.to_string()))?;
        let request = RpcRequest::call(request_id, caller, target, method, payload);
        let response = require_success(
            self.request_delivery_async(request, endpoint.into(), timeout, Vec::new())
                .await?,
        )?;
        decode(&response.reply.payload).map_err(|error| {
            CallError::OutcomeUnknown(format!("typed response is malformed: {error}"))
        })
    }

    pub fn request(
        &self,
        request: RpcRequest,
        endpoint: String,
        timeout: Duration,
    ) -> Result<ReceivedRpcReply, CallError> {
        self.request_delivery(request, endpoint, timeout, Vec::new())
    }

    pub fn request_with_blob_owners(
        &self,
        request: RpcRequest,
        endpoint: String,
        timeout: Duration,
        blob_owners: Vec<BlobRef>,
    ) -> Result<ReceivedRpcReply, CallError> {
        self.request_delivery(request, endpoint, timeout, blob_owners)
    }

    fn request_delivery(
        &self,
        request: RpcRequest,
        endpoint: String,
        timeout: Duration,
        blob_owners: Vec<BlobRef>,
    ) -> Result<ReceivedRpcReply, CallError> {
        self.ensure_current()?;
        let inner = self.inner.clone();
        if let Some(runtime) = &inner.runtime {
            if tokio::runtime::Handle::try_current().is_err() {
                if let Ok(_guard) = inner.blocking.try_lock() {
                    return runtime.block_on(exchange(
                        inner.clone(),
                        endpoint,
                        request,
                        timeout,
                        None,
                        blob_owners,
                    ));
                }
            }
        }
        let handle = inner.handle.clone();
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        handle.spawn(async move {
            let result = exchange(inner, endpoint, request, timeout, None, blob_owners).await;
            let _ = send.send(result);
        });
        receive.recv().map_err(|_| {
            CallError::OutcomeUnknown("the Rust RPC runtime stopped without an outcome".into())
        })?
    }

    pub async fn request_async(
        &self,
        request: RpcRequest,
        endpoint: String,
        timeout: Duration,
    ) -> Result<ReceivedRpcReply, CallError> {
        self.request_delivery_async(request, endpoint, timeout, Vec::new())
            .await
    }

    pub async fn request_with_blob_owners_async(
        &self,
        request: RpcRequest,
        endpoint: String,
        timeout: Duration,
        blob_owners: Vec<BlobRef>,
    ) -> Result<ReceivedRpcReply, CallError> {
        self.request_delivery_async(request, endpoint, timeout, blob_owners)
            .await
    }

    async fn request_delivery_async(
        &self,
        request: RpcRequest,
        endpoint: String,
        timeout: Duration,
        blob_owners: Vec<BlobRef>,
    ) -> Result<ReceivedRpcReply, CallError> {
        self.request_delivery_cancellable_async(
            request,
            endpoint,
            timeout,
            blob_owners,
            ClientRequestCancellation::new(),
        )
        .await
    }

    #[doc(hidden)]
    pub async fn request_with_blob_owners_cancellable_async(
        &self,
        request: RpcRequest,
        endpoint: String,
        timeout: Duration,
        blob_owners: Vec<BlobRef>,
        cancellation: ClientRequestCancellation,
    ) -> Result<ReceivedRpcReply, CallError> {
        self.request_delivery_cancellable_async(
            request,
            endpoint,
            timeout,
            blob_owners,
            cancellation,
        )
        .await
    }

    async fn request_delivery_cancellable_async(
        &self,
        request: RpcRequest,
        endpoint: String,
        timeout: Duration,
        blob_owners: Vec<BlobRef>,
        cancellation: ClientRequestCancellation,
    ) -> Result<ReceivedRpcReply, CallError> {
        self.ensure_current()?;
        let inner = self.inner.clone();
        let guard = CancelOnDrop {
            cancellation: cancellation.inner.clone(),
            armed: true,
        };
        if inner.inline_async {
            let result = exchange(
                inner,
                endpoint,
                request,
                timeout,
                Some(cancellation.inner.clone()),
                blob_owners,
            )
            .await;
            guard.disarm();
            return result;
        }
        let handle = inner.handle.clone();
        let (send, receive) = oneshot::channel();
        handle.spawn(async move {
            let result = exchange(
                inner,
                endpoint,
                request,
                timeout,
                Some(cancellation.inner),
                blob_owners,
            )
            .await;
            let _ = send.send(result);
        });
        let result = receive.await.map_err(|_| {
            CallError::OutcomeUnknown("the Rust RPC runtime stopped without an outcome".into())
        })?;
        guard.disarm();
        result
    }

    pub fn stats(&self) -> ClientStats {
        let pools = self.inner.pools.lock().unwrap();
        let (connections, in_flight) = pools
            .values()
            .map(|pool| pool.counts())
            .fold((0, 0), |current, next| {
                (current.0 + next.0, current.1 + next.2)
            });
        ClientStats {
            pools: pools.len(),
            connections,
            idle_connections: self.inner.idle_connections.load(Ordering::Relaxed),
            in_flight,
            tracked_connections: self.inner.connections.load(Ordering::Relaxed),
            connections_opened: self.inner.connections_opened.load(Ordering::Relaxed),
            connection_limit: MAX_CLIENT_CONNECTIONS,
            endpoint_connection_limit: MAX_CONNECTIONS_PER_ENDPOINT,
            connection_in_flight_limit: MAX_INFLIGHT_PER_CONNECTION,
            endpoint_in_flight_limit: MAX_INFLIGHT_PER_ENDPOINT,
            process_in_flight_limit: MAX_INFLIGHT_PROCESS,
        }
    }

    #[doc(hidden)]
    pub fn debug_fds(&self) -> Vec<i32> {
        self.inner.fds.snapshot()
    }

    pub fn drop_endpoint(&self, endpoint: &str) {
        self.inner.pools.lock().unwrap().remove(endpoint);
    }

    #[doc(hidden)]
    pub fn clear_pools(&self) {
        self.inner.pools.lock().unwrap().clear();
    }

    #[doc(hidden)]
    pub fn abandon_after_fork(&self) {
        self.inner.fds.close_all();
        clear_inherited_rpc_blob_owners();
        reset_blob_reply_budgets_after_fork();
    }

    fn ensure_current(&self) -> Result<(), CallError> {
        if self.inner.pid != std::process::id() {
            self.inner.fds.close_all();
            clear_inherited_rpc_blob_owners();
            reset_blob_reply_budgets_after_fork();
            return Err(CallError::NotDelivered(
                "the Rust RPC client was created before fork; create a new client".into(),
            ));
        }
        Ok(())
    }
}

impl ClientRequestCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    #[doc(hidden)]
    pub fn is_cancelled(&self) -> bool {
        self.inner.requested.load(Ordering::Acquire)
    }

    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

impl Target {
    pub fn caller(mut self, caller: impl Into<String>) -> Self {
        self.caller = caller.into();
        self
    }

    pub fn call_raw(
        &self,
        method: impl Into<String>,
        request_id: impl Into<String>,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<ReceivedRawReply, CallError> {
        self.client.call_raw(
            &self.endpoint,
            &self.identity,
            method,
            &self.caller,
            request_id,
            payload,
            timeout,
        )
    }

    pub fn call_raw_with_blob_owners(
        &self,
        method: impl Into<String>,
        request_id: impl Into<String>,
        payload: Vec<u8>,
        blob_owners: Vec<BlobRef>,
        timeout: Duration,
    ) -> Result<ReceivedRawReply, CallError> {
        self.client.call_raw_with_blob_owners(
            &self.endpoint,
            &self.identity,
            method,
            &self.caller,
            request_id,
            payload,
            blob_owners,
            timeout,
        )
    }

    pub async fn call_raw_async(
        &self,
        method: impl Into<String>,
        request_id: impl Into<String>,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<ReceivedRawReply, CallError> {
        self.client
            .call_raw_async(
                &self.endpoint,
                &self.identity,
                method,
                &self.caller,
                request_id,
                payload,
                timeout,
            )
            .await
    }

    pub async fn call_raw_with_blob_owners_async(
        &self,
        method: impl Into<String>,
        request_id: impl Into<String>,
        payload: Vec<u8>,
        blob_owners: Vec<BlobRef>,
        timeout: Duration,
    ) -> Result<ReceivedRawReply, CallError> {
        self.client
            .call_raw_with_blob_owners_async(
                &self.endpoint,
                &self.identity,
                method,
                &self.caller,
                request_id,
                payload,
                blob_owners,
                timeout,
            )
            .await
    }

    pub fn call_arg<Req: Serialize, Res: DeserializeOwned>(
        &self,
        method: impl Into<String>,
        request_id: impl Into<String>,
        request: &Req,
        timeout: Duration,
    ) -> Result<Res, CallError> {
        self.client.call_arg(
            &self.endpoint,
            &self.identity,
            method,
            &self.caller,
            request_id,
            request,
            timeout,
        )
    }

    pub async fn call_arg_async<Req, Res>(
        &self,
        method: impl Into<String>,
        request_id: impl Into<String>,
        request: &Req,
        timeout: Duration,
    ) -> Result<Res, CallError>
    where
        Req: Serialize + Sync,
        Res: DeserializeOwned,
    {
        self.client
            .call_arg_async(
                &self.endpoint,
                &self.identity,
                method,
                &self.caller,
                request_id,
                request,
                timeout,
            )
            .await
    }

    pub fn call_no_args<Res: DeserializeOwned>(
        &self,
        method: impl Into<String>,
        request_id: impl Into<String>,
        timeout: Duration,
    ) -> Result<Res, CallError> {
        self.client.call_no_args(
            &self.endpoint,
            &self.identity,
            method,
            &self.caller,
            request_id,
            timeout,
        )
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new(ClientConfig::default()).expect("default Rust RPC runtime")
    }
}

struct CancelOnDrop {
    cancellation: Arc<CallCancellation>,
    armed: bool,
}

impl CancelOnDrop {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

#[derive(Serialize)]
struct OneArgRef<'a, T> {
    args: [&'a T; 1],
    kwargs: HashMap<String, ()>,
}

#[derive(Serialize)]
struct NoArgsRef {
    args: Vec<()>,
    kwargs: HashMap<String, ()>,
}

fn require_success(mut received: ReceivedRpcReply) -> Result<ReceivedRpcReply, CallError> {
    if received.reply.status == RpcStatus::Success {
        return Ok(received);
    }
    let ack = received._ack.take();
    let reply = received.reply;
    let error = reply.error.unwrap_or_else(|| RpcError::new("", "", ""));
    let failure = RemoteFailure {
        status: reply.status,
        type_name: error.type_name,
        message: error.message,
        traceback: error.traceback,
        payload: reply.payload,
        batch_index: reply.batch_index,
        completed: reply.completed,
        _blob_ack: ack,
    };
    Err(match failure.status {
        RpcStatus::MethodNotFound => CallError::MethodNotFound(Box::new(failure)),
        RpcStatus::Fenced => CallError::Fenced(Box::new(failure)),
        RpcStatus::CallerFault | RpcStatus::MalformedProtocol => {
            CallError::CallerFault(Box::new(failure))
        }
        RpcStatus::ConcurrencyRefused => CallError::ConcurrencyRefused(Box::new(failure)),
        RpcStatus::RemoteError => CallError::Remote(Box::new(failure)),
        RpcStatus::Internal => CallError::Internal(Box::new(failure)),
        RpcStatus::Success => unreachable!(),
    })
}

struct ConnectionPool {
    endpoint: String,
    last_used: AtomicU64,
    state: Mutex<ConnectionPoolState>,
    available: Notify,
    inflight: Arc<Semaphore>,
    frames: Arc<FrameBudgets>,
}

#[derive(Default)]
struct ConnectionPoolState {
    connections: Vec<Arc<ClientConnection>>,
    connecting: bool,
}

impl ClientInner {
    fn pool(self: &Arc<Self>, endpoint: &str) -> Arc<ConnectionPool> {
        let mut pools = self.pools.lock().unwrap();
        let tick = self.pool_clock.fetch_add(1, Ordering::Relaxed);
        if let Some(pool) = pools.get(endpoint) {
            pool.last_used.store(tick, Ordering::Relaxed);
            return pool.clone();
        }
        if pools.len() >= MAX_ENDPOINT_POOLS {
            if let Some(oldest) = pools
                .iter()
                .min_by_key(|(_, pool)| pool.last_used.load(Ordering::Relaxed))
                .map(|(endpoint, _)| endpoint.clone())
            {
                pools.remove(&oldest);
            }
        }
        let pool = Arc::new(ConnectionPool {
            endpoint: endpoint.to_owned(),
            last_used: AtomicU64::new(tick),
            state: Mutex::new(ConnectionPoolState::default()),
            available: Notify::new(),
            inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_PER_ENDPOINT)),
            frames: Arc::new(FrameBudgets::new(
                MAX_INFLIGHT_PER_ENDPOINT,
                SERVER_SMALL_FRAME_BUDGET_BYTES,
                SERVER_BULK_FRAME_BUDGET_BYTES,
            )),
        });
        pools.insert(endpoint.to_owned(), pool.clone());
        pool
    }
}

struct ClientConnectionState {
    pending: HashMap<String, PendingCall>,
    abandoned: HashMap<String, RpcBlobOwners>,
    guarded_replies: HashSet<String>,
    last_activity: Instant,
    counted_idle: bool,
}

impl ClientConnectionState {
    fn is_quiescent(&self) -> bool {
        self.pending.is_empty() && self.guarded_replies.is_empty()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WriteProgress {
    Queued,
    Writing,
    Written,
}

struct ClientBaseAdmission {
    _global: OwnedSemaphorePermit,
    _endpoint: OwnedSemaphorePermit,
    _frame: FrameAdmission,
}

struct ClientAdmission {
    _base: ClientBaseAdmission,
    _connection: OwnedSemaphorePermit,
}

struct PendingCall {
    sender: oneshot::Sender<Result<ReceivedRpcReply, CallError>>,
    progress: WriteProgress,
    _admission: ClientAdmission,
    blob_owners: RpcBlobOwners,
}

struct WriteCommand {
    request_id: String,
    frame: Vec<u8>,
    deadline: TokioInstant,
    tracked: bool,
}

pub struct ReceivedRpcReply {
    reply: RpcReply,
    _ack: Option<BlobReplyAck>,
}

impl ReceivedRpcReply {
    fn into_raw(self) -> ReceivedRawReply {
        ReceivedRawReply {
            payload: self.reply.payload,
            _ack: self._ack,
        }
    }

    pub fn decode<T: DeserializeOwned>(self) -> Result<T, CallError> {
        let response = require_success(self)?;
        decode(&response.reply.payload)
            .map_err(|error| CallError::OutcomeUnknown(format!("response is malformed: {error}")))
    }
}

impl std::ops::Deref for ReceivedRpcReply {
    type Target = RpcReply;

    fn deref(&self) -> &Self::Target {
        &self.reply
    }
}

pub struct ReceivedRawReply {
    payload: Vec<u8>,
    _ack: Option<BlobReplyAck>,
}

impl ReceivedRawReply {
    pub fn as_bytes(&self) -> &[u8] {
        &self.payload
    }

    pub fn decode<T: DeserializeOwned>(self) -> Result<T, CallError> {
        decode(&self.payload).map_err(|error| {
            CallError::OutcomeUnknown(format!("raw response is malformed: {error}"))
        })
    }
}

impl std::ops::Deref for ReceivedRawReply {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for ReceivedRawReply {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl PartialEq<Vec<u8>> for ReceivedRawReply {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.payload == *other
    }
}

impl std::fmt::Debug for ReceivedRawReply {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceivedRawReply")
            .field("len", &self.payload.len())
            .finish()
    }
}

#[derive(Clone)]
pub(super) struct BlobReplyAck {
    inner: Arc<BlobReplyAckInner>,
}

struct BlobReplyAckInner {
    connection: Weak<ClientConnection>,
    request_id: String,
}

impl BlobReplyAck {
    fn new(connection: &Arc<ClientConnection>, request_id: String) -> Self {
        Self {
            inner: Arc::new(BlobReplyAckInner {
                connection: Arc::downgrade(connection),
                request_id,
            }),
        }
    }
}

impl std::fmt::Debug for BlobReplyAck {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlobReplyAck")
            .field("request_id", &self.inner.request_id)
            .finish()
    }
}

impl Drop for BlobReplyAckInner {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.upgrade() {
            connection.send_blob_ack(&self.request_id);
        }
    }
}

struct PendingConnectSocket {
    socket: Option<TcpSocket>,
    fd: RawFd,
    pid: u32,
    client: Weak<ClientInner>,
    transferred: bool,
}

impl PendingConnectSocket {
    fn new(socket: TcpSocket, client: &Arc<ClientInner>) -> Self {
        #[cfg(unix)]
        let fd = socket.as_raw_fd();
        #[cfg(not(unix))]
        let fd = 0;
        client.fds.register(fd);
        Self {
            socket: Some(socket),
            fd,
            pid: client.pid,
            client: Arc::downgrade(client),
            transferred: false,
        }
    }

    async fn connect(
        mut self,
        address: std::net::SocketAddr,
    ) -> std::io::Result<(TcpStream, ClientTrackedFd)> {
        let socket = self.socket.take().unwrap();
        let stream = socket.connect(address).await?;
        self.transferred = true;
        let tracking = ClientTrackedFd::from_registered(self.fd, &self.client);
        Ok((stream, tracking))
    }
}

impl Drop for PendingConnectSocket {
    fn drop(&mut self) {
        if self.transferred {
            return;
        }
        if let Some(client) = self.client.upgrade() {
            client.fds.unregister(self.fd);
        }
        if std::process::id() != self.pid {
            if let Some(socket) = self.socket.take() {
                std::mem::forget(socket);
            }
        }
    }
}

struct ClientTrackedFd {
    fd: RawFd,
    client: Weak<ClientInner>,
}

impl ClientTrackedFd {
    fn from_registered(fd: RawFd, client: &Weak<ClientInner>) -> Self {
        if let Some(client) = client.upgrade() {
            client.connections.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            fd,
            client: client.clone(),
        }
    }
}

impl Drop for ClientTrackedFd {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            client.fds.unregister(self.fd);
            client.connections.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct ClientConnection {
    pid: u32,
    pool: Weak<ConnectionPool>,
    client: Weak<ClientInner>,
    state: Mutex<ClientConnectionState>,
    outbound: mpsc::Sender<WriteCommand>,
    slots: Arc<Semaphore>,
    poisoned: AtomicBool,
    shutdown: Notify,
    activity: Notify,
    writer_finished: AtomicBool,
    writer_done: Notify,
    tracking: Mutex<Option<ClientTrackedFd>>,
}

struct ConnectionReservation {
    connection: Arc<ClientConnection>,
    permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum AbandonedCall {
    Queued,
    Writing,
    Written,
    Completed,
}

impl ClientConnection {
    fn endpoint(&self) -> String {
        self.pool
            .upgrade()
            .map(|pool| pool.endpoint.clone())
            .unwrap_or_else(|| "the RPC endpoint".into())
    }

    fn new(
        stream: TcpStream,
        tracking: ClientTrackedFd,
        pool: &Arc<ConnectionPool>,
        client: &Arc<ClientInner>,
        _connection: OwnedSemaphorePermit,
    ) -> Arc<Self> {
        let (reader, writer) = stream.into_split();
        let (outbound, receive_outbound) = mpsc::channel(MAX_INFLIGHT_PER_CONNECTION);
        let connection = Arc::new(Self {
            pid: client.pid,
            pool: Arc::downgrade(pool),
            client: Arc::downgrade(client),
            state: Mutex::new(ClientConnectionState {
                pending: HashMap::new(),
                abandoned: HashMap::new(),
                guarded_replies: HashSet::new(),
                last_activity: Instant::now(),
                counted_idle: false,
            }),
            outbound,
            slots: Arc::new(Semaphore::new(MAX_INFLIGHT_PER_CONNECTION)),
            poisoned: AtomicBool::new(false),
            shutdown: Notify::new(),
            activity: Notify::new(),
            writer_finished: AtomicBool::new(false),
            writer_done: Notify::new(),
            tracking: Mutex::new(Some(tracking)),
        });
        client.connections_opened.fetch_add(1, Ordering::Relaxed);
        let held = Arc::new(Mutex::new(Some(_connection)));
        client
            .handle
            .spawn(client_writer(connection.clone(), receive_outbound, writer));
        client
            .handle
            .spawn(client_reader(connection.clone(), reader, held));
        client.handle.spawn(client_idle(connection.clone()));
        connection
    }

    fn load(&self) -> usize {
        MAX_INFLIGHT_PER_CONNECTION - self.slots.available_permits()
    }

    fn try_reserve(self: &Arc<Self>, request_id: &str) -> Option<ConnectionReservation> {
        if self.poisoned.load(Ordering::Acquire) || self.pid != std::process::id() {
            return None;
        }
        let permit = self.slots.clone().try_acquire_owned().ok()?;
        let mut state = self.state.lock().unwrap();
        if self.poisoned.load(Ordering::Acquire)
            || state.pending.contains_key(request_id)
            || state.abandoned.contains_key(request_id)
            || state.guarded_replies.contains(request_id)
        {
            return None;
        }
        if state.counted_idle {
            if let Some(client) = self.client.upgrade() {
                client.idle_connections.fetch_sub(1, Ordering::Relaxed);
            }
            state.counted_idle = false;
        }
        state.last_activity = Instant::now();
        drop(state);
        self.activity.notify_waiters();
        Some(ConnectionReservation {
            connection: self.clone(),
            permit,
        })
    }

    fn register(
        &self,
        request_id: String,
        sender: oneshot::Sender<Result<ReceivedRpcReply, CallError>>,
        admission: ClientAdmission,
        blob_owners: RpcBlobOwners,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        if self.poisoned.load(Ordering::Acquire)
            || state.pending.contains_key(&request_id)
            || state.abandoned.contains_key(&request_id)
        {
            return false;
        }
        state.pending.insert(
            request_id,
            PendingCall {
                sender,
                progress: WriteProgress::Queued,
                _admission: admission,
                blob_owners,
            },
        );
        true
    }

    fn mark_writing(&self, request_id: &str) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(pending) = state.pending.get_mut(request_id) else {
            return false;
        };
        pending.progress = WriteProgress::Writing;
        true
    }

    fn mark_written(&self, request_id: &str) {
        let mut state = self.state.lock().unwrap();
        if let Some(pending) = state.pending.get_mut(request_id) {
            pending.progress = WriteProgress::Written;
        }
        state.last_activity = Instant::now();
        drop(state);
        self.activity.notify_waiters();
    }

    fn send_blob_ack(self: &Arc<Self>, request_id: &str) {
        let mut state = self.state.lock().unwrap();
        if !state.guarded_replies.remove(request_id) {
            return;
        }
        state.last_activity = Instant::now();
        let close = self.mark_idle(&mut state);
        drop(state);
        self.activity.notify_waiters();
        if let Some(pool) = self.pool.upgrade() {
            pool.available.notify_waiters();
        }
        if close {
            self.poison(
                CallError::NotDelivered("idle connection cap reached".into()),
                CallError::OutcomeUnknown("idle connection cap reached".into()),
                true,
            );
            return;
        }
        self.queue_blob_ack(request_id);
    }

    fn queue_blob_ack(self: &Arc<Self>, request_id: &str) {
        let request = RpcRequest::blob_ack(request_id);
        let Ok(frame) = encode_message(&request, MAX_RPC_FRAME_BYTES) else {
            self.poison(
                CallError::NotDelivered("could not encode BlobRef acknowledgement".into()),
                CallError::OutcomeUnknown("could not encode BlobRef acknowledgement".into()),
                false,
            );
            return;
        };
        if self
            .outbound
            .try_send(WriteCommand {
                request_id: request_id.to_owned(),
                frame,
                deadline: TokioInstant::now() + SERVER_WRITE_TIMEOUT,
                tracked: false,
            })
            .is_err()
        {
            self.poison(
                CallError::NotDelivered("could not queue BlobRef acknowledgement".into()),
                CallError::OutcomeUnknown("could not queue BlobRef acknowledgement".into()),
                false,
            );
        }
    }

    fn abandon(self: &Arc<Self>, request_id: &str) -> AbandonedCall {
        let mut state = self.state.lock().unwrap();
        let Some(pending) = state.pending.remove(request_id) else {
            return AbandonedCall::Completed;
        };
        let progress = pending.progress;
        let PendingCall {
            blob_owners,
            sender,
            _admission,
            ..
        } = pending;
        drop(sender);
        drop(_admission);
        if matches!(progress, WriteProgress::Writing | WriteProgress::Written) {
            state.abandoned.insert(request_id.to_owned(), blob_owners);
        }
        let overflow = state.abandoned.len() > MAX_ABANDONED_PER_CONNECTION;
        let close = self.mark_idle(&mut state);
        drop(state);
        self.activity.notify_waiters();
        if let Some(pool) = self.pool.upgrade() {
            pool.available.notify_waiters();
        }
        if overflow || close {
            self.poison(
                CallError::NotDelivered("cancelled reply tracking overflowed".into()),
                CallError::OutcomeUnknown("cancelled reply tracking overflowed".into()),
                false,
            );
        }
        match progress {
            WriteProgress::Queued => AbandonedCall::Queued,
            WriteProgress::Writing => AbandonedCall::Writing,
            WriteProgress::Written => AbandonedCall::Written,
        }
    }

    fn route(self: &Arc<Self>, reply: RpcReply) -> bool {
        let malformed = reply.status == RpcStatus::MalformedProtocol;
        let mut state = self.state.lock().unwrap();
        if let Some(pending) = state.pending.remove(&reply.request_id) {
            let PendingCall {
                sender, _admission, ..
            } = pending;
            drop(_admission);
            if reply.blob_refs {
                state.guarded_replies.insert(reply.request_id.clone());
            }
            state.last_activity = Instant::now();
            let close = self.mark_idle(&mut state);
            drop(state);
            if let Some(pool) = self.pool.upgrade() {
                pool.available.notify_waiters();
            }
            let ack = reply
                .blob_refs
                .then(|| BlobReplyAck::new(self, reply.request_id.clone()));
            let _ = sender.send(Ok(ReceivedRpcReply { reply, _ack: ack }));
            if close {
                self.poison(
                    CallError::NotDelivered("idle connection cap reached".into()),
                    CallError::OutcomeUnknown("idle connection cap reached".into()),
                    true,
                );
            }
            return malformed;
        }
        if state.abandoned.remove(&reply.request_id).is_some() {
            let close = self.mark_idle(&mut state);
            drop(state);
            if reply.blob_refs {
                self.queue_blob_ack(&reply.request_id);
            }
            if close {
                self.poison(
                    CallError::NotDelivered("idle connection cap reached".into()),
                    CallError::OutcomeUnknown("idle connection cap reached".into()),
                    true,
                );
            }
            return false;
        }
        let pending_ids = state.pending.keys().take(4).cloned().collect::<Vec<_>>();
        let abandoned_ids = state.abandoned.keys().take(4).cloned().collect::<Vec<_>>();
        drop(state);
        self.poison(
            CallError::NotDelivered("unknown reply id".into()),
            CallError::OutcomeUnknown(format!(
                "unknown or duplicate reply id {:?}; pending={pending_ids:?} abandoned={abandoned_ids:?}",
                reply.request_id
            )),
            true,
        );
        true
    }

    fn mark_idle(&self, state: &mut ClientConnectionState) -> bool {
        if !state.pending.is_empty()
            || !state.guarded_replies.is_empty()
            || state.counted_idle
            || self.poisoned.load(Ordering::Acquire)
        {
            return false;
        }
        let Some(client) = self.client.upgrade() else {
            return true;
        };
        if client
            .idle_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |idle| {
                (idle < MAX_IDLE_CONNECTIONS).then_some(idle + 1)
            })
            .is_ok()
        {
            state.counted_idle = true;
            state.last_activity = Instant::now();
            false
        } else {
            true
        }
    }

    fn poison(&self, not_delivered: CallError, unknown: CallError, writing_unknown: bool) {
        if self.poisoned.swap(true, Ordering::AcqRel) {
            return;
        }
        let pending = {
            let mut state = self.state.lock().unwrap();
            if state.counted_idle {
                if let Some(client) = self.client.upgrade() {
                    client.idle_connections.fetch_sub(1, Ordering::Relaxed);
                }
            }
            state.abandoned.clear();
            state.guarded_replies.clear();
            std::mem::take(&mut state.pending)
        };
        for (_, pending) in pending {
            let error = match pending.progress {
                WriteProgress::Queued => not_delivered.clone(),
                WriteProgress::Writing if !writing_unknown => not_delivered.clone(),
                WriteProgress::Writing | WriteProgress::Written => unknown.clone(),
            };
            let _ = pending.sender.send(Err(error));
        }
        self.shutdown.notify_waiters();
        self.activity.notify_waiters();
        if let Some(pool) = self.pool.upgrade() {
            pool.available.notify_waiters();
        }
    }

    fn is_idle(&self) -> bool {
        !self.poisoned.load(Ordering::Acquire) && {
            let state = self.state.lock().unwrap();
            state.is_quiescent()
        }
    }
}

impl ConnectionPool {
    async fn reserve(
        self: &Arc<Self>,
        client: &Arc<ClientInner>,
        request_id: &str,
        deadline: TokioInstant,
    ) -> Result<ConnectionReservation, CallError> {
        loop {
            let notified = self.available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let create = {
                let mut state = self.state.lock().unwrap();
                state
                    .connections
                    .retain(|connection| !connection.poisoned.load(Ordering::Acquire));
                let mut connections = state.connections.clone();
                connections.sort_unstable_by_key(|connection| connection.load());
                let least = connections
                    .first()
                    .map_or(usize::MAX, |connection| connection.load());
                let scale = connections.is_empty()
                    || (least >= PREFERRED_INFLIGHT_PER_CONNECTION
                        && connections.len() < MAX_CONNECTIONS_PER_ENDPOINT);
                if scale && !state.connecting {
                    state.connecting = true;
                    true
                } else if scale {
                    false
                } else if let Some(reservation) = connections
                    .iter()
                    .find_map(|connection| connection.try_reserve(request_id))
                {
                    return Ok(reservation);
                } else if connections.len() < MAX_CONNECTIONS_PER_ENDPOINT && !state.connecting {
                    state.connecting = true;
                    true
                } else if state.connecting {
                    false
                } else {
                    return Err(CallError::NotDelivered(
                        "endpoint multiplexing admission is full".into(),
                    ));
                }
            };
            if create {
                return self.connect(client, request_id, deadline).await;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(CallError::NotDelivered(
                    "connection admission exceeded the call timeout".into(),
                ));
            }
        }
    }

    async fn connect(
        self: &Arc<Self>,
        client: &Arc<ClientInner>,
        request_id: &str,
        deadline: TokioInstant,
    ) -> Result<ConnectionReservation, CallError> {
        let admission = match client.connection_admission.clone().try_acquire_owned() {
            Ok(admission) => admission,
            Err(_) => {
                self.finish_connect(None);
                return Err(CallError::NotDelivered(
                    "process RPC connection limit reached".into(),
                ));
            }
        };
        let addresses = match tokio::time::timeout_at(deadline, lookup_host(&self.endpoint)).await {
            Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
            Ok(Err(error)) => {
                self.finish_connect(None);
                return Err(CallError::NotDelivered(error.to_string()));
            }
            Err(_) => {
                self.finish_connect(None);
                return Err(CallError::NotDelivered("connect timed out".into()));
            }
        };
        let mut last_error = None;
        let mut connected = None;
        for address in addresses {
            let socket = if address.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            };
            let socket = match socket {
                Ok(socket) => socket,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            let pending = PendingConnectSocket::new(socket, client);
            match tokio::time::timeout_at(deadline, pending.connect(address)).await {
                Ok(Ok(pair)) => {
                    connected = Some(pair);
                    break;
                }
                Ok(Err(error)) => last_error = Some(error),
                Err(_) => {
                    self.finish_connect(None);
                    return Err(CallError::NotDelivered("connect timed out".into()));
                }
            }
        }
        let Some((stream, tracking)) = connected else {
            self.finish_connect(None);
            return Err(CallError::NotDelivered(
                last_error
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "endpoint resolved to no addresses".into()),
            ));
        };
        if let Err(error) = stream.set_nodelay(true) {
            self.finish_connect(None);
            return Err(CallError::NotDelivered(error.to_string()));
        }
        let connection = ClientConnection::new(stream, tracking, self, client, admission);
        self.finish_connect(Some(connection.clone()));
        connection.try_reserve(request_id).ok_or_else(|| {
            CallError::NotDelivered("new connection could not reserve a request".into())
        })
    }

    fn finish_connect(&self, connection: Option<Arc<ClientConnection>>) {
        let mut state = self.state.lock().unwrap();
        state.connecting = false;
        if let Some(connection) = connection {
            state.connections.push(connection);
        }
        drop(state);
        self.available.notify_waiters();
    }

    fn counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock().unwrap();
        state
            .connections
            .iter()
            .filter(|connection| !connection.poisoned.load(Ordering::Acquire))
            .fold((0, 0, 0), |mut counts, connection| {
                counts.0 += 1;
                let pending = connection.state.lock().unwrap().pending.len();
                counts.1 += usize::from(pending == 0);
                counts.2 += pending;
                counts
            })
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        for connection in self.state.get_mut().unwrap().connections.drain(..) {
            connection.poison(
                CallError::NotDelivered("endpoint pool closed".into()),
                CallError::OutcomeUnknown("endpoint pool closed after delivery".into()),
                true,
            );
        }
    }
}

#[derive(Default)]
struct CallCancellation {
    requested: AtomicBool,
    target: Mutex<Option<CancellationTarget>>,
}

struct CancellationTarget {
    connection: Weak<ClientConnection>,
    request_id: String,
}

impl CallCancellation {
    fn register(&self, connection: &Arc<ClientConnection>, request_id: &str) -> bool {
        let mut target = self.target.lock().unwrap();
        if self.requested.load(Ordering::Acquire) {
            return false;
        }
        *target = Some(CancellationTarget {
            connection: Arc::downgrade(connection),
            request_id: request_id.to_owned(),
        });
        true
    }

    fn cancel(&self) {
        self.requested.store(true, Ordering::Release);
        if let Some(target) = self.target.lock().unwrap().take() {
            if let Some(connection) = target.connection.upgrade() {
                let _ = connection.abandon(&target.request_id);
            }
        }
    }

    fn clear(&self, request_id: &str) {
        let mut target = self.target.lock().unwrap();
        if target
            .as_ref()
            .is_some_and(|target| target.request_id == request_id)
        {
            target.take();
        }
    }
}

fn timed_out_error(
    connection: &Arc<ClientConnection>,
    request_id: &str,
    endpoint: &str,
) -> CallError {
    match connection.abandon(request_id) {
        AbandonedCall::Queued => CallError::NotDelivered(format!(
            "the request was not completely written to {endpoint} before timeout"
        )),
        AbandonedCall::Writing => {
            connection.poison(
                CallError::NotDelivered(format!(
                    "the request was not completely written to {endpoint} before timeout"
                )),
                CallError::OutcomeUnknown(format!(
                    "{endpoint} became uncertain after another complete request"
                )),
                false,
            );
            CallError::NotDelivered(format!(
                "the request was not completely written to {endpoint} before timeout"
            ))
        }
        AbandonedCall::Written | AbandonedCall::Completed => {
            CallError::OutcomeUnknown(format!("{endpoint} did not answer before the call timeout"))
        }
    }
}

async fn exchange(
    client: Arc<ClientInner>,
    endpoint: String,
    request: RpcRequest,
    timeout: Duration,
    cancellation: Option<Arc<CallCancellation>>,
    blob_owners: Vec<BlobRef>,
) -> Result<ReceivedRpcReply, CallError> {
    let blob_owners = RpcBlobOwners::track(blob_owners)
        .map_err(|error| CallError::NotDelivered(error.to_string()))?;
    let frame = encode_message(&request, MAX_RPC_FRAME_BYTES)
        .map_err(|error| CallError::NotDelivered(error.to_string()))?;
    let pool = client.pool(&endpoint);
    let Some(base) = try_admit_client(&client, &pool, frame.len()) else {
        return Err(CallError::NotDelivered(format!(
            "{endpoint} is at the local native RPC in-flight or byte limit"
        )));
    };
    let deadline = TokioInstant::now() + timeout;
    let reservation = pool.reserve(&client, &request.request_id, deadline).await?;
    let connection = reservation.connection;
    let admission = ClientAdmission {
        _base: base,
        _connection: reservation.permit,
    };
    let request_id = request.request_id.clone();
    let (send, mut receive) = oneshot::channel();
    if !connection.register(request_id.clone(), send, admission, blob_owners) {
        return Err(CallError::NotDelivered(format!(
            "request id {request_id:?} is already in flight to {endpoint}"
        )));
    }
    if cancellation
        .as_ref()
        .is_some_and(|cancellation| !cancellation.register(&connection, &request_id))
    {
        connection.abandon(&request_id);
        return Err(CallError::NotDelivered("local call was cancelled".into()));
    }
    if connection
        .outbound
        .try_send(WriteCommand {
            request_id: request_id.clone(),
            frame,
            deadline,
            tracked: true,
        })
        .is_err()
    {
        connection.poison(
            CallError::NotDelivered(format!(
                "the request could not enter the writer for {endpoint}"
            )),
            CallError::OutcomeUnknown(format!(
                "{endpoint} writer stopped after another delivered request"
            )),
            false,
        );
    }
    let result = match tokio::time::timeout_at(deadline, &mut receive).await {
        Ok(result) => result.unwrap_or_else(|_| {
            Err(CallError::OutcomeUnknown(
                "the native RPC connection ended without an outcome".into(),
            ))
        }),
        Err(_) => Err(timed_out_error(&connection, &request_id, &endpoint)),
    };
    if let Some(cancellation) = cancellation {
        cancellation.clear(&request_id);
    }
    result
}

fn try_admit_client(
    client: &Arc<ClientInner>,
    pool: &Arc<ConnectionPool>,
    length: usize,
) -> Option<ClientBaseAdmission> {
    Some(ClientBaseAdmission {
        _global: client.inflight.clone().try_acquire_owned().ok()?,
        _endpoint: pool.inflight.clone().try_acquire_owned().ok()?,
        _frame: try_admit_frame(&client.frames, &pool.frames, length)?,
    })
}

async fn client_writer(
    connection: Arc<ClientConnection>,
    mut requests: mpsc::Receiver<WriteCommand>,
    mut writer: OwnedWriteHalf,
) {
    while let Some(request) = tokio::select! {
        _ = connection.shutdown.notified() => None,
        request = requests.recv() => request,
    } {
        if request.tracked && !connection.mark_writing(&request.request_id) {
            continue;
        }
        match tokio::time::timeout_at(
            request.deadline,
            write_frame_bytes(&mut writer, &request.frame, MAX_RPC_FRAME_BYTES),
        )
        .await
        {
            Ok(Ok(())) => {
                if request.tracked {
                    connection.mark_written(&request.request_id);
                }
            }
            Ok(Err(error)) => {
                let endpoint = connection.endpoint();
                connection.poison(
                    CallError::NotDelivered(format!(
                        "the request was not completely written to {endpoint}: {error}"
                    )),
                    CallError::OutcomeUnknown(format!(
                        "{endpoint} failed after a complete request was written: {error}"
                    )),
                    false,
                );
                break;
            }
            Err(_) => {
                let endpoint = connection.endpoint();
                connection.poison(
                    CallError::NotDelivered(format!(
                        "the request was not completely written to {endpoint} before timeout"
                    )),
                    CallError::OutcomeUnknown(format!(
                        "{endpoint} failed after a complete request was written"
                    )),
                    false,
                );
                break;
            }
        }
    }
    drop(writer);
    connection.writer_finished.store(true, Ordering::Release);
    connection.writer_done.notify_one();
}

async fn client_reader(
    connection: Arc<ClientConnection>,
    mut reader: OwnedReadHalf,
    admission: Arc<Mutex<Option<OwnedSemaphorePermit>>>,
) {
    loop {
        let frame = tokio::select! {
            _ = connection.shutdown.notified() => break,
            frame = read_frame(&mut reader, MAX_RPC_FRAME_BYTES) => frame,
        };
        let bytes = match frame {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                connection.poison(
                    CallError::NotDelivered("peer closed before delivery".into()),
                    CallError::OutcomeUnknown("peer closed after delivery".into()),
                    true,
                );
                break;
            }
            Err(error) => {
                connection.poison(
                    CallError::NotDelivered(error.to_string()),
                    CallError::OutcomeUnknown(error.to_string()),
                    true,
                );
                break;
            }
        };
        let reply: RpcReply = match decode_message(&bytes) {
            Ok(reply) => reply,
            Err(error) => {
                connection.poison(
                    CallError::NotDelivered(error.to_string()),
                    CallError::OutcomeUnknown(error.to_string()),
                    true,
                );
                break;
            }
        };
        if reply.protocol != RPC_PROTOCOL {
            connection.poison(
                CallError::NotDelivered("reply protocol mismatch".into()),
                CallError::OutcomeUnknown("reply protocol mismatch".into()),
                true,
            );
            break;
        }
        if connection.route(reply) {
            break;
        }
    }
    while !connection.writer_finished.load(Ordering::Acquire) {
        connection.writer_done.notified().await;
    }
    connection.tracking.lock().unwrap().take();
    admission.lock().unwrap().take();
}

async fn client_idle(connection: Arc<ClientConnection>) {
    loop {
        if connection.poisoned.load(Ordering::Acquire) {
            return;
        }
        let deadline = {
            let state = connection.state.lock().unwrap();
            (state.is_quiescent())
                .then(|| TokioInstant::from_std(state.last_activity + CLIENT_IDLE_TIMEOUT))
        };
        match deadline {
            Some(deadline) => {
                tokio::select! {
                    _ = connection.shutdown.notified() => return,
                    _ = connection.activity.notified() => continue,
                    _ = tokio::time::sleep_until(deadline) => {
                        if connection.is_idle() {
                            connection.poison(
                                CallError::NotDelivered("idle connection expired".into()),
                                CallError::OutcomeUnknown("idle connection expired".into()),
                                true,
                            );
                            return;
                        }
                    }
                }
            }
            None => {
                tokio::select! {
                    _ = connection.shutdown.notified() => return,
                    _ = connection.activity.notified() => {}
                }
            }
        }
    }
}
