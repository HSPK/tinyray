use super::*;

#[derive(Clone)]
pub struct ServerConfig {
    pub listen: String,
    pub identity: String,
    pub max_concurrency: Option<usize>,
    pub ownership: Option<Arc<AtomicBool>>,
    pub worker_threads: usize,
    pub max_blob_refs_per_reply: usize,
    pub max_blob_bytes_per_reply: usize,
    pub max_unacked_blob_refs_per_connection: usize,
    pub max_unacked_blob_bytes_per_connection: usize,
    pub max_unacked_blob_refs: usize,
    pub max_unacked_blob_bytes: usize,
}

impl ServerConfig {
    pub fn new(listen: impl Into<String>, identity: impl Into<String>) -> Self {
        Self {
            listen: listen.into(),
            identity: identity.into(),
            max_concurrency: None,
            ownership: None,
            worker_threads: 4,
            max_blob_refs_per_reply: MAX_BLOB_REFS_PER_REPLY,
            max_blob_bytes_per_reply: MAX_BLOB_BYTES_PER_REPLY,
            max_unacked_blob_refs_per_connection: MAX_UNACKED_BLOB_REFS_PER_CONNECTION,
            max_unacked_blob_bytes_per_connection: MAX_UNACKED_BLOB_BYTES_PER_CONNECTION,
            max_unacked_blob_refs: MAX_UNACKED_BLOB_REFS_PER_SERVER,
            max_unacked_blob_bytes: MAX_UNACKED_BLOB_BYTES_PER_SERVER,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ServerStats {
    pub calls: u64,
    pub refused: u64,
    pub connections_refused: u64,
    pub failed: u64,
    pub in_flight: usize,
    pub peak_in_flight: usize,
    pub busy_ms: u64,
    pub connections: usize,
    pub frames_in_flight: usize,
    pub small_frame_bytes: usize,
    pub bulk_frame_bytes: usize,
    pub unacked_blob_refs: usize,
    pub unacked_blob_bytes: usize,
}

pub struct Server {
    pid: u32,
    pub(super) runtime: Option<Arc<RuntimeOwner>>,
    state: Arc<ServerState>,
    endpoint: String,
}

struct ServerState {
    identity: String,
    service: Arc<dyn Service>,
    ownership: Option<Arc<AtomicBool>>,
    admission: Option<Arc<Semaphore>>,
    connection_admission: Arc<Semaphore>,
    global_connections: Arc<Semaphore>,
    frames: Arc<FrameBudgets>,
    global_frames: Arc<FrameBudgets>,
    blob_budget: Arc<BlobReplyBudget>,
    global_blob_budget: Arc<BlobReplyBudget>,
    max_blob_refs_per_reply: usize,
    max_blob_bytes_per_reply: usize,
    max_connection_blob_refs: usize,
    max_connection_blob_bytes: usize,
    counters: Arc<ServerCounters>,
    closed: AtomicBool,
    shutdown: Notify,
    finished: (Mutex<bool>, Condvar),
    fds: Mutex<HashSet<i32>>,
}

#[derive(Default)]
struct ServerCounters {
    calls: AtomicU64,
    refused: AtomicU64,
    connections_refused: AtomicU64,
    failed: AtomicU64,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    busy_ns: AtomicU64,
}

impl Server {
    pub fn start(config: ServerConfig, service: Arc<dyn Service>) -> Result<Self, CallError> {
        RpcRuntime::new(config.worker_threads)?.start_server(config, service)
    }

    pub fn start_on(
        handle: &tokio::runtime::Handle,
        config: ServerConfig,
        service: Arc<dyn Service>,
    ) -> Result<Self, CallError> {
        Self::start_inner(config, service, handle.clone(), None)
    }

    pub(super) fn start_inner(
        config: ServerConfig,
        service: Arc<dyn Service>,
        handle: tokio::runtime::Handle,
        runtime: Option<Arc<RuntimeOwner>>,
    ) -> Result<Self, CallError> {
        if config.max_concurrency == Some(0) {
            return Err(CallError::NotDelivered(
                "max_concurrency must be positive".into(),
            ));
        }
        if config.max_blob_refs_per_reply == 0
            || config.max_blob_bytes_per_reply == 0
            || config.max_unacked_blob_refs_per_connection < config.max_blob_refs_per_reply
            || config.max_unacked_blob_bytes_per_connection < config.max_blob_bytes_per_reply
            || config.max_unacked_blob_refs < config.max_unacked_blob_refs_per_connection
            || config.max_unacked_blob_bytes < config.max_unacked_blob_bytes_per_connection
        {
            return Err(CallError::NotDelivered(
                "BlobRef reply budgets must be positive and nested reply <= connection <= server"
                    .into(),
            ));
        }
        let std_listener = bind_tcp_listener(&config.listen)
            .map_err(|error| CallError::NotDelivered(error.to_string()))?;
        let endpoint = std_listener
            .local_addr()
            .map_err(|error| CallError::NotDelivered(error.to_string()))?
            .to_string();
        let listener = {
            let _entered = handle.enter();
            TcpListener::from_std(std_listener)
                .map_err(|error| CallError::NotDelivered(error.to_string()))?
        };
        let state = Arc::new(ServerState {
            identity: config.identity,
            service,
            ownership: config.ownership,
            admission: config
                .max_concurrency
                .map(|limit| Arc::new(Semaphore::new(limit))),
            connection_admission: Arc::new(Semaphore::new(SERVER_CONNECTION_LIMIT)),
            global_connections: server_global_connections(),
            frames: Arc::new(FrameBudgets::new(
                SERVER_FRAME_LIMIT,
                SERVER_SMALL_FRAME_BUDGET_BYTES,
                SERVER_BULK_FRAME_BUDGET_BYTES,
            )),
            global_frames: server_global_frames(),
            blob_budget: Arc::new(BlobReplyBudget::new(
                config.max_unacked_blob_refs,
                config.max_unacked_blob_bytes,
            )),
            global_blob_budget: server_global_blob_replies(),
            max_blob_refs_per_reply: config.max_blob_refs_per_reply,
            max_blob_bytes_per_reply: config.max_blob_bytes_per_reply,
            max_connection_blob_refs: config.max_unacked_blob_refs_per_connection,
            max_connection_blob_bytes: config.max_unacked_blob_bytes_per_connection,
            counters: Arc::new(ServerCounters::default()),
            closed: AtomicBool::new(false),
            shutdown: Notify::new(),
            finished: (Mutex::new(false), Condvar::new()),
            fds: Mutex::new(HashSet::new()),
        });
        handle.spawn(accept_loop(state.clone(), listener));
        Ok(Self {
            pid: std::process::id(),
            runtime,
            state,
            endpoint,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn methods(&self) -> &[String] {
        self.state.service.methods()
    }

    pub fn stats(&self) -> ServerStats {
        let counters = &self.state.counters;
        let (unacked_blob_refs, unacked_blob_bytes) = self.state.blob_budget.usage();
        ServerStats {
            calls: counters.calls.load(Ordering::Relaxed),
            refused: counters.refused.load(Ordering::Relaxed),
            connections_refused: counters.connections_refused.load(Ordering::Relaxed),
            failed: counters.failed.load(Ordering::Relaxed),
            in_flight: counters.in_flight.load(Ordering::Relaxed),
            peak_in_flight: counters.peak_in_flight.load(Ordering::Relaxed),
            busy_ms: counters.busy_ns.load(Ordering::Relaxed) / 1_000_000,
            connections: SERVER_CONNECTION_LIMIT
                - self.state.connection_admission.available_permits(),
            frames_in_flight: self.state.frames.in_flight(),
            small_frame_bytes: self.state.frames.small_bytes(),
            bulk_frame_bytes: self.state.frames.bulk_bytes(),
            unacked_blob_refs,
            unacked_blob_bytes,
        }
    }

    pub fn close(&mut self) {
        self.state.begin_close();
        if std::process::id() == self.pid {
            self.state.wait_finished();
        }
        self.runtime.take();
    }

    pub fn abandon(&mut self) {
        self.state.close_inherited_fds();
        clear_inherited_rpc_blob_owners();
        reset_blob_reply_budgets_after_fork();
        if let Some(runtime) = self.runtime.take() {
            std::mem::forget(runtime);
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.runtime.is_some() {
            self.close();
        }
    }
}

impl ServerState {
    fn begin_close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.shutdown.notify_waiters();
            #[cfg(unix)]
            for fd in self.fds.lock().unwrap().iter().copied() {
                unsafe {
                    libc::shutdown(fd, libc::SHUT_RDWR);
                }
            }
        }
    }

    fn wait_finished(&self) {
        let (lock, bell) = &self.finished;
        let mut finished = lock.lock().unwrap();
        while !*finished {
            finished = bell.wait(finished).unwrap();
        }
    }

    fn mark_finished(&self) {
        let (lock, bell) = &self.finished;
        *lock.lock().unwrap() = true;
        bell.notify_all();
    }

    fn close_inherited_fds(&self) {
        #[cfg(unix)]
        for fd in self.fds.lock().unwrap().drain() {
            unsafe {
                libc::close(fd);
            }
        }
    }
}

struct ConnectionAdmission {
    _global: OwnedSemaphorePermit,
    _server: OwnedSemaphorePermit,
}

fn try_admit_connection(
    global: &Arc<Semaphore>,
    server: &Arc<Semaphore>,
) -> Option<ConnectionAdmission> {
    let global = global.clone().try_acquire_owned().ok()?;
    let server = server.clone().try_acquire_owned().ok()?;
    Some(ConnectionAdmission {
        _global: global,
        _server: server,
    })
}

struct Flight {
    counters: Arc<ServerCounters>,
    started: Instant,
    answered: bool,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Flight {
    fn new(counters: Arc<ServerCounters>, permit: Option<OwnedSemaphorePermit>) -> Self {
        let current = counters.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        counters
            .peak_in_flight
            .fetch_max(current, Ordering::Relaxed);
        Self {
            counters,
            started: Instant::now(),
            answered: false,
            _permit: permit,
        }
    }

    fn answered(&mut self, failed: bool) {
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.counters.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.answered = true;
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        if !self.answered {
            self.counters.calls.fetch_add(1, Ordering::Relaxed);
            self.counters.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.counters.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.counters.busy_ns.fetch_add(
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

async fn accept_loop(state: Arc<ServerState>, listener: TcpListener) {
    #[cfg(unix)]
    state.fds.lock().unwrap().insert(listener.as_raw_fd());
    let mut connections = JoinSet::new();
    loop {
        if state.closed.load(Ordering::Acquire) {
            break;
        }
        tokio::select! {
            _ = state.shutdown.notified() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let Some(admission) = try_admit_connection(
                        &state.global_connections,
                        &state.connection_admission,
                    ) else {
                        state.counters.connections_refused.fetch_add(1, Ordering::Relaxed);
                        continue;
                    };
                    let _ = stream.set_nodelay(true);
                    connections.spawn(serve_connection(
                        state.clone(),
                        stream,
                        admission,
                    ));
                }
                Err(_) if state.closed.load(Ordering::Acquire) => break,
                Err(error) => {
                    eprintln!("tinyray Rust service accept failed: {error}");
                    break;
                }
            },
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    eprintln!("tinyray Rust service connection failed: {error}");
                }
            }
        }
    }
    state.closed.store(true, Ordering::Release);
    state.shutdown.notify_waiters();
    while connections.join_next().await.is_some() {}
    #[cfg(unix)]
    state.fds.lock().unwrap().remove(&listener.as_raw_fd());
    drop(listener);
    state.mark_finished();
}

struct ServerConnection {
    writer: AsyncMutex<Option<OwnedWriteHalf>>,
    closed: AtomicBool,
    shutdown: Notify,
    activity: Notify,
    cancellations: Mutex<HashMap<String, CancellationToken>>,
    blob_responses: Mutex<HashMap<String, BlobResponseLease>>,
    blob_budget: Arc<BlobReplyBudget>,
    server_blob_budget: Arc<BlobReplyBudget>,
    process_blob_budget: Arc<BlobReplyBudget>,
    max_blob_refs_per_reply: usize,
    max_blob_bytes_per_reply: usize,
}

impl ServerConnection {
    async fn write(&self, reply: RpcReply) -> bool {
        let bytes = match encode_message(&reply, MAX_RPC_FRAME_BYTES) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.blob_responses
                    .lock()
                    .unwrap()
                    .remove(&reply.request_id);
                return false;
            }
        };
        let mut writer = self.writer.lock().await;
        let Some(writer) = writer.as_mut() else {
            self.blob_responses
                .lock()
                .unwrap()
                .remove(&reply.request_id);
            return false;
        };
        let written = matches!(
            tokio::time::timeout(
                SERVER_WRITE_TIMEOUT,
                write_frame_bytes(writer, &bytes, MAX_RPC_FRAME_BYTES)
            )
            .await,
            Ok(Ok(()))
        );
        if !written {
            self.blob_responses
                .lock()
                .unwrap()
                .remove(&reply.request_id);
        }
        written
    }

    fn admit_blob_response(&self, reply: &mut RpcReply, blob_owners: Vec<BlobRef>) {
        let mut unique = Vec::with_capacity(blob_owners.len());
        let mut bytes = 0usize;
        for owner in blob_owners {
            if unique
                .iter()
                .any(|existing: &BlobRef| existing.shares_storage(&owner))
            {
                continue;
            }
            bytes = match bytes.checked_add(owner.mapped_len()) {
                Some(bytes) => bytes,
                None => {
                    Self::reject_blob_response(reply, "BlobRef reply byte count overflowed");
                    return;
                }
            };
            unique.push(owner);
        }
        if unique.is_empty() {
            return;
        }
        if unique.len() > self.max_blob_refs_per_reply {
            Self::reject_blob_response(
                reply,
                format!(
                    "reply contains {} unique BlobRefs, above the limit of {}",
                    unique.len(),
                    self.max_blob_refs_per_reply
                ),
            );
            return;
        }
        if bytes > self.max_blob_bytes_per_reply {
            Self::reject_blob_response(
                reply,
                format!(
                    "reply contains {bytes} BlobRef bytes, above the limit of {}",
                    self.max_blob_bytes_per_reply
                ),
            );
            return;
        }
        let Some(process) = self.process_blob_budget.try_acquire(unique.len(), bytes) else {
            Self::reject_blob_response(reply, "process BlobRef reply budget is full");
            return;
        };
        let Some(server) = self.server_blob_budget.try_acquire(unique.len(), bytes) else {
            drop(process);
            Self::reject_blob_response(reply, "server BlobRef reply budget is full");
            return;
        };
        let Some(connection) = self.blob_budget.try_acquire(unique.len(), bytes) else {
            drop(server);
            drop(process);
            Self::reject_blob_response(reply, "connection BlobRef reply budget is full");
            return;
        };
        let owners = match RpcBlobOwners::track(unique) {
            Ok(owners) => owners,
            Err(error) => {
                drop(connection);
                drop(server);
                drop(process);
                Self::reject_blob_response(reply, error.to_string());
                return;
            }
        };
        reply.blob_refs = true;
        self.blob_responses.lock().unwrap().insert(
            reply.request_id.clone(),
            BlobResponseLease {
                _owners: owners,
                _admission: BlobReplyAdmission {
                    _process: process,
                    _server: server,
                    _connection: connection,
                },
                expires_at: Instant::now() + SERVER_BLOB_ACK_TIMEOUT,
            },
        );
        self.activity.notify_waiters();
    }

    fn acknowledge_blobs(&self, request_id: &str) {
        self.blob_responses.lock().unwrap().remove(request_id);
        self.activity.notify_waiters();
    }

    fn reject_blob_response(reply: &mut RpcReply, message: impl Into<String>) {
        reply.status = RpcStatus::Internal;
        reply.batch_index = None;
        reply.completed = None;
        reply.error = Some(RpcError::new("BlobError", message, ""));
        reply.blob_refs = false;
        reply.payload.clear();
    }

    fn has_unacknowledged_blobs(&self, request_id: &str) -> bool {
        self.blob_responses.lock().unwrap().contains_key(request_id)
    }

    fn read_deadline(&self) -> TokioInstant {
        let responses = self.blob_responses.lock().unwrap();
        responses
            .values()
            .map(|lease| lease.expires_at)
            .min()
            .map(TokioInstant::from_std)
            .unwrap_or_else(|| TokioInstant::now() + SERVER_FRAME_TIMEOUT)
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            for token in self.cancellations.lock().unwrap().values() {
                token.cancel();
            }
            self.blob_responses.lock().unwrap().clear();
            self.shutdown.notify_waiters();
            self.activity.notify_waiters();
        }
    }
}

enum ServerConnectionEvent {
    Ready(std::io::Result<()>),
    Joined(Option<Result<(), tokio::task::JoinError>>),
    Activity,
    TimedOut,
    Shutdown,
}

async fn wait_server_readable(
    reader: &mut OwnedReadHalf,
    deadline: TokioInstant,
) -> Result<std::io::Result<()>, tokio::time::error::Elapsed> {
    tokio::time::timeout_at(deadline, reader.readable()).await
}

async fn serve_connection(
    state: Arc<ServerState>,
    stream: TcpStream,
    _admission: ConnectionAdmission,
) {
    #[cfg(unix)]
    let fd = stream.as_raw_fd();
    #[cfg(not(unix))]
    let fd = 0;
    state.fds.lock().unwrap().insert(fd);
    let (mut reader, writer) = stream.into_split();
    let connection = Arc::new(ServerConnection {
        writer: AsyncMutex::new(Some(writer)),
        closed: AtomicBool::new(false),
        shutdown: Notify::new(),
        activity: Notify::new(),
        cancellations: Mutex::new(HashMap::new()),
        blob_responses: Mutex::new(HashMap::new()),
        blob_budget: Arc::new(BlobReplyBudget::new(
            state.max_connection_blob_refs,
            state.max_connection_blob_bytes,
        )),
        server_blob_budget: state.blob_budget.clone(),
        process_blob_budget: state.global_blob_budget.clone(),
        max_blob_refs_per_reply: state.max_blob_refs_per_reply,
        max_blob_bytes_per_reply: state.max_blob_bytes_per_reply,
    });
    let active_ids = Arc::new(Mutex::new(HashSet::new()));
    let mut requests = JoinSet::new();
    loop {
        while let Some(joined) = requests.try_join_next() {
            if joined.is_err() {
                connection.close();
                break;
            }
        }
        if state.closed.load(Ordering::Acquire) || connection.closed.load(Ordering::Acquire) {
            break;
        }
        let mut first = [0u8; 1];
        let activity = connection.activity.notified();
        tokio::pin!(activity);
        activity.as_mut().enable();
        let deadline = connection.read_deadline();
        let event = if requests.is_empty() {
            tokio::select! {
                _ = state.shutdown.notified() => ServerConnectionEvent::Shutdown,
                _ = connection.shutdown.notified() => ServerConnectionEvent::Shutdown,
                _ = activity => ServerConnectionEvent::Activity,
                ready = wait_server_readable(&mut reader, deadline) => match ready {
                    Ok(ready) => ServerConnectionEvent::Ready(ready),
                    Err(_) => ServerConnectionEvent::TimedOut,
                },
            }
        } else {
            tokio::select! {
                _ = state.shutdown.notified() => ServerConnectionEvent::Shutdown,
                _ = connection.shutdown.notified() => ServerConnectionEvent::Shutdown,
                _ = activity => ServerConnectionEvent::Activity,
                joined = requests.join_next() => ServerConnectionEvent::Joined(joined),
                ready = wait_server_readable(&mut reader, deadline) => match ready {
                    Ok(ready) => ServerConnectionEvent::Ready(ready),
                    Err(_) => ServerConnectionEvent::TimedOut,
                },
            }
        };
        match event {
            ServerConnectionEvent::Shutdown | ServerConnectionEvent::TimedOut => break,
            ServerConnectionEvent::Activity | ServerConnectionEvent::Joined(Some(Ok(()))) => {
                continue;
            }
            ServerConnectionEvent::Joined(Some(Err(_))) => {
                connection.close();
                break;
            }
            ServerConnectionEvent::Joined(None) => continue,
            ServerConnectionEvent::Ready(Err(error)) => {
                connection
                    .write(RpcReply::error(
                        "",
                        RpcStatus::MalformedProtocol,
                        "ProtocolError",
                        error.to_string(),
                    ))
                    .await;
                break;
            }
            ServerConnectionEvent::Ready(Ok(())) => match reader.try_read(&mut first) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(error) => {
                    connection
                        .write(RpcReply::error(
                            "",
                            RpcStatus::MalformedProtocol,
                            "ProtocolError",
                            error.to_string(),
                        ))
                        .await;
                    break;
                }
            },
        }
        let frame = tokio::select! {
            _ = state.shutdown.notified() => break,
            _ = connection.shutdown.notified() => break,
            frame = read_server_frame_after_first(
                &mut reader,
                first[0],
                &state.global_frames,
                &state.frames,
                deadline,
            ) => frame,
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(ServerFrameError::TimedOut) => break,
            Err(ServerFrameError::Admission { length }) => {
                connection
                    .write(RpcReply::error(
                        "",
                        RpcStatus::ConcurrencyRefused,
                        "Busy",
                        format!(
                            "cannot admit a declared {length}-byte RPC frame within the bounded connection and byte budgets"
                        ),
                    ))
                    .await;
                break;
            }
            Err(ServerFrameError::Frame(error)) => {
                connection
                    .write(RpcReply::error(
                        "",
                        RpcStatus::MalformedProtocol,
                        "ProtocolError",
                        error.to_string(),
                    ))
                    .await;
                break;
            }
        };
        let header: RpcRequestHeader = match decode_message(&frame.bytes) {
            Ok(header) => header,
            Err(error) => {
                connection
                    .write(RpcReply::error(
                        "",
                        RpcStatus::MalformedProtocol,
                        "ProtocolError",
                        error.to_string(),
                    ))
                    .await;
                break;
            }
        };
        let request = match decode_server_request(&frame.bytes) {
            Ok(request) => request,
            Err(error) => {
                connection
                    .write(RpcReply::error(
                        header.request_id,
                        RpcStatus::MalformedProtocol,
                        "ProtocolError",
                        error.to_string(),
                    ))
                    .await;
                break;
            }
        };
        if let Err(error) = validate_request(&request) {
            connection
                .write(RpcReply::error(
                    request.request_id,
                    RpcStatus::MalformedProtocol,
                    "ProtocolError",
                    error,
                ))
                .await;
            break;
        }
        if request.operation == RpcOperation::BlobAck {
            connection.acknowledge_blobs(&request.request_id);
            continue;
        }
        if connection.has_unacknowledged_blobs(&request.request_id) {
            connection
                .write(RpcReply::error(
                    request.request_id,
                    RpcStatus::MalformedProtocol,
                    "ProtocolError",
                    "request id still owns an unacknowledged BlobRef response",
                ))
                .await;
            break;
        }
        if request.target != state.identity {
            connection
                .write(RpcReply::error(
                    request.request_id,
                    RpcStatus::Fenced,
                    "Fenced",
                    format!("{} is served here, not {}", state.identity, request.target),
                ))
                .await;
            continue;
        }
        if request.operation == RpcOperation::Call
            && request
                .method
                .as_ref()
                .is_none_or(|method| !state.service.methods().contains(method))
        {
            connection
                .write(RpcReply::error(
                    request.request_id,
                    RpcStatus::MethodNotFound,
                    "AttributeError",
                    "method is not advertised",
                ))
                .await;
            continue;
        }
        if state
            .ownership
            .as_ref()
            .is_some_and(|owned| !owned.load(Ordering::Acquire))
        {
            connection
                .write(RpcReply::error(
                    request.request_id,
                    RpcStatus::Fenced,
                    "Fenced",
                    "the member is held by a later tenure",
                ))
                .await;
            continue;
        }
        let duplicate = !active_ids
            .lock()
            .unwrap()
            .insert(request.request_id.clone());
        if duplicate {
            connection
                .write(RpcReply::error(
                    request.request_id,
                    RpcStatus::MalformedProtocol,
                    "ProtocolError",
                    "request id is already in flight",
                ))
                .await;
            break;
        }
        let permit = match &state.admission {
            Some(admission) => match admission.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    active_ids.lock().unwrap().remove(&request.request_id);
                    state.counters.refused.fetch_add(1, Ordering::Relaxed);
                    connection
                        .write(RpcReply::error(
                            request.request_id,
                            RpcStatus::ConcurrencyRefused,
                            "Busy",
                            "at the concurrency limit",
                        ))
                        .await;
                    continue;
                }
            },
            None => None,
        };
        let token = CancellationToken::default();
        connection
            .cancellations
            .lock()
            .unwrap()
            .insert(request.request_id.clone(), token.clone());
        let state_for_request = state.clone();
        let connection_for_request = connection.clone();
        let ids = active_ids.clone();
        requests.spawn(async move {
            let request_id = request.request_id.clone();
            let mut flight = Flight::new(state_for_request.counters.clone(), permit);
            let context = CallContext {
                caller: Arc::from(request.caller),
                request_id: Arc::from(request_id.clone()),
                target: Arc::from(request.target),
                cancellation: token,
            };
            let response = state_for_request
                .service
                .dispatch(ServiceRequest {
                    context,
                    operation: request.operation,
                    method: request.method,
                    batch_len: request.batch_len,
                    payload: request.payload,
                })
                .await;
            let mut reply = RpcReply {
                protocol: RPC_PROTOCOL,
                request_id: request_id.clone(),
                status: response.status,
                batch_index: response.batch_index,
                completed: response.completed,
                error: response.error,
                blob_refs: false,
                payload: response.payload,
            };
            let blob_owners = response.blob_owners;
            connection_for_request.admit_blob_response(&mut reply, blob_owners);
            flight.answered(reply.status != RpcStatus::Success);
            drop(flight);
            if !connection_for_request.write(reply).await {
                connection_for_request.close();
            }
            connection_for_request
                .cancellations
                .lock()
                .unwrap()
                .remove(&request_id);
            ids.lock().unwrap().remove(&request_id);
        });
    }
    connection.close();
    connection.writer.lock().await.take();
    while requests.join_next().await.is_some() {}
    state.fds.lock().unwrap().remove(&fd);
}

struct AdmittedFrame {
    bytes: Vec<u8>,
    _admission: FrameAdmission,
}

enum ServerFrameError {
    Frame(FrameError),
    TimedOut,
    Admission { length: usize },
}

async fn read_server_frame_after_first<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    first: u8,
    global: &Arc<FrameBudgets>,
    server: &Arc<FrameBudgets>,
    deadline: TokioInstant,
) -> Result<AdmittedFrame, ServerFrameError> {
    let mut prefix = [0u8; 4];
    prefix[0] = first;
    let mut received = 1;
    while received < prefix.len() {
        let count = tokio::time::timeout_at(deadline, reader.read(&mut prefix[received..]))
            .await
            .map_err(|_| ServerFrameError::TimedOut)?
            .map_err(|error| ServerFrameError::Frame(FrameError::Io(error)))?;
        if count == 0 {
            return Err(ServerFrameError::Frame(FrameError::TruncatedPrefix {
                received,
            }));
        }
        received += count;
    }
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 {
        return Err(ServerFrameError::Frame(FrameError::EmptyFrame));
    }
    if length > MAX_RPC_FRAME_BYTES {
        return Err(ServerFrameError::Frame(FrameError::FrameTooLarge {
            length,
            maximum: MAX_RPC_FRAME_BYTES,
        }));
    }
    let admission =
        try_admit_frame(global, server, length).ok_or(ServerFrameError::Admission { length })?;
    let bytes = tokio::time::timeout_at(deadline, read_frame_body(reader, length))
        .await
        .map_err(|_| ServerFrameError::TimedOut)?
        .map_err(ServerFrameError::Frame)?;
    Ok(AdmittedFrame {
        bytes,
        _admission: admission,
    })
}

#[derive(Deserialize)]
struct BorrowedRpcRequest<'a> {
    #[serde(rename = "v")]
    protocol: u16,
    #[serde(rename = "id")]
    request_id: String,
    #[serde(rename = "from")]
    caller: String,
    #[serde(rename = "to")]
    target: String,
    #[serde(rename = "op")]
    operation: RpcOperation,
    #[serde(default)]
    method: Option<String>,
    #[serde(rename = "batch", default)]
    batch_len: Option<u16>,
    #[serde(borrow, rename = "body", with = "serde_bytes")]
    payload: &'a [u8],
}

struct ServerRpcRequest {
    protocol: u16,
    request_id: String,
    caller: String,
    target: String,
    operation: RpcOperation,
    method: Option<String>,
    batch_len: Option<u16>,
    payload: Arc<[u8]>,
}

fn decode_server_request(bytes: &[u8]) -> Result<ServerRpcRequest, FrameError> {
    let request: BorrowedRpcRequest<'_> =
        rmp_serde::from_slice(bytes).map_err(|error| FrameError::Decode(error.to_string()))?;
    Ok(ServerRpcRequest {
        protocol: request.protocol,
        request_id: request.request_id,
        caller: request.caller,
        target: request.target,
        operation: request.operation,
        method: request.method,
        batch_len: request.batch_len,
        payload: Arc::from(request.payload),
    })
}

fn validate_request(request: &ServerRpcRequest) -> Result<(), String> {
    if request.protocol != RPC_PROTOCOL {
        return Err(format!(
            "method RPC protocol {} is unsupported; this build speaks {}",
            request.protocol, RPC_PROTOCOL
        ));
    }
    if request.request_id.is_empty()
        || request.request_id.len() > MAX_RPC_REQUEST_ID_BYTES
        || !request
            .request_id
            .bytes()
            .all(|byte| byte.is_ascii() && (b' '..=b'~').contains(&byte))
    {
        return Err("request id must be 1-200 bytes of printable ASCII".into());
    }
    match request.operation {
        RpcOperation::Call => {
            let valid_method = request.method.as_deref().is_some_and(|method| {
                let mut bytes = method.bytes();
                matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic())
                    && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            });
            if request.batch_len.is_some() || !valid_method {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn blob_reply_budget_enforces_count_bytes_and_releases() {
        let budget = Arc::new(BlobReplyBudget::new(2, 8));
        let first = budget.try_acquire(1, 4).unwrap();
        assert_eq!(budget.usage(), (1, 4));
        assert!(budget.try_acquire(2, 1).is_none());
        assert!(budget.try_acquire(1, 5).is_none());
        let second = budget.try_acquire(1, 4).unwrap();
        assert_eq!(budget.usage(), (2, 8));
        drop(first);
        assert_eq!(budget.usage(), (1, 4));
        drop(second);
        assert_eq!(budget.usage(), (0, 0));
    }

    #[test]
    fn connection_admission_is_global_and_per_server() {
        let global = Arc::new(Semaphore::new(2));
        let server = Arc::new(Semaphore::new(1));
        let first = try_admit_connection(&global, &server).unwrap();
        assert!(try_admit_connection(&global, &server).is_none());
        drop(first);
        assert!(try_admit_connection(&global, &server).is_some());
    }

    #[test]
    fn rpc_runtime_owns_one_worker_pool_for_client_and_server() {
        let runtime = RpcRuntime::new(2).unwrap();
        let client = runtime.client();
        let mut server = runtime
            .start_server(
                ServerConfig::new("127.0.0.1:0", "shared-runtime/0#1"),
                Arc::new(crate::service::Router::new()),
            )
            .unwrap();

        let client_runtime = client.runtime_owner().unwrap();
        let server_runtime = server.runtime.as_ref().unwrap();
        assert!(Arc::ptr_eq(&runtime.owner, client_runtime));
        assert!(Arc::ptr_eq(&runtime.owner, server_runtime));
        server.close();
    }

    #[test]
    fn bulk_server_frames_cannot_consume_the_small_control_frame_reserve() {
        let global = Arc::new(FrameBudgets::new(8, 4 << 20, 4 * MAX_RPC_FRAME_BYTES));
        let server = Arc::new(FrameBudgets::new(4, 2 << 20, 2 * MAX_RPC_FRAME_BYTES));
        let first = try_admit_frame(&global, &server, MAX_RPC_FRAME_BYTES).unwrap();
        let second = try_admit_frame(&global, &server, MAX_RPC_FRAME_BYTES).unwrap();
        assert!(try_admit_frame(&global, &server, MAX_RPC_FRAME_BYTES).is_none());
        let ordinary = try_admit_frame(&global, &server, 64 << 10)
            .expect("bulk reservations consumed the small-frame reserve");
        assert_eq!(server.bulk_bytes(), 2 * MAX_RPC_FRAME_BYTES);
        assert_eq!(server.small_bytes(), 64 << 10);
        drop((first, second, ordinary));
        assert_eq!(server.in_flight(), 0);
    }

    #[test]
    fn bulk_server_frame_bytes_are_bounded_globally() {
        let global = Arc::new(FrameBudgets::new(8, 4 << 20, 4 * MAX_RPC_FRAME_BYTES));
        let first_server = Arc::new(FrameBudgets::new(4, 2 << 20, 2 * MAX_RPC_FRAME_BYTES));
        let second_server = Arc::new(FrameBudgets::new(4, 2 << 20, 2 * MAX_RPC_FRAME_BYTES));
        let third_server = Arc::new(FrameBudgets::new(4, 2 << 20, 2 * MAX_RPC_FRAME_BYTES));
        let held = [
            try_admit_frame(&global, &first_server, MAX_RPC_FRAME_BYTES).unwrap(),
            try_admit_frame(&global, &first_server, MAX_RPC_FRAME_BYTES).unwrap(),
            try_admit_frame(&global, &second_server, MAX_RPC_FRAME_BYTES).unwrap(),
            try_admit_frame(&global, &second_server, MAX_RPC_FRAME_BYTES).unwrap(),
        ];
        assert!(try_admit_frame(&global, &third_server, MAX_RPC_FRAME_BYTES).is_none());
        assert_eq!(global.bulk_bytes(), 4 * MAX_RPC_FRAME_BYTES);
        drop(held);
        assert_eq!(global.bulk_bytes(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn silent_first_frame_readiness_uses_the_absolute_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let writer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, _) = stream.into_split();
        let started = TokioInstant::now();
        let deadline = started + Duration::from_secs(1);
        assert!(wait_server_readable(&mut reader, deadline).await.is_err());
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        drop(writer);
    }

    #[tokio::test]
    async fn readiness_does_not_consume_the_first_frame_byte() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut writer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, _) = stream.into_split();
        writer.write_all(&[0x7f]).await.unwrap();
        wait_server_readable(&mut reader, TokioInstant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        let mut first = [0u8; 1];
        assert_eq!(reader.try_read(&mut first).unwrap(), 1);
        assert_eq!(first, [0x7f]);
    }

    #[tokio::test(start_paused = true)]
    async fn prefix_and_body_share_the_original_absolute_deadline() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let sending = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            writer.write_all(&[0, 0, 4, 1, 2]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(600)).await;
            writer.write_all(&[3, 4]).await.unwrap();
        });
        let global = Arc::new(FrameBudgets::new(1, 8, 8));
        let server = Arc::new(FrameBudgets::new(1, 8, 8));
        let deadline = TokioInstant::now() + Duration::from_secs(1);
        assert!(matches!(
            read_server_frame_after_first(&mut reader, 0, &global, &server, deadline).await,
            Err(ServerFrameError::TimedOut)
        ));
        sending.abort();
    }
}
