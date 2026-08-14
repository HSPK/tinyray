//! The actor-side runtime.
//!
//! This is where the design's three-thread model becomes concrete:
//!
//! | thread            | language | job                                    |
//! |-------------------|----------|----------------------------------------|
//! | tokio pool        | Rust     | accept calls, serve result fetches      |
//! | executor          | Python   | run user methods                       |
//! | collective        | Python   | NCCL calls only                        |
//!
//! The tokio pool never needs the GIL, so an actor grinding through a 200 ms
//! step still answers `/task/fetch` immediately. The executor thread pulls work
//! with [`ActorRuntime::next_task`], which blocks *without* the GIL held.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tinyray_core::framing::Message;
use tinyray_core::proto::{Call, CallAck, Envelope, ErrorKind, Fetch, RemoteError, ResultHeader};
use tinyray_core::{ActorId, Limits, TaskId};

use crate::queue::{OrderedQueue, QueuedTask, RejectReason};
use crate::store::{Fetched, LocalStore, StoreConfig, StoredValue};
use crate::transport::paths;
use crate::transport::server::{error_reply, Handler, Reply, ServerConfig};

/// Knobs for an actor process.
#[derive(Debug, Clone)]
pub struct ActorConfig {
    pub actor_id: ActorId,
    /// Results at or below this size ride back inside the call acknowledgement,
    /// saving a round trip. Larger ones stay here until someone fetches them.
    pub inline_threshold: usize,
    /// Calls accepted before the actor starts refusing with 429.
    pub max_pending_calls: usize,
    pub store: StoreConfig,
    pub server: ServerConfig,
    /// Longest a fetch may park waiting for a result to be produced.
    pub max_fetch_wait: Duration,
}

impl Default for ActorConfig {
    fn default() -> Self {
        ActorConfig {
            actor_id: ActorId::NIL,
            // 256 KiB: big enough for scalars, metrics and small actions, well
            // below the 10 MB results that must not pass through the driver.
            inline_threshold: 256 * 1024,
            max_pending_calls: 1000,
            store: StoreConfig::default(),
            server: ServerConfig::default(),
            max_fetch_wait: Duration::from_secs(30),
        }
    }
}

/// A unit of work handed to the Python executor thread.
#[derive(Debug, Clone)]
pub struct Dispatch {
    pub task_id: TaskId,
    pub method: String,
    pub body: Bytes,
    pub frames: Vec<Bytes>,
}

/// Live counters for `/introspect`.
#[derive(Debug, Clone, Default)]
pub struct ActorStats {
    pub accepted: u64,
    pub completed: u64,
    pub failed: u64,
    pub rejected_backpressure: u64,
    pub rejected_duplicate: u64,
    pub reordered: u64,
    pub queued: usize,
    pub ready: usize,
    pub inflight: Option<String>,
    pub inflight_seconds: f64,
    pub store: crate::store::StoreStats,
}

struct Inflight {
    method: String,
    started: Instant,
}

/// Everything an actor process needs, minus the Python.
pub struct ActorRuntime {
    config: ActorConfig,
    queue: Mutex<OrderedQueue>,
    /// Signals the executor thread that `queue` may have work.
    work: tokio::sync::Notify,
    store: Arc<LocalStore>,
    inflight: Mutex<Option<Inflight>>,
    shutting_down: AtomicBool,
    accepted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    rejected_backpressure: AtomicU64,
    rejected_duplicate: AtomicU64,
}

impl ActorRuntime {
    pub fn new(config: ActorConfig) -> Arc<ActorRuntime> {
        let queue = OrderedQueue::new(config.max_pending_calls);
        let store = Arc::new(LocalStore::new(config.store));
        Arc::new(ActorRuntime {
            config,
            queue: Mutex::new(queue),
            work: tokio::sync::Notify::new(),
            store,
            inflight: Mutex::new(None),
            shutting_down: AtomicBool::new(false),
            accepted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            rejected_backpressure: AtomicU64::new(0),
            rejected_duplicate: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &ActorConfig {
        &self.config
    }

    pub fn store(&self) -> &Arc<LocalStore> {
        &self.store
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Wait for the next call to execute.
    ///
    /// Returns `None` once the actor is shutting down and the queue has
    /// drained. The Python executor thread parks here with the GIL released.
    pub async fn next_task(&self) -> Option<Dispatch> {
        loop {
            // Register interest before checking, or a call arriving in between
            // would leave us asleep with work waiting.
            let notified = self.work.notified();
            tokio::pin!(notified);

            if let Some(task) = self.queue.lock().pop() {
                *self.inflight.lock() = Some(Inflight {
                    method: task.method.clone(),
                    started: Instant::now(),
                });
                return Some(Dispatch {
                    task_id: task.task_id,
                    method: task.method,
                    body: task.body,
                    frames: task.frames,
                });
            }
            if self.is_shutting_down() {
                return None;
            }
            notified.await;
        }
    }

    /// Like [`ActorRuntime::next_task`], but gives up after `timeout`.
    ///
    /// The Python executor thread uses this so it returns to the interpreter
    /// regularly. Python only runs signal handlers while the main thread is
    /// executing bytecode, so a thread parked in Rust forever would ignore
    /// SIGTERM entirely and every shutdown would degrade to SIGKILL.
    pub async fn next_task_timeout(&self, timeout: Duration) -> Option<Dispatch> {
        tokio::time::timeout(timeout, self.next_task())
            .await
            .unwrap_or_default()
    }

    /// Publish a successful result.
    pub fn complete(&self, task_id: TaskId, body: Bytes, frames: Vec<Bytes>) {
        self.store.complete(task_id, StoredValue::new(body, frames));
        self.completed.fetch_add(1, Ordering::Relaxed);
        *self.inflight.lock() = None;
    }

    /// Publish a failure, carrying the remote traceback so the caller can see
    /// where it actually went wrong.
    pub fn fail(
        &self,
        task_id: TaskId,
        kind: ErrorKind,
        message: String,
        traceback: Option<String>,
    ) {
        self.store.fail(
            task_id,
            RemoteError {
                task_id,
                kind,
                message,
                traceback,
            },
        );
        self.failed.fetch_add(1, Ordering::Relaxed);
        *self.inflight.lock() = None;
    }

    /// Begin a clean shutdown: stop accepting, wake the executor, and fail
    /// anything still queued so no caller waits forever.
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        let abandoned = self.queue.lock().drain_all();
        for task in abandoned {
            self.store.fail(
                task.task_id,
                RemoteError {
                    task_id: task.task_id,
                    kind: ErrorKind::ActorDied,
                    message: "actor shut down before this call ran".into(),
                    traceback: None,
                },
            );
        }
        self.store.fail_all_pending(RemoteError {
            task_id: TaskId::NIL,
            kind: ErrorKind::ActorDied,
            message: "actor shut down before this call finished".into(),
            traceback: None,
        });
        self.work.notify_waiters();
    }

    pub fn stats(&self) -> ActorStats {
        let queue = self.queue.lock();
        let inflight = self.inflight.lock();
        ActorStats {
            accepted: self.accepted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            rejected_backpressure: self.rejected_backpressure.load(Ordering::Relaxed),
            rejected_duplicate: self.rejected_duplicate.load(Ordering::Relaxed),
            reordered: queue.reordered(),
            queued: queue.pending(),
            ready: queue.ready_len(),
            inflight: inflight.as_ref().map(|f| f.method.clone()),
            inflight_seconds: inflight
                .as_ref()
                .map(|f| f.started.elapsed().as_secs_f64())
                .unwrap_or(0.0),
            store: self.store.stats(),
        }
    }

    fn handle_call(&self, call: Call, message: Message) -> Reply {
        if self.is_shutting_down() {
            return error_reply(call.task_id, ErrorKind::ActorDied, "actor is shutting down");
        }
        if call.actor_id != self.config.actor_id && !self.config.actor_id.is_nil() {
            return error_reply(
                call.task_id,
                ErrorKind::NotFound,
                format!(
                    "call addressed to actor {} but this is {}",
                    call.actor_id, self.config.actor_id
                ),
            );
        }

        let body = message.frames.first().cloned().unwrap_or_default();
        let frames = message.frames.into_iter().skip(1).collect();
        let task = QueuedTask {
            task_id: call.task_id,
            caller_id: call.caller_id,
            seq: call.seq,
            method: call.method,
            body,
            frames,
        };

        // Declare before enqueuing: a fetch that races ahead of execution must
        // park rather than get `Unknown`.
        self.store.declare_pending(call.task_id);

        let outcome = self.queue.lock().push(task);
        match outcome {
            Ok(_) => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
                self.work.notify_one();
                ack(call.task_id, false, vec![])
            }
            Err(RejectReason::Backpressure) => {
                self.rejected_backpressure.fetch_add(1, Ordering::Relaxed);
                self.store.release(call.task_id);
                Reply::backpressure(format!(
                    "actor {} has {} calls queued (limit {})",
                    self.config.actor_id,
                    self.queue.lock().pending(),
                    self.config.max_pending_calls
                ))
            }
            Err(RejectReason::DuplicateSeq) => {
                // A retransmit of a call we already have. Acknowledge it
                // instead of running the method twice.
                self.rejected_duplicate.fetch_add(1, Ordering::Relaxed);
                ack(call.task_id, false, vec![])
            }
        }
    }

    async fn handle_fetch(&self, fetch: Fetch) -> Reply {
        let wait = Duration::from_millis(fetch.timeout_ms).min(self.config.max_fetch_wait);
        match self.store.get(fetch.task_id, wait).await {
            Fetched::Ready(value) => {
                // A status-only probe gets the verdict without the payload.
                let frames = if fetch.status_only {
                    Vec::new()
                } else {
                    let mut frames = Vec::with_capacity(value.frames.len() + 1);
                    frames.push(value.body);
                    frames.extend(value.frames);
                    frames
                };
                match Envelope::Result(ResultHeader {
                    task_id: fetch.task_id,
                })
                .into_message(frames)
                {
                    Ok(message) => Reply::Message(message),
                    Err(err) => error_reply(
                        fetch.task_id,
                        ErrorKind::Internal,
                        format!("failed to encode result: {err}"),
                    ),
                }
            }
            Fetched::Failed(err) => match Envelope::Error(err).into_message(vec![]) {
                Ok(message) => Reply::Message(message),
                Err(err) => error_reply(
                    fetch.task_id,
                    ErrorKind::Internal,
                    format!("failed to encode error: {err}"),
                ),
            },
            Fetched::NotReady => {
                // Distinct from an error: the caller should simply ask again.
                match Envelope::CallAck(CallAck {
                    task_id: fetch.task_id,
                    inline: false,
                })
                .into_message(vec![])
                {
                    Ok(message) => Reply::Message(message),
                    Err(err) => error_reply(
                        fetch.task_id,
                        ErrorKind::Internal,
                        format!("failed to encode ack: {err}"),
                    ),
                }
            }
            Fetched::Lost => error_reply(
                fetch.task_id,
                ErrorKind::ObjectLost,
                format!(
                    "result {} was evicted or expired; raise max_bytes/ttl or fetch sooner",
                    fetch.task_id
                ),
            ),
            Fetched::Unknown => error_reply(
                fetch.task_id,
                ErrorKind::NotFound,
                format!("this actor never produced result {}", fetch.task_id),
            ),
        }
    }
}

fn ack(task_id: TaskId, inline: bool, frames: Vec<Bytes>) -> Reply {
    match Envelope::CallAck(CallAck { task_id, inline }).into_message(frames) {
        Ok(message) => Reply::Message(message),
        Err(err) => Reply::Status(
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode ack: {err}"),
        ),
    }
}

impl Handler for ActorRuntime {
    fn handle<'a>(
        &'a self,
        path: &'a str,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = Reply> + Send + 'a>> {
        Box::pin(async move {
            let envelope = match Envelope::decode(&message.header) {
                Ok(envelope) => envelope,
                Err(err) => {
                    return Reply::Status(
                        hyper::StatusCode::BAD_REQUEST,
                        format!("undecodable header: {err}"),
                    )
                }
            };

            match (path, envelope) {
                (paths::CALL, Envelope::Call(call)) => self.handle_call(call, message),
                (paths::FETCH, Envelope::Fetch(fetch)) => self.handle_fetch(fetch).await,
                (paths::RELEASE, Envelope::Fetch(fetch)) => {
                    self.store.release(fetch.task_id);
                    ack(fetch.task_id, false, vec![])
                }
                (paths::SHUTDOWN, envelope) => {
                    self.begin_shutdown();
                    ack(envelope.task_id(), false, vec![])
                }
                (path, envelope) => Reply::not_found(format!(
                    "{path} does not accept a {} envelope",
                    envelope_name(&envelope)
                )),
            }
        })
    }

    fn handle_get<'a>(&'a self, path: &'a str) -> Pin<Box<dyn Future<Output = Reply> + Send + 'a>> {
        Box::pin(async move {
            match path {
                paths::HEALTH => Reply::Json(format!(
                    r#"{{"status":"ok","actor":"{}","shutting_down":{}}}"#,
                    self.config.actor_id,
                    self.is_shutting_down()
                )),
                paths::INTROSPECT => Reply::Json(self.introspect_json()),
                other => Reply::not_found(format!("no such path: {other}")),
            }
        })
    }
}

impl ActorRuntime {
    /// The answer to "what is this actor stuck on?", which in practice is the
    /// most common question in a distributed ML run.
    pub fn introspect_json(&self) -> String {
        let stats = self.stats();
        let stuck = self.queue.lock().stuck_callers();
        let stuck_json = stuck
            .iter()
            .map(|(caller, next_seq, buffered)| {
                format!(
                    r#"{{"caller":"{caller}","awaiting_seq":{next_seq},"buffered":{buffered}}}"#
                )
            })
            .collect::<Vec<_>>()
            .join(",");

        format!(
            r#"{{"actor":"{}","accepted":{},"completed":{},"failed":{},"rejected_backpressure":{},"rejected_duplicate":{},"reordered":{},"queued":{},"ready":{},"inflight":{},"inflight_seconds":{:.3},"store":{{"pending":{},"ready":{},"failed":{},"bytes":{},"evictions":{},"expirations":{}}},"stuck_callers":[{}]}}"#,
            self.config.actor_id,
            stats.accepted,
            stats.completed,
            stats.failed,
            stats.rejected_backpressure,
            stats.rejected_duplicate,
            stats.reordered,
            stats.queued,
            stats.ready,
            stats
                .inflight
                .as_ref()
                .map(|m| format!("\"{m}\""))
                .unwrap_or_else(|| "null".to_string()),
            stats.inflight_seconds,
            stats.store.pending,
            stats.store.ready,
            stats.store.failed,
            stats.store.bytes,
            stats.store.evictions,
            stats.store.expirations,
            stuck_json,
        )
    }
}

fn envelope_name(envelope: &Envelope) -> &'static str {
    match envelope {
        Envelope::Call(_) => "Call",
        Envelope::CallAck(_) => "CallAck",
        Envelope::Fetch(_) => "Fetch",
        Envelope::Result(_) => "Result",
        Envelope::Error(_) => "Error",
    }
}

/// Build a call message ready to send.
///
/// The parameter list is long because a call header genuinely carries this
/// much: identity, ordering and payload all have to travel together.
#[allow(clippy::too_many_arguments)]
pub fn build_call(
    task_id: TaskId,
    actor_id: ActorId,
    caller_id: tinyray_core::CallerId,
    seq: u64,
    method: &str,
    body: Bytes,
    frames: Vec<Bytes>,
    want_inline: bool,
) -> Result<Message, tinyray_core::error::ProtoError> {
    let mut all = Vec::with_capacity(frames.len() + 1);
    all.push(body);
    all.extend(frames);
    Envelope::Call(Call {
        task_id,
        actor_id,
        caller_id,
        seq,
        method: method.to_string(),
        want_inline,
    })
    .into_message(all)
}

/// Build a fetch request.
///
/// `status_only` asks whether the result has settled without transferring it,
/// which is what `wait` needs: the alternative is moving every payload to the
/// driver just to answer a yes/no question.
pub fn build_fetch(
    task_id: TaskId,
    timeout: Duration,
    status_only: bool,
) -> Result<Message, tinyray_core::error::ProtoError> {
    Envelope::Fetch(Fetch {
        task_id,
        timeout_ms: timeout.as_millis().min(u64::MAX as u128) as u64,
        status_only,
    })
    .into_message(vec![])
}

/// Limits used by actors unless overridden.
pub fn default_limits() -> Limits {
    Limits::DEFAULT
}
