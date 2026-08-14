//! The driver-side runtime.
//!
//! Owns the tokio runtime, the connection pool, and the futures behind
//! `ObjectRef`. Python calls in here and gets out immediately: `.remote()`
//! submits and returns, `get()` blocks in Rust with the GIL released.
//!
//! The important structural property is that a reference can be *resolved by
//! whoever holds it*. An `ObjectRef` names `(task_id, owner_endpoint)`, so when
//! a rollout result is passed to the learner, the learner fetches it straight
//! from the rollout actor. The driver only ever moves the few dozen bytes of
//! the reference itself.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tinyray_core::proto::{Envelope, ErrorKind, RemoteError};
use tinyray_core::{ActorId, CallerId, TaskId};

use crate::actor::{build_call, build_fetch};
use crate::transport::client::{ClientConfig, TransportClient, TransportError};
use crate::transport::paths;

/// Where a result lives.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnerRef {
    pub task_id: TaskId,
    pub actor_id: ActorId,
    /// `host:port` of the actor that produced it.
    pub endpoint: String,
}

/// A value fetched from its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedValue {
    pub body: Bytes,
    pub frames: Vec<Bytes>,
}

/// Errors surfaced to the driver.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    #[error("{0}")]
    Remote(RemoteError),
    #[error("transport failure: {0}")]
    Transport(String),
    #[error("timed out waiting for result {task_id}")]
    Timeout { task_id: TaskId },
    #[error("no actor registered as {0}")]
    UnknownActor(ActorId),
}

impl ClientError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            ClientError::Remote(err) => err.kind,
            ClientError::Transport(_) => ErrorKind::Internal,
            ClientError::Timeout { .. } => ErrorKind::Internal,
            ClientError::UnknownActor(_) => ErrorKind::NotFound,
        }
    }

    pub fn traceback(&self) -> Option<&str> {
        match self {
            ClientError::Remote(err) => err.traceback.as_deref(),
            _ => None,
        }
    }
}

impl From<TransportError> for ClientError {
    fn from(err: TransportError) -> Self {
        ClientError::Transport(err.to_string())
    }
}

/// Per-actor state the driver keeps: where it is, and how many calls we have
/// sent it, which is what makes ordering work.
struct ActorEntry {
    endpoint: String,
    next_seq: AtomicU64,
}

/// Driver-side client.
pub struct ClientRuntime {
    caller_id: CallerId,
    transport: Arc<TransportClient>,
    actors: Mutex<HashMap<ActorId, Arc<ActorEntry>>>,
    inline_threshold: usize,
}

impl ClientRuntime {
    pub fn new(config: ClientConfig) -> Arc<ClientRuntime> {
        Arc::new(ClientRuntime {
            caller_id: CallerId::generate(),
            transport: TransportClient::new(config),
            actors: Mutex::new(HashMap::new()),
            inline_threshold: 256 * 1024,
        })
    }

    pub fn caller_id(&self) -> CallerId {
        self.caller_id
    }

    pub fn transport(&self) -> &Arc<TransportClient> {
        &self.transport
    }

    /// Record where an actor lives. Called after the node agent reports a
    /// freshly started actor, and again after a restart moves it.
    pub fn register_actor(&self, actor_id: ActorId, endpoint: String) {
        let mut actors = self.actors.lock();
        match actors.get(&actor_id) {
            // A restart resets the sequence numbering: the new process has no
            // memory of what the old one had already run.
            Some(entry) if entry.endpoint != endpoint => {
                actors.insert(
                    actor_id,
                    Arc::new(ActorEntry {
                        endpoint,
                        next_seq: AtomicU64::new(0),
                    }),
                );
            }
            Some(_) => {}
            None => {
                actors.insert(
                    actor_id,
                    Arc::new(ActorEntry {
                        endpoint,
                        next_seq: AtomicU64::new(0),
                    }),
                );
            }
        }
    }

    pub fn forget_actor(&self, actor_id: ActorId) {
        self.actors.lock().remove(&actor_id);
    }

    pub fn endpoint_of(&self, actor_id: ActorId) -> Option<String> {
        self.actors
            .lock()
            .get(&actor_id)
            .map(|entry| entry.endpoint.clone())
    }

    /// Submit a call and return the reference to its eventual result.
    ///
    /// Awaits only the acknowledgement, not the method: that is what makes
    /// `.remote()` non-blocking from Python's point of view.
    pub async fn submit(
        &self,
        actor_id: ActorId,
        method: &str,
        body: Bytes,
        frames: Vec<Bytes>,
    ) -> Result<OwnerRef, ClientError> {
        let entry = self
            .actors
            .lock()
            .get(&actor_id)
            .cloned()
            .ok_or(ClientError::UnknownActor(actor_id))?;

        let task_id = TaskId::generate();
        // Assign the sequence number here, at submission time, so it reflects
        // program order rather than the order requests happen to hit the wire.
        let seq = entry.next_seq.fetch_add(1, Ordering::SeqCst);
        let message = build_call(
            task_id,
            actor_id,
            self.caller_id,
            seq,
            method,
            body,
            frames,
            true,
        )
        .map_err(|err| ClientError::Transport(err.to_string()))?;

        let reply = self
            .transport
            .request(&entry.endpoint, paths::CALL, &message)
            .await?;

        match Envelope::decode(&reply.header) {
            Ok(Envelope::CallAck(_)) => Ok(OwnerRef {
                task_id,
                actor_id,
                endpoint: entry.endpoint.clone(),
            }),
            Ok(Envelope::Error(err)) => Err(ClientError::Remote(err)),
            Ok(other) => Err(ClientError::Transport(format!(
                "unexpected reply to call: {other:?}"
            ))),
            Err(err) => Err(ClientError::Transport(err.to_string())),
        }
    }

    /// Fetch a result from whoever owns it, parking until it is ready.
    pub async fn fetch(
        &self,
        reference: &OwnerRef,
        timeout: Duration,
    ) -> Result<FetchedValue, ClientError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(ClientError::Timeout {
                    task_id: reference.task_id,
                });
            }
            // Long-poll: the owner holds the request open rather than making
            // us spin, but we cap each leg so a dead peer is noticed.
            let leg = remaining.min(Duration::from_secs(10));
            let message = build_fetch(reference.task_id, leg, false)
                .map_err(|err| ClientError::Transport(err.to_string()))?;
            let reply = self
                .transport
                .request(&reference.endpoint, paths::FETCH, &message)
                .await?;

            match Envelope::decode(&reply.header) {
                Ok(Envelope::Result(_)) => {
                    let mut frames = reply.frames;
                    if frames.is_empty() {
                        return Err(ClientError::Transport(
                            "result message carried no frames".into(),
                        ));
                    }
                    let body = frames.remove(0);
                    return Ok(FetchedValue { body, frames });
                }
                // Not ready yet; go around again until the deadline.
                Ok(Envelope::CallAck(_)) => continue,
                Ok(Envelope::Error(err)) => return Err(ClientError::Remote(err)),
                Ok(other) => {
                    return Err(ClientError::Transport(format!(
                        "unexpected reply to fetch: {other:?}"
                    )))
                }
                Err(err) => return Err(ClientError::Transport(err.to_string())),
            }
        }
    }

    /// Tell the owner a result is no longer needed. Best effort by design: if
    /// it does not arrive, the watermark and TTL clean up eventually.
    pub async fn release(&self, reference: &OwnerRef) {
        let Ok(message) = build_fetch(reference.task_id, Duration::ZERO, false) else {
            return;
        };
        let _ = self
            .transport
            .request(&reference.endpoint, paths::RELEASE, &message)
            .await;
    }

    /// Wait for `num_returns` of `refs` to be ready.
    ///
    /// Returns `(ready, pending)` preserving the caller's order. This is what
    /// lets an RL loop drop stragglers: 24 of 32 rollouts is enough to train
    /// on. Note that a straggler dropped here is still obliged to show up for
    /// the next collective barrier.
    pub async fn wait(
        &self,
        refs: &[OwnerRef],
        num_returns: usize,
        timeout: Duration,
    ) -> (Vec<OwnerRef>, Vec<OwnerRef>) {
        let num_returns = num_returns.min(refs.len());
        if num_returns == 0 {
            return (vec![], refs.to_vec());
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (index, reference) in refs.iter().enumerate() {
            let reference = reference.clone();
            let transport = self.transport.clone();
            tasks.spawn(async move {
                let outcome = poll_ready(&transport, &reference, timeout).await;
                (index, outcome)
            });
        }

        let mut ready_flags = vec![false; refs.len()];
        let mut settled = 0usize;
        while settled < num_returns {
            match tasks.join_next().await {
                Some(Ok((index, true))) => {
                    ready_flags[index] = true;
                    settled += 1;
                }
                // A failed result still counts as settled: `get` will raise.
                Some(Ok((index, false))) => {
                    ready_flags[index] = false;
                    let _ = index;
                }
                Some(Err(_)) => {}
                None => break,
            }
        }
        tasks.abort_all();

        let mut ready = Vec::new();
        let mut pending = Vec::new();
        for (index, reference) in refs.iter().enumerate() {
            if ready_flags[index] {
                ready.push(reference.clone());
            } else {
                pending.push(reference.clone());
            }
        }
        (ready, pending)
    }

    pub fn inline_threshold(&self) -> usize {
        self.inline_threshold
    }
}

/// True once the reference has settled, either with a value or an error.
async fn poll_ready(transport: &TransportClient, reference: &OwnerRef, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        let leg = remaining.min(Duration::from_secs(10));
        // Status only: `wait` reports which references have settled, and
        // pulling their payloads here would route every result through the
        // driver -- the exact relay this design avoids.
        let Ok(message) = build_fetch(reference.task_id, leg, true) else {
            return false;
        };
        match transport
            .request(&reference.endpoint, paths::FETCH, &message)
            .await
        {
            Ok(reply) => match Envelope::decode(&reply.header) {
                Ok(Envelope::Result(_)) | Ok(Envelope::Error(_)) => return true,
                Ok(_) => continue,
                Err(_) => return false,
            },
            Err(_) => return false,
        }
    }
}
