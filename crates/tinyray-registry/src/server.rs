use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tinyray_proto::wire::{
    decode_message, read_frame, write_frame, DebugPool, DebugPools, FrameError, RegistryEnvelope,
    RegistryEnvelopeHeader, RegistryHealth, RegistryProtocolError, RegistryRequestIdHeader,
    MAX_REQUEST_FRAME_BYTES, MAX_RESPONSE_FRAME_BYTES, OP_BEAT, OP_BEAT_ACK, OP_DEBUG_POOLS,
    OP_DEBUG_POOLS_ACK, OP_ERROR, OP_HEALTH, OP_HEALTH_ACK,
};
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{delta::SharedBeatAck, state::Registry};

const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(35);
const MAX_REGISTRY_CONNECTIONS: usize = 131_072;

#[derive(Default)]
pub struct ServerStats {
    connections_accepted: AtomicU64,
    connections_active: AtomicU64,
    frames_received: AtomicU64,
}

impl ServerStats {
    pub fn connections_accepted(&self) -> u64 {
        self.connections_accepted.load(Ordering::Relaxed)
    }

    pub fn connections_active(&self) -> u64 {
        self.connections_active.load(Ordering::Relaxed)
    }

    pub fn frames_received(&self) -> u64 {
        self.frames_received.load(Ordering::Relaxed)
    }
}

struct ActiveConnection(Arc<ServerStats>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.connections_active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Answer a beat, holding the answer back while there is nothing to say.
///
/// The lease is renewed by `beat()` the moment the request lands -- only the
/// reply waits. Holding the renewal too would let a member expire while it
/// was parked, which is the opposite of the point.
async fn hold(reg: &Registry, beat: &tinyray_proto::Beat) -> SharedBeatAck {
    let mut ack = reg.beat_shared(beat);
    // Capped at half a lease: a member is renewed by the arrival of its beat,
    // so parking longer than that would starve its own lease.
    let budget = beat.hold_ms.min(reg.ttl.as_millis() as u64 / 2);
    if budget == 0 || !ack.accepted || !ack.pools.is_empty() || beat.watch.is_empty() {
        return ack;
    }
    // Up to an eighth of the budget, keyed off the caller so it is stable for
    // them and spread across everyone else.
    let jitter = beat.id % (budget / 8 + 1);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(budget + jitter);
    let bell = Arc::new(tokio::sync::Notify::new());
    loop {
        // Register first, then look. The other order loses a change that lands
        // between the look and the wait.
        reg.park(&beat.watch, &bell);
        let waiting = bell.notified();
        let fresh = reg.deltas_shared_for(beat);
        if !fresh.is_empty() {
            ack.pools = fresh;
            return ack;
        }
        if tokio::time::timeout_at(deadline, waiting).await.is_err() {
            return ack;
        }
    }
}

async fn send<T: Serialize>(
    writer: &mut OwnedWriteHalf,
    request_id: u64,
    operation: &str,
    payload: T,
) -> Result<(), String> {
    let envelope = RegistryEnvelope::new(request_id, operation, payload);
    match tokio::time::timeout(
        RESPONSE_WRITE_TIMEOUT,
        write_frame(writer, &envelope, MAX_RESPONSE_FRAME_BYTES),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error @ FrameError::FrameTooLarge { .. })) if operation != OP_ERROR => {
            let fallback = RegistryEnvelope::new(
                request_id,
                OP_ERROR,
                RegistryProtocolError::new("response_too_large", error.to_string()),
            );
            tokio::time::timeout(
                RESPONSE_WRITE_TIMEOUT,
                write_frame(writer, &fallback, MAX_RESPONSE_FRAME_BYTES),
            )
            .await
            .map_err(|_| "protocol error reply exceeded its write deadline".to_string())?
            .map_err(|e| format!("cannot write protocol error reply: {e}"))
        }
        Ok(Err(error)) => Err(format!("cannot write {operation} reply: {error}")),
        Err(_) => Err(format!(
            "{operation} reply exceeded its {}ms write deadline",
            RESPONSE_WRITE_TIMEOUT.as_millis()
        )),
    }
}

async fn send_error(
    writer: &mut OwnedWriteHalf,
    request_id: u64,
    code: &str,
    message: impl Into<String>,
) -> Result<(), String> {
    send(
        writer,
        request_id,
        OP_ERROR,
        RegistryProtocolError::new(code, message),
    )
    .await
}

fn decode_request<T: DeserializeOwned>(
    frame: &[u8],
    header: &RegistryEnvelopeHeader,
) -> Result<T, RegistryProtocolError> {
    let envelope: RegistryEnvelope<T> = decode_message(frame).map_err(|error| {
        RegistryProtocolError::new(
            "malformed_request",
            format!("{}: {error}", header.operation),
        )
    })?;
    if envelope.request_id != header.request_id {
        return Err(RegistryProtocolError::new(
            "correlation_mismatch",
            "the typed request ID did not match its envelope header",
        ));
    }
    if envelope.operation != header.operation {
        return Err(RegistryProtocolError::new(
            "operation_mismatch",
            "the typed request operation did not match its envelope header",
        ));
    }
    Ok(envelope.payload)
}

enum PeerEvent {
    Closed,
    ExtraData,
}

async fn peer_event(reader: &mut OwnedReadHalf) -> io::Result<PeerEvent> {
    let mut byte = [0u8; 1];
    match reader.read(&mut byte).await {
        Ok(0) => Ok(PeerEvent::Closed),
        Ok(_) => Ok(PeerEvent::ExtraData),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::UnexpectedEof
            ) =>
        {
            Ok(PeerEvent::Closed)
        }
        Err(error) => Err(error),
    }
}

async fn read_request_frame(
    reader: &mut OwnedReadHalf,
    writer: &mut OwnedWriteHalf,
    first: bool,
    idle_timeout: Duration,
) -> Result<Option<Vec<u8>>, String> {
    let timeout = if first {
        REQUEST_READ_TIMEOUT
    } else {
        idle_timeout
    };
    match tokio::time::timeout(timeout, read_frame(reader, MAX_REQUEST_FRAME_BYTES)).await {
        Ok(Ok(Some(frame))) => Ok(Some(frame)),
        Ok(Ok(None)) => Ok(None),
        Ok(Err(error)) => {
            let code = match error {
                FrameError::FrameTooLarge { .. } => "frame_too_large",
                FrameError::TruncatedPrefix { .. } | FrameError::TruncatedBody { .. } => {
                    "truncated_frame"
                }
                FrameError::EmptyFrame => "empty_frame",
                _ => "malformed_frame",
            };
            let _ = send_error(writer, 0, code, error.to_string()).await;
            Ok(None)
        }
        Err(_) if !first => Ok(None),
        Err(_) => {
            let _ = send_error(
                writer,
                0,
                "request_timeout",
                format!(
                    "request frame was not complete within {}ms",
                    REQUEST_READ_TIMEOUT.as_millis()
                ),
            )
            .await;
            Ok(None)
        }
    }
}

async fn serve_connection(
    stream: TcpStream,
    reg: Arc<Registry>,
    stats: Arc<ServerStats>,
    idle_timeout: Duration,
    _admission: OwnedSemaphorePermit,
) -> Result<(), String> {
    stream
        .set_nodelay(true)
        .map_err(|e| format!("cannot enable TCP_NODELAY: {e}"))?;
    let (mut reader, mut writer) = stream.into_split();
    let mut first = true;
    loop {
        let Some(frame) = read_request_frame(&mut reader, &mut writer, first, idle_timeout).await?
        else {
            return Ok(());
        };
        first = false;
        stats.frames_received.fetch_add(1, Ordering::Relaxed);

        let request_id = match decode_message::<RegistryRequestIdHeader>(&frame) {
            Ok(header) => header.request_id,
            Err(error) => {
                send_error(&mut writer, 0, "malformed_frame", error.to_string()).await?;
                return Ok(());
            }
        };
        let header: RegistryEnvelopeHeader = match decode_message(&frame) {
            Ok(header) => header,
            Err(error) => {
                send_error(
                    &mut writer,
                    request_id,
                    "malformed_request",
                    error.to_string(),
                )
                .await?;
                return Ok(());
            }
        };

        match header.operation.as_str() {
            OP_BEAT => {
                let beat: tinyray_proto::Beat = match decode_request(&frame, &header) {
                    Ok(beat) => beat,
                    Err(error) => {
                        send_error(&mut writer, header.request_id, &error.code, error.message)
                            .await?;
                        return Ok(());
                    }
                };
                if !tinyray_proto::value_within_depth_limit(&beat.state) {
                    send_error(
                        &mut writer,
                        header.request_id,
                        "malformed_request",
                        "beat state exceeds the recursion limit",
                    )
                    .await?;
                    return Ok(());
                }
                let sent = tokio::select! {
                    ack = hold(&reg, &beat) => {
                        send(&mut writer, header.request_id, OP_BEAT_ACK, ack).await
                    }
                    event = peer_event(&mut reader) => {
                        match event.map_err(|e| format!("cannot monitor a parked peer: {e}"))? {
                            PeerEvent::Closed => return Ok(()),
                            PeerEvent::ExtraData => {
                                let out = send_error(
                                    &mut writer,
                                    header.request_id,
                                    "multiple_requests",
                                    "registry connections carry one request at a time",
                                ).await;
                                out?;
                                return Ok(());
                            }
                        }
                    }
                };
                sent?;
            }
            OP_HEALTH => {
                if let Err(error) = decode_request::<()>(&frame, &header) {
                    send_error(&mut writer, header.request_id, &error.code, error.message).await?;
                    return Ok(());
                }
                send(
                    &mut writer,
                    header.request_id,
                    OP_HEALTH_ACK,
                    RegistryHealth {
                        status: "ok".into(),
                        version: env!("CARGO_PKG_VERSION").into(),
                        protocol: tinyray_proto::PROTOCOL,
                        connections_accepted: stats.connections_accepted(),
                        connections_active: stats.connections_active(),
                        frames_received: stats.frames_received(),
                    },
                )
                .await?;
                return Ok(());
            }
            OP_DEBUG_POOLS => {
                if let Err(error) = decode_request::<()>(&frame, &header) {
                    send_error(&mut writer, header.request_id, &error.code, error.message).await?;
                    return Ok(());
                }
                let pools = reg
                    .snapshot()
                    .into_iter()
                    .map(|(name, (version, roster, members))| {
                        (
                            name,
                            DebugPool {
                                version,
                                roster,
                                members,
                            },
                        )
                    })
                    .collect();
                send(
                    &mut writer,
                    header.request_id,
                    OP_DEBUG_POOLS_ACK,
                    DebugPools { pools },
                )
                .await?;
                return Ok(());
            }
            operation => {
                send_error(
                    &mut writer,
                    header.request_id,
                    "unknown_operation",
                    format!("unknown registry operation {operation:?}"),
                )
                .await?;
                return Ok(());
            }
        }
    }
}

pub async fn serve(listener: TcpListener, reg: Arc<Registry>) {
    serve_with_stats(listener, reg, Arc::new(ServerStats::default())).await;
}

pub async fn serve_with_stats(listener: TcpListener, reg: Arc<Registry>, stats: Arc<ServerStats>) {
    serve_with_stats_and_idle(listener, reg, stats, CONNECTION_IDLE_TIMEOUT).await;
}

pub async fn serve_with_stats_and_idle(
    listener: TcpListener,
    reg: Arc<Registry>,
    stats: Arc<ServerStats>,
    idle_timeout: Duration,
) {
    let admission = Arc::new(Semaphore::new(MAX_REGISTRY_CONNECTIONS));
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(value) => value,
            Err(error) => {
                eprintln!("tinyray registry accept failed: {error}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        let Ok(permit) = admission.clone().try_acquire_owned() else {
            drop(stream);
            continue;
        };
        stats.connections_accepted.fetch_add(1, Ordering::Relaxed);
        stats.connections_active.fetch_add(1, Ordering::Relaxed);
        let reg = reg.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            let _active = ActiveConnection(stats.clone());
            if let Err(error) = serve_connection(stream, reg, stats, idle_timeout, permit).await {
                eprintln!("tinyray registry connection failed: {error}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    #[tokio::test]
    async fn held_replies_share_one_consistent_delta_after_a_change() {
        let reg = Registry::new(Duration::from_secs(10));
        let mut member: tinyray_proto::Beat = serde_json::from_value(serde_json::json!({
            "pool": "p", "id": 1, "incarnation": 1,
            "publication": 0, "policy": "churn"
        }))
        .unwrap();
        assert!(reg.beat(&member).accepted);
        let seen = reg.snapshot()["p"].0;
        let watchers: Vec<tinyray_proto::Beat> = (0..4)
            .map(|id| {
                serde_json::from_value(serde_json::json!({
                    "pool": "watchers", "id": id, "incarnation": 1,
                    "publication": 0, "policy": "churn",
                    "watch": ["p"], "seen": {"p": seen}, "hold_ms": 1000
                }))
                .unwrap()
            })
            .collect();
        let mut context = Context::from_waker(Waker::noop());
        let mut replies: Vec<_> = watchers.iter().map(|b| Box::pin(hold(&reg, b))).collect();
        for reply in &mut replies {
            assert!(matches!(reply.as_mut().poll(&mut context), Poll::Pending));
        }
        member.publication = Some(1);
        member.state = serde_json::json!({"step": 7});
        member.ready = true;
        assert!(reg.beat(&member).accepted);
        let mut first = None;
        for reply in replies {
            let ack = tokio::time::timeout(Duration::from_millis(100), reply)
                .await
                .expect("notification was lost");
            let d = &ack.pools["p"];
            assert!(d.changed[0].ready);
            assert_eq!(d.changed[0].state, member.state);
            if let Some(previous) = &first {
                assert!(Arc::ptr_eq(previous, d));
            }
            first = Some(d.clone());
            let bytes = rmp_serde::to_vec_named(&ack).unwrap();
            let decoded: tinyray_proto::BeatAck = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(decoded.pools["p"].version, d.version);
            assert!(decoded.accepted);
        }
    }
}
