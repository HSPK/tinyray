use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tinyray_proto::wire::{
    decode_message, encode_message, read_frame, write_frame, DebugPools, RegistryEnvelope,
    RegistryEnvelopeHeader, RegistryHealth, RegistryProtocolError, MAX_REQUEST_FRAME_BYTES,
    MAX_RESPONSE_FRAME_BYTES, OP_BEAT, OP_BEAT_ACK, OP_DEBUG_POOLS, OP_DEBUG_POOLS_ACK, OP_ERROR,
    OP_HEALTH, OP_HEALTH_ACK,
};
use tinyray_proto::{Beat, BeatAck};
use tinyray_registry::server::ServerStats;
use tinyray_registry::state::Registry;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

async fn start() -> (String, tokio::task::JoinHandle<()>) {
    let (endpoint, _, task) = start_with_stats(Duration::from_secs(35)).await;
    (endpoint, task)
}

async fn start_with_stats(
    idle_timeout: Duration,
) -> (String, Arc<ServerStats>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let registry = Arc::new(Registry::new(Duration::from_secs(10)));
    let stats = Arc::new(ServerStats::default());
    let task = tokio::spawn(tinyray_registry::server::serve_with_stats_and_idle(
        listener,
        registry,
        stats.clone(),
        idle_timeout,
    ));
    (endpoint, stats, task)
}

async fn raw_request<T: Serialize>(endpoint: &str, envelope: &T) -> Vec<u8> {
    let mut stream = TcpStream::connect(endpoint).await.unwrap();
    stream.set_nodelay(true).unwrap();
    write_frame(&mut stream, envelope, MAX_REQUEST_FRAME_BYTES)
        .await
        .unwrap();
    read_frame(&mut stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap()
}

async fn request<T: Serialize, R: DeserializeOwned>(
    endpoint: &str,
    request_id: u64,
    operation: &str,
    payload: T,
    response_operation: &str,
) -> R {
    let raw = raw_request(
        endpoint,
        &RegistryEnvelope::new(request_id, operation, payload),
    )
    .await;
    let header: RegistryEnvelopeHeader = decode_message(&raw).unwrap();
    assert_eq!(header.request_id, request_id);
    assert_eq!(header.operation, response_operation);
    decode_message::<RegistryEnvelope<R>>(&raw).unwrap().payload
}

async fn request_on<T: Serialize, R: DeserializeOwned>(
    stream: &mut TcpStream,
    request_id: u64,
    operation: &str,
    payload: T,
    response_operation: &str,
) -> R {
    write_frame(
        stream,
        &RegistryEnvelope::new(request_id, operation, payload),
        MAX_REQUEST_FRAME_BYTES,
    )
    .await
    .unwrap();
    let raw = read_frame(stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    let header: RegistryEnvelopeHeader = decode_message(&raw).unwrap();
    assert_eq!(header.request_id, request_id);
    assert_eq!(header.operation, response_operation);
    decode_message::<RegistryEnvelope<R>>(&raw).unwrap().payload
}

fn beat(id: u64, incarnation: u64) -> Beat {
    Beat {
        pool: "p".into(),
        slot: None,
        id,
        incarnation,
        publication: Some(0),
        policy: "churn".into(),
        size: None,
        url: None,
        state: Value::Null,
        ready: true,
        leaving: false,
        exclusive: false,
        methods: Vec::new(),
        watch: vec!["p".into()],
        seen: HashMap::new(),
        hold_ms: 0,
    }
}

#[tokio::test]
async fn serial_beats_and_goodbye_share_one_connection() {
    let (endpoint, stats, server) = start_with_stats(Duration::from_secs(1)).await;
    let mut stream = TcpStream::connect(&endpoint).await.unwrap();
    stream.set_nodelay(true).unwrap();

    let first = beat(1, 2);
    let first_ack: BeatAck = request_on(&mut stream, 1, OP_BEAT, &first, OP_BEAT_ACK).await;
    assert!(first_ack.accepted);

    let mut second = first.clone();
    second.publication = Some(1);
    second.state = serde_json::json!({"step": 1});
    let second_ack: BeatAck = request_on(&mut stream, 2, OP_BEAT, &second, OP_BEAT_ACK).await;
    assert!(second_ack.accepted);

    let mut goodbye = second;
    goodbye.leaving = true;
    let goodbye_ack: BeatAck = request_on(&mut stream, 3, OP_BEAT, &goodbye, OP_BEAT_ACK).await;
    assert!(goodbye_ack.accepted);
    assert_eq!(stats.connections_accepted(), 1);
    assert_eq!(stats.frames_received(), 3);

    let pools: DebugPools = request(&endpoint, 4, OP_DEBUG_POOLS, (), OP_DEBUG_POOLS_ACK).await;
    assert_eq!(pools.pools["p"].members, 0);
    server.abort();
}

#[tokio::test]
async fn pipelining_is_refused_and_the_consumed_connection_is_closed() {
    let (endpoint, stats, server) = start_with_stats(Duration::from_secs(1)).await;
    let mut stream = TcpStream::connect(&endpoint).await.unwrap();
    let first: BeatAck = request_on(&mut stream, 9, OP_BEAT, beat(1, 1), OP_BEAT_ACK).await;
    let seen = first.pools["p"].version;
    let mut held = beat(2, 1);
    held.pool = "watcher".into();
    held.hold_ms = 1000;
    held.seen.insert("p".into(), seen);
    write_frame(
        &mut stream,
        &RegistryEnvelope::new(10, OP_BEAT, &held),
        MAX_REQUEST_FRAME_BYTES,
    )
    .await
    .unwrap();
    write_frame(
        &mut stream,
        &RegistryEnvelope::new(11, OP_BEAT, &held),
        MAX_REQUEST_FRAME_BYTES,
    )
    .await
    .unwrap();
    let raw = read_frame(&mut stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    let error: RegistryEnvelope<RegistryProtocolError> = decode_message(&raw).unwrap();
    assert_eq!(error.request_id, 10);
    assert_eq!(error.payload.code, "multiple_requests");
    assert!(read_frame(&mut stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .is_none());
    assert_eq!(stats.connections_accepted(), 1);
    server.abort();
}

#[tokio::test]
async fn malformed_followup_and_idle_expiry_force_a_fresh_connection() {
    let (endpoint, stats, server) = start_with_stats(Duration::from_millis(200)).await;
    let mut stream = TcpStream::connect(&endpoint).await.unwrap();
    let ack: BeatAck = request_on(&mut stream, 20, OP_BEAT, beat(1, 1), OP_BEAT_ACK).await;
    assert!(ack.accepted);

    stream.write_all(&1u32.to_be_bytes()).await.unwrap();
    stream.write_all(&[0xc1]).await.unwrap();
    let raw = read_frame(&mut stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    let error: RegistryEnvelope<RegistryProtocolError> = decode_message(&raw).unwrap();
    assert_eq!(error.payload.code, "malformed_frame");
    assert!(read_frame(&mut stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .is_none());

    let mut fresh = TcpStream::connect(&endpoint).await.unwrap();
    let ack: BeatAck = request_on(&mut fresh, 21, OP_BEAT, beat(2, 1), OP_BEAT_ACK).await;
    assert!(ack.accepted);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(read_frame(&mut fresh, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .is_none());
    assert_eq!(stats.connections_accepted(), 2);
    server.abort();
}

#[tokio::test]
async fn many_members_reuse_one_accept_each_and_release_every_task() {
    const MEMBERS: u64 = 64;
    const BEATS: u64 = 4;
    let (endpoint, stats, server) = start_with_stats(Duration::from_secs(1)).await;
    let mut tasks = Vec::new();
    for id in 0..MEMBERS {
        let endpoint = endpoint.clone();
        tasks.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(endpoint).await.unwrap();
            for publication in 0..BEATS {
                let mut request = beat(id, 1);
                request.watch.clear();
                request.publication = Some(publication);
                request.state = serde_json::json!({"publication": publication});
                let ack: BeatAck = request_on(
                    &mut stream,
                    id * 100 + publication,
                    OP_BEAT,
                    request,
                    OP_BEAT_ACK,
                )
                .await;
                assert!(ack.accepted);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while stats.connections_active() != 0 && tokio::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    assert_eq!(stats.connections_accepted(), MEMBERS);
    assert_eq!(stats.frames_received(), MEMBERS * BEATS);
    assert_eq!(stats.connections_active(), 0);
    server.abort();
}

#[tokio::test]
async fn health_beat_debug_refusal_and_goodbye_are_correlated_operations() {
    let (endpoint, server) = start().await;

    let health: RegistryHealth = request(&endpoint, 11, OP_HEALTH, (), OP_HEALTH_ACK).await;
    assert_eq!(health.status, "ok");
    assert_eq!(health.protocol, tinyray_proto::PROTOCOL);
    assert!(health.connections_accepted >= 1);
    assert!(health.connections_active >= 1);
    assert!(health.frames_received >= 1);

    let first = beat(1, 2);
    let ack: BeatAck = request(&endpoint, 12, OP_BEAT, &first, OP_BEAT_ACK).await;
    assert!(ack.accepted);
    assert_eq!(ack.pools["p"].changed[0].id, 1);

    let pools: DebugPools = request(&endpoint, 13, OP_DEBUG_POOLS, (), OP_DEBUG_POOLS_ACK).await;
    assert_eq!(pools.pools["p"].members, 1);

    let stale = beat(1, 1);
    let refused: BeatAck = request(&endpoint, 14, OP_BEAT, &stale, OP_BEAT_ACK).await;
    assert!(!refused.accepted, "a refusal is still a valid BeatAck");

    let mut leaving = first;
    leaving.leaving = true;
    let goodbye: BeatAck = request(&endpoint, 15, OP_BEAT, &leaving, OP_BEAT_ACK).await;
    assert!(goodbye.accepted);
    let pools: DebugPools = request(&endpoint, 16, OP_DEBUG_POOLS, (), OP_DEBUG_POOLS_ACK).await;
    assert_eq!(pools.pools["p"].members, 0);

    server.abort();
}

#[tokio::test]
async fn malformed_and_unknown_operations_return_structured_errors() {
    let (endpoint, server) = start().await;

    let raw = raw_request(&endpoint, &RegistryEnvelope::new(41, "teleport", ())).await;
    let unknown: RegistryEnvelope<RegistryProtocolError> = decode_message(&raw).unwrap();
    assert_eq!(unknown.request_id, 41);
    assert_eq!(unknown.operation, OP_ERROR);
    assert_eq!(unknown.payload.code, "unknown_operation");

    let raw = raw_request(&endpoint, &RegistryEnvelope::new(42, OP_BEAT, "not a Beat")).await;
    let malformed: RegistryEnvelope<RegistryProtocolError> = decode_message(&raw).unwrap();
    assert_eq!(malformed.request_id, 42);
    assert_eq!(malformed.payload.code, "malformed_request");

    for (request_id, request) in [
        (
            43,
            serde_json::json!({"request_id": 43, "operation": OP_BEAT}),
        ),
        (
            44,
            serde_json::json!({"request_id": 44, "operation": 7, "payload": null}),
        ),
        (45, serde_json::json!({"request_id": 45, "payload": null})),
        (
            46,
            serde_json::json!({
                "request_id": 46,
                "operation": OP_BEAT,
                "payload": {"pool": "p", "id": "wrong"}
            }),
        ),
    ] {
        let raw = raw_request(&endpoint, &request).await;
        let malformed: RegistryEnvelope<RegistryProtocolError> = decode_message(&raw).unwrap();
        assert_eq!(malformed.request_id, request_id);
        assert_eq!(malformed.payload.code, "malformed_request");
    }

    let raw = raw_request(
        &endpoint,
        &serde_json::json!({"request_id": 47, "operation": "teleport"}),
    )
    .await;
    let unknown: RegistryEnvelope<RegistryProtocolError> = decode_message(&raw).unwrap();
    assert_eq!(unknown.request_id, 47);
    assert_eq!(unknown.payload.code, "unknown_operation");

    let mut stream = TcpStream::connect(&endpoint).await.unwrap();
    stream.write_all(&1u32.to_be_bytes()).await.unwrap();
    stream.write_all(&[0xc1]).await.unwrap();
    let raw = read_frame(&mut stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    let malformed: RegistryEnvelope<RegistryProtocolError> = decode_message(&raw).unwrap();
    assert_eq!(malformed.request_id, 0);
    assert_eq!(malformed.payload.code, "malformed_frame");

    let mut stream = TcpStream::connect(&endpoint).await.unwrap();
    stream
        .write_all(&((MAX_REQUEST_FRAME_BYTES + 1) as u32).to_be_bytes())
        .await
        .unwrap();
    let raw = read_frame(&mut stream, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    let oversized: RegistryEnvelope<RegistryProtocolError> = decode_message(&raw).unwrap();
    assert_eq!(oversized.request_id, 0);
    assert_eq!(oversized.payload.code, "frame_too_large");

    server.abort();
}

#[test]
fn every_operation_envelope_is_named_messagepack_not_json_text() {
    for operation in [OP_BEAT, OP_HEALTH, OP_DEBUG_POOLS] {
        let payload = encode_message(
            &RegistryEnvelope::new(7, operation, ()),
            MAX_REQUEST_FRAME_BYTES,
        )
        .unwrap();
        assert_ne!(payload.first(), Some(&b'{'));
        let decoded: RegistryEnvelope<()> = decode_message(&payload).unwrap();
        assert_eq!(decoded.operation, operation);
        assert_eq!(decoded.request_id, 7);
    }
}
