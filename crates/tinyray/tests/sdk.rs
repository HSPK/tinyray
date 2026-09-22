use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use std::io::{Read, Write};
use tinyray::{
    BlobError, BlobRef, CallError, Client, ClientRequestCancellation, MemberBuilder, Router,
    ServiceError,
};
use tinyray_proto::rpc::{RpcReply, RpcRequest};
use tinyray_proto::wire::{decode_message, encode_message, read_frame, write_frame_bytes};
use tinyray_registry::state::Registry;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Job {
    name: String,
    step: u64,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct ContextSeen {
    caller: String,
    request_id: String,
}

fn router() -> Router {
    let mut router = Router::new();
    router
        .typed_arg("echo", |_context, value: Job| async move { Ok(value) })
        .unwrap()
        .typed_no_args("context", |context| async move {
            Ok(ContextSeen {
                caller: context.caller.to_string(),
                request_id: context.request_id.to_string(),
            })
        })
        .unwrap()
        .typed_arg("sleep", |context, millis: u64| async move {
            tokio::select! {
                _ = context.cancellation.cancelled() => Err(ServiceError::Cancelled),
                _ = tokio::time::sleep(Duration::from_millis(millis)) => Ok(millis),
            }
        })
        .unwrap()
        .typed_arg("delayed_blob", |_context, millis: u64| async move {
            tokio::time::sleep(Duration::from_millis(millis)).await;
            BlobRef::from_bytes(b"late-blob")
                .map_err(|error| ServiceError::Internal(error.to_string()))
        })
        .unwrap()
        .raw(
            "raw",
            |_context, payload| async move { Ok(payload.to_vec()) },
        )
        .unwrap()
        .typed_no_args("make_blob", |_context| async move {
            BlobRef::from_bytes(b"made-by-rust")
                .map_err(|error| ServiceError::Internal(error.to_string()))
        })
        .unwrap()
        .typed_no_args::<(), _, _>("fail", |_context| async move {
            Err(ServiceError::remote("RustError", "expected"))
        })
        .unwrap();
    router
}

async fn registry() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let registry = Arc::new(Registry::new(Duration::from_secs(10)));
    let task = tokio::spawn(tinyray_registry::server::serve(listener, registry));
    (endpoint, task)
}

#[derive(Deserialize)]
struct BlobArgument {
    args: Vec<BlobRef>,
    kwargs: HashMap<String, ()>,
}

async fn delayed_blob_peer() -> (
    String,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Vec<u8>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let (received_tx, received_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        let frame = read_frame(&mut connection, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        let request: RpcRequest = decode_message(&frame).unwrap();
        let _ = received_tx.send(());
        let _ = release_rx.await;
        let mut decoded: BlobArgument = tinyray::decode_msgpack(&request.payload).unwrap();
        assert!(decoded.kwargs.is_empty());
        let bytes = decoded.args[0].as_slice().unwrap().to_vec();
        decoded.args[0].close();
        let reply = RpcReply::success(request.request_id, rmp_serde::to_vec_named(&()).unwrap());
        let frame = encode_message(&reply, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES).unwrap();
        write_frame_bytes(
            &mut connection,
            &frame,
            tinyray_proto::rpc::MAX_RPC_FRAME_BYTES,
        )
        .await
        .unwrap();
        bytes
    });
    (endpoint, received_rx, release_tx, task)
}

#[cfg(target_os = "linux")]
fn socket_links(fds: &[i32]) -> Vec<String> {
    fds.iter()
        .filter_map(|fd| std::fs::read_link(format!("/proc/self/fd/{fd}")).ok())
        .map(|path| path.to_string_lossy().into_owned())
        .filter(|path| path.starts_with("socket:["))
        .collect()
}

async fn raw_rpc(stream: &mut tokio::net::TcpStream, request: RpcRequest) -> RpcReply {
    let frame = encode_message(&request, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES).unwrap();
    write_frame_bytes(stream, &frame, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES)
        .await
        .unwrap();
    let reply = read_frame(stream, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    decode_message(&reply).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_member_serves_typed_raw_context_errors_and_batches() {
    let (registry, registry_task) = registry().await;
    let member = tokio::task::spawn_blocking(move || {
        MemberBuilder::new(registry, "rust-sdk")
            .policy("stateful")
            .slot(0)
            .size(1)
            .router(router())
            .join(Duration::from_secs(5))
            .unwrap()
    })
    .await
    .unwrap();
    member.ready(&serde_json::json!({"kind": "rust"})).unwrap();
    member.flush(Duration::from_secs(5)).unwrap();

    let client = Client::default();
    let endpoint = member.endpoint().unwrap();
    let target = member.identity().to_owned();
    let job = Job {
        name: "rollout".into(),
        step: 7,
    };
    let echoed: Job = client
        .call_arg(
            &endpoint,
            &target,
            "echo",
            "caller/3#9",
            "typed-1",
            &job,
            Duration::from_secs(2),
        )
        .unwrap();
    assert_eq!(echoed, job);
    let mut made: BlobRef = client
        .call_no_args(
            &endpoint,
            &target,
            "make_blob",
            "caller/3#9",
            "blob-response",
            Duration::from_secs(2),
        )
        .unwrap();
    assert_eq!(made.as_slice().unwrap(), b"made-by-rust");
    made.close();

    let context: ContextSeen = client
        .call_no_args(
            &endpoint,
            &target,
            "context",
            "caller/3#9",
            "context-1",
            Duration::from_secs(2),
        )
        .unwrap();
    assert_eq!(
        context,
        ContextSeen {
            caller: "caller/3#9".into(),
            request_id: "context-1".into(),
        }
    );

    let raw = rmp_serde::to_vec_named(&serde_json::json!({"opaque": [1, 2, 3]})).unwrap();
    assert_eq!(
        client
            .call_raw(
                &endpoint,
                &target,
                "raw",
                "caller",
                "raw-1",
                raw.clone(),
                Duration::from_secs(2),
            )
            .unwrap(),
        raw
    );

    assert!(matches!(
        client.call_no_args::<()>(
            &endpoint,
            &target,
            "fail",
            "caller",
            "fail-1",
            Duration::from_secs(2),
        ),
        Err(CallError::Remote(error)) if error.type_name == "RustError"
    ));
    assert!(matches!(
        client.call_no_args::<()>(
            &endpoint,
            "rust-sdk/0#0",
            "context",
            "caller",
            "fenced-1",
            Duration::from_secs(2),
        ),
        Err(CallError::Fenced(_))
    ));

    #[derive(Serialize)]
    struct Batch<'a> {
        calls: Vec<BatchCall<'a>>,
    }
    #[derive(Serialize)]
    struct BatchCall<'a> {
        method: &'a str,
        args: Vec<serde_json::Value>,
        kwargs: HashMap<String, serde_json::Value>,
    }
    let batch = Batch {
        calls: vec![
            BatchCall {
                method: "echo",
                args: vec![serde_json::to_value(&job).unwrap()],
                kwargs: HashMap::new(),
            },
            BatchCall {
                method: "context",
                args: vec![],
                kwargs: HashMap::new(),
            },
        ],
    };
    let reply = client
        .request(
            RpcRequest::batch(
                "batch-1",
                "caller/3#9",
                &target,
                2,
                rmp_serde::to_vec_named(&batch).unwrap(),
            ),
            endpoint.clone(),
            Duration::from_secs(2),
        )
        .unwrap();
    assert_eq!(reply.status, tinyray::RpcStatus::Success);
    let values: Vec<serde_json::Value> = rmp_serde::from_slice(&reply.payload).unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0]["name"], "rollout", "{values:?}");
    assert_eq!(values[1]["caller"], "caller/3#9", "{values:?}");

    let failed_batch = Batch {
        calls: vec![
            BatchCall {
                method: "echo",
                args: vec![serde_json::to_value(&job).unwrap()],
                kwargs: HashMap::new(),
            },
            BatchCall {
                method: "fail",
                args: vec![],
                kwargs: HashMap::new(),
            },
            BatchCall {
                method: "echo",
                args: vec![serde_json::to_value(&job).unwrap()],
                kwargs: HashMap::new(),
            },
        ],
    };
    let failed = client
        .request(
            RpcRequest::batch(
                "batch-fail",
                "caller/3#9",
                &target,
                3,
                rmp_serde::to_vec_named(&failed_batch).unwrap(),
            ),
            endpoint.clone(),
            Duration::from_secs(2),
        )
        .unwrap();
    assert_eq!(failed.status, tinyray::RpcStatus::RemoteError);
    assert_eq!((failed.batch_index, failed.completed), (Some(1), Some(1)));
    assert_eq!(
        rmp_serde::from_slice::<Vec<rmpv::Value>>(&failed.payload)
            .unwrap()
            .len(),
        1
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let members = member.members("rust-sdk", true);
        if members.len() == 1 {
            assert_eq!(
                member.pool_methods("rust-sdk"),
                vec![
                    "context",
                    "delayed_blob",
                    "echo",
                    "fail",
                    "make_blob",
                    "raw",
                    "sleep",
                ]
            );
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    member.leave();
    registry_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_client_multiplexes_out_of_order_and_refuses_overload() {
    let mut config = tinyray::ServerConfig::new("127.0.0.1:0", "service/0#1");
    config.max_concurrency = Some(2);
    let mut server = tinyray::Server::start(config, Arc::new(router())).unwrap();
    let client = Client::from_current().unwrap();

    let slow = client.call_arg_async::<_, u64>(
        server.endpoint(),
        "service/0#1",
        "sleep",
        "caller",
        "slow",
        &100,
        Duration::from_secs(2),
    );
    let fast = client.call_arg_async::<_, u64>(
        server.endpoint(),
        "service/0#1",
        "sleep",
        "caller",
        "fast",
        &1,
        Duration::from_secs(2),
    );
    let (slow, fast) = tokio::join!(slow, fast);
    assert_eq!(slow.unwrap(), 100);
    assert_eq!(fast.unwrap(), 1);
    assert_eq!(client.stats().connections, 1);

    let first = client.call_arg_async::<_, u64>(
        server.endpoint(),
        "service/0#1",
        "sleep",
        "caller",
        "hold-1",
        &200,
        Duration::from_secs(2),
    );
    let second = client.call_arg_async::<_, u64>(
        server.endpoint(),
        "service/0#1",
        "sleep",
        "caller",
        "hold-2",
        &200,
        Duration::from_secs(2),
    );
    let refused = client.call_arg_async::<_, u64>(
        server.endpoint(),
        "service/0#1",
        "sleep",
        "caller",
        "refused",
        &1,
        Duration::from_secs(2),
    );
    let (first, second, refused) = tokio::join!(first, second, refused);
    assert_eq!(first.unwrap(), 200);
    assert_eq!(second.unwrap(), 200);
    assert!(matches!(refused, Err(CallError::ConcurrencyRefused(_))));
    assert_eq!(server.stats().refused, 1);

    let timed_out = client
        .call_arg_async::<_, u64>(
            server.endpoint(),
            "service/0#1",
            "sleep",
            "caller",
            "timeout",
            &100,
            Duration::from_millis(10),
        )
        .await;
    assert!(matches!(timed_out, Err(CallError::OutcomeUnknown(_))));
    tokio::time::sleep(Duration::from_millis(120)).await;
    let context: ContextSeen = client
        .call_no_args_async(
            server.endpoint(),
            "service/0#1",
            "context",
            "caller",
            "after-timeout",
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(context.request_id, "after-timeout");

    let cancelled_client = client.clone();
    let cancelled_endpoint = server.endpoint().to_owned();
    let cancelled = tokio::spawn(async move {
        cancelled_client
            .call_arg_async::<_, u64>(
                cancelled_endpoint,
                "service/0#1",
                "sleep",
                "caller",
                "cancelled",
                &200,
                Duration::from_secs(2),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    tokio::time::sleep(Duration::from_millis(220)).await;
    assert_eq!(client.stats().in_flight, 0);
    server.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_client_cancellation_removes_the_waiter_synchronously() {
    let mut server = tinyray::Server::start(
        tinyray::ServerConfig::new("127.0.0.1:0", "cancel/0#1"),
        Arc::new(router()),
    )
    .unwrap();
    let client = Client::from_current().unwrap();
    let cancellation = ClientRequestCancellation::new();
    let request = RpcRequest::call(
        "external-cancel",
        "caller",
        "cancel/0#1",
        "sleep",
        rmp_serde::to_vec_named(&serde_json::json!({
            "args": [200],
            "kwargs": {},
        }))
        .unwrap(),
    );
    let calling = client.clone();
    let endpoint = server.endpoint().to_owned();
    let request_cancellation = cancellation.clone();
    let pending = tokio::spawn(async move {
        calling
            .request_with_blob_owners_cancellable_async(
                request,
                endpoint,
                Duration::from_secs(2),
                Vec::new(),
                request_cancellation,
            )
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while server.stats().in_flight == 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    cancellation.cancel();
    assert_eq!(client.stats().in_flight, 0);
    assert!(pending.await.unwrap().is_err());
    tokio::time::sleep(Duration::from_millis(220)).await;
    let context: ContextSeen = client
        .call_no_args_async(
            server.endpoint(),
            "cancel/0#1",
            "context",
            "caller",
            "after-external-cancel",
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(context.request_id, "after-external-cancel");
    server.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_capable_rust_transport_preserves_128_concurrent_raw_calls() {
    const CALLERS: usize = 128;
    const CALLS_PER_CALLER: usize = 64;
    const MAX_CONNECTIONS_PER_ENDPOINT: usize = 4;

    let mut server = tinyray::Server::start_on(
        &tokio::runtime::Handle::current(),
        tinyray::ServerConfig::new("127.0.0.1:0", "stress/0#1"),
        Arc::new(router()),
    )
    .unwrap();
    let client = Client::from_current().unwrap();

    let mut blob: BlobRef = client
        .call_no_args_async(
            server.endpoint(),
            "stress/0#1",
            "make_blob",
            "caller",
            "blob-before-raw-stress",
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(blob.as_slice().unwrap(), b"made-by-rust");
    blob.close();
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.stats().unacked_blob_refs != 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }

    let endpoint = server.endpoint().to_owned();
    let gate = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
    let mut calls = tokio::task::JoinSet::new();
    for caller in 0..CALLERS {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let gate = gate.clone();
        calls.spawn(async move {
            gate.wait().await;
            for call in 0..CALLS_PER_CALLER {
                let request_id = format!("raw-{caller}-{call}");
                let mut payload = vec![caller as u8; 4096];
                payload[..8].copy_from_slice(&(call as u64).to_be_bytes());
                match client
                    .call_raw_async(
                        &endpoint,
                        "stress/0#1",
                        "raw",
                        "caller",
                        request_id.clone(),
                        payload.clone(),
                        Duration::from_secs(5),
                    )
                    .await
                {
                    Ok(reply) if reply.as_bytes() == payload => {}
                    Ok(reply) => {
                        return Err(format!(
                            "{request_id} received {} bytes for a {}-byte payload",
                            reply.len(),
                            payload.len()
                        ));
                    }
                    Err(error) => return Err(format!("{request_id}: {error}")),
                }
            }
            Ok(())
        });
    }
    gate.wait().await;
    let mut failures = Vec::new();
    while let Some(result) = calls.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(error),
            Err(error) => failures.push(format!("caller task failed: {error}")),
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");

    let stats = client.stats();
    assert!(
        stats.connections <= MAX_CONNECTIONS_PER_ENDPOINT,
        "live connections exceeded the endpoint bound: {stats:?}"
    );
    assert!(
        stats.connections_opened <= MAX_CONNECTIONS_PER_ENDPOINT as u64,
        "connection churn exceeded the endpoint bound: {stats:?}"
    );
    server.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_raw_128_way_multiplexing_keeps_frame_boundaries() {
    let pong = Arc::new(rmp_serde::to_vec_named("pong").unwrap());
    let mut router = Router::new();
    router
        .raw("ping_raw", {
            let pong = pong.clone();
            move |_context, _payload| {
                let pong = pong.clone();
                async move {
                    tokio::task::yield_now().await;
                    Ok(pong.as_ref().clone())
                }
            }
        })
        .unwrap();
    let mut server = tinyray::Server::start_on(
        &tokio::runtime::Handle::current(),
        tinyray::ServerConfig::new("127.0.0.1:0", "frame-race/0#1"),
        Arc::new(router),
    )
    .unwrap();
    let client = Client::from_current().unwrap();
    let endpoint = server.endpoint().to_owned();
    let gate = Arc::new(tokio::sync::Barrier::new(129));
    let sequence = Arc::new(AtomicU64::new(1));
    let no_args = Arc::new(
        rmp_serde::to_vec_named(&serde_json::json!({
            "args": [],
            "kwargs": {},
        }))
        .unwrap(),
    );
    let mut callers = Vec::new();
    for _ in 0..128 {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let gate = gate.clone();
        let sequence = sequence.clone();
        let no_args = no_args.clone();
        let expected = pong.clone();
        callers.push(tokio::spawn(async move {
            gate.wait().await;
            for _ in 0..64 {
                let request_id = sequence.fetch_add(1, Ordering::Relaxed);
                let reply = client
                    .call_raw_async(
                        &endpoint,
                        "frame-race/0#1",
                        "ping_raw",
                        "caller",
                        format!("frame-{request_id}"),
                        no_args.as_ref().clone(),
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
                assert_eq!(reply.as_bytes(), expected.as_slice());
            }
        }));
    }
    gate.wait().await;
    for caller in callers {
        caller.await.unwrap();
    }
    assert!(client.stats().connections <= 4);
    server.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_rust_call_retains_blob_until_delayed_decode() {
    let (endpoint, received, release, server) = delayed_blob_peer().await;
    let source = BlobRef::from_bytes(b"delayed-owner").unwrap();
    let descriptor = source.descriptor_bytes().unwrap();
    let client = Client::default();
    let calling_client = client.clone();
    let call = tokio::task::spawn_blocking(move || {
        let outcome = calling_client.call_arg::<_, ()>(
            endpoint,
            "service/0#1",
            "consume",
            "caller",
            "blob-timeout",
            &source,
            Duration::from_millis(50),
        );
        drop(source);
        outcome
    });
    received.await.unwrap();
    assert!(matches!(
        call.await.unwrap(),
        Err(CallError::OutcomeUnknown(_))
    ));
    release.send(()).unwrap();
    assert_eq!(server.await.unwrap(), b"delayed-owner");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match BlobRef::open_descriptor(&descriptor) {
            Err(BlobError::Stale(_)) => break,
            Ok(mut opened) => opened.close(),
            Err(error) => panic!("unexpected descriptor error: {error}"),
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_rust_call_retains_blob_until_delayed_decode() {
    let (endpoint, received, release, server) = delayed_blob_peer().await;
    let client = Client::from_current().unwrap();
    let calling_client = client.clone();
    let call = tokio::spawn(async move {
        let source = BlobRef::from_bytes(b"delayed-owner").unwrap();
        calling_client
            .call_arg_async::<_, ()>(
                endpoint,
                "service/0#1",
                "consume",
                "caller",
                "blob-cancel",
                &source,
                Duration::from_secs(5),
            )
            .await
    });
    received.await.unwrap();
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    assert_eq!(server.await.unwrap(), b"delayed-owner");
    assert_eq!(client.stats().in_flight, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_abandoned_blob_replies_are_acked_and_connection_stays_reusable() {
    let mut server = tinyray::Server::start(
        tinyray::ServerConfig::new("127.0.0.1:0", "late-blob/0#1"),
        Arc::new(router()),
    )
    .unwrap();
    let client = Client::from_current().unwrap();
    for index in 0..130 {
        let result = client
            .call_arg_async::<_, BlobRef>(
                server.endpoint(),
                "late-blob/0#1",
                "delayed_blob",
                "caller",
                format!("timeout-blob-{index}"),
                &50u64,
                Duration::from_millis(20),
            )
            .await;
        assert!(matches!(result, Err(CallError::OutcomeUnknown(_))));
        tokio::time::sleep(Duration::from_millis(55)).await;
    }

    for index in 0..130 {
        let calling = client.clone();
        let endpoint = server.endpoint().to_owned();
        let pending = tokio::spawn(async move {
            calling
                .call_arg_async::<_, BlobRef>(
                    endpoint,
                    "late-blob/0#1",
                    "delayed_blob",
                    "caller",
                    format!("cancel-blob-{index}"),
                    &50u64,
                    Duration::from_secs(2),
                )
                .await
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while server.stats().in_flight == 0 {
            assert!(Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_millis(55)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(3);
    while server.stats().unacked_blob_refs != 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pong: String = client
        .call_no_args_async(
            server.endpoint(),
            "late-blob/0#1",
            "context",
            "caller",
            "after-late-blobs",
            Duration::from_secs(2),
        )
        .await
        .map(|context: ContextSeen| context.request_id)
        .unwrap();
    assert_eq!(pong, "after-late-blobs");
    assert_eq!(client.stats().connections, 1);
    server.close();
}

#[test]
#[cfg(target_os = "linux")]
fn fork_closes_a_connect_in_progress_rust_client_socket() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    assert_eq!(unsafe { libc::listen(listener.as_raw_fd(), 0) }, 0);
    listener
        .set_nonblocking(false)
        .expect("blocking accept for the test");
    let endpoint = listener.local_addr().unwrap().to_string();
    let filler = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let client = Client::default();
    let calling = client.clone();
    let endpoint_for_call = endpoint.clone();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let caller = std::thread::spawn(move || {
        let result = calling.call_no_args::<String>(
            endpoint_for_call,
            "pending/0#1",
            "ping",
            "caller",
            "pending-connect",
            Duration::from_secs(10),
        );
        let _ = result_tx.send(result);
    });

    let deadline = Instant::now() + Duration::from_secs(3);
    let inherited = loop {
        let links = socket_links(&client.debug_fds());
        if !links.is_empty() {
            break links;
        }
        assert!(
            Instant::now() < deadline,
            "pending connect fd was never tracked"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    assert!(
        !caller.is_finished(),
        "connect completed before backlog saturation"
    );

    let mut result_pipe = [0; 2];
    assert_eq!(
        unsafe { libc::pipe2(result_pipe.as_mut_ptr(), libc::O_CLOEXEC) },
        0
    );
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        unsafe {
            libc::close(result_pipe[0]);
        }
        let child_result = client.call_no_args::<String>(
            &endpoint,
            "pending/0#1",
            "ping",
            "child",
            "child-after-fork",
            Duration::from_millis(10),
        );
        assert!(matches!(child_result, Err(CallError::NotDelivered(_))));
        let leaked = inherited
            .iter()
            .filter(|inode| {
                std::fs::read_dir("/proc/self/fd")
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter_map(|entry| std::fs::read_link(entry.path()).ok())
                    .any(|path| path.to_string_lossy() == inode.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        let encoded = format!("{leaked:?}");
        let mut writer = unsafe { std::fs::File::from_raw_fd(result_pipe[1]) };
        writer.write_all(encoded.as_bytes()).unwrap();
        writer.flush().unwrap();
        unsafe { libc::_exit(0) };
    }
    unsafe {
        libc::close(result_pipe[1]);
    }
    let mut reader = unsafe { std::fs::File::from_raw_fd(result_pipe[0]) };
    let mut child_result = String::new();
    reader.read_to_string(&mut child_result).unwrap();
    let mut status = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }
    assert_eq!(child_result, "[]");
    assert!(inherited
        .iter()
        .all(|inode| std::path::Path::new("/proc/self/fd")
            .read_dir()
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_link(entry.path()).ok())
            .any(|path| path.to_string_lossy() == inode.as_str())));

    let (first, _) = listener.accept().unwrap();
    drop(first);
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut connection = loop {
        match listener.accept() {
            Ok((connection, _)) => break connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "parent pending connect did not finish"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("accept failed: {error}"),
        }
    };
    let mut prefix = [0u8; 4];
    connection.read_exact(&mut prefix).unwrap();
    let mut body = vec![0; u32::from_be_bytes(prefix) as usize];
    connection.read_exact(&mut body).unwrap();
    let request: RpcRequest = decode_message(&body).unwrap();
    let reply = RpcReply::success(request.request_id, rmp_serde::to_vec_named("pong").unwrap());
    let body = encode_message(&reply, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES).unwrap();
    connection
        .write_all(&(body.len() as u32).to_be_bytes())
        .unwrap();
    connection.write_all(&body).unwrap();
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap(),
        "pong"
    );
    caller.join().unwrap();
    drop(connection);
    drop(filler);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_service_rejects_blob_count_before_handler() {
    let called = Arc::new(AtomicBool::new(false));
    let mut router = Router::new();
    router
        .typed_arg("many", {
            let called = called.clone();
            move |_context, values: Vec<BlobRef>| {
                let called = called.clone();
                async move {
                    called.store(true, Ordering::Release);
                    Ok(values.len())
                }
            }
        })
        .unwrap();
    let mut server = tinyray::Server::start(
        tinyray::ServerConfig::new("127.0.0.1:0", "budget/0#1"),
        Arc::new(router),
    )
    .unwrap();
    let client = Client::from_current().unwrap();
    let source = BlobRef::from_bytes(b"bounded").unwrap();
    let values = vec![source; tinyray::MAX_BLOB_REFS_PER_MESSAGE + 1];
    let result = client
        .call_arg_async::<_, usize>(
            server.endpoint(),
            "budget/0#1",
            "many",
            "caller",
            "too-many-blobs",
            &values,
            Duration::from_secs(2),
        )
        .await;
    assert!(matches!(result, Err(CallError::CallerFault(_))));
    assert!(!called.load(Ordering::Acquire));
    server.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unacknowledged_blob_replies_are_bounded_and_release_on_close() {
    let mut router = Router::new();
    router
        .typed_no_args("fresh", |_context| async move {
            BlobRef::from_bytes(b"x").map_err(|error| ServiceError::Internal(error.to_string()))
        })
        .unwrap()
        .typed_no_args("duplicate", |_context| async move {
            let blob = BlobRef::from_bytes(b"x")
                .map_err(|error| ServiceError::Internal(error.to_string()))?;
            Ok(vec![blob.clone(), blob])
        })
        .unwrap()
        .typed_no_args("distinct", |_context| async move {
            Ok(vec![
                BlobRef::from_bytes(b"x")
                    .map_err(|error| ServiceError::Internal(error.to_string()))?,
                BlobRef::from_bytes(b"y")
                    .map_err(|error| ServiceError::Internal(error.to_string()))?,
            ])
        })
        .unwrap()
        .typed_no_args("large", |_context| async move {
            BlobRef::from_bytes(b"123456789")
                .map_err(|error| ServiceError::Internal(error.to_string()))
        })
        .unwrap()
        .typed_no_args("ping", |_context| async move { Ok("pong") })
        .unwrap();
    let mut config = tinyray::ServerConfig::new("127.0.0.1:0", "budget/0#1");
    config.max_blob_refs_per_reply = 1;
    config.max_blob_bytes_per_reply = 40;
    config.max_unacked_blob_refs_per_connection = 2;
    config.max_unacked_blob_bytes_per_connection = 80;
    config.max_unacked_blob_refs = 3;
    config.max_unacked_blob_bytes = 120;
    let mut server =
        tinyray::Server::start_on(&tokio::runtime::Handle::current(), config, Arc::new(router))
            .unwrap();
    let payload = rmp_serde::to_vec_named(&serde_json::json!({
        "args": [],
        "kwargs": {},
    }))
    .unwrap();
    let request = |id: &str, method: &str| {
        RpcRequest::call(id, "malicious/0#1", "budget/0#1", method, payload.clone())
    };
    let mut first = tokio::net::TcpStream::connect(server.endpoint())
        .await
        .unwrap();
    let duplicate = raw_rpc(&mut first, request("duplicate", "duplicate")).await;
    assert_eq!(duplicate.status, tinyray::RpcStatus::Success);
    assert!(duplicate.blob_refs);
    assert_eq!(server.stats().unacked_blob_refs, 1);
    let distinct = raw_rpc(&mut first, request("distinct", "distinct")).await;
    assert_eq!(distinct.status, tinyray::RpcStatus::Internal);
    assert!(distinct.error.unwrap().message.contains("unique BlobRefs"));
    let large = raw_rpc(&mut first, request("large", "large")).await;
    assert_eq!(large.status, tinyray::RpcStatus::Internal);
    assert!(large.error.unwrap().message.contains("BlobRef bytes"));

    assert!(
        raw_rpc(&mut first, request("first-1", "fresh"))
            .await
            .blob_refs
    );
    let connection_full = raw_rpc(&mut first, request("first-2", "fresh")).await;
    assert_eq!(connection_full.status, tinyray::RpcStatus::Internal);
    assert!(connection_full
        .error
        .unwrap()
        .message
        .contains("connection BlobRef reply budget"));
    assert_eq!(
        raw_rpc(&mut first, request("ping-1", "ping")).await.status,
        tinyray::RpcStatus::Success
    );

    let mut second = tokio::net::TcpStream::connect(server.endpoint())
        .await
        .unwrap();
    assert!(
        raw_rpc(&mut second, request("second-1", "fresh"))
            .await
            .blob_refs
    );
    let server_full = raw_rpc(&mut second, request("second-2", "fresh")).await;
    assert_eq!(server_full.status, tinyray::RpcStatus::Internal);
    assert!(server_full
        .error
        .unwrap()
        .message
        .contains("server BlobRef reply budget"));
    assert_eq!(
        raw_rpc(&mut second, request("ping-2", "ping")).await.status,
        tinyray::RpcStatus::Success
    );
    assert_eq!(server.stats().unacked_blob_refs, 3);

    drop(first);
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.stats().unacked_blob_refs != 1 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    assert!(
        raw_rpc(&mut second, request("second-3", "fresh"))
            .await
            .blob_refs
    );
    drop(second);
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.stats().unacked_blob_refs != 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    server.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_guard_holds_blob_owner_until_decode_or_drop() {
    let mut router = Router::new();
    router
        .typed_no_args("fresh", |_context| async move {
            BlobRef::from_bytes(b"guarded")
                .map_err(|error| ServiceError::Internal(error.to_string()))
        })
        .unwrap();
    let mut server = tinyray::Server::start_on(
        &tokio::runtime::Handle::current(),
        tinyray::ServerConfig::new("127.0.0.1:0", "raw-blob/0#1"),
        Arc::new(router),
    )
    .unwrap();
    let client = Client::from_current().unwrap();
    let no_args = rmp_serde::to_vec_named(&serde_json::json!({
        "args": [],
        "kwargs": {},
    }))
    .unwrap();

    let raw = client
        .call_raw_async(
            server.endpoint(),
            "raw-blob/0#1",
            "fresh",
            "caller",
            "raw-blob-1",
            no_args.clone(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(server.stats().unacked_blob_refs, 1);
    tokio::time::sleep(Duration::from_secs(16)).await;
    if client.stats().connections != 1 {
        drop(raw);
        server.close();
        panic!("a live raw reply guard did not keep its client connection active");
    }
    let mut decoded: BlobRef = match raw.decode() {
        Ok(decoded) => decoded,
        Err(error) => {
            server.close();
            panic!("guarded BlobRef response became stale: {error}");
        }
    };
    assert_eq!(decoded.as_slice().unwrap(), b"guarded");
    decoded.close();

    let deadline = Instant::now() + Duration::from_secs(2);
    while server.stats().unacked_blob_refs != 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let deadline = Instant::now() + Duration::from_secs(12);
    while client.stats().connections != 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let guarded_reply = client
        .request_async(
            RpcRequest::call(
                "raw-request",
                "caller",
                "raw-blob/0#1",
                "fresh",
                no_args.clone(),
            ),
            server.endpoint().to_owned(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(server.stats().unacked_blob_refs, 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut request_decoded: BlobRef = guarded_reply.decode().unwrap();
    assert_eq!(request_decoded.as_slice().unwrap(), b"guarded");
    request_decoded.close();
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.stats().unacked_blob_refs != 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }

    let sync_client = Client::default();
    let calling_client = sync_client.clone();
    let endpoint = server.endpoint().to_owned();
    let sync_raw = tokio::task::spawn_blocking(move || {
        calling_client.call_raw(
            endpoint,
            "raw-blob/0#1",
            "fresh",
            "caller",
            "raw-blob-sync",
            rmp_serde::to_vec_named(&serde_json::json!({
                "args": [],
                "kwargs": {},
            }))
            .unwrap(),
            Duration::from_secs(2),
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(server.stats().unacked_blob_refs, 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut sync_decoded: BlobRef = sync_raw.decode().unwrap();
    assert_eq!(sync_decoded.as_slice().unwrap(), b"guarded");
    sync_decoded.close();
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.stats().unacked_blob_refs != 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }

    let dropped = client
        .call_raw_async(
            server.endpoint(),
            "raw-blob/0#1",
            "fresh",
            "caller",
            "raw-blob-2",
            no_args,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(server.stats().unacked_blob_refs, 1);
    drop(dropped);
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.stats().unacked_blob_refs != 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    server.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_success_errors_hold_partial_blob_payload_until_error_drop() {
    for (index, status) in [
        tinyray::RpcStatus::RemoteError,
        tinyray::RpcStatus::MethodNotFound,
        tinyray::RpcStatus::Fenced,
    ]
    .into_iter()
    .enumerate()
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let frame = read_frame(&mut connection, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES)
                .await
                .unwrap()
                .unwrap();
            let request: RpcRequest = decode_message(&frame).unwrap();
            let blob = BlobRef::from_bytes(b"partial-result").unwrap();
            let mut reply = RpcReply::error(
                request.request_id.clone(),
                status,
                "Expected",
                "expected failure",
            );
            reply.batch_index = Some(1);
            reply.completed = Some(1);
            reply.blob_refs = true;
            reply.payload = rmp_serde::to_vec_named(&vec![blob.clone()]).unwrap();
            let frame = encode_message(&reply, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES).unwrap();
            write_frame_bytes(
                &mut connection,
                &frame,
                tinyray_proto::rpc::MAX_RPC_FRAME_BYTES,
            )
            .await
            .unwrap();
            let ack = read_frame(&mut connection, tinyray_proto::rpc::MAX_RPC_FRAME_BYTES)
                .await
                .unwrap()
                .unwrap();
            let ack: RpcRequest = decode_message(&ack).unwrap();
            assert_eq!(ack.operation, tinyray::RpcOperation::BlobAck);
            assert_eq!(ack.request_id, request.request_id);
        });

        let client = Client::from_current().unwrap();
        let reply = client
            .request_async(
                RpcRequest::batch(
                    format!("partial-{index}"),
                    "caller",
                    "service/0#1",
                    2,
                    vec![0x80],
                ),
                endpoint,
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        let error = reply.decode::<Vec<BlobRef>>().unwrap_err();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!server.is_finished());
        let failure = match &error {
            CallError::Remote(failure)
            | CallError::MethodNotFound(failure)
            | CallError::Fenced(failure) => failure,
            other => panic!("unexpected error: {other}"),
        };
        let mut completed: Vec<BlobRef> = failure.decode_payload().unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].as_slice().unwrap(), b"partial-result");
        completed[0].close();
        drop(error);
        server.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_service_shutdown_cancels_handlers_and_client_reconnects_after_restart() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut router = Router::new();
    router
        .typed_no_args("wait", {
            let cancelled = cancelled.clone();
            move |context| {
                let cancelled = cancelled.clone();
                async move {
                    context.cancellation.cancelled().await;
                    cancelled.store(true, Ordering::Release);
                    Err::<(), _>(ServiceError::Cancelled)
                }
            }
        })
        .unwrap()
        .typed_no_args("ping", |_context| async move { Ok("pong") })
        .unwrap();
    let mut server = tinyray::Server::start(
        tinyray::ServerConfig::new("127.0.0.1:0", "restart/0#1"),
        Arc::new(router),
    )
    .unwrap();
    let endpoint = server.endpoint().to_owned();
    let client = Client::from_current().unwrap();
    let waiting_client = client.clone();
    let waiting_endpoint = endpoint.clone();
    let waiting = tokio::spawn(async move {
        waiting_client
            .call_no_args_async::<()>(
                waiting_endpoint,
                "restart/0#1",
                "wait",
                "caller",
                "wait-1",
                Duration::from_secs(5),
            )
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.stats().in_flight == 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    tokio::task::spawn_blocking(move || server.close())
        .await
        .unwrap();
    assert!(cancelled.load(Ordering::Acquire));
    assert!(matches!(
        waiting.await.unwrap(),
        Err(CallError::OutcomeUnknown(_)) | Err(CallError::Internal(_))
    ));

    let mut replacement_router = Router::new();
    replacement_router
        .typed_no_args("ping", |_context| async move { Ok("pong") })
        .unwrap();
    let mut replacement = tinyray::Server::start(
        tinyray::ServerConfig::new(endpoint.clone(), "restart/0#1"),
        Arc::new(replacement_router),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut attempt = 0;
    loop {
        attempt += 1;
        match client
            .call_no_args_async::<String>(
                &endpoint,
                "restart/0#1",
                "ping",
                "caller",
                format!("restart-{attempt}"),
                Duration::from_millis(200),
            )
            .await
        {
            Ok(response) => {
                assert_eq!(response, "pong");
                break;
            }
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Err(error) => panic!("client did not reconnect after restart: {error}"),
        }
    }
    replacement.close();
}
