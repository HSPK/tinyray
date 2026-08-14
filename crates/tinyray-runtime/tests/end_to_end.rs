//! End-to-end tests over a real HTTP listener.
//!
//! These exercise the whole Rust data path: a client submits calls over TCP, a
//! simulated executor thread runs them, and results come back through the
//! store. No Python involved, so anything that fails here is a runtime bug
//! rather than a binding bug.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tinyray_core::proto::ErrorKind;
use tinyray_core::ActorId;
use tinyray_runtime::actor::{ActorConfig, ActorRuntime};
use tinyray_runtime::client::{ClientError, ClientRuntime, OwnerRef};
use tinyray_runtime::store::StoreConfig;
use tinyray_runtime::transport::client::ClientConfig;
use tinyray_runtime::transport::server::{serve, ServerConfig};

/// A running actor plus the address it is reachable at.
struct Harness {
    runtime: Arc<ActorRuntime>,
    endpoint: String,
    actor_id: ActorId,
    _server: tinyray_runtime::transport::server::RunningServer,
}

async fn start_actor(config: ActorConfig) -> Harness {
    let actor_id = config.actor_id;
    let server_config = ServerConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        limits: config.server.limits,
    };
    let runtime = ActorRuntime::new(config);
    let server = serve(server_config, runtime.clone()).await.expect("bind");
    Harness {
        endpoint: server.addr().to_string(),
        runtime,
        actor_id,
        _server: server,
    }
}

fn actor_config() -> ActorConfig {
    ActorConfig {
        actor_id: ActorId::generate(),
        ..Default::default()
    }
}

/// Stand-in for the Python executor thread: echoes the method name and body.
fn spawn_echo_executor(runtime: Arc<ActorRuntime>) -> tokio::task::JoinHandle<Vec<String>> {
    tokio::spawn(async move {
        let mut executed = Vec::new();
        while let Some(task) = runtime.next_task().await {
            executed.push(task.method.clone());
            if task.method == "boom" {
                runtime.fail(
                    task.task_id,
                    ErrorKind::UserException,
                    "the user's method raised".into(),
                    Some("Traceback (most recent call last):\n  ValueError: boom".into()),
                );
                continue;
            }
            let mut body = task.method.clone().into_bytes();
            body.extend_from_slice(&task.body);
            runtime.complete(task.task_id, Bytes::from(body), task.frames);
        }
        executed
    })
}

fn new_client() -> Arc<ClientRuntime> {
    ClientRuntime::new(ClientConfig {
        request_timeout: Duration::from_secs(10),
        ..Default::default()
    })
}

#[tokio::test]
async fn submit_and_fetch_over_http() {
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());

    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let reference = client
        .submit(
            actor.actor_id,
            "inc",
            Bytes::from_static(b"-payload"),
            vec![],
        )
        .await
        .expect("submit");
    let value = client
        .fetch(&reference, Duration::from_secs(5))
        .await
        .expect("fetch");

    assert_eq!(value.body, Bytes::from_static(b"inc-payload"));
    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn frames_survive_the_round_trip() {
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    // Ten megabytes, the size the design targets.
    let big = Bytes::from(vec![0x5A; 10 * 1024 * 1024]);
    let reference = client
        .submit(
            actor.actor_id,
            "echo",
            Bytes::from_static(b""),
            vec![big.clone(), Bytes::from_static(b"small")],
        )
        .await
        .expect("submit");
    let value = client
        .fetch(&reference, Duration::from_secs(30))
        .await
        .expect("fetch");

    assert_eq!(value.frames.len(), 2);
    assert_eq!(value.frames[0].len(), big.len());
    assert_eq!(value.frames[0], big);
    assert_eq!(value.frames[1], Bytes::from_static(b"small"));

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn calls_execute_in_submission_order_despite_concurrency() {
    // The whole reason sequence numbers exist. Submitting concurrently over a
    // pool of connections must still run the methods in program order.
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let mut refs = Vec::new();
    for index in 0..40 {
        refs.push(
            client
                .submit(
                    actor.actor_id,
                    &format!("m{index:03}"),
                    Bytes::new(),
                    vec![],
                )
                .await
                .expect("submit"),
        );
    }
    for reference in &refs {
        client
            .fetch(reference, Duration::from_secs(10))
            .await
            .expect("fetch");
    }

    actor.runtime.begin_shutdown();
    let executed = executor.await.unwrap();
    let expected: Vec<String> = (0..40).map(|i| format!("m{i:03}")).collect();
    assert_eq!(executed, expected, "methods ran out of order");
}

#[tokio::test]
async fn a_user_exception_carries_its_traceback_home() {
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let reference = client
        .submit(actor.actor_id, "boom", Bytes::new(), vec![])
        .await
        .expect("submit");
    let err = client
        .fetch(&reference, Duration::from_secs(5))
        .await
        .expect_err("must surface the failure");

    assert_eq!(err.kind(), ErrorKind::UserException);
    assert!(
        err.traceback().unwrap().contains("ValueError: boom"),
        "the remote traceback is the whole point: {err}"
    );

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn fetch_parks_until_the_result_exists() {
    let actor = start_actor(actor_config()).await;
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let reference = client
        .submit(actor.actor_id, "slow", Bytes::new(), vec![])
        .await
        .expect("submit");

    // No executor yet: the fetch must block rather than spin or fail.
    let fetcher = {
        let client = client.clone();
        let reference = reference.clone();
        tokio::spawn(async move { client.fetch(&reference, Duration::from_secs(10)).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let executor = spawn_echo_executor(actor.runtime.clone());

    let value = fetcher.await.unwrap().expect("fetch");
    assert_eq!(value.body, Bytes::from_static(b"slow"));

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn backpressure_is_applied_and_then_retried() {
    // A tiny queue and no executor: the actor must refuse rather than buffer
    // without bound, and the client must recover once space appears.
    let actor = start_actor(ActorConfig {
        actor_id: ActorId::generate(),
        max_pending_calls: 2,
        ..Default::default()
    })
    .await;
    let client = ClientRuntime::new(ClientConfig {
        max_retries: 0,
        request_timeout: Duration::from_secs(5),
        ..Default::default()
    });
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    client
        .submit(actor.actor_id, "a", Bytes::new(), vec![])
        .await
        .unwrap();
    client
        .submit(actor.actor_id, "b", Bytes::new(), vec![])
        .await
        .unwrap();
    let refused = client
        .submit(actor.actor_id, "c", Bytes::new(), vec![])
        .await
        .expect_err("third call must be refused");
    assert!(
        matches!(&refused, ClientError::Transport(detail) if detail.contains("backpressure")),
        "unexpected error: {refused}"
    );
    assert_eq!(actor.runtime.stats().rejected_backpressure, 1);

    // Drain, and the actor accepts again.
    let executor = spawn_echo_executor(actor.runtime.clone());
    tokio::time::sleep(Duration::from_millis(50)).await;
    client
        .submit(actor.actor_id, "d", Bytes::new(), vec![])
        .await
        .expect("accepted once the queue drained");

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn a_backpressured_client_retries_by_itself() {
    let actor = start_actor(ActorConfig {
        actor_id: ActorId::generate(),
        max_pending_calls: 1,
        ..Default::default()
    })
    .await;
    // Default config retries backpressure, which is the only failure that is
    // safe to retry blindly.
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    client
        .submit(actor.actor_id, "a", Bytes::new(), vec![])
        .await
        .unwrap();

    let submitter = {
        let client = client.clone();
        let actor_id = actor.actor_id;
        tokio::spawn(async move { client.submit(actor_id, "b", Bytes::new(), vec![]).await })
    };
    tokio::time::sleep(Duration::from_millis(60)).await;
    let executor = spawn_echo_executor(actor.runtime.clone());

    submitter.await.unwrap().expect("retry eventually succeeds");
    assert!(actor.runtime.stats().rejected_backpressure >= 1);

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn wait_returns_the_fastest_and_leaves_the_stragglers() {
    // The RL pattern: train on 3 of 5 rollouts rather than block on the slowest.
    let actor = start_actor(actor_config()).await;
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let mut refs = Vec::new();
    for index in 0..5 {
        refs.push(
            client
                .submit(actor.actor_id, &format!("m{index}"), Bytes::new(), vec![])
                .await
                .unwrap(),
        );
    }

    // Complete three of them out of band.
    for reference in refs.iter().take(3) {
        actor
            .runtime
            .complete(reference.task_id, Bytes::from_static(b"done"), vec![]);
    }

    let (ready, pending) = client.wait(&refs, 3, Duration::from_secs(5)).await;
    assert_eq!(ready.len(), 3);
    assert_eq!(pending.len(), 2);
    let ready_ids: Vec<_> = ready.iter().map(|r| r.task_id).collect();
    for reference in refs.iter().take(3) {
        assert!(ready_ids.contains(&reference.task_id));
    }
}

#[tokio::test]
async fn results_are_fetched_directly_from_their_owner() {
    // Two actors. A produces, B consumes. The point is that B talks to A
    // directly: with no object store, this is what keeps 10 MB results off the
    // driver.
    let producer = start_actor(actor_config()).await;
    let consumer = start_actor(actor_config()).await;
    let producer_exec = spawn_echo_executor(producer.runtime.clone());

    let client = new_client();
    client.register_actor(producer.actor_id, producer.endpoint.clone());
    client.register_actor(consumer.actor_id, consumer.endpoint.clone());

    let reference = client
        .submit(
            producer.actor_id,
            "produce",
            Bytes::from_static(b"!"),
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(reference.endpoint, producer.endpoint);

    // A second client stands in for the consumer actor's own runtime.
    let consumer_side = new_client();
    let value = consumer_side
        .fetch(&reference, Duration::from_secs(5))
        .await
        .expect("consumer fetches straight from the producer");
    assert_eq!(value.body, Bytes::from_static(b"produce!"));

    // The consumer's own endpoint was never involved in moving the payload.
    let stats = consumer_side.transport().stats();
    assert!(stats.contains_key(&producer.endpoint));
    assert!(!stats.contains_key(&consumer.endpoint));

    producer.runtime.begin_shutdown();
    producer_exec.await.unwrap();
}

#[tokio::test]
async fn released_results_report_object_lost() {
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let reference = client
        .submit(actor.actor_id, "produce", Bytes::new(), vec![])
        .await
        .unwrap();
    client
        .fetch(&reference, Duration::from_secs(5))
        .await
        .unwrap();
    client.release(&reference).await;

    let err = client
        .fetch(&reference, Duration::from_secs(2))
        .await
        .expect_err("a released result is gone");
    // `ObjectLost`, not `NotFound`: it existed, the consumer was late.
    assert_eq!(err.kind(), ErrorKind::ObjectLost);

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn evicted_results_report_object_lost() {
    let actor = start_actor(ActorConfig {
        actor_id: ActorId::generate(),
        store: StoreConfig {
            max_bytes: 1024,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    let executor = spawn_echo_executor(actor.runtime.clone());
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let first = client
        .submit(
            actor.actor_id,
            "big",
            Bytes::new(),
            vec![Bytes::from(vec![0u8; 4096])],
        )
        .await
        .unwrap();
    client.fetch(&first, Duration::from_secs(5)).await.unwrap();

    // Push it out with another oversized result.
    let second = client
        .submit(
            actor.actor_id,
            "big",
            Bytes::new(),
            vec![Bytes::from(vec![1u8; 4096])],
        )
        .await
        .unwrap();
    client.fetch(&second, Duration::from_secs(5)).await.unwrap();

    let err = client
        .fetch(&first, Duration::from_secs(2))
        .await
        .expect_err("the first result was evicted");
    assert_eq!(err.kind(), ErrorKind::ObjectLost);
    assert!(err.to_string().contains("evicted or expired"));

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn shutdown_fails_queued_calls_instead_of_hanging_them() {
    let actor = start_actor(actor_config()).await;
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let reference = client
        .submit(actor.actor_id, "never-runs", Bytes::new(), vec![])
        .await
        .unwrap();
    actor.runtime.begin_shutdown();

    let err = client
        .fetch(&reference, Duration::from_secs(5))
        .await
        .expect_err("must not hang");
    assert_eq!(err.kind(), ErrorKind::ActorDied);
}

#[tokio::test]
async fn health_and_introspect_are_readable() {
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let health = client
        .transport()
        .get_text(&actor.endpoint, "/health")
        .await
        .expect("health");
    assert!(health.contains("\"status\":\"ok\""));

    let reference = client
        .submit(actor.actor_id, "work", Bytes::new(), vec![])
        .await
        .unwrap();
    client
        .fetch(&reference, Duration::from_secs(5))
        .await
        .unwrap();

    let introspect = client
        .transport()
        .get_text(&actor.endpoint, "/introspect")
        .await
        .expect("introspect");
    assert!(introspect.contains("\"accepted\":1"));
    assert!(introspect.contains("\"completed\":1"));
    assert!(introspect.contains("\"stuck_callers\":[]"));

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn unknown_result_is_not_found_rather_than_lost() {
    let actor = start_actor(actor_config()).await;
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let bogus = OwnerRef {
        task_id: tinyray_core::TaskId::generate(),
        actor_id: actor.actor_id,
        endpoint: actor.endpoint.clone(),
    };
    let err = client
        .fetch(&bogus, Duration::from_secs(2))
        .await
        .expect_err("nothing to fetch");
    // The distinction matters when debugging: `NotFound` means a bug,
    // `ObjectLost` means the watermark or TTL did its job.
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn calls_to_the_wrong_actor_are_rejected() {
    let actor = start_actor(actor_config()).await;
    let client = new_client();
    // Point a different actor id at this endpoint, as a stale handle would
    // after the actor was replaced.
    let stale = ActorId::generate();
    client.register_actor(stale, actor.endpoint.clone());

    let err = client
        .submit(stale, "method", Bytes::new(), vec![])
        .await
        .expect_err("must not run someone else's method");
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn thirty_two_concurrent_consumers_share_one_result() {
    // The fan-out shape of a broadcast-free RL loop: many readers, one buffer.
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let reference = client
        .submit(
            actor.actor_id,
            "produce",
            Bytes::new(),
            vec![Bytes::from(vec![7u8; 1024 * 1024])],
        )
        .await
        .unwrap();

    let mut readers = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let client = client.clone();
        let reference = reference.clone();
        readers.spawn(async move { client.fetch(&reference, Duration::from_secs(20)).await });
    }
    let mut count = 0;
    while let Some(result) = readers.join_next().await {
        let value = result.unwrap().expect("fetch");
        assert_eq!(value.frames[0].len(), 1024 * 1024);
        count += 1;
    }
    assert_eq!(count, 32);
    // One stored copy, however many readers.
    assert_eq!(actor.runtime.store().stats().ready, 1);

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn wait_probes_readiness_without_moving_the_payload() {
    // `wait` answers a yes/no question. If it fetched the value to do so, a
    // driver waiting on 32 rollouts of 10 MB would pull 320 MB it immediately
    // discards -- the star-shaped relay this design exists to avoid.
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());
    let client = new_client();
    client.register_actor(actor.actor_id, actor.endpoint.clone());

    let big = Bytes::from(vec![0x7F; 8 * 1024 * 1024]);
    let reference = client
        .submit(actor.actor_id, "produce", Bytes::new(), vec![big.clone()])
        .await
        .unwrap();
    client
        .fetch(&reference, Duration::from_secs(30))
        .await
        .unwrap();

    // Probe directly, so the assertion is about bytes on the wire rather than
    // about how long something took.
    let probe =
        tinyray_runtime::actor::build_fetch(reference.task_id, Duration::from_millis(100), true)
            .unwrap();
    let reply = client
        .transport()
        .request(
            &actor.endpoint,
            tinyray_runtime::transport::paths::FETCH,
            &probe,
        )
        .await
        .expect("probe");

    assert!(
        matches!(
            tinyray_core::proto::Envelope::decode(&reply.header),
            Ok(tinyray_core::proto::Envelope::Result(_))
        ),
        "a status probe must still report that the result is ready"
    );
    assert!(
        reply.frames.is_empty(),
        "status probe carried {} frame(s) totalling {} bytes; wait() is relaying payloads",
        reply.frames.len(),
        reply.frames.iter().map(|f| f.len()).sum::<usize>()
    );

    // And a real fetch still returns the value.
    let value = client
        .fetch(&reference, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(value.frames[0], big);

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}

#[tokio::test]
async fn wait_moves_almost_no_bytes() {
    // The assertion the previous test could not make: this one goes through
    // `wait` itself rather than hand-building a probe, and counts the bytes
    // that actually crossed the wire.
    let actor = start_actor(actor_config()).await;
    let executor = spawn_echo_executor(actor.runtime.clone());

    let producer_side = new_client();
    producer_side.register_actor(actor.actor_id, actor.endpoint.clone());

    let payload = 8 * 1024 * 1024;
    let reference = producer_side
        .submit(
            actor.actor_id,
            "produce",
            Bytes::new(),
            vec![Bytes::from(vec![0x33; payload])],
        )
        .await
        .unwrap();
    producer_side
        .fetch(&reference, Duration::from_secs(30))
        .await
        .unwrap();

    // A fresh client, so its counters describe `wait` and nothing else.
    let waiter = new_client();
    let (ready, pending) = waiter
        .wait(std::slice::from_ref(&reference), 1, Duration::from_secs(30))
        .await;
    assert_eq!(ready.len(), 1);
    assert!(pending.is_empty());

    let received: u64 = waiter
        .transport()
        .stats()
        .values()
        .map(|stats| stats.bytes_received)
        .sum();
    assert!(
        received < 4096,
        "wait() pulled {received} bytes for an {payload}-byte result; it is \
         relaying the payload through the driver instead of probing readiness"
    );

    // The value is still retrievable in full.
    let value = waiter
        .fetch(&reference, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(value.frames[0].len(), payload);

    actor.runtime.begin_shutdown();
    executor.await.unwrap();
}
