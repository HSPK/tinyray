use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tinyray::MemberBuilder;
use tinyray_registry::state::Registry;
use tokio::net::TcpListener;

async fn registry() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let registry = Arc::new(Registry::new(Duration::from_secs(10)));
    let task = tokio::spawn(tinyray_registry::server::serve(listener, registry));
    (endpoint, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_discovery_views_filter_freeze_wait_and_track_replacements() {
    let (endpoint, registry) = registry().await;
    let observer = MemberBuilder::new(&endpoint, "observer")
        .join_async(Duration::from_secs(5))
        .await
        .unwrap();
    let workers = observer.pool("workers").unwrap();

    let first = MemberBuilder::new(&endpoint, "workers")
        .policy("stateful")
        .slot(0)
        .size(1)
        .join_async(Duration::from_secs(5))
        .await
        .unwrap();
    first.ready(&json!({"role": "gpu", "step": 1})).unwrap();
    first.flush_async(Duration::from_secs(5)).await.unwrap();

    let frozen = workers
        .wait_count_async(1, &json!({"role": "gpu"}), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(frozen.len(), 1);
    assert_eq!(frozen.fingerprint(), frozen.roster());
    let first_ref = frozen.slot(0).unwrap();
    let first_identity = first_ref.identity();
    assert_eq!(first_ref.state, json!({"role": "gpu", "step": 1}));
    assert_eq!(
        frozen.get(&first_identity).unwrap().identity(),
        first_identity
    );
    assert_eq!(workers.count(&json!({"role": "gpu"}), true).unwrap(), 1);
    assert_eq!(workers.count(&json!({"role": "cpu"}), true).unwrap(), 0);
    assert_eq!(
        workers
            .snapshot_where(&json!({"step": 999}), true)
            .unwrap()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        workers
            .pick(&json!({"role": "gpu"}), true)
            .unwrap()
            .unwrap()
            .identity(),
        first_identity
    );

    first.leave_async().await.unwrap();
    assert!(workers
        .wait_departure_async(&first_identity, Duration::from_secs(5))
        .await
        .unwrap());
    assert!(workers.snapshot(true).unwrap().is_empty());
    assert_eq!(frozen.len(), 1);
    assert_eq!(frozen.slot(0).unwrap().identity(), first_identity);

    let waiting_pool = workers.clone();
    let previous = first_identity.clone();
    let replacement = tokio::spawn(async move {
        waiting_pool
            .wait_replacement_async(0, Some(&previous), Duration::from_secs(5))
            .await
    });
    let second = MemberBuilder::new(&endpoint, "workers")
        .policy("stateful")
        .slot(0)
        .size(1)
        .join_async(Duration::from_secs(5))
        .await
        .unwrap();
    second.ready(&json!({"role": "gpu", "step": 2})).unwrap();
    second.flush_async(Duration::from_secs(5)).await.unwrap();

    let second_ref = replacement.await.unwrap().unwrap().unwrap();
    assert_ne!(second_ref.identity(), first_identity);
    assert_eq!(second_ref.state, json!({"role": "gpu", "step": 2}));

    let epoch_pool = workers.clone();
    let epoch =
        tokio::task::spawn_blocking(move || epoch_pool.epoch(Some(1), Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(epoch.len(), 1);
    assert!(epoch.valid());
    assert_eq!(epoch.slot(0).unwrap().identity(), second_ref.identity());

    let second_identity = second_ref.identity();
    second.leave_async().await.unwrap();
    assert!(workers
        .wait_departure_async(&second_identity, Duration::from_secs(5))
        .await
        .unwrap());
    assert!(!epoch.valid());
    observer.leave_async().await.unwrap();
    registry.abort();
}
