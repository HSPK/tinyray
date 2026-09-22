#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::time::Duration;
#[cfg(target_os = "linux")]
use tinyray::{MemberBuilder, Router};
#[cfg(target_os = "linux")]
use tinyray_registry::state::Registry;
#[cfg(target_os = "linux")]
use tokio::net::TcpListener;

#[cfg(target_os = "linux")]
fn named_threads(name: &str) -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("comm")).ok())
        .filter(|comm| comm.trim() == name)
        .count()
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_builder_shares_one_rpc_worker_pool() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let registry = Arc::new(Registry::new(Duration::from_secs(10)));
    let registry_task = tokio::spawn(tinyray_registry::server::serve(listener, registry));

    let member = MemberBuilder::new(endpoint, "runtime-test")
        .router(Router::new())
        .rpc_worker_threads(3)
        .join_async(Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(named_threads("tinyray-rpc"), 3);
    assert_eq!(named_threads("tinyray"), 2);

    member.leave_async().await.unwrap();
    registry_task.abort();
}
