use serde_json::json;
use std::time::{Duration, Instant};
use tinyray::{MemberBuilder, Router};

#[cfg(target_os = "linux")]
fn named_threads(name: &str) -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("comm")).ok())
        .filter(|comm| comm.trim() == name)
        .count()
}

#[cfg(not(target_os = "linux"))]
fn named_threads(_name: &str) -> usize {
    0
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = std::env::args()
        .nth(1)
        .ok_or("usage: rust_runtime_bench REGISTRY")?;
    let started = Instant::now();
    let member = MemberBuilder::new(registry, "runtime-bench")
        .router(Router::new())
        .rpc_worker_threads(4)
        .join(Duration::from_secs(15))?;
    let join_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let rpc_workers = named_threads("tinyray-rpc");
    let membership_workers = named_threads("tinyray");
    println!(
        "{}",
        json!({
            "rpc_workers": rpc_workers,
            "membership_workers": membership_workers,
            "total_native_workers": rpc_workers + membership_workers,
            "join_ms": join_ms,
        })
    );
    member.leave();
    Ok(())
}
