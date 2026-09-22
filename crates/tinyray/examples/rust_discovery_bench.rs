use serde_json::json;
use std::time::{Duration, Instant};
use tinyray::MemberBuilder;

fn median_ms(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn measure(mut operation: impl FnMut(), rounds: usize) -> f64 {
    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let started = Instant::now();
        operation();
        samples.push(started.elapsed().as_secs_f64() * 1_000.0);
    }
    median_ms(samples)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let registry = args
        .next()
        .ok_or("usage: rust_discovery_bench ENDPOINT MEMBERS")?;
    let members: usize = args
        .next()
        .ok_or("usage: rust_discovery_bench ENDPOINT MEMBERS")?
        .parse()?;
    let observer =
        MemberBuilder::new(registry, "rust-discovery-bench").join(Duration::from_secs(15))?;
    let pool = observer.pool("load")?;
    let snapshot = pool.wait_count(members, &json!({}), Duration::from_secs(20))?;
    let identity = snapshot
        .iter()
        .next()
        .ok_or("load pool is empty")?
        .identity();

    let rounds = if members >= 5_000 { 400 } else { 1_000 };
    let snapshot_ms = measure(
        || {
            std::hint::black_box(pool.snapshot(false));
        },
        rounds,
    );
    let snapshot_len_ms = measure(
        || {
            std::hint::black_box(snapshot.len());
        },
        rounds,
    );
    let snapshot_get_ms = measure(
        || {
            std::hint::black_box(snapshot.get(&identity));
        },
        rounds,
    );
    let refs_ms = measure(
        || {
            std::hint::black_box(snapshot.members());
        },
        rounds,
    );
    let owned_ms = measure(
        || {
            std::hint::black_box(observer.members("load", true));
        },
        rounds,
    );
    let count_ms = measure(
        || {
            std::hint::black_box(pool.count(&json!({}), true).unwrap());
        },
        rounds,
    );
    let filtered_count_ms = measure(
        || {
            std::hint::black_box(pool.count(&json!({"shard": 3}), true).unwrap());
        },
        rounds,
    );
    let pick_ms = measure(
        || {
            std::hint::black_box(pool.pick(&json!({"shard": 3}), true).unwrap());
        },
        rounds,
    );

    println!(
        "{}",
        serde_json::to_string(&json!({
            "members": members,
            "snapshot_ms": snapshot_ms,
            "snapshot_len_ms": snapshot_len_ms,
            "snapshot_get_ms": snapshot_get_ms,
            "refs_ms": refs_ms,
            "owned_ms": owned_ms,
            "count_ms": count_ms,
            "filtered_count_ms": filtered_count_ms,
            "pick_ms": pick_ms,
            "clone_speedup": owned_ms / refs_ms,
        }))?
    );
    observer.leave();
    Ok(())
}
