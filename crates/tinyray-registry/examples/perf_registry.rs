//! Portable before/after benchmark using only the v0.15.0 public API.
//! Run in an otherwise idle measurement window:
//! cargo run --release -p tinyray-registry --example perf_registry -- --iterations 2000
//! This measures owned acknowledgment assembly, not HTTP/JSON encoding.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};
use tinyray_proto::{Beat, BeatAck};
use tinyray_registry::state::Registry;

struct Config {
    members: usize,
    payload: usize,
    methods: usize,
    watched: usize,
    history: usize,
    iterations: usize,
    cohort: usize,
    rounds: usize,
}

impl Config {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut c = Self {
            members: 256,
            payload: 512,
            methods: 128,
            watched: 8,
            history: 8192,
            iterations: 2000,
            cohort: 16,
            rounds: 16,
        };
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            let value: usize = args.next().ok_or("each flag needs a value")?.parse()?;
            match flag.as_str() {
                "--members" => c.members = value,
                "--payload-bytes" => c.payload = value,
                "--methods" => c.methods = value,
                "--watched-pools" => c.watched = value,
                "--history" => c.history = value,
                "--iterations" => c.iterations = value,
                "--cohort" => c.cohort = value,
                "--rounds" => c.rounds = value,
                _ => return Err(format!("unknown option {flag}").into()),
            }
        }
        if !(1..=4096).contains(&c.members)
            || c.payload > 8192
            || c.methods > 256
            || !(1..=64).contains(&c.watched)
            || !(4096..=65536).contains(&c.history)
            || !(1..=50000).contains(&c.iterations)
            || !(1..=512).contains(&c.cohort)
            || !(1..=1000).contains(&c.rounds)
        {
            return Err("configuration exceeds the documented benchmark bounds".into());
        }
        let snapshot = c.members as u128 * (c.payload as u128 + 256) + c.methods as u128 * 96;
        let copied = snapshot * c.cohort as u128 * c.rounds as u128;
        let quiet_copies = c.iterations as u128 * c.watched as u128 * (c.methods as u128 * 96 + 64);
        if snapshot > 16 << 20 || copied > 2 << 30 || quiet_copies > 2 << 30 {
            return Err("limit: 16MiB estimated snapshot, 2GiB estimated copying per case".into());
        }
        Ok(c)
    }
}

fn member(pool: &str, id: u64) -> Beat {
    Beat {
        pool: pool.into(),
        slot: None,
        id,
        incarnation: 1,
        publication: Some(0),
        policy: "churn".into(),
        size: None,
        url: None,
        state: Value::Null,
        ready: true,
        leaving: false,
        exclusive: false,
        methods: Vec::new(),
        watch: Vec::new(),
        seen: HashMap::new(),
        hold_ms: 0,
    }
}

fn observer(id: u64, pool: &str) -> Beat {
    let mut b = member("observers", id);
    b.watch.push(pool.into());
    b
}

fn timing(name: &str, operations: usize, duration: Duration) -> Value {
    json!({
        "case": name,
        "operations": operations,
        "total_ns": duration.as_nanos(),
        "ns_per_operation": duration.as_nanos() as f64 / operations as f64,
    })
}

fn verify_payload(ack: &BeatAck, expected: usize, full: bool, methods: usize, revision: u64) {
    assert!(ack.accepted);
    let d = &ack.pools["fleet"];
    assert_eq!(d.full, full);
    assert_eq!(d.changed.len(), expected);
    assert!(d.removed.is_empty());
    assert_eq!(d.methods.len(), methods);
    assert_eq!(d.changed[0].state["revision"], json!(revision));
    assert!(d.changed[0].ready);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let c = Config::parse()?;
    let reg = Registry::new(Duration::from_secs(3600));
    let methods: Vec<_> = (0..c.methods)
        .map(|i| format!("method_{i:03}_with_a_representative_serving_name"))
        .collect();
    let mut owners: Vec<_> = (0..c.members)
        .map(|id| {
            let mut b = member("fleet", id as u64);
            b.methods = methods.clone();
            b.state = json!({"payload": "x".repeat(c.payload), "revision": 0});
            assert!(reg.beat(&b).accepted);
            b
        })
        .collect();
    let mut quiet = observer(10_000, "fleet");
    for i in 1..c.watched {
        let name = format!("quiet_{i}");
        let mut b = member(&name, 1);
        b.methods = methods.clone();
        assert!(reg.beat(&b).accepted);
        quiet.watch.push(name);
    }
    let versions = reg.snapshot();
    for name in &quiet.watch {
        quiet.seen.insert(name.clone(), versions[name].0);
    }
    for _ in 0..32 {
        assert!(reg.beat(&quiet).pools.is_empty());
    }
    let started = Instant::now();
    for _ in 0..c.iterations {
        let ack = reg.beat(black_box(&quiet));
        assert!(ack.accepted && ack.pools.is_empty());
        drop(black_box(ack));
    }
    let mut timings = vec![timing(
        "quiet_caught_up_many_methods",
        c.iterations,
        started.elapsed(),
    )];

    let mut history = member("history", 1);
    history.state = json!({"revision": 0});
    assert!(reg.beat(&history).accepted);
    for revision in 1..=c.history {
        history.publication = Some(revision as u64);
        history.state = json!({"revision": revision});
        assert!(reg.beat(&history).accepted);
    }
    let mut cursor = observer(10_001, "history");
    let mut version = reg.snapshot()["history"].0;
    cursor.seen.insert("history".into(), version);
    assert!(reg.beat(&cursor).pools.is_empty());
    let mut elapsed = Duration::ZERO;
    for i in 1..=c.iterations {
        let revision = c.history + i;
        history.publication = Some(revision as u64);
        history.state = json!({"revision": revision});
        // Preparation is excluded; every timed read needs a new one-entry
        // delta, so this case does not merely time a warmed cursor cache.
        assert!(reg.beat(&history).accepted);
        cursor.seen.insert("history".into(), version);
        version += 1;
        let started = Instant::now();
        let ack = reg.beat(black_box(&cursor));
        assert!(ack.accepted);
        let d = &ack.pools["history"];
        assert!(!d.full && d.removed.is_empty());
        assert_eq!(d.version, version);
        assert_eq!(d.changed.len(), 1);
        assert_eq!(d.changed[0].state["revision"], json!(revision));
        drop(black_box(ack));
        elapsed += started.elapsed();
    }
    timings.push(timing(
        "long_history_fresh_one_change_delta",
        c.iterations,
        elapsed,
    ));

    let mut cohort: Vec<_> = (0..c.cohort)
        .map(|i| observer(20_000 + i as u64, "fleet"))
        .collect();
    for b in &cohort {
        verify_payload(&reg.beat(b), c.members, true, c.methods, 0);
    }
    let operations = c.cohort * c.rounds;
    let started = Instant::now();
    for _ in 0..c.rounds {
        for b in &cohort {
            let ack = reg.beat(black_box(b));
            verify_payload(&ack, c.members, true, c.methods, 0);
            drop(black_box(ack));
        }
    }
    timings.push(timing(
        "cohort_shared_full_roster_owned_ack",
        operations,
        started.elapsed(),
    ));

    let before = reg.snapshot()["fleet"].0;
    let changed = (c.members / 4).max(1);
    for b in owners.iter_mut().take(changed) {
        b.publication = Some(1);
        b.state["revision"] = json!(1);
        assert!(reg.beat(b).accepted);
    }
    for b in &mut cohort {
        b.seen.insert("fleet".into(), before);
    }
    let started = Instant::now();
    for _ in 0..c.rounds {
        for b in &cohort {
            let ack = reg.beat(black_box(b));
            verify_payload(&ack, changed, false, c.methods, 1);
            drop(black_box(ack));
        }
    }
    timings.push(timing(
        "cohort_shared_delta_owned_ack",
        operations,
        started.elapsed(),
    ));
    println!(
        "{}",
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "scope": "public Registry::beat; owned ack assembly and assertions; no HTTP/JSON encoding",
            "note": "Owned API still materializes shared payloads. Measure HTTP separately for shared-reply gains.",
            "config": {
                "members": c.members, "payload_bytes": c.payload, "methods": c.methods,
                "watched_pools": c.watched, "history_updates": c.history,
                "iterations": c.iterations, "cohort": c.cohort, "rounds": c.rounds,
                "changed_members": changed,
            },
            "timings": timings,
        })
    );
    Ok(())
}
