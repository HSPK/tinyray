use super::*;
use crate::delta::{CACHE_BYTES, CACHE_ENTRIES};
use serde_json::json;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

fn beat(pool: &str, id: u64) -> Beat {
    serde_json::from_value(json!({
        "pool": pool, "id": id, "incarnation": 1,
        "publication": 0, "policy": "churn",
    }))
    .unwrap()
}

fn registry() -> Registry {
    Registry::new(Duration::from_secs(30))
}

fn watch(pool: &str, seen: Option<u64>) -> Beat {
    let mut b = beat("observer", 999);
    b.watch.push(pool.into());
    if let Some(seen) = seen {
        b.seen.insert(pool.into(), seen);
    }
    b
}

fn delta(reg: &Registry, pool: &str, seen: Option<u64>) -> Arc<PoolDelta> {
    reg.deltas_shared_for(&watch(pool, seen))
        .remove(pool)
        .unwrap()
}

fn publish(reg: &Registry, b: &mut Beat) {
    b.publication = Some(b.publication.unwrap() + 1);
    assert!(reg.beat_shared(b).accepted);
}

fn expire(reg: &Registry, pool: &str, id: u64) {
    reg.pools
        .write()
        .unwrap()
        .get_mut(pool)
        .unwrap()
        .members
        .get_mut(&id)
        .unwrap()
        .expires_at = Instant::now();
}

/// The pre-optimization traversal, used for result equivalence and the ignored
/// assembly microbenchmark. It deliberately scans the whole retained log.
fn reference_delta(pool: &Pool, seen: Option<u64>) -> Option<PoolDelta> {
    let mut d = PoolDelta {
        version: pool.version,
        roster: pool.roster,
        policy: pool.policy.clone(),
        methods: pool.methods.clone(),
        size: pool.size,
        changed: Vec::new(),
        removed: Vec::new(),
        full: false,
    };
    let oldest = pool.log.front().map(|(v, _)| *v).unwrap_or(0);
    match seen {
        Some(v) if v == pool.version => return None,
        Some(v) if v < pool.version && v + 1 >= oldest => {
            let mut ids: Vec<_> = pool
                .log
                .iter()
                .filter(|(version, _)| *version > v)
                .map(|(_, id)| *id)
                .collect();
            ids.sort_unstable();
            ids.dedup();
            for id in ids {
                match pool.members.get(&id) {
                    Some(r) => d.changed.push(r.member.clone()),
                    None => d.removed.push(id),
                }
            }
        }
        _ => {
            d.full = true;
            d.changed = pool.members.values().map(|r| r.member.clone()).collect();
        }
    }
    Some(d)
}

#[test]
fn quiet_cursors_skip_metadata_and_do_not_populate_the_delta_cache() {
    let reg = registry();
    let mut b = beat("p", 1);
    b.methods = (0..MAX_METHODS).map(|i| format!("method_{i}")).collect();
    assert!(reg.beat_shared(&b).accepted);
    let version = reg.snapshot()["p"].0;
    assert!(reg.deltas_shared_for(&watch("p", Some(version))).is_empty());
    let quiet = reg.beat_shared(&watch("p", Some(version)));
    assert!(quiet.pools.is_empty());
    let pools = reg.pools.read().unwrap();
    assert_eq!(pools["p"].cache.lock().unwrap().usage(), (0, 0));
    drop(pools);
    assert_eq!(delta(&reg, "p", None).methods, b.methods);
}

#[test]
fn suffix_replay_matches_full_scan_across_wrapping_and_resync_boundaries() {
    let reg = registry();
    let mut b = beat("p", 1);
    assert!(reg.beat_shared(&b).accepted);
    for n in 0..LOG_CAP + 17 {
        b.state = json!(n);
        publish(&reg, &mut b);
    }
    let mut removed = beat("p", 2);
    assert!(reg.beat_shared(&removed).accepted);
    removed.leaving = true;
    assert!(reg.beat_shared(&removed).accepted);

    let pools = reg.pools.read().unwrap();
    let pool = &pools["p"];
    let oldest = pool.log.front().unwrap().0;
    let version = pool.version;
    for seen in [
        None,
        Some(0),
        Some(oldest - 2),
        Some(oldest - 1),
        Some(oldest),
        Some(version - 3),
        Some(version - 1),
        Some(version),
        Some(version + 1),
        Some(u64::MAX),
    ] {
        let mut deferred = Deferred::default();
        let actual = pool.delta(seen, &mut deferred);
        let expected = reference_delta(pool, seen);
        assert_eq!(
            actual.as_deref().map(|d| serde_json::to_value(d).unwrap()),
            expected.as_ref().map(|d| serde_json::to_value(d).unwrap()),
            "cursor {seen:?}"
        );
    }
    drop(pools);
    let full = delta(&reg, "p", None);
    assert!(Arc::ptr_eq(&full, &delta(&reg, "p", Some(u64::MAX))));
    assert!(Arc::ptr_eq(&full, &delta(&reg, "p", Some(oldest - 2))));
    let recent = delta(&reg, "p", Some(version - 3));
    assert_eq!(recent.removed, vec![2]);
    assert_eq!(recent.changed.len(), 1);
    assert_eq!(recent.changed[0].id, 1);
}

#[test]
fn cache_invalidation_preserves_publications_metadata_and_snapshot_ownership() {
    let reg = registry();
    let mut b = beat("p", 1);
    assert!(reg.beat_shared(&b).accepted);
    let initial = delta(&reg, "p", None);
    assert!(Arc::ptr_eq(&initial, &delta(&reg, "p", None)));
    let mut previous = initial.clone();
    for change in 0..3 {
        match change {
            0 => b.state = json!({"step": 7}),
            1 => b.ready = true,
            _ => b.url = Some("http://new".into()),
        }
        publish(&reg, &mut b);
        let current = delta(&reg, "p", None);
        assert!(!Arc::ptr_eq(&previous, &current));
        assert_eq!(current.changed[0].state, b.state);
        assert_eq!(current.changed[0].ready, b.ready);
        assert_eq!(current.changed[0].url, b.url);
        previous = current;
    }
    assert!(initial.changed[0].state.is_null());
    assert!(!initial.changed[0].ready);
    assert!(initial.changed[0].url.is_none());

    let mut delayed = b.clone();
    delayed.publication = Some(0);
    delayed.state = json!("old");
    assert!(reg.beat_shared(&delayed).accepted);
    assert!(Arc::ptr_eq(&previous, &delta(&reg, "p", None)));
    assert_eq!(delta(&reg, "p", None).changed[0].state, b.state);

    // Learning methods from an existing member currently need not bump the
    // roster version, but a cached full snapshot must still learn them.
    let version = previous.version;
    b.methods.push("infer".into());
    assert!(reg.beat_shared(&b).accepted);
    let with_methods = delta(&reg, "p", None);
    assert_eq!(with_methods.version, version);
    assert_eq!(with_methods.methods, vec!["infer"]);
    assert!(!Arc::ptr_eq(&previous, &with_methods));

    b.leaving = true;
    assert!(reg.beat_shared(&b).accepted);
    let empty = delta(&reg, "p", None);
    assert!(empty.changed.is_empty());
    assert_eq!(delta(&reg, "p", Some(version)).removed, vec![1]);
    b.leaving = false;
    b.incarnation += 1;
    b.policy = "collective".into();
    b.size = Some(4);
    b.methods = vec!["train".into()];
    assert!(reg.beat_shared(&b).accepted);
    let relaunched = delta(&reg, "p", None);
    assert_eq!(relaunched.policy, "collective");
    assert_eq!(relaunched.size, Some(4));
    assert_eq!(relaunched.methods, vec!["train"]);
    assert!(!Arc::ptr_eq(&empty, &relaunched));

    expire(&reg, "p", 1);
    assert_eq!(reg.sweep(), 1);
    let expired = delta(&reg, "p", None);
    assert!(expired.changed.is_empty());
    assert!(!Arc::ptr_eq(&relaunched, &expired));
    let restarted = registry();
    assert!(restarted.beat_shared(&beat("p", 5)).accepted);
    let fresh = delta(&restarted, "p", Some(expired.version));
    assert!(fresh.full);
    assert_eq!(fresh.changed[0].id, 5);
}

#[test]
fn snapshot_size_estimates_cover_escaping_and_caches_stay_bounded() {
    let reg = registry();
    let mut b = beat("p", u64::MAX);
    b.policy = "\u{0000}\n\"".repeat(100);
    b.methods = vec!["\u{0000}\\".repeat(100); MAX_METHODS];
    b.url = Some("\u{0000}\\".repeat(100));
    b.state = json!({"nested": ["☃", "\n\u{0000}", u64::MAX]});
    assert!(reg.beat_shared(&b).accepted);
    {
        let pools = reg.pools.read().unwrap();
        let (full, bytes) = pools["p"].build_delta(None);
        assert!(bytes >= serde_json::to_vec(&full).unwrap().len());
    }
    for n in 0..20 {
        b.state = json!(n);
        publish(&reg, &mut b);
    }
    let version = reg.snapshot()["p"].0;
    for seen in version - 12..version {
        delta(&reg, "p", Some(seen));
        let pools = reg.pools.read().unwrap();
        let (entries, bytes) = pools["p"].cache.lock().unwrap().usage();
        assert!(entries <= CACHE_ENTRIES);
        assert!(bytes <= CACHE_BYTES);
    }

    let large = registry();
    for id in 0..CACHE_BYTES / MAX_STATE + 2 {
        let mut b = beat("large", id as u64);
        b.state = json!({"blob": "x".repeat(MAX_STATE - 11)});
        assert!(large.beat_shared(&b).accepted);
    }
    let full = delta(&large, "large", None);
    assert!(!Arc::ptr_eq(&full, &delta(&large, "large", None)));
    let pools = large.pools.read().unwrap();
    assert_eq!(pools["large"].cache.lock().unwrap().usage(), (0, 0));
}

#[test]
fn escaped_state_over_the_limit_is_refused_before_creating_a_pool() {
    let reg = registry();
    let mut b = beat("escaped", 1);
    b.state = json!("\u{0000}".repeat(MAX_STATE / 2));
    assert!(!reg.beat_shared(&b).accepted);
    assert!(reg.snapshot().is_empty());
}

struct LockProbe {
    reg: Arc<Registry>,
    wakes: AtomicUsize,
    outside: AtomicBool,
}

impl Wake for LockProbe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.outside
            .fetch_and(self.reg.pools.try_write().is_ok(), Ordering::SeqCst);
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn change_and_expiry_wake_waiters_only_after_releasing_the_registry_lock() {
    for expiry in [false, true] {
        let reg = Arc::new(registry());
        let mut b = beat("p", 1);
        assert!(reg.beat_shared(&b).accepted);
        let bell = Arc::new(Notify::new());
        reg.park(&["p".into()], &bell);
        let probe = Arc::new(LockProbe {
            reg: reg.clone(),
            wakes: AtomicUsize::new(0),
            outside: AtomicBool::new(true),
        });
        let waker = Waker::from(probe.clone());
        let mut context = Context::from_waker(&waker);
        let mut waiting = Box::pin(bell.notified());
        assert_eq!(waiting.as_mut().poll(&mut context), Poll::Pending);
        if expiry {
            expire(&reg, "p", 1);
            assert_eq!(reg.sweep(), 1);
        } else {
            b.ready = true;
            publish(&reg, &mut b);
        }
        assert_eq!(waiting.as_mut().poll(&mut context), Poll::Ready(()));
        assert!(probe.wakes.load(Ordering::SeqCst) > 0);
        assert!(probe.outside.load(Ordering::SeqCst));
    }
}

#[test]
fn registering_before_rechecking_cannot_lose_changes_on_either_side_of_park() {
    for change_before_park in [false, true] {
        let reg = Arc::new(registry());
        let mut b = beat("p", 1);
        assert!(reg.beat_shared(&b).accepted);
        let seen = reg.snapshot()["p"].0;
        let bell = Arc::new(Notify::new());
        if change_before_park {
            b.ready = true;
            publish(&reg, &mut b);
        }
        reg.park(&["p".into()], &bell);
        if !change_before_park {
            b.ready = true;
            publish(&reg, &mut b);
        }
        let fresh = reg.deltas_shared_for(&watch("p", Some(seen)));
        assert!(fresh["p"].changed[0].ready);
        if !change_before_park {
            let probe = Arc::new(LockProbe {
                reg: reg.clone(),
                wakes: AtomicUsize::new(0),
                outside: AtomicBool::new(true),
            });
            let waker = Waker::from(probe);
            let mut context = Context::from_waker(&waker);
            let mut waiting = Box::pin(bell.notified());
            assert_eq!(waiting.as_mut().poll(&mut context), Poll::Ready(()));
        }
    }
}

#[test]
#[ignore = "microbenchmark: run only in a coordinated, idle measurement window"]
fn registry_delta_microbenchmark() {
    use std::hint::black_box;

    fn time(label: &str, iterations: usize, mut run: impl FnMut()) {
        let started = Instant::now();
        for _ in 0..iterations {
            run();
        }
        println!(
            "{label}: {:.0} ns/op ({iterations} operations)",
            started.elapsed().as_nanos() as f64 / iterations as f64
        );
    }

    let iterations = std::env::var("TINYRAY_REGISTRY_BENCH_ITERS")
        .ok()
        .map(|v| v.parse::<usize>().expect("positive iteration count"))
        .unwrap_or(2000)
        .max(1);
    let reg = registry();
    for id in 0..512 {
        let mut b = beat("p", id);
        b.state = json!({"payload": "x".repeat(256), "shard": id % 8});
        b.methods = (0..32).map(|i| format!("method_{i}")).collect();
        assert!(reg.beat_shared(&b).accepted);
    }
    let mut updating = beat("p", 0);
    for n in 0..LOG_CAP + 16 {
        updating.state = json!({"step": n});
        publish(&reg, &mut updating);
    }
    let pools = reg.pools.read().unwrap();
    let pool = &pools["p"];
    let mut deferred = Deferred::default();
    time("quiet/reference_metadata_copies", iterations, || {
        black_box(reference_delta(pool, black_box(Some(pool.version))));
    });
    time("quiet/fast_cursor", iterations, || {
        black_box(pool.delta(black_box(Some(pool.version)), &mut deferred));
    });
    time("history/reference_scan_4096", iterations, || {
        black_box(reference_delta(pool, black_box(Some(pool.version - 1))));
    });
    time("history/one_entry_suffix", iterations, || {
        black_box(pool.build_delta(black_box(Some(pool.version - 1))));
    });
    let shared = pool.delta(None, &mut deferred).unwrap();
    assert!(Arc::ptr_eq(
        &shared,
        &pool.delta(None, &mut deferred).unwrap()
    ));
    time("fanout/reference_full_roster_clone", iterations, || {
        black_box(reference_delta(pool, None));
    });
    time("fanout/shared_full_roster", iterations, || {
        black_box(pool.delta(None, &mut deferred));
    });
    println!("Snapshot assembly only: no JSON encoding, network, or end-to-end latency.");
}
