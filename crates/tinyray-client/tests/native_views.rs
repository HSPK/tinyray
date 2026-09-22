use _tinyray::beat::{CacheWaiter, CachedPool, Shared, WaitStatus};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tinyray_proto::{Member, PoolDelta};

fn member(id: u64, slot: Option<u64>, incarnation: u64, ready: bool, step: u64) -> Member {
    Member {
        id,
        slot,
        incarnation,
        url: None,
        state: json!({"step": step, "payload": "x".repeat(16_000)}),
        ready,
    }
}

fn full(version: u64, members: Vec<Member>) -> PoolDelta {
    PoolDelta {
        version,
        roster: members
            .iter()
            .fold(0, |roster, member| roster ^ member.roster_hash()),
        policy: "stateful".into(),
        methods: vec!["ping".into()],
        size: Some(members.len() as u64),
        changed: members,
        removed: Vec::new(),
        full: true,
    }
}

#[test]
fn frozen_views_share_arcs_and_keep_indexes_and_state_after_invalidation() {
    let low = member(5, Some(2), 9, false, 1);
    let high = member(50, Some(2), 10, true, 1);
    let free = member(7, None, 11, true, 1);
    let mut cache = CachedPool::default();
    cache.apply(&full(1, vec![high.clone(), free.clone(), low.clone()]));

    let all = cache.native(false);
    let ready = cache.native(true);
    assert!(Arc::ptr_eq(&all.members()[0], &cache.members[&5]));
    assert_eq!(all.slot(2), Some(&low));
    assert_eq!(ready.slot(2), Some(&high));
    assert_eq!(all.get("workers", "workers/2#10"), Some(&high));
    assert_eq!(all.get("workers", "workers/7#11"), Some(&free));
    assert_eq!(cache.count(&json!({"step": 1.0}), true), 2);
    let mut rng = fastrand::Rng::with_seed(7);
    let chosen = cache
        .choose_arc(&json!({"step": 1}), true, &mut rng)
        .unwrap();
    assert!(Arc::ptr_eq(&chosen, &cache.members[&7]) || Arc::ptr_eq(&chosen, &cache.members[&50]));
    assert!(Arc::ptr_eq(
        &cache.slot_owned(2, false).unwrap(),
        &cache.members[&5]
    ));

    let changed = member(50, Some(2), 10, true, 2);
    let mut update = full(2, vec![low, free, changed.clone()]);
    update.full = false;
    update.changed = vec![changed];
    cache.apply(&update);
    let current = cache.native(false);
    assert!(!Arc::ptr_eq(&all, &current));
    assert_eq!(all.get("workers", "workers/2#10").unwrap().state["step"], 1);
    assert_eq!(
        current.get("workers", "workers/2#10").unwrap().state["step"],
        2
    );
    drop(cache);
    assert_eq!(all.slot(2).unwrap().state["step"], 1);
}

#[test]
fn count_wait_handoff_is_safe_when_the_change_races_subscription() {
    let shared = Arc::new(Shared::new(
        "127.0.0.1:1".into(),
        "observer".into(),
        1,
        1,
        "churn".into(),
        None,
        None,
        None,
        Vec::new(),
        false,
        0,
    ));
    let mut cache = CachedPool::default();
    cache.apply(&full(1, vec![member(1, Some(0), 1, false, 1)]));
    shared
        .cache
        .write()
        .unwrap()
        .insert("workers".into(), cache);

    let waiter = Arc::new(CacheWaiter::count(
        shared.clone(),
        "workers".into(),
        json!({"step": 1}),
        1,
    ));
    let (started, begin) = std::sync::mpsc::channel();
    let waiting = waiter.clone();
    let thread = std::thread::spawn(move || {
        started.send(()).unwrap();
        waiting.wait(Some(Duration::from_secs(1)), true)
    });

    begin.recv().unwrap();
    let ready = member(1, Some(0), 1, true, 1);
    let update = PoolDelta {
        version: 2,
        roster: ready.roster_hash(),
        policy: "stateful".into(),
        methods: vec!["ping".into()],
        size: Some(1),
        changed: vec![ready],
        removed: Vec::new(),
        full: false,
    };
    shared
        .cache
        .write()
        .unwrap()
        .get_mut("workers")
        .unwrap()
        .apply(&update);
    shared.ring();

    let result = thread.join().unwrap();
    assert_eq!(result.status, WaitStatus::Ready);
    assert_eq!(result.matched, 1);
    assert_eq!(result.view.unwrap().members.len(), 1);
}

#[test]
fn absence_conditions_wait_for_the_first_pool_answer() {
    let shared = Arc::new(Shared::new(
        "127.0.0.1:1".into(),
        "observer".into(),
        1,
        1,
        "churn".into(),
        None,
        None,
        None,
        Vec::new(),
        false,
        0,
    ));
    let count = CacheWaiter::count(shared.clone(), "cold".into(), json!({}), 0);
    let departure = CacheWaiter::departure(shared.clone(), "cold".into(), "cold/0#1".into());
    assert_eq!(count.check(true).status, WaitStatus::Pending);
    assert_eq!(departure.check(true).status, WaitStatus::Pending);

    let mut empty = CachedPool::default();
    empty.apply(&full(1, Vec::new()));
    shared.cache.write().unwrap().insert("cold".into(), empty);
    assert_eq!(count.check(true).status, WaitStatus::Ready);
    assert_eq!(departure.check(true).status, WaitStatus::Ready);
}

#[test]
fn replacement_waiters_capture_the_current_tenure_without_python_snapshotting() {
    let shared = Arc::new(Shared::new(
        "127.0.0.1:1".into(),
        "observer".into(),
        1,
        1,
        "churn".into(),
        None,
        None,
        None,
        Vec::new(),
        false,
        0,
    ));
    let first = member(0, Some(0), 1, true, 1);
    let mut cache = CachedPool::default();
    cache.apply(&full(1, vec![first]));
    shared
        .cache
        .write()
        .unwrap()
        .insert("workers".into(), cache);

    let waiter = CacheWaiter::replacement(shared.clone(), "workers".into(), 0, None, true);
    assert_eq!(waiter.check(true).status, WaitStatus::Pending);

    let second = member(0, Some(0), 2, true, 1);
    let update = PoolDelta {
        version: 2,
        roster: second.roster_hash(),
        policy: "stateful".into(),
        methods: vec!["ping".into()],
        size: Some(1),
        changed: vec![second],
        removed: Vec::new(),
        full: false,
    };
    shared
        .cache
        .write()
        .unwrap()
        .get_mut("workers")
        .unwrap()
        .apply(&update);
    let result = waiter.check(false);
    assert_eq!(result.status, WaitStatus::Ready);
    assert_eq!(result.view.unwrap().members.slot(0).unwrap().incarnation, 2);
}
