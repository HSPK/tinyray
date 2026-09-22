use super::cache::*;
use super::heartbeat::*;
use super::*;
use serde_json::json;
use tinyray_proto::PoolDelta;
use tokio::io::AsyncWriteExt;

fn member(id: u64, slot: Option<u64>, ready: bool) -> Member {
    Member {
        id,
        slot,
        incarnation: 1,
        url: None,
        state: json!({"n": 3, "flag": true, "nested": {"values": [3]}}),
        ready,
    }
}

fn member_with_state(id: u64, ready: bool, state: serde_json::Value) -> Member {
    let mut member = member(id, Some(id), ready);
    member.state = state;
    member
}

fn delta(version: u64, full: bool, changed: Vec<Member>, removed: Vec<u64>) -> PoolDelta {
    PoolDelta {
        version,
        roster: changed.iter().fold(0, |h, m| h ^ m.roster_hash()),
        policy: "stateful".into(),
        methods: vec!["ping".into()],
        size: Some(4),
        changed,
        removed,
        full,
    }
}

fn decoded(c: &CachedPool, require_ready: bool) -> Vec<Member> {
    let (raw, fingerprint) = c.serialized(require_ready);
    let members: Vec<Member> = rmp_serde::from_slice(&raw).unwrap();
    assert_eq!(
        fingerprint,
        members.iter().fold(0, |h, m| h ^ m.roster_hash())
    );
    members
}

#[test]
fn slot_index_tracks_wire_ids_readiness_and_duplicate_slots() {
    let mut c = CachedPool::default();
    c.apply(&delta(
        1,
        true,
        vec![
            member(50, Some(2), true),
            member(30, Some(2), false),
            member(2, None, true),
        ],
        vec![],
    ));
    assert_eq!(c.slot(2, false).unwrap().id, 30);
    assert_eq!(c.slot(2, true).unwrap().id, 50);
    assert!(c.slot(30, false).is_none());
    assert_eq!(c.ids(false), &[2, 30, 50]);
    assert_eq!(c.ids(true), &[2, 50]);

    c.apply(&delta(2, false, vec![member(30, Some(3), true)], vec![50]));
    assert!(c.slot(2, false).is_none());
    assert_eq!(c.slot(3, true).unwrap().id, 30);
    assert_eq!(c.ids(true), &[2, 30]);
    c.apply(&delta(3, false, vec![member(30, None, true)], vec![]));
    assert!(c.slots.is_empty());
    c.apply(&delta(4, false, vec![], vec![30, 12345]));
    assert_eq!(c.ids(false), &[2]);
}

#[test]
fn snapshots_digests_and_indices_are_invalidated_together() {
    let mut c = CachedPool::default();
    c.apply(&delta(
        1,
        true,
        vec![member(90, Some(0), true), member(40, Some(1), false)],
        vec![],
    ));
    assert_eq!(
        decoded(&c, false).iter().map(|m| m.id).collect::<Vec<_>>(),
        vec![40, 90]
    );
    assert_eq!(decoded(&c, true).len(), 1);
    let fields = vec!["n".into(), "ready".into(), "url".into()];
    let before = c.field_digest(&fields);
    assert_eq!(before, c.field_digest(&fields));
    let mut changed = member(40, Some(3), true);
    changed.state = json!({"n": 4});
    changed.url = Some("http://new".into());
    changed.incarnation = 2;
    c.apply(&delta(2, false, vec![changed.clone()], vec![]));
    assert!(c.ids.get().is_some());
    assert!(c.ready_ids.get().is_none());
    assert_ne!(before, c.field_digest(&fields));
    assert!(c.slot(1, false).is_none());
    assert_eq!(c.slot(3, true), Some(&changed));
    for ready in [false, true] {
        let members = decoded(&c, ready);
        assert_eq!(members.len(), 2);
        assert_eq!(members[0], changed);
    }
    c.apply(&delta(3, false, vec![], vec![90]));
    assert_eq!(decoded(&c, true), vec![changed]);
    c.apply(&delta(4, true, vec![member(7, Some(2), false)], vec![]));
    assert!(c.slot(3, false).is_none());
    assert_eq!(c.ids(false), &[7]);
    assert!(decoded(&c, true).is_empty());
    assert_eq!(decoded(&c, false).len(), 1);
}

#[test]
fn native_views_share_member_arcs_and_keep_old_publications_frozen() {
    let mut c = CachedPool::default();
    let mut original = member(7, Some(0), true);
    original.state = json!({"large": "x".repeat(32_000), "step": 1});
    c.apply(&delta(1, true, vec![original], vec![]));

    let first = c.native(false);
    let ready = c.native(true);
    assert!(Arc::ptr_eq(&first.members()[0], &c.members[&7]));
    assert!(Arc::ptr_eq(&ready.members()[0], &c.members[&7]));
    assert!(Arc::ptr_eq(&first, &c.native(false)));

    let mut changed = member(7, Some(0), true);
    changed.state = json!({"large": "y".repeat(32_000), "step": 2});
    c.apply(&delta(2, false, vec![changed], vec![]));
    assert!(c.native.lock().unwrap().iter().all(Option::is_none));

    let second = c.native(false);
    assert!(!Arc::ptr_eq(&first.members()[0], &second.members()[0]));
    assert_eq!(first.members()[0].state["step"], 1);
    assert_eq!(second.members()[0].state["step"], 2);
    drop(c);
    assert_eq!(first.members()[0].state["large"], "x".repeat(32_000));
}

#[test]
fn native_views_index_duplicates_identity_readiness_and_counts() {
    let mut c = CachedPool::default();
    let mut low = member(5, Some(2), false);
    low.incarnation = 9;
    let mut high = member(50, Some(2), true);
    high.incarnation = 10;
    let mut free = member(7, None, true);
    free.incarnation = 11;
    c.apply(&delta(
        1,
        true,
        vec![high.clone(), free.clone(), low.clone()],
        vec![],
    ));

    let all = c.native(false);
    assert_eq!(
        all.members()
            .iter()
            .map(|member| member.id)
            .collect::<Vec<_>>(),
        vec![5, 7, 50]
    );
    assert_eq!(all.slot(2), Some(&low));
    assert_eq!(all.get("p", "p/2#9"), Some(&low));
    assert_eq!(all.get("p", "p/2#10"), Some(&high));
    assert_eq!(all.get("p", "p/7#11"), Some(&free));
    assert!(all.get("other", "p/7#11").is_none());
    assert_eq!(c.native(true).slot(2), Some(&high));
    assert_eq!(c.count(&json!({}), false), 3);
    assert_eq!(c.count(&json!({"n": 3.0}), true), 2);
    assert_eq!(c.count(&json!({"flag": 1}), false), 0);
}

#[test]
fn scalar_filter_index_builds_once_and_serves_count_pick_and_views() {
    let mut c = CachedPool::default();
    c.apply(&delta(
        1,
        true,
        vec![
            member_with_state(8, true, json!({"shard": 3, "zone": "a"})),
            member_with_state(2, false, json!({"shard": 3, "zone": "a"})),
            member_with_state(5, true, json!({"shard": 4, "zone": "b"})),
        ],
        vec![],
    ));
    let filter = json!({"shard": 3});
    assert_eq!(c.count(&filter, true), 1);
    let built = c.filter_index_stats();
    assert_eq!(built["entries"], 1);
    assert_eq!(built["builds"], 1);
    assert_eq!(built["hits"], 0);

    assert_eq!(c.count(&filter, true), 1);
    let mut rng = fastrand::Rng::with_seed(7);
    assert_eq!(c.choose_arc(&filter, true, &mut rng).unwrap().id, 8);
    assert_eq!(
        c.filtered(&filter, true)
            .members
            .members()
            .iter()
            .map(|member| member.id)
            .collect::<Vec<_>>(),
        vec![8]
    );
    let reused = c.filter_index_stats();
    assert_eq!(reused["builds"], 1);
    assert_eq!(reused["hits"], 3);

    assert_eq!(c.count(&filter, false), 2);
    let both_readiness_modes = c.filter_index_stats();
    assert_eq!(both_readiness_modes["entries"], 2);
    assert_eq!(both_readiness_modes["builds"], 2);
}

#[test]
fn scalar_filter_index_invalidates_updates_removals_and_full_resyncs() {
    let mut c = CachedPool::default();
    c.apply(&delta(
        1,
        true,
        vec![
            member_with_state(1, true, json!({"tag": "old"})),
            member_with_state(2, true, json!({"tag": "stay"})),
        ],
        vec![],
    ));
    assert_eq!(c.count(&json!({"tag": "old"}), true), 1);
    assert_eq!(c.filter_index_stats()["entries"], 1);

    c.apply(&delta(
        2,
        false,
        vec![member_with_state(1, true, json!({"tag": "new"}))],
        vec![],
    ));
    assert_eq!(c.filter_index_stats()["entries"], 0);
    assert_eq!(c.count(&json!({"tag": "old"}), true), 0);
    assert_eq!(c.count(&json!({"tag": "new"}), true), 1);

    c.apply(&delta(3, false, vec![], vec![1]));
    assert_eq!(c.filter_index_stats()["entries"], 0);
    assert_eq!(c.count(&json!({"tag": "new"}), true), 0);

    c.apply(&delta(
        4,
        true,
        vec![member_with_state(9, false, json!({"tag": "resynced"}))],
        vec![],
    ));
    assert_eq!(c.filter_index_stats()["entries"], 0);
    assert_eq!(c.count(&json!({"tag": "stay"}), false), 0);
    assert_eq!(c.count(&json!({"tag": "resynced"}), false), 1);
    assert_eq!(c.count(&json!({"tag": "resynced"}), true), 0);
}

#[test]
fn scalar_filter_index_preserves_numeric_boolean_and_missing_semantics() {
    let mut c = CachedPool::default();
    c.apply(&delta(
        1,
        true,
        vec![
            member_with_state(1, true, json!({"value": true})),
            member_with_state(2, true, json!({"value": 1})),
            member_with_state(3, true, json!({"value": 3})),
            member_with_state(4, true, json!({"value": 3.0})),
            member_with_state(5, true, json!({"value": 9_223_372_036_854_775_808u64})),
            member_with_state(6, true, json!({"value": null})),
            member_with_state(7, true, json!({"other": null})),
            member_with_state(8, true, json!({"value": u64::MAX})),
            member_with_state(9, true, json!({"value": i64::MIN})),
        ],
        vec![],
    ));
    assert_eq!(c.debug_scan_ids(&json!({"value": true}), true), vec![1]);
    assert_eq!(c.count(&json!({"value": true}), true), 1);
    assert_eq!(
        c.filtered(&json!({"value": true}), true).members.members()[0].id,
        1
    );
    assert_eq!(c.count(&json!({"value": 1}), true), 1);
    assert_eq!(
        c.filtered(&json!({"value": 1}), true).members.members()[0].id,
        2
    );
    assert_eq!(c.count(&json!({"value": 1.0}), true), 1);
    assert_eq!(c.count(&json!({"value": 3}), true), 2);
    assert_eq!(c.count(&json!({"value": 3.0}), true), 2);
    assert_eq!(
        c.count(&json!({"value": 9_223_372_036_854_775_808u64}), true),
        1
    );
    assert_eq!(
        c.count(&json!({"value": 9_223_372_036_854_775_808.0f64}), true),
        1
    );
    assert_eq!(c.count(&json!({"value": u64::MAX}), true), 1);
    assert_eq!(c.count(&json!({"value": u64::MAX as f64}), true), 0);
    assert_eq!(c.count(&json!({"value": i64::MIN}), true), 1);
    assert_eq!(c.count(&json!({"value": i64::MIN as f64}), true), 1);
    assert_eq!(c.count(&json!({"value": null}), true), 1);
    assert_eq!(c.count(&json!({"missing": null}), true), 0);
}

#[test]
fn unsupported_filters_fall_back_without_changing_results() {
    let mut c = CachedPool::default();
    c.apply(&delta(
        1,
        true,
        vec![
            member_with_state(
                1,
                true,
                json!({"nested": {"values": [3]}, "a": 1, "b": 2, "c": 3, "d": 4,
                        "e": 5, "f": 6, "g": 7, "h": 8, "i": 9}),
            ),
            member_with_state(2, true, json!({"nested": {"values": [4]}})),
        ],
        vec![],
    ));
    let nested = json!({"nested": {"values": [3.0]}});
    assert_eq!(c.count(&nested, true), 1);
    assert_eq!(c.debug_scan_ids(&nested, true), vec![1]);

    let too_many_fields =
        json!({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9});
    assert_eq!(c.count(&too_many_fields, true), 1);
    let oversized = json!({"huge": "x".repeat(FILTER_INDEX_MAX_KEY_BYTES + 1)});
    assert_eq!(c.count(&oversized, true), 0);
    let stats = c.filter_index_stats();
    assert_eq!(stats["entries"], 0);
    assert_eq!(stats["fallbacks"], 3);
}

#[test]
fn filter_index_lru_and_cardinality_limits_are_hard_bounds() {
    let mut c = CachedPool::default();
    c.apply(&delta(
        1,
        true,
        (0..100)
            .map(|id| member_with_state(id, true, json!({"value": id})))
            .collect(),
        vec![],
    ));
    for value in 0..(FILTER_INDEX_MAX_ENTRIES as u64 + 12) {
        assert_eq!(c.count(&json!({"value": value}), true), 1);
    }
    let bounded = c.filter_index_stats();
    assert_eq!(bounded["entries"], FILTER_INDEX_MAX_ENTRIES as u64);
    assert_eq!(bounded["evictions"], 12);
    assert!(bounded["bytes"] <= bounded["max_bytes"]);

    let mut large = CachedPool::default();
    large.apply(&delta(
        1,
        true,
        (0..=FILTER_INDEX_MAX_IDS_PER_ENTRY as u64)
            .map(|id| member_with_state(id, true, json!({"group": "all"})))
            .collect(),
        vec![],
    ));
    assert_eq!(
        large.count(&json!({"group": "all"}), true),
        FILTER_INDEX_MAX_IDS_PER_ENTRY + 1
    );
    let uncached = large.filter_index_stats();
    assert_eq!(uncached["entries"], 0);
    assert_eq!(uncached["uncached"], 1);
    assert!(uncached["bytes"] <= uncached["max_bytes"]);
}

#[test]
fn metadata_only_versions_reuse_native_member_indexes() {
    let mut c = CachedPool::default();
    c.apply(&delta(1, true, vec![member(1, Some(0), true)], vec![]));
    let first = c.frozen(true);
    assert_eq!(c.count(&json!({"n": 3}), true), 1);
    assert_eq!(c.filter_index_stats()["entries"], 1);
    c.apply(&delta(2, false, vec![], vec![]));
    let second = c.frozen(true);
    assert!(Arc::ptr_eq(&first.members, &second.members));
    assert_eq!(c.filter_index_stats()["entries"], 1);
    assert_eq!(c.count(&json!({"n": 3}), true), 1);
    assert_eq!(c.filter_index_stats()["hits"], 1);
    assert_eq!(first.version, 1);
    assert_eq!(second.version, 2);
}

#[test]
fn native_wait_handoff_cannot_miss_a_change_before_subscription() {
    let shared = Arc::new(shared());
    let mut cached = CachedPool::default();
    cached.apply(&delta(1, true, vec![member(1, Some(0), false)], vec![]));
    shared.cache.write().unwrap().insert("p".into(), cached);
    let waiter = Arc::new(CacheWaiter::count(shared.clone(), "p".into(), json!({}), 1));
    let (started, begin) = std::sync::mpsc::channel();
    let waiting = waiter.clone();
    let thread = std::thread::spawn(move || {
        started.send(()).unwrap();
        waiting.wait(Some(Duration::from_secs(1)), true)
    });
    begin.recv().unwrap();
    shared
        .cache
        .write()
        .unwrap()
        .get_mut("p")
        .unwrap()
        .apply(&delta(2, false, vec![member(1, Some(0), true)], vec![]));
    shared.ring();
    let result = thread.join().unwrap();
    assert_eq!(result.status, WaitStatus::Ready);
    assert_eq!(result.view.unwrap().members.len(), 1);
}

#[test]
fn epoch_waits_for_the_ready_view_fingerprint_not_only_the_count() {
    let shared = Arc::new(shared());
    let mut cached = CachedPool::default();
    cached.apply(&delta(
        1,
        true,
        vec![member(1, Some(0), true), member(2, Some(1), false)],
        vec![],
    ));
    shared.cache.write().unwrap().insert("p".into(), cached);
    let blocked = shared.wait_epoch("p", Some(1), Duration::ZERO);
    assert_eq!(blocked.status, WaitStatus::Mismatch);
    assert_eq!(blocked.found, 1);
    assert!(blocked.mismatched);

    let second = member(2, Some(1), true);
    let mut update = delta(2, false, vec![second.clone()], vec![]);
    update.roster = member(1, Some(0), true).roster_hash() ^ second.roster_hash();
    shared
        .cache
        .write()
        .unwrap()
        .get_mut("p")
        .unwrap()
        .apply(&update);
    let ready = shared.wait_epoch("p", Some(1), Duration::ZERO);
    assert_eq!(ready.status, WaitStatus::Ready);
    assert_eq!(ready.view.unwrap().members.len(), 2);
}

#[test]
fn no_change_beats_reuse_derived_data_but_full_resyncs_do_not() {
    let mut c = CachedPool::default();
    c.apply(&delta(1, true, vec![member(90, Some(0), true)], vec![]));
    let original = decoded(&c, true);
    let fields = vec!["ready".into()];
    c.field_digest(&fields);
    c.apply(&delta(2, false, vec![], vec![]));
    assert!(c.ids.get().is_some());
    assert!(c.ready_ids.get().is_some());
    assert!(c.snapshots.lock().unwrap()[1].is_some());
    assert!(c.digest.lock().unwrap().is_some());
    assert_eq!(c.version, 2);
    assert_eq!(decoded(&c, true), original);

    let mut updated = original[0].clone();
    updated.state = json!({"n": 42});
    c.apply(&delta(3, false, vec![updated], vec![]));
    assert!(c.ids.get().is_some());
    assert!(c.ready_ids.get().is_some());
    assert!(c.snapshots.lock().unwrap().iter().all(Option::is_none));
    assert!(c.digest.lock().unwrap().is_none());
    assert_eq!(decoded(&c, true)[0].state, json!({"n": 42}));

    c.apply(&delta(3, true, vec![], vec![]));
    assert!(c.ids.get().is_none());
    assert!(c.ready_ids.get().is_none());
    assert!(c.snapshots.lock().unwrap().iter().all(Option::is_none));
    assert!(c.digest.lock().unwrap().is_none());
    assert!(c.slot(0, false).is_none());
    assert!(decoded(&c, false).is_empty());
}

#[test]
fn derived_cache_sizes_are_bounded() {
    let mut c = CachedPool::default();
    let mut large = member(1, Some(0), true);
    large.state = json!({"large": "x".repeat(SNAPSHOT_BYTES)});
    c.apply(&delta(1, true, vec![large.clone()], vec![]));
    for ready in [false, true] {
        assert_eq!(decoded(&c, ready), vec![large.clone()]);
    }
    assert!(c.snapshots.lock().unwrap().iter().all(Option::is_none));
    c.field_digest(&vec!["x".into(); 65]);
    assert!(c.digest.lock().unwrap().is_none());
    c.field_digest(&["x".repeat(4097)]);
    assert!(c.digest.lock().unwrap().is_none());
    c.field_digest(&["large".into()]);
    for i in 0..100 {
        let field = format!("key{i}");
        c.field_digest(std::slice::from_ref(&field));
        assert_eq!(c.digest.lock().unwrap().as_ref().unwrap().0, vec![field]);
    }
}

#[test]
fn serialized_state_batches_are_cached_and_invalidated() {
    let mut c = CachedPool::default();
    c.apply(&delta(1, true, vec![member(1, Some(0), true)], vec![]));
    let frozen = c.native(false);
    let first = frozen.serialized_states();
    assert!(Arc::ptr_eq(&first, &frozen.serialized_states()));

    let changed = member_with_state(1, true, json!({"n": 4}));
    c.apply(&delta(2, false, vec![changed], vec![]));
    let updated = c.native(false).serialized_states();
    assert!(!Arc::ptr_eq(&first, &updated));
    assert_eq!(
        rmp_serde::from_slice::<Vec<serde_json::Value>>(&updated).unwrap(),
        vec![json!({"n": 4})]
    );

    let mut large = member(2, Some(0), true);
    large.state = json!({"large": "x".repeat(SNAPSHOT_BYTES)});
    c.apply(&delta(3, true, vec![large], vec![]));
    let frozen = c.native(false);
    let first = frozen.serialized_states();
    assert!(!Arc::ptr_eq(&first, &frozen.serialized_states()));
}

#[test]
fn native_selection_is_uniform_and_filters_only_eligible_members() {
    let mut rng = fastrand::Rng::with_seed(0x715E1EC7);
    let mut c = CachedPool::default();
    c.apply(&delta(
        1,
        true,
        (0..8).map(|id| member(id, Some(id + 10), id < 7)).collect(),
        vec![],
    ));
    for filter in [
        json!({}),
        json!({"n": 3.0}),
        json!({"nested": {"values": [3.0]}}),
    ] {
        let mut counts = [0; 7];
        for _ in 0..35_000 {
            let m = c.choose(&filter, true, &mut rng).unwrap();
            assert!(m.ready);
            counts[m.id as usize] += 1;
        }
        for count in counts {
            assert!((4500..5500).contains(&count), "{counts:?}");
        }
    }
    assert!(c.choose(&json!({"flag": 1}), false, &mut rng).is_none());
    assert!(c
        .choose(&json!({"missing": null}), false, &mut rng)
        .is_none());
    assert!(c
        .choose(&json!({"nested": {"values": [true]}}), false, &mut rng)
        .is_none());
    c.apply(&delta(2, true, vec![member(99, Some(0), false)], vec![]));
    assert!(c.choose(&json!({}), true, &mut rng).is_none());
    assert_eq!(
        c.choose(&json!({"n": 3.0}), false, &mut rng).unwrap().id,
        99
    );
    c.apply(&delta(3, true, vec![], vec![]));
    assert!(c.choose(&json!({}), false, &mut rng).is_none());
}

#[test]
fn restart_and_refusal_do_not_leak_stale_indices_or_snapshots() {
    let s = shared();
    let ack = |epoch, accepted, d| BeatAck {
        epoch,
        protocol: tinyray_proto::PROTOCOL,
        version: String::new(),
        ttl_ms: 2000,
        accepted,
        refused: None,
        pools: HashMap::from([("p".into(), d)]),
    };
    assert!(
        s.apply(&ack(
            1,
            true,
            delta(5, true, vec![member(42, Some(1), true)], vec![])
        ))
        .0
    );
    {
        let cache = s.cache.read().unwrap();
        assert_eq!(cache["p"].slot(1, true).unwrap().id, 42);
        decoded(&cache["p"], true);
        assert_eq!(cache["p"].count(&json!({"n": 3}), true), 1);
        assert_eq!(cache["p"].filter_index_stats()["entries"], 1);
    }
    assert!(
        s.apply(&ack(
            1,
            true,
            delta(4, true, vec![member(41, Some(2), true)], vec![])
        ))
        .0
    );
    {
        let cache = s.cache.read().unwrap();
        assert!(cache["p"].slot(2, false).is_none());
        assert_eq!(decoded(&cache["p"], true)[0].id, 42);
    }
    assert!(s.apply(&ack(2, true, delta(1, false, vec![], vec![]))).0);
    assert!(!s.cache.read().unwrap().contains_key("p"));
    assert!(
        s.apply(&ack(
            2,
            true,
            delta(1, true, vec![member(17, Some(3), true)], vec![])
        ))
        .0
    );
    assert!(!s.apply(&ack(2, false, delta(2, true, vec![], vec![]))).0);
    assert!(!s.accepted.load(Ordering::Relaxed));
    {
        let cache = s.cache.read().unwrap();
        assert!(cache["p"].slot(1, false).is_none());
        assert_eq!(decoded(&cache["p"], true)[0].id, 17);
        assert_eq!(cache["p"].filter_index_stats()["entries"], 0);
        assert_eq!(cache["p"].count(&json!({"n": 3}), true), 1);
    }
}

#[test]
fn coalescing_is_bounded_by_the_renewal_budget() {
    for ttl in [200, 201, 500, 2000, 20_000, 120_000] {
        let interval = ttl / 4;
        let hold = interval.clamp(50, 30_000);
        assert_eq!(coalesce_gap(50, interval), Duration::from_millis(50));
        assert_eq!(coalesce_gap(0, interval), Duration::ZERO);
        let gap = coalesce_gap(u64::MAX, interval);
        assert!(gap <= Duration::from_millis(interval));
        assert!(gap + beat_timeout(interval, hold) < Duration::from_millis(ttl));
    }
    assert_eq!(coalesce_gap(7, 500), Duration::from_millis(7));
    assert_eq!(coalesce_gap(u64::MAX, 0), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn coalescing_latency_and_wakeup_order_are_deterministic() {
    let mut s = shared();
    for requested in [0, 7, 50, 10_000] {
        s.coalesce_ms = requested;
        s.interval_ms.store(50, Ordering::Relaxed);
        for interruptible in [false, true] {
            let started = tokio::time::Instant::now();
            coalesce(&s, started, interruptible).await;
            assert_eq!(started.elapsed(), coalesce_gap(requested, 50));
        }
    }
    s.coalesce_ms = 50;
    let started = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(30)).await;
    coalesce(&s, started, false).await;
    assert_eq!(started.elapsed(), Duration::from_millis(50));

    s.wake.notify_one();
    let started = tokio::time::Instant::now();
    coalesce(&s, started, false).await;
    assert_eq!(started.elapsed(), Duration::from_millis(50));
    let started = tokio::time::Instant::now();
    coalesce(&s, started, true).await;
    assert_eq!(started.elapsed(), Duration::ZERO);

    s.coalesce_ms = 0;
    s.wake.notify_one();
    coalesce(&s, tokio::time::Instant::now(), true).await;
    s.coalesce_ms = 50;
    let started = tokio::time::Instant::now();
    coalesce(&s, started, true).await;
    assert_eq!(started.elapsed(), Duration::ZERO);
    coalesce(&s, started, true).await;
    assert_eq!(started.elapsed(), Duration::from_millis(50));
}

/// The deadline for a beat that is not being parked has to follow the
/// interval, not sit at a constant.
///
/// Nothing in the Python suite notices if it does: everything there talks
/// to a registry on loopback, and 200ms is plenty for that. On a real link
/// with a 4s lease the interval is 1s and the budget should be 750ms; a
/// flat 200ms turns an ordinary slow answer into a failed beat, and enough
/// failed beats cost the seat. That is the whole point of making the
/// deadline proportional, and it needs asserting where it can be seen.
///
/// `hold_ms == 0` is reached twice over: the first beat, before any ack
/// has said what the lease is, and every beat right after a watch
/// cancelled the held one.
#[test]
fn an_unparked_beat_follows_the_interval() {
    assert_eq!(beat_timeout(1000, 0).as_millis(), 750);
    assert_eq!(beat_timeout(500, 0).as_millis(), 375);
    assert_eq!(beat_timeout(200, 0).as_millis(), 150);
    // Strictly increasing, which a constant would not be.
    assert!(beat_timeout(1000, 0) > beat_timeout(500, 0));
    assert!(beat_timeout(500, 0) > beat_timeout(200, 0));
    // Clamped at both ends so a nonsense interval cannot make the deadline
    // nonsense too.
    assert_eq!(beat_timeout(1, 0).as_millis(), 37);
    assert_eq!(beat_timeout(1_000_000, 0).as_millis(), 22_500);
}

/// Network slack must not make the deadline outlive a short lease.
#[test]
fn a_parked_beat_keeps_network_slack_inside_half_the_lease() {
    assert_eq!(beat_timeout(50, 50).as_millis(), 100);
    assert_eq!(beat_timeout(125, 125).as_millis(), 249);
    assert_eq!(beat_timeout(500, 500).as_millis(), 950);
    assert_eq!(beat_timeout(500, 2000).as_millis(), 3200);
    for ttl_ms in [200u64, 201, 250, 500, 999, 1_000, 2_000, 8_000, 30_000] {
        let hold = (ttl_ms / 4).clamp(50, 30_000);
        let budget = beat_timeout(ttl_ms / 4, hold).as_millis() as u64;
        assert!(
            budget <= ttl_ms / 2,
            "budget {budget} leaves too little of the {ttl_ms}ms lease to retry"
        );
        assert!(budget > hold + hold / 8, "allow the registry's jitter");
    }
}

#[tokio::test]
async fn registry_beat_connections_disable_nagle() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connecting = TcpStream::connect(address);
    let accepting = listener.accept();
    let (client, accepted) = tokio::join!(connecting, accepting);
    let client = client.unwrap();
    let _accepted = accepted.unwrap();
    assert!(!client.nodelay().unwrap());
    configure_registry_stream(&client).unwrap();
    assert!(client.nodelay().unwrap());
}

fn successful_ack(epoch: u64) -> BeatAck {
    BeatAck {
        epoch,
        protocol: tinyray_proto::PROTOCOL,
        version: env!("CARGO_PKG_VERSION").into(),
        ttl_ms: 2000,
        accepted: true,
        refused: None,
        pools: HashMap::new(),
    }
}

async fn answer_one(stream: &mut TcpStream, epoch: u64, request_offset: u64) {
    let raw = tinyray_proto::wire::read_frame(stream, MAX_REQUEST_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    let header: RegistryEnvelopeHeader = decode_message(&raw).unwrap();
    let reply = RegistryEnvelope::new(
        header.request_id + request_offset,
        OP_BEAT_ACK,
        successful_ack(epoch),
    );
    write_frame(stream, &reply, MAX_RESPONSE_FRAME_BYTES)
        .await
        .unwrap();
}

#[tokio::test]
async fn complete_beats_reuse_one_tracked_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        answer_one(&mut stream, 1, 0).await;
        answer_one(&mut stream, 1, 0).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let shared = Arc::new(Shared::new(
        endpoint,
        "p".into(),
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
    let (beat, _) = shared.compose();
    let (_, connection) = post(shared.clone(), &beat, Duration::from_secs(1), None)
        .await
        .unwrap();
    let (_, connection) = post(
        shared.clone(),
        &beat,
        Duration::from_secs(1),
        Some(connection),
    )
    .await
    .unwrap();
    assert_eq!(shared.registry_connects.load(Ordering::Relaxed), 1);
    assert_eq!(shared.registry_reuses.load(Ordering::Relaxed), 1);
    assert_eq!(shared.registry_fds.snapshot().len(), 1);
    drop(connection);
    assert!(shared.registry_fds.snapshot().is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn cancelled_and_mismatched_replies_cannot_reach_the_next_beat() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let (seen, request_read) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut cancelled, _) = listener.accept().await.unwrap();
        let raw = tinyray_proto::wire::read_frame(&mut cancelled, MAX_REQUEST_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        let header: RegistryEnvelopeHeader = decode_message(&raw).unwrap();
        seen.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let stale = RegistryEnvelope::new(header.request_id, OP_BEAT_ACK, successful_ack(99));
        let _ = write_frame(&mut cancelled, &stale, MAX_RESPONSE_FRAME_BYTES).await;

        let (mut mismatched, _) = listener.accept().await.unwrap();
        answer_one(&mut mismatched, 1, 1).await;

        let (mut fresh, _) = listener.accept().await.unwrap();
        answer_one(&mut fresh, 2, 0).await;
    });
    let shared = Arc::new(Shared::new(
        endpoint,
        "p".into(),
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
    let (beat, _) = shared.compose();

    {
        let sending = post(shared.clone(), &beat, Duration::from_secs(1), None);
        tokio::pin!(sending);
        tokio::select! {
            _ = &mut sending => panic!("held request completed unexpectedly"),
            _ = request_read => {}
        }
    }

    let mismatch = post(shared.clone(), &beat, Duration::from_secs(1), None).await;
    let Err(error) = mismatch else {
        panic!("a mismatched response was accepted");
    };
    assert!(error.contains("replied to request"));
    let (ack, connection) = post(shared.clone(), &beat, Duration::from_secs(1), None)
        .await
        .unwrap();
    assert_eq!(ack.epoch, 2);
    assert_eq!(shared.registry_connects.load(Ordering::Relaxed), 3);
    drop(connection);
    server.await.unwrap();
}

#[tokio::test]
async fn timed_out_reply_on_a_reused_stream_reconnects_before_retry() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (mut timed_out, _) = listener.accept().await.unwrap();
        answer_one(&mut timed_out, 1, 0).await;
        let _ = tinyray_proto::wire::read_frame(&mut timed_out, MAX_REQUEST_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (mut fresh, _) = listener.accept().await.unwrap();
        answer_one(&mut fresh, 3, 0).await;
    });
    let shared = Arc::new(Shared::new(
        endpoint,
        "p".into(),
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
    let (beat, _) = shared.compose();
    let (_, connection) = post(shared.clone(), &beat, Duration::from_secs(1), None)
        .await
        .unwrap();
    let timed_out = post(
        shared.clone(),
        &beat,
        Duration::from_millis(20),
        Some(connection),
    )
    .await;
    let Err(error) = timed_out else {
        panic!("a reply timeout was accepted");
    };
    assert!(error.contains("no reply within 20ms"));
    let (ack, connection) = post(shared.clone(), &beat, Duration::from_secs(1), None)
        .await
        .unwrap();
    assert_eq!(ack.epoch, 3);
    assert_eq!(shared.registry_connects.load(Ordering::Relaxed), 2);
    assert_eq!(shared.registry_reuses.load(Ordering::Relaxed), 1);
    drop(connection);
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_reply_on_a_reused_stream_reconnects_before_retry() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (mut malformed, _) = listener.accept().await.unwrap();
        answer_one(&mut malformed, 1, 0).await;
        let _ = tinyray_proto::wire::read_frame(&mut malformed, MAX_REQUEST_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        tinyray_proto::wire::write_frame_bytes(&mut malformed, &[0xc1], MAX_RESPONSE_FRAME_BYTES)
            .await
            .unwrap();

        let (mut fresh, _) = listener.accept().await.unwrap();
        answer_one(&mut fresh, 4, 0).await;
    });
    let shared = Arc::new(Shared::new(
        endpoint,
        "p".into(),
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
    let (beat, _) = shared.compose();
    let (_, connection) = post(shared.clone(), &beat, Duration::from_secs(1), None)
        .await
        .unwrap();
    let malformed = post(
        shared.clone(),
        &beat,
        Duration::from_secs(1),
        Some(connection),
    )
    .await;
    let Err(error) = malformed else {
        panic!("a malformed reply was accepted");
    };
    assert!(error.contains("reply is not a registry envelope"));

    let (ack, connection) = post(shared.clone(), &beat, Duration::from_secs(1), None)
        .await
        .unwrap();
    assert_eq!(ack.epoch, 4);
    assert_eq!(shared.registry_connects.load(Ordering::Relaxed), 2);
    assert_eq!(shared.registry_reuses.load(Ordering::Relaxed), 1);
    drop(connection);
    server.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn registry_reply_body_keeps_the_exchange_deadline() {
    let (mut writer, mut reader) = tokio::io::duplex(8);
    let started = tokio::time::Instant::now();
    let budget = Duration::from_secs(1);
    let deadline = started + budget;
    tokio::time::sleep(Duration::from_millis(600)).await;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(600)).await;
        writer.write_all(&[1, 2]).await.unwrap();
    });
    let error = read_reply_body(&mut reader, 2, deadline, budget)
        .await
        .unwrap_err();
    assert!(error.contains("stalled"));
    assert_eq!(started.elapsed(), budget);
}

fn shared() -> Shared {
    Shared::new(
        "127.0.0.1:1".into(),
        "p".into(),
        1,
        1,
        "churn".into(),
        None,
        None,
        None,
        Vec::new(),
        false,
        50,
    )
}

#[test]
fn a_composed_beat_carries_the_version_of_its_whole_publication() {
    let s = shared();
    let (initial, version) = s.compose();
    assert_eq!(initial.publication, Some(version));
    assert_eq!(version, 0);

    *s.published.lock().unwrap() = Published {
        state: json!({"value": "new"}),
        ready: true,
        url: Some("http://new".into()),
        version: 2,
    };
    let (beat, version) = s.compose();
    assert_eq!(version, 2);
    assert_eq!(beat.publication, Some(2));
    assert_eq!(beat.url.as_deref(), Some("http://new"));
    assert_eq!(beat.state, json!({"value": "new"}));
    assert!(beat.ready);
}

#[test]
fn a_late_ack_does_not_roll_back_a_newer_cached_publication() {
    let s = shared();
    let ack = |epoch, version, state| BeatAck {
        epoch,
        protocol: tinyray_proto::PROTOCOL,
        version: String::new(),
        ttl_ms: 2000,
        accepted: true,
        refused: None,
        pools: HashMap::from([(
            "p".into(),
            PoolDelta {
                version,
                roster: 1,
                policy: "churn".into(),
                methods: Vec::new(),
                size: None,
                changed: vec![Member {
                    id: 1,
                    slot: None,
                    incarnation: 1,
                    url: None,
                    state,
                    ready: true,
                }],
                removed: Vec::new(),
                full: true,
            },
        )]),
    };
    assert!(s.apply(&ack(1, 2, json!("new"))).0);
    assert!(s.apply(&ack(1, 1, json!("old"))).0);
    {
        let cache = s.cache.read().unwrap();
        assert_eq!(cache["p"].version, 2);
        assert_eq!(cache["p"].members[&1].state, json!("new"));
    }
    assert!(s.apply(&ack(2, 1, json!("restarted"))).0);
    let cache = s.cache.read().unwrap();
    assert_eq!(cache["p"].version, 1);
    assert_eq!(cache["p"].members[&1].state, json!("restarted"));
}
