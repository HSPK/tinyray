//! The registry's state machine, exercised directly.
//!
//! Everything in `state.rs` is synchronous and free of I/O, but until now it
//! was only ever reached through a socket from Python. That works, and it is
//! how the real behaviour is confirmed, but it means a break in seat handling
//! surfaces minutes later as a hung roll call in a subprocess whose output has
//! already been captured. These tests fail on the line that broke.
//!
//! `admissible`, `disagreement` and `Pool::delta` are private, and stay that
//! way: each is fully observable from the public surface -- respectively as a
//! refusal carrying no reason, a refusal carrying one, and the deltas that come
//! back on a watching beat -- so testing through it keeps these tests honest
//! about what a client can actually tell.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Value};
use tinyray_proto::{Beat, BeatAck, PoolDelta, MAX_WATCH};
use tinyray_registry::state::{Registry, MAX_STATE};

fn registry() -> Registry {
    // Long enough that nothing expires unless a test asks it to.
    Registry::new(Duration::from_secs(30))
}

fn beat(pool: &str, id: u64) -> Beat {
    Beat {
        pool: pool.into(),
        slot: None,
        id,
        incarnation: 1,
        policy: "serving".into(),
        size: None,
        url: None,
        state: Value::Null,
        ready: false,
        leaving: false,
        exclusive: false,
        methods: Vec::new(),
        watch: Vec::new(),
        seen: HashMap::new(),
        hold_ms: 0,
    }
}

/// A beat that only subscribes: it joins a pool of its own so it never
/// perturbs the pool under test.
fn watcher(pool: &str, seen: Option<u64>) -> Beat {
    let mut b = beat("watchers", 999);
    b.watch = vec![pool.into()];
    if let Some(v) = seen {
        b.seen.insert(pool.into(), v);
    }
    b
}

/// (version, roster, member count) for one pool.
fn pool_of(reg: &Registry, name: &str) -> (u64, u64, usize) {
    *reg.snapshot()
        .get(name)
        .unwrap_or_else(|| panic!("pool {name:?} is not in the snapshot"))
}

fn version(reg: &Registry, name: &str) -> u64 {
    pool_of(reg, name).0
}

fn roster(reg: &Registry, name: &str) -> u64 {
    pool_of(reg, name).1
}

fn delta_for<'a>(ack: &'a BeatAck, pool: &str) -> &'a PoolDelta {
    ack.pools
        .get(pool)
        .unwrap_or_else(|| panic!("expected a delta for {pool:?}, got {:?}", ack.pools.keys()))
}

// --- the shape of a pool ----------------------------------------------------

#[test]
fn the_first_member_sets_the_shape_and_is_accepted() {
    let reg = registry();
    let mut b = beat("trainer", 0);
    b.slot = Some(0);
    b.policy = "collective".into();
    b.size = Some(8);

    let ack = reg.beat(&b);
    assert!(ack.accepted);
    assert_eq!(ack.refused, None);
    assert_eq!(pool_of(&reg, "trainer"), (1, b_hash(0, 1), 1));
}

/// The registry's own fingerprint arithmetic, restated independently so a
/// change to `roster_hash` has to be deliberate on both sides.
fn b_hash(id: u64, incarnation: u64) -> u64 {
    let mut h = 1469598103934665603u64;
    for byte in id
        .to_le_bytes()
        .iter()
        .chain(incarnation.to_le_bytes().iter())
    {
        h ^= *byte as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

#[test]
fn an_emptied_pool_takes_its_shape_from_whoever_arrives_next() {
    // A pool that pinned its first size forever meant a job relaunched at a
    // different world size silently kept the old one, and the new ranks waited
    // out their timeout against a roll call that could never complete.
    let reg = registry();
    let mut first = beat("trainer", 0);
    first.slot = Some(0);
    first.policy = "collective".into();
    first.size = Some(8);
    assert!(reg.beat(&first).accepted);

    let mut leaving = first.clone();
    leaving.leaving = true;
    assert!(reg.beat(&leaving).accepted);
    assert_eq!(pool_of(&reg, "trainer").2, 0, "the pool is empty");

    let mut relaunch = beat("trainer", 0);
    relaunch.slot = Some(0);
    relaunch.policy = "collective".into();
    relaunch.size = Some(4);
    relaunch.incarnation = 2;
    let ack = reg.beat(&relaunch);
    assert!(ack.accepted, "refused: {:?}", ack.refused);
}

#[test]
fn a_member_that_disagrees_about_the_policy_is_told_what_it_disagreed_about() {
    let reg = registry();
    assert!(reg.beat(&beat("engine", 1)).accepted);

    let mut other = beat("engine", 2);
    other.policy = "collective".into();
    let ack = reg.beat(&other);

    assert!(!ack.accepted);
    let why = ack.refused.expect("a shape refusal must carry its reason");
    assert!(
        why.contains("serving") && why.contains("collective"),
        "{why}"
    );
    assert_eq!(
        pool_of(&reg, "engine").2,
        1,
        "the refused member is not stored"
    );
}

#[test]
fn a_member_that_disagrees_about_the_size_is_told_both_numbers() {
    let reg = registry();
    let mut first = beat("trainer", 0);
    first.slot = Some(0);
    first.policy = "collective".into();
    first.size = Some(8);
    assert!(reg.beat(&first).accepted);

    let mut wrong = beat("trainer", 1);
    wrong.slot = Some(1);
    wrong.policy = "collective".into();
    wrong.size = Some(4);
    let why = reg
        .beat(&wrong)
        .refused
        .expect("must state the disagreement");
    assert!(why.contains('8') && why.contains('4'), "{why}");
}

#[test]
fn asking_for_a_size_in_a_pool_that_has_none_says_so_in_words() {
    // "was opened with 0" would be a lie and would send whoever reads it
    // looking for a zero-sized pool.
    let reg = registry();
    assert!(reg.beat(&beat("engine", 1)).accepted);

    let mut sized = beat("engine", 2);
    sized.size = Some(4);
    let why = reg
        .beat(&sized)
        .refused
        .expect("must state the disagreement");
    assert!(why.contains("no size"), "{why}");
}

#[test]
fn differing_methods_are_not_a_disagreement_and_the_pool_learns_them() {
    // An empty method list means "I do not serve", not "I disagree". Mixed
    // pools where only some members serve are normal, and the first member
    // through should not decide that the pool serves nothing.
    let reg = registry();
    assert!(reg.beat(&beat("collector", 1)).accepted);

    let mut serving = beat("collector", 2);
    serving.methods = vec!["assign".into()];
    let ack = reg.beat(&serving);
    assert!(ack.accepted, "refused: {:?}", ack.refused);

    let seen = reg.beat(&watcher("collector", None));
    assert_eq!(delta_for(&seen, "collector").methods, vec!["assign"]);
}

// --- admission --------------------------------------------------------------

#[test]
fn an_inadmissible_beat_is_refused_without_a_reason_and_stores_nothing() {
    // Admission failures and shape refusals both arrive as accepted=false; the
    // absence of a reason is what separates "you sent something damaging" from
    // "you disagree with the pool".
    let reg = registry();
    let mut fat = beat("engine", 1);
    fat.state = json!({"blob": "x".repeat(MAX_STATE + 1)});

    let ack = reg.beat(&fat);
    assert!(!ack.accepted);
    assert_eq!(ack.refused, None, "an admission failure states no reason");
    assert!(ack.pools.is_empty());
    assert!(
        reg.snapshot().is_empty(),
        "an inadmissible beat must not even create the pool"
    );
}

#[test]
fn a_state_right_at_the_ceiling_is_still_admitted() {
    // A ceiling nobody can reach is a ceiling nobody can test. This pins the
    // comparison as <=, so shrinking the limit by one has to be deliberate.
    let reg = registry();
    let mut b = beat("engine", 1);
    // {"blob":"..."} costs 11 bytes around the padding.
    b.state = json!({"blob": "x".repeat(MAX_STATE - 11)});
    assert_eq!(serde_json::to_vec(&b.state).unwrap().len(), MAX_STATE);
    assert!(reg.beat(&b).accepted);
}

#[test]
fn a_beat_watching_more_pools_than_the_limit_is_refused() {
    let reg = registry();
    let mut b = beat("engine", 1);
    b.watch = (0..MAX_WATCH + 1).map(|i| format!("p{i}")).collect();
    assert!(!reg.beat(&b).accepted);

    // And exactly at the limit it is fine, which is the case that used to kill
    // the member: refusing the whole beat stopped its heartbeat loop.
    let mut ok = beat("engine", 1);
    ok.watch = (0..MAX_WATCH).map(|i| format!("p{i}")).collect();
    assert!(reg.beat(&ok).accepted);
}

#[test]
fn a_tenure_from_a_broken_clock_is_refused_rather_than_recorded() {
    // A tenure centuries in the future raises the seat's high-water mark past
    // anything a healthy process can produce, and locks the seat out forever.
    // For a trainer rank that means the job can never start again.
    let reg = registry();
    let mut b = beat("trainer", 0);
    b.slot = Some(0);
    b.incarnation = u64::MAX;

    assert!(!reg.beat(&b).accepted);
    assert!(reg.snapshot().is_empty(), "nothing may be recorded");

    // The seat must still be usable by a sane tenure afterwards.
    let mut sane = beat("trainer", 0);
    sane.slot = Some(0);
    sane.incarnation = 1;
    assert!(reg.beat(&sane).accepted);
}

// --- seats and tenure -------------------------------------------------------

#[test]
fn an_older_tenure_cannot_displace_the_one_holding_the_seat() {
    let reg = registry();
    let mut current = beat("trainer", 0);
    current.slot = Some(0);
    current.incarnation = 5;
    current.url = Some("http://new".into());
    assert!(reg.beat(&current).accepted);

    let mut ghost = beat("trainer", 0);
    ghost.slot = Some(0);
    ghost.incarnation = 3;
    ghost.url = Some("http://old".into());
    assert!(
        !reg.beat(&ghost).accepted,
        "a ghost must be told to give up"
    );

    let held = reg.beat(&watcher("trainer", None));
    let members = &delta_for(&held, "trainer").changed;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].incarnation, 5);
    assert_eq!(members[0].url.as_deref(), Some("http://new"));
}

#[test]
fn a_seat_remembers_it_moved_on_even_after_the_occupant_leaves() {
    // Without the high-water mark the record vanishes on removal, so a
    // superseded process could sit and wait for its replacement to die and
    // then quietly take the seat back.
    let reg = registry();
    let mut current = beat("trainer", 0);
    current.slot = Some(0);
    current.incarnation = 5;
    assert!(reg.beat(&current).accepted);

    let mut leaving = current.clone();
    leaving.leaving = true;
    assert!(reg.beat(&leaving).accepted);
    assert_eq!(pool_of(&reg, "trainer").2, 0);

    let mut ghost = beat("trainer", 0);
    ghost.slot = Some(0);
    ghost.incarnation = 3;
    assert!(!reg.beat(&ghost).accepted, "the seat has moved on");

    // A newer tenure still takes it, or the seat would be dead rather than
    // merely closed to the past.
    let mut newer = beat("trainer", 0);
    newer.slot = Some(0);
    newer.incarnation = 6;
    assert!(reg.beat(&newer).accepted);
}

#[test]
fn an_unslotted_member_keeps_no_watermark_because_its_id_is_never_reused() {
    // The high-water mark is per seat and is never reclaimed, so it is kept
    // only for slotted pools: interchangeable members get a fresh id each
    // time, and remembering every one of them would just grow forever.
    //
    // A lease that lapsed is the case to test it with. A goodbye is different
    // -- see the departure tests below: that is remembered for one lease, in a
    // map the sweeper prunes, so it costs nothing permanent either way.
    let reg = Registry::new(Duration::from_millis(50));
    let mut first = beat("engine", 7);
    first.incarnation = 5;
    assert!(reg.beat(&first).accepted);

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(reg.sweep(), 1);

    let mut lower = beat("engine", 7);
    lower.incarnation = 3;
    assert!(reg.beat(&lower).accepted, "no seat memory outside slots");

    // The same thing in a slotted pool is refused, which is what the mark is
    // there for.
    let slotted = Registry::new(Duration::from_millis(50));
    let mut held = beat("trainer", 0);
    held.slot = Some(0);
    held.incarnation = 5;
    assert!(slotted.beat(&held).accepted);

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(slotted.sweep(), 1);

    let mut ghost = beat("trainer", 0);
    ghost.slot = Some(0);
    ghost.incarnation = 3;
    assert!(
        !slotted.beat(&ghost).accepted,
        "the seat remembers it moved on"
    );
}

#[test]
fn an_exclusive_beat_is_refused_while_anyone_else_holds_the_seat() {
    // Asking exclusively means "only if nobody holds it". The lease has not
    // lapsed, so somebody does.
    let reg = registry();
    let mut held = beat("leader", 0);
    held.slot = Some(0);
    held.incarnation = 1;
    assert!(reg.beat(&held).accepted);

    let mut challenger = beat("leader", 0);
    challenger.slot = Some(0);
    challenger.incarnation = 2;
    challenger.exclusive = true;
    assert!(!reg.beat(&challenger).accepted);

    let still = reg.beat(&watcher("leader", None));
    assert_eq!(delta_for(&still, "leader").changed[0].incarnation, 1);
}

#[test]
fn an_exclusive_beat_still_renews_the_holder_s_own_lease() {
    // The holder beats exclusively too. If its own tenure counted as an
    // occupant it would refuse itself on the second beat and die holding a
    // seat it had just won.
    let reg = registry();
    let mut b = beat("leader", 0);
    b.slot = Some(0);
    b.exclusive = true;
    assert!(reg.beat(&b).accepted);
    assert!(reg.beat(&b).accepted, "the holder renews itself");
}

#[test]
fn a_replacement_takes_the_seat_without_waiting_for_the_lease_to_lapse() {
    // A restarting rank must reclaim its seat while the dead process's lease
    // is still running, which is why exclusivity is opt-in.
    let reg = registry();
    let mut dead = beat("trainer", 0);
    dead.slot = Some(0);
    dead.incarnation = 1;
    assert!(reg.beat(&dead).accepted);
    let before = roster(&reg, "trainer");

    let mut replacement = beat("trainer", 0);
    replacement.slot = Some(0);
    replacement.incarnation = 2;
    assert!(reg.beat(&replacement).accepted);

    assert_eq!(pool_of(&reg, "trainer").2, 1, "one occupant, not two");
    assert_ne!(roster(&reg, "trainer"), before, "the roster moved");
    assert_eq!(roster(&reg, "trainer"), b_hash(0, 2));
}

// --- the roster fingerprint -------------------------------------------------

#[test]
fn updating_state_moves_the_version_but_never_the_roster() {
    // This is what makes a frozen epoch survive a member calling ready(). If
    // the fingerprint moved, every rank would see its round invalidated by an
    // unrelated peer publishing a metric.
    let reg = registry();
    let b = beat("trainer", 0);
    assert!(reg.beat(&b).accepted);
    let (v1, r1, _) = pool_of(&reg, "trainer");

    let mut updated = b.clone();
    updated.state = json!({"step": 100});
    updated.ready = true;
    assert!(reg.beat(&updated).accepted);
    let (v2, r2, _) = pool_of(&reg, "trainer");

    assert!(v2 > v1, "subscribers must learn about the change");
    assert_eq!(r1, r2, "but the roster did not change");
}

#[test]
fn an_identical_beat_renews_the_lease_without_moving_the_version() {
    // Heartbeats vastly outnumber changes. If each one bumped the version,
    // every subscriber would be woken on every beat by every member.
    let reg = registry();
    let b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);
    let v1 = version(&reg, "engine");

    for _ in 0..5 {
        assert!(reg.beat(&b).accepted);
    }
    assert_eq!(
        version(&reg, "engine"),
        v1,
        "nothing changed, nothing moved"
    );
}

#[test]
fn a_pool_that_returns_to_empty_returns_to_a_zero_fingerprint() {
    let reg = registry();
    let a = beat("engine", 1);
    let mut c = beat("engine", 2);
    c.incarnation = 4;
    assert!(reg.beat(&a).accepted);
    assert!(reg.beat(&c).accepted);
    assert_eq!(roster(&reg, "engine"), b_hash(1, 1) ^ b_hash(2, 4));

    let (mut a_out, mut c_out) = (a.clone(), c.clone());
    a_out.leaving = true;
    c_out.leaving = true;
    assert!(reg.beat(&a_out).accepted);
    assert!(reg.beat(&c_out).accepted);

    assert_eq!(pool_of(&reg, "engine").2, 0);
    assert_eq!(roster(&reg, "engine"), 0, "every member was XORed back out");
}

#[test]
fn leaving_removes_the_tenure_that_is_stored_not_the_one_in_the_goodbye() {
    // A goodbye carrying a tenure the registry never stored would otherwise
    // XOR out a fingerprint that was never XORed in, leaving the pool with a
    // permanently wrong roster that no later change can correct.
    let reg = registry();
    let mut joined = beat("engine", 1);
    joined.incarnation = 1;
    assert!(reg.beat(&joined).accepted);

    let mut goodbye = beat("engine", 1);
    goodbye.incarnation = 2; // newer than anything stored
    goodbye.leaving = true;
    assert!(reg.beat(&goodbye).accepted);

    assert_eq!(pool_of(&reg, "engine").2, 0);
    assert_eq!(roster(&reg, "engine"), 0, "not b_hash(1,1) ^ b_hash(1,2)");
}

#[test]
fn the_roster_does_not_depend_on_the_order_members_arrived_in() {
    // Ranks race on startup. If the fingerprint depended on arrival order, two
    // ranks would compute different ones for the same set and never agree that
    // the round was frozen.
    let (forwards, backwards) = (registry(), registry());
    let ids: [u64; 4] = [3, 1, 4, 2];
    for id in ids {
        assert!(forwards.beat(&beat("trainer", id)).accepted);
    }
    for id in ids.iter().rev() {
        assert!(backwards.beat(&beat("trainer", *id)).accepted);
    }
    assert_eq!(roster(&forwards, "trainer"), roster(&backwards, "trainer"));
}

// --- expiry -----------------------------------------------------------------

#[test]
fn a_lapsed_lease_is_swept_and_the_change_reaches_subscribers() {
    let reg = Registry::new(Duration::from_millis(50));
    assert!(reg.beat(&beat("engine", 1)).accepted);
    let v1 = version(&reg, "engine");

    assert_eq!(reg.sweep(), 0, "nothing has lapsed yet");
    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(reg.sweep(), 1);

    let (v2, r2, n) = pool_of(&reg, "engine");
    assert_eq!(n, 0);
    assert_eq!(
        r2, 0,
        "a swept member is XORed out like any other departure"
    );
    assert!(
        v2 > v1,
        "a silent expiry would leave every subscriber stale"
    );
    assert_eq!(reg.sweep(), 0, "sweeping again finds nothing");
}

#[test]
fn a_beat_before_the_lease_lapses_keeps_the_member() {
    let reg = Registry::new(Duration::from_millis(200));
    let b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(50));
        assert!(reg.beat(&b).accepted);
        assert_eq!(reg.sweep(), 0);
    }
    assert_eq!(pool_of(&reg, "engine").2, 1);
}

// --- deltas -----------------------------------------------------------------

#[test]
fn a_subscriber_that_never_synced_gets_the_whole_roster() {
    let reg = registry();
    assert!(reg.beat(&beat("engine", 1)).accepted);
    assert!(reg.beat(&beat("engine", 2)).accepted);

    let d = reg.beat(&watcher("engine", None));
    let d = delta_for(&d, "engine");
    assert!(
        d.full,
        "there is no earlier position to send a delta against"
    );
    assert_eq!(d.changed.len(), 2);
    assert!(d.removed.is_empty());
}

#[test]
fn a_caught_up_subscriber_is_told_nothing_at_all() {
    // An empty body per watched pool per beat is the difference between a
    // control plane that idles and one that spends its bandwidth saying
    // nothing.
    let reg = registry();
    assert!(reg.beat(&beat("engine", 1)).accepted);
    let v = version(&reg, "engine");

    let ack = reg.beat(&watcher("engine", Some(v)));
    assert!(
        !ack.pools.contains_key("engine"),
        "expected the pool to be omitted, got {:?}",
        ack.pools.get("engine")
    );
}

#[test]
fn a_subscriber_in_step_is_sent_only_what_moved() {
    let reg = registry();
    assert!(reg.beat(&beat("engine", 1)).accepted);
    assert!(reg.beat(&beat("engine", 2)).accepted);
    let synced = version(&reg, "engine");

    let mut gone = beat("engine", 1);
    gone.leaving = true;
    assert!(reg.beat(&gone).accepted);
    assert!(reg.beat(&beat("engine", 3)).accepted);

    let ack = reg.beat(&watcher("engine", Some(synced)));
    let d = delta_for(&ack, "engine");
    assert!(!d.full, "the log still covers this position");
    assert_eq!(d.removed, vec![1]);
    assert_eq!(d.changed.len(), 1);
    assert_eq!(d.changed[0].id, 3);
}

#[test]
fn a_member_that_changed_repeatedly_appears_once_in_the_delta() {
    let reg = registry();
    let mut b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);
    let synced = version(&reg, "engine");

    for step in 1..=5 {
        b.state = json!({"step": step});
        assert!(reg.beat(&b).accepted);
    }

    let ack = reg.beat(&watcher("engine", Some(synced)));
    let d = delta_for(&ack, "engine");
    assert_eq!(d.changed.len(), 1, "five bumps, one member, one copy");
    assert_eq!(d.changed[0].state, json!({"step": 5}), "and the latest one");
}

#[test]
fn a_subscriber_that_fell_off_the_end_of_the_log_gets_a_full_roster() {
    // Sending a delta against a position the log no longer covers would leave
    // the client holding members that had already left, forever and silently.
    let reg = registry();
    let mut b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);

    // LOG_CAP is 4096; overrun it comfortably.
    for step in 0..4200 {
        b.state = json!({"step": step});
        assert!(reg.beat(&b).accepted);
    }

    let ack = reg.beat(&watcher("engine", Some(1)));
    let d = delta_for(&ack, "engine");
    assert!(d.full, "position 1 is long gone");
    assert_eq!(d.changed.len(), 1);
}

#[test]
fn a_pool_that_emptied_is_still_reported_to_a_new_subscriber() {
    // "No delta" and "an empty pool" must not look the same to someone who has
    // never synced, or a client would keep waiting for a roster it already has.
    let reg = registry();
    let b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);
    let mut gone = b.clone();
    gone.leaving = true;
    assert!(reg.beat(&gone).accepted);

    let ack = reg.beat(&watcher("engine", None));
    let d = delta_for(&ack, "engine");
    assert!(d.full);
    assert!(d.changed.is_empty(), "full, and the answer is nobody");
}

#[test]
fn watching_a_pool_that_does_not_exist_yet_is_quiet_rather_than_an_error() {
    // Watching before the other half of the job has started is the normal
    // case, not a mistake.
    let reg = registry();
    let ack = reg.beat(&watcher("not_yet", None));
    assert!(ack.accepted);
    assert!(ack.pools.is_empty());
}

#[test]
fn a_delta_carries_the_shape_so_a_subscriber_can_check_it_locally() {
    let reg = registry();
    let mut b = beat("trainer", 0);
    b.slot = Some(0);
    b.policy = "collective".into();
    b.size = Some(8);
    assert!(reg.beat(&b).accepted);

    let ack = reg.beat(&watcher("trainer", None));
    let d = delta_for(&ack, "trainer");
    assert_eq!(d.policy, "collective");
    assert_eq!(d.size, Some(8));
    assert_eq!(d.roster, roster(&reg, "trainer"));
    assert_eq!(d.version, version(&reg, "trainer"));
}

// --- what every ack carries -------------------------------------------------

#[test]
fn every_ack_reports_the_lease_length_including_the_refusals() {
    // A member whose beat was refused still has to know how fast to retry.
    let reg = Registry::new(Duration::from_millis(2500));
    assert_eq!(reg.beat(&beat("engine", 1)).ttl_ms, 2500);

    let mut wrong = beat("engine", 2);
    wrong.policy = "collective".into();
    assert_eq!(reg.beat(&wrong).ttl_ms, 2500, "a shape refusal");

    let mut fat = beat("engine", 3);
    fat.state = json!({"blob": "x".repeat(MAX_STATE + 1)});
    assert_eq!(reg.beat(&fat).ttl_ms, 2500, "an admission failure");
}

#[test]
fn the_epoch_is_non_zero_and_the_same_on_every_ack_from_one_process() {
    // Clients tell a restart from a quiet period by this number. A zero would
    // be indistinguishable from the default on a client that had never heard
    // one, and a changing one would look like a restart on every beat.
    let reg = registry();
    let first = reg.beat(&beat("engine", 1)).epoch;
    assert_ne!(first, 0);
    assert_eq!(reg.beat(&beat("engine", 2)).epoch, first);
    assert_eq!(reg.beat(&watcher("engine", None)).epoch, first);

    // And two processes do not share one, or a restart would go unnoticed.
    assert_ne!(registry().beat(&beat("engine", 1)).epoch, 0);
}

#[test]
fn a_lease_below_the_floor_is_refused_at_startup_with_a_reason() {
    // Measured at 40ms: a healthy member was visible 20% of the time and
    // vanished for 690ms at a stretch, with every heartbeat succeeding and
    // nothing anywhere saying why.
    //
    // On its own thread, because run() serves forever once it accepts: called
    // directly, a dropped guard would hang this test until the CI job timed
    // out instead of failing. Found by deleting the guard to check that this
    // test could see it -- it could not, it simply stopped.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome =
            tinyray_registry::run("127.0.0.1:0", tinyray_registry::MIN_TTL_MS - 1, |_| {})
                .map_err(|e| e.to_string());
        let _ = tx.send(outcome);
    });

    let returned = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("run() never returned: a lease below the floor was allowed to start serving");
    let msg = returned.expect_err("a lease shorter than the beat interval must not start");
    assert!(msg.contains("floor") && msg.contains("expire"), "{msg}");
}

// --- goodbyes that have to stick -------------------------------------------

#[test]
fn a_beat_that_arrives_after_its_own_goodbye_does_not_put_the_member_back() {
    // A client-side timeout means the caller stopped waiting, not that the
    // registry never got the request. So a beat composed before leave() -- it
    // says leaving=false and carries the same tenure -- can still be delivered
    // after the goodbye and re-create the member, which the registry then
    // holds for a full lease. Measured at the 200ms floor: 6 of 300 leave()
    // calls left the member registered, with the goodbye reporting no failure
    // of its own.
    let reg = registry();
    let b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);

    let mut goodbye = b.clone();
    goodbye.leaving = true;
    assert!(reg.beat(&goodbye).accepted);
    assert_eq!(pool_of(&reg, "engine").2, 0);

    assert!(
        !reg.beat(&b).accepted,
        "the same tenure already said goodbye"
    );
    assert_eq!(pool_of(&reg, "engine").2, 0, "and it is still gone");
    assert_eq!(roster(&reg, "engine"), 0);
}

#[test]
fn a_goodbye_stays_idempotent() {
    // leave() sends one, and the beat loop may send another behind it. Neither
    // is wrong, and refusing the second would report a failure that is not one.
    let reg = registry();
    let mut b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);
    b.leaving = true;
    assert!(reg.beat(&b).accepted);
    assert!(reg.beat(&b).accepted, "saying it twice is not an error");
}

#[test]
fn a_later_tenure_still_takes_the_seat_after_a_goodbye() {
    // Remembering a departure must not lock the seat: a process that leaves
    // and comes back carries a newer tenure and has to be let in.
    let reg = registry();
    let mut first = beat("trainer", 0);
    first.slot = Some(0);
    first.incarnation = 5;
    assert!(reg.beat(&first).accepted);

    let mut goodbye = first.clone();
    goodbye.leaving = true;
    assert!(reg.beat(&goodbye).accepted);

    let mut back = beat("trainer", 0);
    back.slot = Some(0);
    back.incarnation = 6;
    assert!(
        reg.beat(&back).accepted,
        "a newer tenure is not a straggler"
    );
    assert_eq!(pool_of(&reg, "trainer").2, 1);
}

#[test]
fn a_lease_that_merely_lapsed_lets_the_same_tenure_come_back() {
    // Sweeping is not a goodbye. That member is alive and simply missed some
    // beats -- soft state is supposed to refill itself, so its next beat has
    // to be allowed to put it back on the tenure it already has.
    let reg = Registry::new(Duration::from_millis(50));
    let b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(reg.sweep(), 1);
    assert_eq!(pool_of(&reg, "engine").2, 0);

    assert!(reg.beat(&b).accepted, "a swept member may return as itself");
    assert_eq!(pool_of(&reg, "engine").2, 1);
}

#[test]
fn a_departure_is_forgotten_once_the_lease_it_had_would_have_run_out() {
    // The memory has to be bounded: a pool that churns for a week cannot keep
    // every id that ever left. One lease is already several times longer than
    // any beat still in flight, since a beat gives up at three quarters of an
    // interval and an interval is a quarter of the lease.
    let reg = Registry::new(Duration::from_millis(50));
    let b = beat("engine", 1);
    assert!(reg.beat(&b).accepted);
    let mut goodbye = b.clone();
    goodbye.leaving = true;
    assert!(reg.beat(&goodbye).accepted);
    assert!(!reg.beat(&b).accepted, "still remembered right afterwards");

    std::thread::sleep(Duration::from_millis(120));
    reg.sweep();
    assert!(
        reg.beat(&b).accepted,
        "past the lease the departure is forgotten, so nothing accumulates"
    );
}
