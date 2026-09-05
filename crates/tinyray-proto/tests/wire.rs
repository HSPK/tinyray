//! The wire types carry two invariants that the rest of the system leans on
//! and that neither side can restate: what a roster fingerprint is allowed to
//! depend on, and when two filter values count as the same label.
//!
//! Both were previously only observable through a running registry, which
//! meant a break showed up as "the roll call never froze" several layers away.

use serde_json::{json, Value};
use tinyray_proto::{Beat, BeatAck, Member};

fn member(id: u64, incarnation: u64) -> Member {
    Member {
        id,
        slot: None,
        incarnation,
        url: None,
        state: Value::Null,
        ready: false,
    }
}

#[test]
fn the_roster_hash_ignores_everything_except_seat_and_tenure() {
    // The point of the fingerprint is that a member updating its state cannot
    // invalidate a frozen epoch. If any of these fields leaked into the hash,
    // a trainer calling ready() mid-round would break the round.
    let bare = member(7, 3);
    let dressed = Member {
        id: 7,
        slot: Some(7),
        incarnation: 3,
        url: Some("http://10.0.0.1:9000".into()),
        state: json!({"model_version": 17, "free": true}),
        ready: true,
    };
    assert_eq!(bare.roster_hash(), dressed.roster_hash());
}

#[test]
fn the_roster_hash_moves_when_the_seat_or_the_tenure_moves() {
    let base = member(7, 3).roster_hash();
    assert_ne!(base, member(8, 3).roster_hash(), "a different seat");
    assert_ne!(base, member(7, 4).roster_hash(), "a different tenure");
}

#[test]
fn the_roster_hash_cancels_itself_under_xor() {
    // The registry maintains the pool fingerprint by XORing members in and out
    // rather than recomputing it, so self-cancellation is load-bearing: without
    // it a pool that returned to empty would keep a non-zero fingerprint.
    let h = member(7, 3).roster_hash();
    assert_eq!(h ^ h, 0);
}

#[test]
fn a_filter_number_matches_by_value_not_by_json_type() {
    // shard=6/2 is the obvious way to compute a shard index in Python and
    // produces 3.0, which is a different JSON node from the stored 3.
    let m = Member {
        state: json!({"shard": 3}),
        ..member(1, 1)
    };
    assert!(m.matches(&json!({"shard": 3})));
    assert!(m.matches(&json!({"shard": 3.0})));

    // And the same the other way round, because which side is a float depends
    // on which side did the arithmetic.
    let stored_float = Member {
        state: json!({"shard": 3.0}),
        ..member(1, 1)
    };
    assert!(stored_float.matches(&json!({"shard": 3})));

    assert!(!m.matches(&json!({"shard": 4})));
    assert!(!m.matches(&json!({"shard": 3.5})));
}

#[test]
fn a_filter_number_matches_by_value_outside_the_i64_range() {
    // Past i64 both sides fall back to f64; the comparison must still happen
    // rather than defaulting to "different".
    let m = Member {
        state: json!({"tag": 18446744073709551615u64}),
        ..member(1, 1)
    };
    assert!(m.matches(&json!({"tag": 18446744073709551615u64})));

    let negative = Member {
        state: json!({"shard": -2}),
        ..member(1, 1)
    };
    assert!(negative.matches(&json!({"shard": -2.0})));
    assert!(!negative.matches(&json!({"shard": 2})));
}

#[test]
fn a_filter_boolean_stays_strict_about_numbers() {
    // True == 1 is Python's anomaly, not a fact about labels. Letting free=1
    // match free=true would silently widen every filter written by someone who
    // meant the number.
    let flag = Member {
        state: json!({"free": true}),
        ..member(1, 1)
    };
    assert!(flag.matches(&json!({"free": true})));
    assert!(!flag.matches(&json!({"free": 1})));

    let number = Member {
        state: json!({"free": 1}),
        ..member(1, 1)
    };
    assert!(!number.matches(&json!({"free": true})));
}

#[test]
fn a_filter_needs_every_key_and_an_absent_key_never_matches() {
    let m = Member {
        state: json!({"shard": 3, "role": "head"}),
        ..member(1, 1)
    };
    assert!(m.matches(&json!({"shard": 3, "role": "head"})));
    assert!(!m.matches(&json!({"shard": 3, "role": "tail"})));
    // Asking about something the member never said is not a match. Treating a
    // missing key as a wildcard would hand back members that answer nothing.
    assert!(!m.matches(&json!({"missing": 1})));
}

#[test]
fn a_filter_that_asks_nothing_matches_everyone() {
    let m = member(1, 1);
    assert!(m.matches(&json!({})), "an empty object asks nothing");
    assert!(m.matches(&Value::Null), "no filter at all");
    // A non-object filter cannot express a constraint, so it is not treated as
    // one that nobody satisfies.
    assert!(m.matches(&json!("head")));

    // Even a member carrying no state answers an empty filter.
    assert!(member(2, 1).matches(&json!({})));
    assert!(!member(2, 1).matches(&json!({"shard": 0})));
}

#[test]
fn a_beat_deserialises_from_only_its_required_fields() {
    // The Python client omits everything it has nothing to say about. If a
    // default were dropped from the wire type, beats would start failing to
    // parse as a 400 rather than as anything legible.
    let b: Beat = serde_json::from_value(json!({
        "pool": "engine", "id": 1, "incarnation": 2, "policy": "serving"
    }))
    .expect("a minimal beat must parse");
    assert_eq!(b.pool, "engine");
    assert_eq!(b.slot, None);
    assert_eq!(b.size, None);
    assert_eq!(b.publication, None);
    assert_eq!(b.url, None);
    assert_eq!(b.state, Value::Null);
    assert!(!b.ready && !b.leaving && !b.exclusive);
    assert!(b.methods.is_empty() && b.watch.is_empty() && b.seen.is_empty());
}

#[test]
fn publication_sequence_distinguishes_zero_from_a_legacy_beat() {
    let raw = json!({"pool": "p", "id": 1, "incarnation": 1, "policy": "churn"});
    let mut beat: Beat = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(beat.publication, None);
    assert!(serde_json::to_value(&beat)
        .unwrap()
        .get("publication")
        .is_none());
    beat.publication = Some(0);
    let wire = serde_json::to_value(&beat).unwrap();
    assert_eq!(wire["publication"], json!(0));
    let back: Beat = serde_json::from_value(wire).unwrap();
    assert_eq!(back.publication, Some(0));
    let mut null = raw;
    null["publication"] = Value::Null;
    assert_eq!(
        serde_json::from_value::<Beat>(null).unwrap().publication,
        None
    );
}

#[test]
fn mixed_numeric_comparisons_are_exact_and_symmetric_at_integer_boundaries() {
    let cases = [
        (
            json!(9_007_199_254_740_993i64),
            json!(9_007_199_254_740_992.0),
            false,
        ),
        (
            json!(-9_007_199_254_740_993i64),
            json!(-9_007_199_254_740_992.0),
            false,
        ),
        (
            json!(9_007_199_254_740_992i64),
            json!(9_007_199_254_740_992.0),
            true,
        ),
        (
            json!(-9_007_199_254_740_992i64),
            json!(-9_007_199_254_740_992.0),
            true,
        ),
        (json!(i64::MIN), json!(i64::MIN as f64), true),
        (json!(i64::MIN), json!((i64::MIN as f64) - 2048.0), false),
        (json!(i64::MAX), json!(i64::MAX as f64), false),
        (json!(1u64 << 63), json!((1u64 << 63) as f64), true),
        (json!(u64::MAX), json!(u64::MAX as f64), false),
        (
            json!(u64::MAX - 2047),
            json!((u64::MAX as f64) - 2048.0),
            true,
        ),
        (json!(u64::MAX), json!(-1.0), false),
        (json!(-1), json!(u64::MAX as f64), false),
        (json!(0), json!(-0.0), true),
        (json!(3), json!(3.0), true),
        (json!(-3), json!(-3.0), true),
        (json!(3), json!(3.5), false),
        (json!(-3), json!(-3.5), false),
        (json!(0), json!(f64::MIN_POSITIVE), false),
        (json!(u64::MAX), json!(f64::MAX), false),
        (json!(true), json!(1.0), false),
        (json!(false), json!(0.0), false),
    ];
    for (a, b, expected) in cases {
        for (stored, filter) in [(&a, &b), (&b, &a)] {
            let m = Member {
                state: json!({"tag": stored, "nested": {"values": [stored]}}),
                ..member(1, 1)
            };
            assert_eq!(
                m.matches(&json!({"tag": filter})),
                expected,
                "{stored} compared with {filter}"
            );
            assert_eq!(
                m.matches(&json!({"nested": {"values": [filter]}})),
                expected,
                "nested {stored} compared with {filter}"
            );
        }
    }
}

#[test]
fn a_member_on_the_wire_carries_no_lease_and_no_empty_optionals() {
    // expires_at is registry-internal. If it ever crossed the wire every
    // heartbeat would count as a change, every pool version would move, and
    // every subscriber would get a full roster on every beat.
    let wire = serde_json::to_value(member(7, 3)).unwrap();
    let keys: Vec<&str> = wire
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert!(!keys.contains(&"expires_at"), "got {keys:?}");
    assert!(
        !keys.contains(&"slot"),
        "an absent slot is omitted, got {keys:?}"
    );
    assert!(
        !keys.contains(&"url"),
        "an absent url is omitted, got {keys:?}"
    );
    assert_eq!(wire["id"], json!(7));
    assert_eq!(wire["incarnation"], json!(3));
}

#[test]
fn an_ack_omits_a_refusal_it_does_not_have() {
    // The client distinguishes "refused for a stated reason" from "not
    // accepted"; a null in the field would read as a reason that is missing.
    let ack = BeatAck {
        epoch: 1,
        protocol: tinyray_proto::PROTOCOL,
        version: String::new(),
        ttl_ms: 2000,
        accepted: true,
        refused: None,
        pools: Default::default(),
    };
    let wire = serde_json::to_value(&ack).unwrap();
    assert!(wire.get("refused").is_none(), "got {wire}");
}

/// A registry from before the field existed does not send it, and its absence
/// has to read as "cannot do it" rather than as a broken reply. Getting this
/// wrong would turn an upgrade-in-progress deployment into a hard failure,
/// which is worse than the silent degradation it is meant to replace.
#[test]
fn an_ack_without_a_protocol_reads_as_protocol_zero() {
    let raw = r#"{"epoch":7,"ttl_ms":2000,"accepted":true,"pools":{}}"#;
    let ack: tinyray_proto::BeatAck = serde_json::from_str(raw).unwrap();
    assert_eq!(ack.protocol, 0);
    assert_eq!(ack.version, "");
    assert_eq!(ack.epoch, 7);
    assert!(ack.accepted);
}

/// The other direction: a current registry says both, and an old client that
/// does not know the fields must still parse the rest.
#[test]
fn a_current_ack_carries_the_protocol_and_the_version() {
    let ack = tinyray_proto::BeatAck {
        epoch: 1,
        protocol: tinyray_proto::PROTOCOL,
        version: "9.9.9".into(),
        ttl_ms: 1000,
        accepted: true,
        refused: None,
        pools: Default::default(),
    };
    let raw = serde_json::to_string(&ack).unwrap();
    assert!(
        raw.contains(&format!("\"protocol\":{}", tinyray_proto::PROTOCOL)),
        "{raw}"
    );
    assert!(raw.contains("\"version\":\"9.9.9\""), "{raw}");
    let back: tinyray_proto::BeatAck = serde_json::from_str(&raw).unwrap();
    assert_eq!(back.protocol, tinyray_proto::PROTOCOL);
}
