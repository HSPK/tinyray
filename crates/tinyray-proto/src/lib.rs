//! Wire types shared by the registry and the in-process client.
//!
//! There are exactly two messages on the wire: `Beat` and `BeatAck`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// A member as seen by other processes. Being in a pool listing means alive,
/// so there is no `alive` field. `expires_at` is registry-internal and never
/// crosses the wire: if it did, every heartbeat would bump the pool version
/// and every client would receive a full roster on every beat.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Member {
    /// Key within the pool. Equals `slot` for slotted pools.
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<u64>,
    pub incarnation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default)]
    pub state: Value,
    pub ready: bool,
}

impl Member {
    /// Contribution to the roster fingerprint. Only seat and tenure take part,
    /// so adding fields to `Member` can never corrupt it.
    pub fn roster_hash(&self) -> u64 {
        let mut h = 1469598103934665603u64;
        for b in self
            .id
            .to_le_bytes()
            .iter()
            .chain(self.incarnation.to_le_bytes().iter())
        {
            h ^= *b as u64;
            h = h.wrapping_mul(1099511628211);
        }
        h
    }

    pub fn matches(&self, filter: &Value) -> bool {
        match filter.as_object() {
            None => true,
            Some(f) => f
                .iter()
                .all(|(k, v)| self.state.get(k).is_some_and(|got| same_value(got, v))),
        }
    }
}

/// How many pools one member may subscribe to. The list rides on every beat
/// and its answer rides back, so it is bounded. Crossing it used to make the
/// registry refuse the whole beat, which stopped the loop and killed the
/// member in silence.
pub const MAX_WATCH: usize = 64;

/// JSON keeps 3 and 3.0 apart and Python does not, so `shard=6/2` -- the
/// obvious way to compute a shard index -- found nobody while `shard=3` found
/// the member. A filter is a label, so numbers compare by value.
///
/// Booleans stay strict: `True == 1` is Python's anomaly, and letting
/// `free=1` match `free=true` would surprise more than it helps.
///
/// Mixed comparisons convert the float to an integer, never the reverse:
/// rounding an integer to f64 would make 2**53+1 match the float 2**53.
/// Bounds are checked before casting because Rust's casts saturate.
///
/// The rule follows the value down. It used to stop at the top, so the very
/// example it was written for came back one level in: a member publishing
/// `cfg={"shard": 3}` was found by `cfg={"shard": 3}` and not by
/// `cfg={"shard": 6/2}`. Shape stays exact -- same length, same keys -- so
/// nothing but numbers is relaxed at any depth.
fn same_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.is_f64(), y.is_f64()) {
            (false, false) => x == y,
            (true, true) => x.as_f64() == y.as_f64(),
            (false, true) => integer_matches_float(x, y.as_f64().unwrap()),
            (true, false) => integer_matches_float(y, x.as_f64().unwrap()),
        },
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same_value(p, q))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| same_value(v, w)))
        }
        _ => a == b,
    }
}

fn integer_matches_float(integer: &serde_json::Number, float: f64) -> bool {
    if float.fract() != 0.0 {
        return false;
    }
    if float < 0.0 {
        float >= i64::MIN as f64 && integer.as_i64() == Some(float as i64)
    } else {
        // u64::MAX rounds up to 2**64, the exclusive upper bound.
        float < u64::MAX as f64 && integer.as_u64() == Some(float as u64)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Beat {
    pub pool: String,
    #[serde(default)]
    pub slot: Option<u64>,
    pub id: u64,
    pub incarnation: u64,
    /// Monotonic within one incarnation; covers state, readiness and URL.
    /// Missing means a legacy client without publication ordering. Once a
    /// tenure has sent a sequence, older or missing sequences only renew its
    /// lease: delayed requests must not undo an acknowledged publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<u64>,
    pub policy: String,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub state: Value,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub leaving: bool,
    /// Take the seat only if it is free. Restarting members want the opposite
    /// -- a rank must reclaim its seat even while the dead one's lease runs --
    /// so this is opt-in.
    #[serde(default)]
    pub exclusive: bool,
    #[serde(default)]
    pub methods: Vec<String>,
    /// Pools this process wants to hear about.
    #[serde(default)]
    pub watch: Vec<String>,
    /// Version of each watched pool already held locally.
    #[serde(default)]
    pub seen: HashMap<String, u64>,
    /// How long this caller is willing to have its answer held back when there
    /// is nothing to say. Zero -- and so any client that predates this field --
    /// gets an immediate answer, exactly as before.
    ///
    /// A member sends the interval it would otherwise have slept, which keeps
    /// the request rate identical to the polling it replaces and turns the
    /// discovery delay from "up to one interval" into "one round trip".
    #[serde(default)]
    pub hold_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolDelta {
    pub version: u64,
    pub roster: u64,
    pub policy: String,
    #[serde(default)]
    pub methods: Vec<String>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub changed: Vec<Member>,
    #[serde(default)]
    pub removed: Vec<u64>,
    /// `changed` is the whole roster; drop whatever was held before.
    #[serde(default)]
    pub full: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BeatAck {
    /// Random per registry process. Versions restart from zero when it does,
    /// so a client holding a higher one would be told nothing had changed and
    /// would sit on a stale roster forever, silently.
    #[serde(default)]
    pub epoch: u64,
    /// What this registry can do, as a number that only goes up. Absent means
    /// a registry from before it existed, which deserializes to 0.
    ///
    /// A client cannot infer this from behaviour: an old registry answers a
    /// long-poll request immediately and correctly, it just answers it at
    /// once, so "parked and nothing happened" and "does not park" look the
    /// same. The difference was measured at 14.5 requests a second against
    /// 0.12 -- a hundredfold, silently.
    #[serde(default)]
    pub protocol: u32,
    /// The registry's own version, for saying so in a message. The number
    /// above is what code should branch on.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    pub ttl_ms: u64,
    /// False means the seat was taken by a later incarnation. Give up.
    pub accepted: bool,
    /// Set when the refusal was about the pool's shape rather than the seat,
    /// so the caller can be told what it disagreed about instead of guessing
    /// from a roll call that never completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
    #[serde(default)]
    pub pools: HashMap<String, PoolDelta>,
}

/// What this build of the registry can do.
///
///   0  everything before long polling existed (pre-0.7.0)
///   1  honours `Beat.hold_ms`: parks a reply that has nothing to say and
///      answers the moment a watched pool moves
///   2  honours `Beat.publication`: a delayed beat cannot roll back the
///      state, readiness or URL of the same incarnation
pub const PROTOCOL: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn member_with(state: Value) -> Member {
        Member {
            id: 1,
            slot: None,
            incarnation: 1,
            url: None,
            state,
            ready: true,
        }
    }

    #[test]
    fn big_integers_compare_as_integers_not_as_doubles() {
        // 2**63 is a u64 and not an i64, which used to send both sides through
        // f64. At that magnitude a double steps by 2048, so everything within
        // 1024 collapsed onto the same value.
        let m = member_with(json!({ "tag": 9_223_372_036_854_775_808u64 }));
        assert!(m.matches(&json!({ "tag": 9_223_372_036_854_775_808u64 })));
        for off in [1u64, 2, 1024, 2048] {
            let other = 9_223_372_036_854_775_808u64 + off;
            assert!(
                !m.matches(&json!({ "tag": other })),
                "tag+{off} matched a member whose tag it is not"
            );
        }
        assert!(!m.matches(&json!({ "tag": u64::MAX })));
    }

    #[test]
    fn a_whole_float_still_matches_the_integer_it_names() {
        let m = member_with(json!({ "shard": 3, "rank": 2.0 }));
        assert!(m.matches(&json!({ "shard": 3.0 })));
        assert!(m.matches(&json!({ "rank": 2 })));
        assert!(!m.matches(&json!({ "shard": 3.5 })));
    }

    #[test]
    fn a_negative_is_not_a_large_unsigned() {
        let m = member_with(json!({ "n": -1 }));
        assert!(m.matches(&json!({ "n": -1 })));
        assert!(!m.matches(&json!({ "n": u64::MAX })));
        let big = member_with(json!({ "n": u64::MAX }));
        assert!(!big.matches(&json!({ "n": -1 })));
    }
    #[test]
    fn the_number_rule_follows_the_value_down() {
        // 3 vs 6/2 是这条规则当初的招牌例子，从前它在第一层就失效。
        let m = member_with(json!({ "cfg": { "shard": 3 }, "tags": [3] }));
        assert!(m.matches(&json!({ "cfg": { "shard": 3.0 } })));
        assert!(m.matches(&json!({ "tags": [3.0] })));
        assert!(!m.matches(&json!({ "cfg": { "shard": 3.5 } })));

        let deep = member_with(json!({ "a": { "b": { "c": [ { "d": 7 } ] } } }));
        assert!(deep.matches(&json!({ "a": { "b": { "c": [ { "d": 7.0 } ] } } })));
        assert!(!deep.matches(&json!({ "a": { "b": { "c": [ { "d": 8 } ] } } })));
    }

    #[test]
    fn only_numbers_are_relaxed_shape_is_still_exact() {
        let m = member_with(json!({ "cfg": { "zone": "a", "rank": 1 }, "tags": [1, 2] }));
        // 少一个键、多一个键、键名不同，都不算同一个标签
        assert!(!m.matches(&json!({ "cfg": { "zone": "a" } })));
        assert!(!m.matches(&json!({ "cfg": { "zone": "a", "rank": 1, "x": 0 } })));
        assert!(!m.matches(&json!({ "cfg": { "zone": "a", "rankk": 1 } })));
        // 数组按顺序、按长度
        assert!(!m.matches(&json!({ "tags": [2, 1] })));
        assert!(!m.matches(&json!({ "tags": [1] })));
        assert!(m.matches(&json!({ "tags": [1.0, 2.0] })));
        // 布尔仍然不是数字，任何深度都一样
        let b = member_with(json!({ "cfg": { "free": true } }));
        assert!(!b.matches(&json!({ "cfg": { "free": 1 } })));
    }
}
