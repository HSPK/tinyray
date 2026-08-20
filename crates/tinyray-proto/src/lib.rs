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
fn same_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(i), Some(j)) => i == j,
            _ => x.as_f64() == y.as_f64(),
        },
        _ => a == b,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Beat {
    pub pool: String,
    #[serde(default)]
    pub slot: Option<u64>,
    pub id: u64,
    pub incarnation: u64,
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
