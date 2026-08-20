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
        for b in self.id.to_le_bytes().iter().chain(self.incarnation.to_le_bytes().iter()) {
            h ^= *b as u64;
            h = h.wrapping_mul(1099511628211);
        }
        h
    }

    pub fn matches(&self, filter: &Value) -> bool {
        match filter.as_object() {
            None => true,
            Some(f) => f.iter().all(|(k, v)| self.state.get(k) == Some(v)),
        }
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
    #[serde(default)]
    pub pools: HashMap<String, PoolDelta>,
}
