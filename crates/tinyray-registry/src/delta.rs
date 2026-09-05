use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::sync::Arc;
use tinyray_proto::{BeatAck, PoolDelta};

pub(crate) const CACHE_ENTRIES: usize = 8;
/// A conservative serialized-payload budget, not an RSS or in-flight limit.
pub(crate) const CACHE_BYTES: usize = 2 << 20;

pub(crate) type SharedPools = HashMap<String, Arc<PoolDelta>>;

/// The HTTP path shares snapshots without changing the public wire structs.
#[derive(Serialize)]
pub(crate) struct SharedBeatAck {
    pub epoch: u64,
    pub protocol: u32,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub version: String,
    pub ttl_ms: u64,
    pub accepted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
    #[serde(serialize_with = "serialize_pools")]
    pub pools: SharedPools,
}

fn serialize_pools<S: Serializer>(pools: &SharedPools, serializer: S) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(pools.len()))?;
    for (name, delta) in pools {
        map.serialize_entry(name, delta.as_ref())?;
    }
    map.end()
}

impl SharedBeatAck {
    pub fn into_owned(self) -> BeatAck {
        BeatAck {
            epoch: self.epoch,
            protocol: self.protocol,
            version: self.version,
            ttl_ms: self.ttl_ms,
            accepted: self.accepted,
            refused: self.refused,
            pools: owned_pools(self.pools),
        }
    }
}

pub(crate) fn owned_pools(pools: SharedPools) -> HashMap<String, PoolDelta> {
    pools
        .into_iter()
        .map(|(name, delta)| (name, Arc::unwrap_or_clone(delta)))
        .collect()
}

struct Cached {
    since: Option<u64>,
    delta: Arc<PoolDelta>,
    bytes: usize,
}

#[derive(Default)]
pub(crate) struct DeltaCache {
    entries: VecDeque<Cached>,
    bytes: usize,
}

impl DeltaCache {
    pub fn get(&self, since: Option<u64>) -> Option<Arc<PoolDelta>> {
        self.entries
            .iter()
            .find(|entry| entry.since == since)
            .map(|entry| entry.delta.clone())
    }

    pub fn insert(
        &mut self,
        since: Option<u64>,
        delta: Arc<PoolDelta>,
        bytes: usize,
        retired: &mut Vec<Arc<PoolDelta>>,
    ) {
        if bytes > CACHE_BYTES {
            return;
        }
        while self.entries.len() >= CACHE_ENTRIES || self.bytes + bytes > CACHE_BYTES {
            let old = self.entries.pop_front().unwrap();
            self.bytes -= old.bytes;
            retired.push(old.delta);
        }
        self.bytes += bytes;
        self.entries.push_back(Cached {
            since,
            delta,
            bytes,
        });
    }

    pub fn clear(&mut self, retired: &mut Vec<Arc<PoolDelta>>) {
        retired.extend(self.entries.drain(..).map(|entry| entry.delta));
        self.bytes = 0;
    }

    #[cfg(test)]
    pub fn usage(&self) -> (usize, usize) {
        (self.entries.len(), self.bytes)
    }
}

/// Use the serializer's exact escaping/number rules without allocating output.
pub(crate) fn serialized_len(value: &impl Serialize, limit: usize) -> Option<usize> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit - self.bytes {
                return Err(io::Error::other("serialized value exceeds its limit"));
            }
            self.bytes += bytes.len();
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value).ok()?;
    Some(counter.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn delta() -> Arc<PoolDelta> {
        Arc::new(PoolDelta {
            version: 7,
            roster: 9,
            policy: "churn".into(),
            methods: vec!["infer".into()],
            size: None,
            changed: Vec::new(),
            removed: vec![3],
            full: false,
        })
    }

    #[test]
    fn counting_uses_the_exact_wire_escaping_and_limit() {
        for value in [
            json!(null),
            json!({"s": "quote\" slash\\ control\u{0000}\n snow☃"}),
            json!({"n": [i64::MIN, 0, 3], "float": -3.25, "large": u64::MAX}),
            json!({"blob": "x".repeat(crate::state::MAX_STATE - 11)}),
        ] {
            let bytes = serde_json::to_vec(&value).unwrap();
            assert_eq!(serialized_len(&value, bytes.len()), Some(bytes.len()));
            assert_eq!(serialized_len(&value, bytes.len() - 1), None);
            assert_eq!(serialized_len(&value, bytes.len() + 1), Some(bytes.len()));
        }
        assert_eq!(serialized_len(&json!(""), 0), None);
    }

    #[test]
    fn shared_ack_has_the_same_json_as_the_public_owned_ack() {
        for refused in [None, Some("shape mismatch".to_string())] {
            let ack = SharedBeatAck {
                epoch: 1,
                protocol: tinyray_proto::PROTOCOL,
                version: env!("CARGO_PKG_VERSION").into(),
                ttl_ms: 2000,
                accepted: refused.is_none(),
                refused,
                pools: HashMap::from([("p".into(), delta())]),
            };
            let shared = serde_json::to_value(&ack).unwrap();
            let owned = ack.into_owned();
            assert_eq!(shared, serde_json::to_value(&owned).unwrap());
            let decoded: BeatAck = serde_json::from_value(shared).unwrap();
            assert_eq!(decoded.pools["p"].removed, vec![3]);
            assert_eq!(decoded.accepted, owned.accepted);
        }
    }

    #[test]
    fn cursor_cache_is_bounded_by_both_entry_count_and_payload_budget() {
        let mut cache = DeltaCache::default();
        let mut retired = Vec::new();
        for cursor in 0..CACHE_ENTRIES as u64 + 4 {
            cache.insert(Some(cursor), delta(), 256, &mut retired);
            let (entries, bytes) = cache.usage();
            assert!(entries <= CACHE_ENTRIES);
            assert!(bytes <= CACHE_BYTES);
        }
        assert!(cache.get(Some(0)).is_none());
        assert!(cache.get(Some(CACHE_ENTRIES as u64 + 3)).is_some());

        cache.clear(&mut retired);
        assert_eq!(cache.usage(), (0, 0));
        for cursor in 0..4 {
            cache.insert(Some(cursor), delta(), CACHE_BYTES / 2, &mut retired);
            assert!(cache.usage().0 <= 2);
            assert!(cache.usage().1 <= CACHE_BYTES);
        }
        let before = cache.usage();
        cache.insert(None, delta(), CACHE_BYTES + 1, &mut retired);
        assert_eq!(cache.usage(), before);
        assert!(
            cache.get(None).is_none(),
            "oversized snapshots are not retained"
        );
        assert!(
            !retired.is_empty(),
            "evicted payloads can be dropped after unlocking"
        );
    }
}
