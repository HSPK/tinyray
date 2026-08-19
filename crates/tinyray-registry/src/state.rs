//! Soft-state membership. Nothing here is persisted: every record is
//! re-asserted by its owner each heartbeat, so a restart refills itself
//! within one interval.

use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tinyray_proto::{Beat, BeatAck, Member, PoolDelta};

/// How many past versions of change history to keep per pool. A client that
/// falls further behind than this gets a full roster instead of a delta.
const LOG_CAP: usize = 4096;

struct Record {
    member: Member,
    expires_at: Instant,
}

#[derive(Default)]
struct Pool {
    /// Bumped by any change a client should learn about. Drives delta sync.
    version: u64,
    /// XOR of every member's seat+tenure hash. Only changes when the set of
    /// occupants changes, so freezing a round cannot be invalidated by a
    /// member merely updating its state.
    roster: u64,
    policy: String,
    methods: Vec<String>,
    size: Option<u64>,
    members: HashMap<u64, Record>,
    /// (version, member id) of each change, oldest first.
    log: VecDeque<(u64, u64)>,
}

impl Pool {
    fn bump(&mut self, id: u64) {
        self.version += 1;
        self.log.push_back((self.version, id));
        while self.log.len() > LOG_CAP {
            self.log.pop_front();
        }
    }

    fn delta(&self, seen: Option<u64>) -> PoolDelta {
        let mut d = PoolDelta {
            version: self.version,
            roster: self.roster,
            policy: self.policy.clone(),
            methods: self.methods.clone(),
            size: self.size,
            changed: Vec::new(),
            removed: Vec::new(),
            full: false,
        };
        let oldest = self.log.front().map(|(v, _)| *v).unwrap_or(0);
        match seen {
            // Caught up already.
            Some(v) if v == self.version => return d,
            // Known position still covered by the log: send only what moved.
            Some(v) if v + 1 >= oldest => {
                let mut ids: Vec<u64> =
                    self.log.iter().filter(|(lv, _)| *lv > v).map(|(_, id)| *id).collect();
                ids.sort_unstable();
                ids.dedup();
                for id in ids {
                    match self.members.get(&id) {
                        Some(r) => d.changed.push(r.member.clone()),
                        None => d.removed.push(id),
                    }
                }
            }
            // Never synced, or fell off the end of the log.
            _ => {
                d.full = true;
                d.changed = self.members.values().map(|r| r.member.clone()).collect();
            }
        }
        d
    }
}

pub struct Registry {
    pools: RwLock<HashMap<String, Pool>>,
    pub ttl: Duration,
}

impl Registry {
    pub fn new(ttl: Duration) -> Self {
        Self { pools: RwLock::new(HashMap::new()), ttl }
    }

    pub fn beat(&self, b: &Beat) -> BeatAck {
        let mut pools = self.pools.write().unwrap();
        let p = pools.entry(b.pool.clone()).or_default();
        if p.policy.is_empty() {
            p.policy = b.policy.clone();
            p.size = b.size;
            p.methods = b.methods.clone();
        }

        let mut accepted = true;
        match p.members.get(&b.id).map(|r| r.member.incarnation) {
            // A later tenure already holds this seat: this beat is a ghost.
            Some(cur) if cur > b.incarnation => accepted = false,
            Some(cur) if cur == b.incarnation && !b.leaving => {
                let changed = {
                    let r = p.members.get(&b.id).unwrap();
                    r.member.url != b.url || r.member.state != b.state || r.member.ready != b.ready
                };
                let r = p.members.get_mut(&b.id).unwrap();
                r.expires_at = Instant::now() + self.ttl;
                if changed {
                    r.member.url = b.url.clone();
                    r.member.state = b.state.clone();
                    r.member.ready = b.ready;
                    p.bump(b.id);
                }
            }
            _ if b.leaving => {
                if p.members.remove(&b.id).is_some() {
                    p.roster ^= roster_of(b.id, b.incarnation);
                    p.bump(b.id);
                }
            }
            // New arrival, or a replacement taking over the seat.
            other => {
                if let Some(old) = other {
                    p.roster ^= roster_of(b.id, old);
                }
                let m = Member {
                    id: b.id,
                    slot: b.slot,
                    incarnation: b.incarnation,
                    url: b.url.clone(),
                    state: b.state.clone(),
                    ready: b.ready,
                };
                p.roster ^= m.roster_hash();
                p.members.insert(b.id, Record { member: m, expires_at: Instant::now() + self.ttl });
                p.bump(b.id);
            }
        }

        let mut out = HashMap::new();
        for name in &b.watch {
            if let Some(wp) = pools.get(name) {
                let d = wp.delta(b.seen.get(name).copied());
                // Nothing new: leave it out entirely rather than send an empty body.
                if d.full || !d.changed.is_empty() || !d.removed.is_empty() || b.seen.get(name).is_none() {
                    out.insert(name.clone(), d);
                }
            }
        }
        BeatAck { ttl_ms: self.ttl.as_millis() as u64, accepted, pools: out }
    }

    /// Drop members whose lease ran out. Runs on a timer, never on the request
    /// path -- an earlier version swept inside `lookup` and made it O(N).
    pub fn sweep(&self) -> usize {
        let now = Instant::now();
        let mut pools = self.pools.write().unwrap();
        let mut dropped = 0;
        for p in pools.values_mut() {
            let dead: Vec<u64> = p
                .members
                .iter()
                .filter(|(_, r)| r.expires_at <= now)
                .map(|(id, _)| *id)
                .collect();
            for id in dead {
                if let Some(r) = p.members.remove(&id) {
                    p.roster ^= r.member.roster_hash();
                    p.bump(id);
                    dropped += 1;
                }
            }
        }
        dropped
    }

    pub fn snapshot(&self) -> HashMap<String, (u64, u64, usize)> {
        self.pools
            .read()
            .unwrap()
            .iter()
            .map(|(k, p)| (k.clone(), (p.version, p.roster, p.members.len())))
            .collect()
    }
}

fn roster_of(id: u64, incarnation: u64) -> u64 {
    Member { id, slot: None, incarnation, url: None, state: serde_json::Value::Null, ready: false }
        .roster_hash()
}
