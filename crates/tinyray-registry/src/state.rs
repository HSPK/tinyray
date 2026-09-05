//! Soft-state membership. Nothing here is persisted: every record is
//! re-asserted by its owner each heartbeat, so a restart refills itself
//! within one interval.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};
use tinyray_proto::{Beat, BeatAck, Member, PoolDelta, MAX_WATCH};
use tokio::sync::Notify;

use crate::delta::{owned_pools, serialized_len, DeltaCache, SharedBeatAck, SharedPools};

/// How many past versions of change history to keep per pool. A client that
/// falls further behind than this gets a full roster instead of a delta.
const LOG_CAP: usize = 4096;

/// A tenure is milliseconds shifted left twenty bits, so anything past a few
/// centuries in the future came from a broken clock rather than a real process.
/// Accepting one would raise the high-water mark beyond anything a healthy peer
/// can produce and lock that seat out permanently -- for a trainer rank, that
/// means the job can never start again.
const MAX_INCARNATION: u64 = (4_000_000_000_000u64) << 20;

/// Names are used as map keys and echoed back to every subscriber.
const MAX_NAME: usize = 512;

/// State is stored per member and pushed to every subscriber whenever it
/// changes, so its size is multiplied by the audience. Measured with one
/// member holding 6 MB and 20 subscribers: 120 MB moved in 0.9s, a 20x
/// amplification that extrapolates to 6 GB per change at 1000 subscribers.
/// The control plane carries facts about where things are, not the things.
pub const MAX_STATE: usize = 16 << 10;

/// Methods are stored once per pool and echoed to every subscriber too.
const MAX_METHODS: usize = 256;

/// What this beat says about the pool that the pool does not say about itself.
/// Methods are left out on purpose: an empty list means "I do not serve", not
/// "I disagree", and mixed pools where only some members serve are normal.
fn disagreement(p: &Pool, b: &Beat) -> Option<String> {
    if p.policy != b.policy {
        return Some(format!(
            "pool {:?} is running as {:?}, this member says {:?}",
            b.pool, p.policy, b.policy
        ));
    }
    if let Some(want) = b.size {
        if p.size != Some(want) {
            let held = p.size.map_or("no size".to_string(), |n| n.to_string());
            return Some(format!(
                "pool {:?} was opened with size {}, this member says {}",
                b.pool, held, want
            ));
        }
    }
    None
}

struct Record {
    member: Member,
    publication: Option<u64>,
    expires_at: Instant,
    state_bytes: usize,
}

impl Record {
    fn delta_bytes(&self) -> usize {
        // JSON string escaping costs at most six bytes per source byte.
        256 + self.state_bytes + self.member.url.as_ref().map_or(0, |url| url.len() * 6)
    }
}

#[derive(Default)]
struct Deferred {
    waiters: Vec<Vec<Weak<Notify>>>,
    retired: Vec<Arc<PoolDelta>>,
}

impl Deferred {
    fn finish(self) {
        for waiters in self.waiters {
            for waiter in waiters {
                if let Some(bell) = waiter.upgrade() {
                    bell.notify_one();
                }
            }
        }
        // The last reference to a large cached snapshot is dropped here too,
        // after the registry lock has been released.
    }
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
    /// Highest tenure ever seen for a seat, kept even after the occupant is
    /// gone. Without it a superseded process could sit and wait for its
    /// replacement to die and then take the seat back -- the record vanishes
    /// on removal, so the pool would have no memory that the seat had moved on.
    /// Only slotted pools need this; interchangeable members get a fresh id
    /// each time, so an old id is never reused.
    high: HashMap<u64, u64>,
    /// Tenures that said goodbye, and when it stops mattering.
    ///
    /// A beat that timed out on the client was not necessarily lost: the
    /// caller stopped waiting, but the request can still arrive here
    /// afterwards -- saying leaving=false, on the tenure that has just said
    /// goodbye -- and put the member back for a whole lease, which is the one
    /// thing saying goodbye is meant to avoid.
    ///
    /// Only explicit goodbyes go in here. A lease that merely lapsed must not:
    /// that member is alive and simply missed some beats, and soft state is
    /// supposed to let its next beat put it back.
    gone: HashMap<u64, (u64, Instant)>,
    /// (version, member id) of each change, oldest first.
    log: VecDeque<(u64, u64)>,
    /// Callers parked on this pool with nothing to tell them yet.
    ///
    /// Per pool and not per registry, which is the whole cost of the thing:
    /// one shared bell woke every parked caller for a change in a pool none of
    /// them were watching. Measured at 40,000 parked, that was 70.8% of a core
    /// with nothing happening; ringing only the pool that moved brought it to
    /// 10.3%.
    ///
    /// Weak, so a caller that gave up and went away is not kept alive by the
    /// list it is still sitting in.
    waiters: Vec<Weak<Notify>>,
    cache: Mutex<DeltaCache>,
}

impl Pool {
    fn invalidate_cache(&mut self, deferred: &mut Deferred) {
        self.cache.get_mut().unwrap().clear(&mut deferred.retired);
    }

    fn bump(&mut self, id: u64, deferred: &mut Deferred) {
        self.version += 1;
        self.log.push_back((self.version, id));
        while self.log.len() > LOG_CAP {
            self.log.pop_front();
        }
        self.invalidate_cache(deferred);
        if !self.waiters.is_empty() {
            deferred.waiters.push(std::mem::take(&mut self.waiters));
        }
    }

    fn delta(&self, seen: Option<u64>, deferred: &mut Deferred) -> Option<Arc<PoolDelta>> {
        // Quiet subscribers need neither metadata copies nor a cache lookup.
        if seen == Some(self.version) {
            return None;
        }
        let oldest = self.log.front().map(|(v, _)| *v).unwrap_or(0);
        // All full-resync cursors share one snapshot. A future cursor belongs
        // to a previous registry process, not to this change log.
        let since = seen.filter(|v| *v < self.version && *v + 1 >= oldest);
        let mut cache = self.cache.lock().unwrap();
        if let Some(delta) = cache.get(since) {
            return Some(delta);
        }
        let (delta, bytes) = self.build_delta(since);
        let delta = Arc::new(delta);
        cache.insert(since, delta.clone(), bytes, &mut deferred.retired);
        Some(delta)
    }

    fn build_delta(&self, since: Option<u64>) -> (PoolDelta, usize) {
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
        let mut bytes = 256
            + self.policy.len() * 6
            + self.methods.iter().map(|m| m.len() * 6 + 3).sum::<usize>();
        match since {
            Some(v) => {
                let oldest = self.log.front().map(|(v, _)| *v).unwrap_or(0);
                let start = (v + 1 - oldest) as usize;
                let mut ids: Vec<u64> = self.log.range(start..).map(|(_, id)| *id).collect();
                ids.sort_unstable();
                ids.dedup();
                for id in ids {
                    match self.members.get(&id) {
                        Some(r) => {
                            bytes = bytes.saturating_add(r.delta_bytes());
                            d.changed.push(r.member.clone());
                        }
                        None => {
                            bytes = bytes.saturating_add(21);
                            d.removed.push(id);
                        }
                    }
                }
            }
            // Never synced, or fell off the end of the log.
            _ => {
                d.full = true;
                d.changed = self
                    .members
                    .values()
                    .map(|r| {
                        bytes = bytes.saturating_add(r.delta_bytes());
                        r.member.clone()
                    })
                    .collect();
            }
        }
        (d, bytes)
    }
}

pub struct Registry {
    pools: RwLock<HashMap<String, Pool>>,
    pub ttl: Duration,
    /// Identifies this registry process, so clients can tell a restart from a
    /// quiet period.
    epoch: u64,
}

impl Registry {
    pub fn new(ttl: Duration) -> Self {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1)
            | 1;
        Self {
            pools: RwLock::new(HashMap::new()),
            ttl,
            epoch,
        }
    }

    /// Rejects a beat that could damage state rather than merely being wrong.
    fn admissible(b: &Beat) -> Option<usize> {
        let valid = b.pool.len() <= MAX_NAME
            && b.incarnation <= MAX_INCARNATION
            && b.watch.len() <= MAX_WATCH
            && b.watch.iter().all(|w| w.len() <= MAX_NAME)
            && b.url.as_ref().is_none_or(|u| u.len() <= MAX_NAME)
            && b.methods.len() <= MAX_METHODS
            && b.methods.iter().all(|m| m.len() <= MAX_NAME);
        if valid {
            serialized_len(&b.state, MAX_STATE)
        } else {
            None
        }
    }

    pub fn beat(&self, b: &Beat) -> BeatAck {
        self.beat_shared(b).into_owned()
    }

    pub(crate) fn beat_shared(&self, b: &Beat) -> SharedBeatAck {
        let Some(state_bytes) = Self::admissible(b) else {
            return SharedBeatAck {
                epoch: self.epoch,
                protocol: tinyray_proto::PROTOCOL,
                version: env!("CARGO_PKG_VERSION").to_string(),
                ttl_ms: self.ttl.as_millis() as u64,
                accepted: false,
                refused: None,
                pools: HashMap::new(),
            };
        };
        let mut deferred = Deferred::default();
        let mut pools = self.pools.write().unwrap();
        let p = pools.entry(b.pool.clone()).or_default();
        if p.members.is_empty() {
            // The shape belongs to whoever is here now, not to whoever was
            // here first. An empty pool that pinned its first size forever
            // meant a job relaunched at a different world size silently kept
            // the old one.
            if p.policy != b.policy || p.size != b.size || p.methods != b.methods {
                p.invalidate_cache(&mut deferred);
                p.policy = b.policy.clone();
                p.size = b.size;
                p.methods = b.methods.clone();
            }
        } else if let Some(why) = disagreement(p, b) {
            // Silently keeping the first arrival's word turns a config typo
            // into a roll call that never completes: measured as every rank
            // waiting out its timeout on "4 of 8 present", with no missing
            // rank to find.
            return SharedBeatAck {
                epoch: self.epoch,
                protocol: tinyray_proto::PROTOCOL,
                version: env!("CARGO_PKG_VERSION").to_string(),
                ttl_ms: self.ttl.as_millis() as u64,
                accepted: false,
                refused: Some(why),
                pools: HashMap::new(),
            };
        } else if p.methods.is_empty() && !b.methods.is_empty() {
            // Only some members pass serves=; the first one through should not
            // decide that the pool serves nothing.
            p.invalidate_cache(&mut deferred);
            p.methods = b.methods.clone();
        }

        let stored = p.members.get(&b.id).map(|r| r.member.incarnation);
        let watermark = p.high.get(&b.id).copied().unwrap_or(0);
        // A beat this tenure sent before it said goodbye, arriving after it.
        // Measured at the 200ms lease floor: 6 of 300 leave() calls left the
        // member registered, with the goodbye itself reporting no failure.
        // A goodbye is exempt, so repeating one is not an error: leave() sends
        // one and the beat loop may send another behind it.
        let straggler = !b.leaving
            && p.gone
                .get(&b.id)
                .is_some_and(|(tenure, _)| b.incarnation <= *tenure);
        // Asking exclusively means "only if nobody holds it": the lease has
        // not lapsed, so somebody does.
        let occupied = b.exclusive && stored.is_some_and(|cur| cur != b.incarnation);
        let superseded = occupied
            || straggler
            || b.incarnation < watermark
            || stored.is_some_and(|cur| cur > b.incarnation);

        let accepted = !superseded;
        if superseded {
            // A ghost: a later tenure holds this seat, or held it and left.
        } else if b.leaving {
            if let Some(r) = p.members.remove(&b.id) {
                // XOR out what is actually stored. Using the beat's tenure
                // would leave a permanently wrong fingerprint if they differ.
                p.roster ^= r.member.roster_hash();
                p.bump(b.id, &mut deferred);
            }
            // Remembered for one lease: several times longer than any beat
            // still in flight, since a beat gives up at three quarters of an
            // interval and an interval is a quarter of the lease. The sweeper
            // drops it afterwards, so a pool that churns for a week does not
            // keep every id that ever left.
            p.gone
                .insert(b.id, (b.incarnation, Instant::now() + self.ttl));
        } else if stored == Some(b.incarnation) {
            let r = p.members.get_mut(&b.id).unwrap();
            r.expires_at = Instant::now() + self.ttl;
            let newer = match (r.publication, b.publication) {
                (None, _) => true,
                (Some(held), Some(incoming)) => incoming > held,
                (Some(_), None) => false,
            };
            if newer {
                let changed =
                    r.member.url != b.url || r.member.state != b.state || r.member.ready != b.ready;
                r.publication = b.publication;
                if changed {
                    r.member.url = b.url.clone();
                    r.member.state = b.state.clone();
                    r.member.ready = b.ready;
                    r.state_bytes = state_bytes;
                    p.bump(b.id, &mut deferred);
                }
            }
        } else {
            // New arrival, or a replacement taking over the seat.
            if let Some(r) = p.members.remove(&b.id) {
                p.roster ^= r.member.roster_hash();
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
            p.members.insert(
                b.id,
                Record {
                    member: m,
                    publication: b.publication,
                    expires_at: Instant::now() + self.ttl,
                    state_bytes,
                },
            );
            p.bump(b.id, &mut deferred);
        }
        if b.slot.is_some() && b.incarnation > watermark && !superseded {
            p.high.insert(b.id, b.incarnation);
        }

        let mut out = HashMap::new();
        for name in &b.watch {
            if let Some(wp) = pools.get(name) {
                if let Some(d) = wp.delta(b.seen.get(name).copied(), &mut deferred) {
                    out.insert(name.clone(), d);
                }
            }
        }
        let ack = SharedBeatAck {
            epoch: self.epoch,
            protocol: tinyray_proto::PROTOCOL,
            version: env!("CARGO_PKG_VERSION").to_string(),
            ttl_ms: self.ttl.as_millis() as u64,
            accepted,
            refused: None,
            pools: out,
        };
        drop(pools);
        deferred.finish();
        ack
    }

    /// Park `bell` on every pool in `watch`, to be rung when one of them moves.
    ///
    /// Registering costs one write lock, taken once per hold rather than once
    /// per re-check: the caller is told to look again, and looks for itself.
    pub fn park(&self, watch: &[String], bell: &Arc<Notify>) {
        let mut pools = self.pools.write().unwrap();
        for name in watch {
            pools
                .entry(name.clone())
                .or_default()
                .waiters
                .push(Arc::downgrade(bell));
        }
    }

    /// What a watcher would be told right now, without touching any state.
    pub fn deltas_for(&self, b: &Beat) -> HashMap<String, PoolDelta> {
        owned_pools(self.deltas_shared_for(b))
    }

    pub(crate) fn deltas_shared_for(&self, b: &Beat) -> SharedPools {
        let mut deferred = Deferred::default();
        let pools = self.pools.read().unwrap();
        let mut out = HashMap::new();
        for name in &b.watch {
            if let Some(wp) = pools.get(name) {
                if let Some(d) = wp.delta(b.seen.get(name).copied(), &mut deferred) {
                    out.insert(name.clone(), d);
                }
            }
        }
        drop(pools);
        deferred.finish();
        out
    }

    /// Drop members whose lease ran out. Runs on a timer, never on the request
    /// path -- an earlier version swept inside `lookup` and made it O(N).
    pub fn sweep(&self) -> usize {
        let now = Instant::now();
        let mut deferred = Deferred::default();
        let mut pools = self.pools.write().unwrap();
        let mut dropped = 0;
        for p in pools.values_mut() {
            // Departures stop mattering once the lease they had would have run
            // out, and dropping them here is what keeps that memory bounded.
            p.gone.retain(|_, (_, forget_at)| *forget_at > now);
            // A pool that never changes never drains its list, so callers that
            // timed out would pile up there. This is the only other place that
            // already walks every pool.
            p.waiters.retain(|w| w.strong_count() > 0);
            let dead: Vec<u64> = p
                .members
                .iter()
                .filter(|(_, r)| r.expires_at <= now)
                .map(|(id, _)| *id)
                .collect();
            for id in dead {
                if let Some(r) = p.members.remove(&id) {
                    p.roster ^= r.member.roster_hash();
                    p.bump(id, &mut deferred);
                    dropped += 1;
                }
            }
        }
        drop(pools);
        deferred.finish();
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

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
