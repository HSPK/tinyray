//! The heartbeat loop and the local roster cache.
//!
//! This runs on native OS threads owned by tokio, never on a Python thread.
//! That is the entire reason this crate is Rust: when the main thread sits
//! inside `dist.all_reduce()` holding the GIL, a Python thread cannot run and
//! the lease would expire -- declaring a healthy rank dead and voiding the
//! round. A native thread does not need the GIL as long as it never calls
//! into Python, and this loop never does.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tinyray_proto::{Beat, BeatAck, Member, PoolDelta};
use tokio::sync::Notify;

#[derive(Default)]
pub struct CachedPool {
    pub version: u64,
    pub roster: u64,
    pub methods: Vec<String>,
    pub size: Option<u64>,
    pub members: HashMap<u64, Member>,
    slots: HashMap<u64, BTreeSet<u64>>,
    ids: OnceLock<Vec<u64>>,
    ready_ids: OnceLock<Vec<u64>>,
    snapshots: Mutex<[Option<SerializedMembers>; 2]>,
    digest: Mutex<Option<(Vec<String>, u64)>>,
}

struct SerializedMembers {
    json: String,
    roster: u64,
}

// At most two strings per pool, never an entry per filter or revision.
const SNAPSHOT_BYTES: usize = 1024 * 1024;

impl CachedPool {
    fn unindex(&mut self, slot: u64, id: u64) {
        if let Some(ids) = self.slots.get_mut(&slot) {
            ids.remove(&id);
            if ids.is_empty() {
                self.slots.remove(&slot);
            }
        }
    }

    fn remove(&mut self, id: u64) -> bool {
        let Some(m) = self.members.remove(&id) else {
            return false;
        };
        if let Some(slot) = m.slot {
            self.unindex(slot, id);
        }
        true
    }

    fn apply(&mut self, d: &PoolDelta) {
        if d.full || !d.changed.is_empty() || !d.removed.is_empty() {
            *self.snapshots.get_mut().unwrap() = Default::default();
            *self.digest.get_mut().unwrap() = None;
        }
        let mut membership_changed = d.full;
        let mut readiness_changed = false;
        if d.full {
            self.members.clear();
            self.slots.clear();
        }
        for m in &d.changed {
            let old = self.members.insert(m.id, m.clone());
            membership_changed |= old.is_none();
            readiness_changed |= old.as_ref().map(|m| m.ready) != Some(m.ready);
            let old_slot = old.and_then(|m| m.slot);
            if old_slot != m.slot {
                if let Some(slot) = old_slot {
                    self.unindex(slot, m.id);
                }
                if let Some(slot) = m.slot {
                    self.slots.entry(slot).or_default().insert(m.id);
                }
            }
        }
        for id in &d.removed {
            membership_changed |= self.remove(*id);
        }
        if membership_changed {
            self.ids.take();
        }
        if membership_changed || readiness_changed {
            self.ready_ids.take();
        }
        self.version = d.version;
        self.roster = d.roster;
        self.methods.clone_from(&d.methods);
        self.size = d.size;
    }

    pub fn ids(&self, require_ready: bool) -> &[u64] {
        let ids = self.ids.get_or_init(|| {
            let mut ids: Vec<u64> = self.members.keys().copied().collect();
            ids.sort_unstable();
            ids
        });
        if require_ready {
            self.ready_ids.get_or_init(|| {
                ids.iter()
                    .copied()
                    .filter(|id| self.members[id].ready)
                    .collect()
            })
        } else {
            ids
        }
    }

    pub fn slot(&self, slot: u64, require_ready: bool) -> Option<&Member> {
        self.slots.get(&slot)?.iter().find_map(|id| {
            let m = &self.members[id];
            (!require_ready || m.ready).then_some(m)
        })
    }

    pub fn choose(
        &self,
        filter: &serde_json::Value,
        require_ready: bool,
        rng: &mut fastrand::Rng,
    ) -> Option<&Member> {
        if filter.as_object().is_none_or(|f| f.is_empty()) {
            let ids = self.ids(require_ready);
            return (!ids.is_empty()).then(|| &self.members[&ids[rng.usize(..ids.len())]]);
        }
        let mut chosen = None;
        let mut count = 0;
        for m in self.members.values() {
            if (!require_ready || m.ready) && m.matches(filter) {
                count += 1;
                if rng.usize(..count) == 0 {
                    chosen = Some(m);
                }
            }
        }
        chosen
    }

    pub fn serialized(&self, require_ready: bool) -> (String, u64) {
        let mut snapshots = self.snapshots.lock().unwrap();
        let cached = &mut snapshots[usize::from(require_ready)];
        if let Some(cached) = cached {
            return (cached.json.clone(), cached.roster);
        }
        let members: Vec<&Member> = self
            .ids(require_ready)
            .iter()
            .map(|id| &self.members[id])
            .collect();
        let roster = members.iter().fold(0, |h, m| h ^ m.roster_hash());
        let json = serde_json::to_string(&members).unwrap();
        if json.len() <= SNAPSHOT_BYTES {
            *cached = Some(SerializedMembers {
                json: json.clone(),
                roster,
            });
        }
        (json, roster)
    }

    pub fn field_digest(&self, fields: &[String]) -> u64 {
        let mut cached = self.digest.lock().unwrap();
        if let Some((keys, digest)) = cached.as_ref() {
            if keys == fields {
                return *digest;
            }
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for id in self.ids(false) {
            let m = &self.members[id];
            m.id.hash(&mut h);
            m.incarnation.hash(&mut h);
            for f in fields {
                match f.as_str() {
                    "ready" => m.ready.hash(&mut h),
                    "url" => m.url.hash(&mut h),
                    other => m
                        .state
                        .get(other)
                        .map(|v| v.to_string())
                        .unwrap_or_default()
                        .hash(&mut h),
                }
            }
        }
        let digest = h.finish();
        // A caller-controlled list of fields must not become an unbounded cache.
        if fields.len() <= 64 && fields.iter().map(String::len).sum::<usize>() <= 4096 {
            *cached = Some((fields.to_vec(), digest));
        }
        digest
    }
}

/// Everything the beat loop needs, without touching Python.
pub struct Shared {
    pub endpoint: String,
    pub pool: String,
    pub id: u64,
    pub slot: Option<u64>,
    pub incarnation: u64,
    pub policy: String,
    pub size: Option<u64>,
    pub methods: Vec<String>,
    /// The payload and its sequence are captured under one lock, including
    /// the URL: a sequence must never describe two different publications.
    pub published: Mutex<Published>,
    pub leaving: AtomicBool,
    pub exclusive: bool,
    pub watch: Mutex<Vec<String>>,
    pub cache: RwLock<HashMap<String, CachedPool>>,
    pub accepted: AtomicBool,
    pub beats_ok: AtomicU64,
    pub beats_failed: AtomicU64,
    /// Why the last beat failed. Every failure path used to discard its error,
    /// so a refused connection, a timeout, a peer that only speaks HTTP/1.1
    /// and a malformed reply were all reported the same way: silence.
    pub last_error: Mutex<String>,
    /// Set when the registry refused because the pool's shape was disagreed
    /// with, as opposed to the seat being held by a later tenure.
    pub refused: Mutex<String>,
    /// The published version the registry has acked. Only ever moves forward:
    /// a beat composed before a change can be answered after it, and that ack
    /// says nothing about the newer state.
    pub confirmed: AtomicU64,
    pub interval_ms: AtomicU64,
    pub coalesce_ms: u64,
    /// How long we let the registry sit on an answer that says nothing. Set to
    /// the interval we would otherwise have slept, so the request rate is the
    /// one the polling had and the delay before hearing about a change becomes
    /// a round trip instead of an interval.
    pub hold_ms: AtomicU64,
    /// Monotonic ms of the last successful beat. Freezing a round on a stale
    /// roster is unsafe, so epoch() needs to know when we are flying blind.
    pub last_ok_ms: AtomicU64,
    /// Which registry process the cache came from.
    pub seen_epoch: AtomicU64,
    /// What the registry last said it can do. Zero until the first ack, and
    /// zero afterwards if the registry is old enough not to say.
    pub registry_protocol: AtomicU64,
    pub registry_version: Mutex<String>,
    pub started: std::time::Instant,
    /// Rung when something we publish changes. Without it, subscribing to a
    /// pool or declaring readiness costs a full heartbeat interval of silence,
    /// which is long enough for short-lived peers to come and go unseen.
    pub wake: Notify,
    /// Rung when a beat from the loop has been acked. The synchronous first
    /// beat waits on this as well as on its own request, because either one
    /// landing means the caller is registered -- which is the only thing it
    /// was blocking for.
    pub acked: Notify,
    /// Rung once per beat, after the cache has been written. Waiters block on
    /// this instead of polling: every wait in the Python layer was a sleep
    /// loop, the tightest of them turning 500 times a second per pool.
    ///
    /// A std condvar rather than a tokio one on purpose -- it is waited on
    /// from Python threads, and it has to keep working after leave() has taken
    /// the runtime away.
    pub revision: Mutex<u64>,
    pub bell: Condvar,
    /// How many times the bell has actually woken somebody. The revision moves
    /// once a beat whether or not anyone cares; this counts the ones that had
    /// a waiter, which is what says whether watching is costing anything.
    pub wakeups: AtomicU64,
    /// Times the loop waited on a timer instead of on the registry.
    ///
    /// Only the path before the first ack should ever do this: with nobody
    /// answering there is nothing to park on, and hammering a dead registry
    /// is worse than waiting. Once a beat has landed the loop always parks,
    /// so a number that keeps climbing means this client is polling rather
    /// than being told -- which is the whole difference long polling buys.
    ///
    /// It is also the deterministic form of a bug that was otherwise only
    /// visible as a race: reading the per-request hold here rather than the
    /// loop's intent sent the loop to sleep unparked after every publish.
    /// Measured over twenty publishes: 0 against 12.
    pub short_polls: AtomicU64,
    /// Pipes written one byte at a time when the bell rings, so an event loop
    /// can wait on an fd instead of parking a thread in `wait_revision`. One
    /// per loop rather than one per client: a second loop in the same process
    /// would otherwise never be woken, and would hang rather than fail.
    /// Python owns the pipes and deregisters before closing them.
    pub wake_fds: Mutex<Vec<i32>>,
}

/// What this member is telling the pool about itself.
#[derive(Clone, PartialEq)]
pub struct Published {
    pub state: serde_json::Value,
    pub ready: bool,
    pub url: Option<String>,
    /// Bumped whenever the publication changes, under the same lock, so a beat
    /// composed from it carries a number that says exactly which version it is
    /// showing the registry. flush() needs that: counting beats cannot tell an
    /// ack for the state it published from an ack for the one before it.
    pub version: u64,
}

impl Shared {
    /// One tick of the bell, rung after the cache has been written so a waiter
    /// woken by it sees the new cache rather than the old one.
    pub fn ring(&self) {
        *self.revision.lock().unwrap() += 1;
        self.wakeups.fetch_add(1, Ordering::Relaxed);
        self.bell.notify_all();
        // One byte is enough: the reader drains and re-checks the revision it
        // actually cares about, so a full pipe is not a lost wakeup.
        // ManuallyDrop so the fds are not closed when these go out of scope:
        // Python owns them. The write ends are non-blocking, so a full pipe
        // fails here rather than stalling the beat loop, and that is the right
        // answer -- a byte already waiting says the same thing.
        use std::os::fd::FromRawFd;
        for fd in self.wake_fds.lock().unwrap().iter() {
            let f = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(*fd) });
            let _ = (&*f).write(&[1u8]);
        }
    }

    pub fn mark_ok(&self) {
        self.last_ok_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// Milliseconds since the last successful beat.
    pub fn silence_ms(&self) -> u64 {
        let now = self.started.elapsed().as_millis() as u64;
        now.saturating_sub(self.last_ok_ms.load(Ordering::Relaxed))
    }

    fn compose(&self) -> (Beat, u64) {
        let published = self.published.lock().unwrap().clone();
        let cache = self.cache.read().unwrap();
        let watch = self.watch.lock().unwrap().clone();
        let seen = watch
            .iter()
            .filter_map(|n| cache.get(n).map(|c| (n.clone(), c.version)))
            .collect();
        let beat = Beat {
            pool: self.pool.clone(),
            slot: self.slot,
            id: self.id,
            incarnation: self.incarnation,
            publication: Some(published.version),
            policy: self.policy.clone(),
            size: self.size,
            url: published.url,
            state: published.state,
            ready: published.ready,
            leaving: self.leaving.load(Ordering::Relaxed),
            exclusive: self.exclusive,
            methods: self.methods.clone(),
            watch,
            seen,
            hold_ms: self.hold_ms.load(Ordering::Relaxed),
        };
        (beat, published.version)
    }

    /// Returns false once the seat has been taken by a later tenure.
    fn note_registry(&self, ack: &BeatAck) {
        self.registry_protocol
            .store(ack.protocol as u64, Ordering::Relaxed);
        let mut v = self.registry_version.lock().unwrap();
        if *v != ack.version {
            v.clone_from(&ack.version);
        }
    }

    fn apply(&self, ack: &BeatAck) -> bool {
        // A restarted registry counts from zero again. Keeping a cache built
        // against the old numbering means asking for changes since a version
        // it has never reached, being told there are none, and holding a stale
        // roster forever with nothing to show for it.
        let previous = self.seen_epoch.swap(ack.epoch, Ordering::Relaxed);
        let restarted = previous != 0 && previous != ack.epoch;
        if restarted {
            self.cache.write().unwrap().clear();
        }
        if !ack.accepted {
            self.accepted.store(false, Ordering::Relaxed);
            if let Some(why) = &ack.refused {
                *self.refused.lock().unwrap() = why.clone();
            }
            return false;
        }
        let mut cache = self.cache.write().unwrap();
        for (name, d) in &ack.pools {
            // An incremental delta answers "what changed since version V", and
            // after a restart the V we asked from was issued by the previous
            // process. This one has never used that numbering, so the answer
            // silently omits everyone it placed at or below that number, and
            // the version it comes with says we are up to date -- so nothing
            // ever asks again. Measured: two members registered, the client
            // holding one of them, and its roster fingerprint equal to the
            // registry's, which is what an epoch freezes on.
            //
            // A full roster describes itself and is safe whoever numbered it.
            // Dropping the rest leaves no entry for the pool, so the next beat
            // asks with no position at all and is sent one.
            //
            // Note for whoever tries to test the `clear()` below: a restart
            // empties the whole cache above, so the only way to reach a full
            // roster with stale entries under it is to fall off the change log
            // -- 4096 versions behind. That is hard to build on purpose,
            // because the registry collapses a member's changes to one per
            // beat: 4,300 rapid updates moved the version by 8. It stays
            // reachable at scale, where a large pool can move that far during
            // a stall shorter than a lease, so the clear stays.
            // Kept without a mutant aimed at it, which wants saying. The
            // registry now answers a position it never issued with a full
            // roster, and clearing the cache above drops the `seen` we would
            // have asked from, so the next beat asks fresh and is answered in
            // full either way. Tried to build a case where this line is what
            // saves us and could not: a restart with the watcher's position
            // below the new version, which is the only path left, still ends
            // with the whole roster. It stays because an older registry
            // answers such a position incrementally and this is the only thing
            // that would notice.
            if restarted && !d.full {
                continue;
            }
            let c = cache.entry(name.clone()).or_default();
            if d.version < c.version {
                continue;
            }
            c.apply(d);
        }
        // There used to be a loop here recording every watched pool as empty,
        // for fear that a pool we asked about and heard nothing back for would
        // stay missing and make the first lookup wait. It never did anything:
        // the registry creates a pool the moment somebody watches it, and the
        // first delta for a name we have not seen is a full one, so the entry
        // arrives in the answer. Measured with the loop taken out -- the whole
        // suite green, the cache entry still there for a pool nobody ever
        // joined, and the first lookup of one still 41ms against 41ms.
        true
    }
}

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

/// hyper's connector leaves Nagle on, which pairs with the peer's delayed ACK
/// to add a fixed stall to a small write that follows another one closely.
fn nodelay_connector() -> hyper_util::client::legacy::connect::HttpConnector {
    let mut c = hyper_util::client::legacy::connect::HttpConnector::new();
    c.set_nodelay(true);
    c
}

/// Leave the rest of the lease available for a timed-out request and a retry.
pub fn coalesce_gap(requested_ms: u64, interval_ms: u64) -> Duration {
    Duration::from_millis(requested_ms.min(interval_ms).min(30_000))
}

async fn coalesce(shared: &Shared, started: tokio::time::Instant, interruptible: bool) {
    let gap = coalesce_gap(
        shared.coalesce_ms,
        shared.interval_ms.load(Ordering::Relaxed),
    );
    let delay = gap.saturating_sub(started.elapsed());
    if !delay.is_zero() {
        if interruptible {
            let _ = tokio::time::timeout(delay, shared.wake.notified()).await;
        } else {
            tokio::time::sleep(delay).await;
        }
    }
}

/// The serial loop must retry a lost request well before its lease expires.
fn beat_timeout(interval_ms: u64, hold_ms: u64) -> Duration {
    if hold_ms == 0 {
        return Duration::from_millis(interval_ms.clamp(50, 30_000) * 3 / 4);
    }
    // The registry may sit on this for `hold_ms` plus its own jitter, and that
    // is the answer arriving on time rather than late. Giving up before then
    // would turn every quiet interval into a failed beat.
    //
    // Keep the normal network margin, but bound it by the hold at short
    // leases. At the 200ms TTL floor this gives 100ms, not 275ms: waiting
    // longer than the lease before retrying expires a healthy upstream.
    Duration::from_millis(hold_ms + hold_ms / 2 + (hold_ms / 2).min(200))
}

async fn post(
    http: &HttpClient,
    endpoint: &str,
    beat: &Beat,
    budget: Duration,
) -> Result<BeatAck, String> {
    let body = Full::new(Bytes::from(
        serde_json::to_vec(beat).map_err(|e| format!("cannot encode the beat: {e}"))?,
    ));
    let req = hyper::Request::builder()
        .method("POST")
        .uri(format!("{endpoint}/v1/beat"))
        .header("content-type", "application/json")
        .body(body)
        .map_err(|e| format!("cannot build the request: {e}"))?;
    let deadline = tokio::time::Instant::now() + budget;
    let resp = tokio::time::timeout_at(deadline, http.request(req))
        .await
        .map_err(|_| format!("no reply within {}ms", budget.as_millis()))?
        .map_err(|e| {
            // Only once the socket is up is h2c a plausible culprit. Saying it
            // on a refused connection or a name that will not resolve sends
            // the reader hunting for a proxy that is not the problem.
            if e.is_connect() {
                format!("cannot reach it: {e}")
            } else {
                format!(
                    "{e}; the connection came up but the exchange did not -- \
                     this client speaks h2c only, so whatever answered has to too"
                )
            }
        })?;
    if !resp.status().is_success() {
        // A 4xx is our fault and retrying will not fix it, but it is still not
        // an answer, so it counts as a failed beat rather than a hang.
        return Err(format!(
            "the registry answered HTTP {}",
            resp.status().as_u16()
        ));
    }
    let bytes = tokio::time::timeout_at(deadline, resp.into_body().collect())
        .await
        .map_err(|_| format!("reply body stalled past {}ms", budget.as_millis()))?
        .map_err(|e| format!("reply body broke off: {e}"))?
        .to_bytes();
    serde_json::from_slice(&bytes).map_err(|e| format!("the reply is not a BeatAck: {e}"))
}

pub fn spawn(shared: Arc<Shared>) -> tokio::runtime::Runtime {
    // Two workers, fixed. tokio defaults to one per core, which on a 128-core
    // trainer node means 128 threads competing with the job for CPU.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("tinyray")
        .build()
        .expect("tokio runtime");

    rt.spawn(async move {
        // `.timer()` is required or hyper panics with "You must supply a timer".
        // h2c with prior knowledge. HTTP/1.1 cannot multiplex, so every
        // concurrent call needs its own socket -- the churn that drained the
        // ephemeral port range and drove throughput to zero. The registry
        // negotiates either, so asking for h2 costs nothing.
        let http: HttpClient = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .http2_only(true)
            .pool_idle_timeout(Duration::from_secs(60))
            .build(nodelay_connector());
        let mut cancelled_last = false;
        loop {
            if shared.leaving.load(Ordering::Relaxed) && shared.beats_ok.load(Ordering::Relaxed) > 0
            {
                // One final beat carrying `leaving` was already sent by leave().
                return;
            }
            let started = tokio::time::Instant::now();
            let (mut beat, showing) = shared.compose();
            let interval = shared.interval_ms.load(Ordering::Relaxed);
            // The request that replaces a cancelled one is never parked. It
            // carries something the last one did not, so there is nothing to
            // wait for, and completing it at round-trip speed is what bounds
            // the cost of not being allowed to cancel it: one RTT rather than
            // a whole hold. Measured as subscription latency going from 432ms
            // against a 500ms interval back under 125ms.
            let hold = if cancelled_last {
                0
            } else {
                shared.hold_ms.load(Ordering::Relaxed)
            };
            beat.hold_ms = hold;
            let budget = beat_timeout(interval, hold);
            // A held request is the loop's resting place, so this is also
            // where a publish has to be able to interrupt it. Dropping the
            // future resets the stream; the registry has already renewed the
            // lease from the request that arrived, and a beat is idempotent,
            // so nothing is lost by abandoning the answer.
            let sending = post(&http, &shared.endpoint, &beat, budget);
            tokio::pin!(sending);
            let outcome = tokio::select! {
                done = &mut sending => Some(done),
                _ = shared.wake.notified(), if hold > 0 => None,
            };
            // A publisher faster than the round trip always has something
            // newer to say, so if every publish could cancel, no request would
            // ever be answered: the cache stops updating and flush() never
            // returns, while the member stays registered and looks fine. The
            // replacement is sent unparked (above), which both makes it
            // uncancellable here and lets it complete at round-trip speed.
            cancelled_last = outcome.is_none();
            let Some(outcome) = outcome else {
                // Something to publish. Go straight back round with it, but
                // no faster than the configured gap: a process publishing flat out would
                // otherwise turn the loop into a request generator bounded
                // only by the round trip. Measured under an unthrottled
                // publisher, this halves it -- 24 to 11 requests a second.
                // What keeps such a publisher *alive* is the unparked
                // replacement above, not this.
                coalesce(&shared, started, false).await;
                continue;
            };
            match outcome {
                Ok(ack) => {
                    shared.interval_ms.store(ack.ttl_ms / 4, Ordering::Relaxed);
                    // Ask to be parked for the interval we would have slept.
                    // Same number of requests as the polling this replaces;
                    // the difference is that the answer now arrives when
                    // something happens rather than when the timer says so.
                    shared
                        .hold_ms
                        .store((ack.ttl_ms / 4).clamp(50, 30_000), Ordering::Relaxed);
                    shared.note_registry(&ack);
                    let alive = shared.apply(&ack);
                    shared.beats_ok.fetch_add(1, Ordering::Relaxed);
                    shared.mark_ok();
                    // Only an accepted beat left the state anywhere. A refusal
                    // -- the seat taken by a later tenure, a state over the
                    // size cap, a pool whose shape we disagree with -- comes
                    // back as an ordinary reply with `accepted: false`, and
                    // the registry never stored what it carried. Counting it
                    // as confirmed made flush() report that the registry had
                    // the state when it had refused it: measured as
                    // test_flush_says_the_seat_was_taken returning instead of
                    // raising, intermittently, depending on whether the loop
                    // got a refusal in before it stopped.
                    if alive {
                        shared.confirmed.fetch_max(showing, Ordering::Relaxed);
                    }
                    shared.acked.notify_one();
                    if !alive {
                        // Superseded. Beating on would only be waiting for the
                        // replacement to die so we could take the seat back.
                        shared.ring();
                        return;
                    }
                }
                // Losing the registry is survivable: lookups keep working from
                // cache, and the roster regrows within one interval when it
                // comes back. Nothing here needs to escalate.
                Err(why) => {
                    shared.beats_failed.fetch_add(1, Ordering::Relaxed);
                    *shared.last_error.lock().unwrap() = why;
                }
            }
            // Whatever the beat did, somebody may be waiting on the answer.
            shared.ring();
            // Whether to sleep is decided by what this loop *intends* to do,
            // not by what the last request happened to ask for. Reading the
            // per-request `hold` here confused an unparked replacement with a
            // registry too old to park anything, and sent the loop to sleep
            // for a whole interval -- unparked, so the registry could no
            // longer reach it. Measured on a superseded member: it took 940ms
            // to notice it had been fenced, against 1-2ms when parked.
            if shared.hold_ms.load(Ordering::Relaxed) == 0 {
                shared.short_polls.fetch_add(1, Ordering::Relaxed);
                let ms = shared.interval_ms.load(Ordering::Relaxed).clamp(50, 30_000);
                // Wake early if the process has something new to say.
                let _ =
                    tokio::time::timeout(Duration::from_millis(ms), shared.wake.notified()).await;
            } else {
                // The wait now happens inside the request, so the only reason
                // to pause is to keep a pool that changes constantly from
                // turning this into a spin: the answer would come back at once
                // every time, and we would ask again just as fast.
                coalesce(&shared, started, true).await;
            }
        }
    });
    rt
}

/// Send one beat synchronously, used by join() and leave() so that arrival and
/// departure are visible immediately instead of at the next tick.
///
/// `stop_when_registered` is for join(): the loop is already beating alongside
/// this call, so the caller can be registered by an ack this request knows
/// nothing about. Measured on a 40%-loss link, `join(timeout=30)` six times:
/// the loop was acked at 0.01s and this call still sat there until its 5s
/// budget ran out, three times out of six. leave() passes false -- its beat
/// carries `leaving`, and no other beat can say that for it.
pub fn beat_once(
    rt: &tokio::runtime::Runtime,
    shared: &Arc<Shared>,
    budget: Duration,
    stop_when_registered: bool,
) -> bool {
    let s = shared.clone();
    rt.block_on(async move {
        // Already done by the loop before we even started: nothing to send.
        if stop_when_registered && s.beats_ok.load(Ordering::Relaxed) > 0 {
            return true;
        }
        let http: HttpClient = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .http2_only(true)
            .build(nodelay_connector());
        let (mut beat, showing) = s.compose();
        // One-shot, with a caller waiting: never parked, and given the
        // caller's budget rather than the loop's. A fixed five seconds here
        // meant join(timeout=) could not make the call shorter -- only longer.
        beat.hold_ms = 0;
        let sending = post(&http, &s.endpoint, &beat, budget);
        tokio::pin!(sending);
        // `notify_one` leaves a permit when nobody is waiting, so an ack that
        // lands between the check above and this select is not missed. A
        // permit left over from an older ack cannot mislead us: it implies
        // beats_ok > 0, which returned already.
        let landed = tokio::select! {
            done = &mut sending => Some(done),
            _ = s.acked.notified(), if stop_when_registered => None,
        };
        let Some(landed) = landed else {
            // The loop got there first. Dropping the request in flight is what
            // the loop already does to swap a parked beat for a fresher one:
            // a beat is idempotent and the lease is renewed by the one that
            // arrived, so there is nothing to finish.
            return true;
        };
        let out = match landed {
            Ok(ack) => {
                s.interval_ms.store(ack.ttl_ms / 4, Ordering::Relaxed);
                s.hold_ms
                    .store((ack.ttl_ms / 4).clamp(50, 30_000), Ordering::Relaxed);
                s.note_registry(&ack);
                let alive = s.apply(&ack);
                s.beats_ok.fetch_add(1, Ordering::Relaxed);
                s.mark_ok();
                // Same rule as the loop: a refusal is an ordinary reply that
                // stored nothing, so it confirms nothing.
                if alive {
                    s.confirmed.fetch_max(showing, Ordering::Relaxed);
                }
                true
            }
            Err(why) => {
                s.beats_failed.fetch_add(1, Ordering::Relaxed);
                *s.last_error.lock().unwrap() = why;
                // The request failed, but the loop may have landed one while
                // it was failing -- and the caller asked to be registered,
                // not to have this particular packet arrive.
                stop_when_registered && s.beats_ok.load(Ordering::Relaxed) > 0
            }
        };
        s.ring();
        out
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tinyray_proto::PoolDelta;

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
        let members: Vec<Member> = serde_json::from_str(&raw).unwrap();
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
    fn native_selection_is_uniform_and_filters_only_eligible_members() {
        let mut rng = fastrand::Rng::with_seed(0x715E1EC7);
        let mut c = CachedPool::default();
        c.apply(&delta(
            1,
            true,
            (0..8).map(|id| member(id, Some(id + 10), id < 7)).collect(),
            vec![],
        ));
        for filter in [json!({}), json!({"nested": {"values": [3.0]}})] {
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
        assert!(s.apply(&ack(
            1,
            true,
            delta(5, true, vec![member(42, Some(1), true)], vec![])
        )));
        {
            let cache = s.cache.read().unwrap();
            assert_eq!(cache["p"].slot(1, true).unwrap().id, 42);
            decoded(&cache["p"], true);
        }
        assert!(s.apply(&ack(
            1,
            true,
            delta(4, true, vec![member(41, Some(2), true)], vec![])
        )));
        {
            let cache = s.cache.read().unwrap();
            assert!(cache["p"].slot(2, false).is_none());
            assert_eq!(decoded(&cache["p"], true)[0].id, 42);
        }
        assert!(s.apply(&ack(2, true, delta(1, false, vec![], vec![]))));
        assert!(!s.cache.read().unwrap().contains_key("p"));
        assert!(s.apply(&ack(
            2,
            true,
            delta(1, true, vec![member(17, Some(3), true)], vec![])
        )));
        assert!(!s.apply(&ack(2, false, delta(2, true, vec![], vec![]))));
        assert!(!s.accepted.load(Ordering::Relaxed));
        {
            let cache = s.cache.read().unwrap();
            assert!(cache["p"].slot(1, false).is_none());
            assert_eq!(decoded(&cache["p"], true)[0].id, 17);
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

    fn shared() -> Shared {
        Shared {
            endpoint: "http://127.0.0.1:1".into(),
            pool: "p".into(),
            id: 1,
            slot: None,
            incarnation: 1,
            policy: "churn".into(),
            size: None,
            methods: Vec::new(),
            published: Mutex::new(Published {
                state: json!({}),
                ready: false,
                url: None,
                version: 0,
            }),
            leaving: AtomicBool::new(false),
            exclusive: false,
            watch: Mutex::new(Vec::new()),
            cache: RwLock::new(HashMap::new()),
            accepted: AtomicBool::new(true),
            beats_ok: AtomicU64::new(0),
            beats_failed: AtomicU64::new(0),
            last_error: Mutex::new(String::new()),
            refused: Mutex::new(String::new()),
            confirmed: AtomicU64::new(0),
            interval_ms: AtomicU64::new(1000),
            coalesce_ms: 50,
            hold_ms: AtomicU64::new(0),
            last_ok_ms: AtomicU64::new(0),
            seen_epoch: AtomicU64::new(0),
            registry_protocol: AtomicU64::new(0),
            registry_version: Mutex::new(String::new()),
            started: std::time::Instant::now(),
            wake: Notify::new(),
            acked: Notify::new(),
            revision: Mutex::new(0),
            bell: Condvar::new(),
            wakeups: AtomicU64::new(0),
            short_polls: AtomicU64::new(0),
            wake_fds: Mutex::new(Vec::new()),
        }
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
        assert!(s.apply(&ack(1, 2, json!("new"))));
        assert!(s.apply(&ack(1, 1, json!("old"))));
        {
            let cache = s.cache.read().unwrap();
            assert_eq!(cache["p"].version, 2);
            assert_eq!(cache["p"].members[&1].state, json!("new"));
        }
        assert!(s.apply(&ack(2, 1, json!("restarted"))));
        let cache = s.cache.read().unwrap();
        assert_eq!(cache["p"].version, 1);
        assert_eq!(cache["p"].members[&1].state, json!("restarted"));
    }
}
