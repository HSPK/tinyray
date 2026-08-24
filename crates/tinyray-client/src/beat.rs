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
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;
use tinyray_proto::{Beat, BeatAck, Member};
use tokio::sync::Notify;

#[derive(Default)]
pub struct CachedPool {
    pub version: u64,
    pub roster: u64,
    pub methods: Vec<String>,
    pub size: Option<u64>,
    pub members: HashMap<u64, Member>,
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
    pub url: Mutex<Option<String>>,
    /// State and readiness together, because they are published together. Read
    /// apart, a beat could carry a new state under the previous readiness --
    /// the pair is what `ready(**kw)` means, so the pair is what is locked.
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
    pub interval_ms: AtomicU64,
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

    fn compose(&self) -> Beat {
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
            policy: self.policy.clone(),
            size: self.size,
            url: self.url.lock().unwrap().clone(),
            state: published.state,
            ready: published.ready,
            leaving: self.leaving.load(Ordering::Relaxed),
            exclusive: self.exclusive,
            methods: self.methods.clone(),
            watch,
            seen,
            hold_ms: self.hold_ms.load(Ordering::Relaxed),
        };
        beat
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

    fn apply(&self, ack: &BeatAck, watch: &[String]) -> bool {
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
            if restarted && !d.full {
                continue;
            }
            let c = cache.entry(name.clone()).or_default();
            if d.full {
                c.members.clear();
            }
            for m in &d.changed {
                c.members.insert(m.id, m.clone());
            }
            for id in &d.removed {
                c.members.remove(id);
            }
            c.version = d.version;
            c.roster = d.roster;
            c.methods = d.methods.clone();
            c.size = d.size;
        }
        // A pool we asked about and heard nothing back for is empty, not
        // unknown. Without this an entry stays missing and the first lookup
        // cannot tell "nobody joined" from "I subscribed a moment ago" -- it
        // calls the pool empty either way, about a pool that may be full.
        // Skipped right after a restart, when the cleared cache really is
        // ignorance rather than an answer.
        if !restarted {
            for name in watch {
                cache.entry(name.clone()).or_default();
            }
        }
        true
    }
}

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

/// A beat is small and local; anything slower than this is a lost packet, not
/// a slow server. Without a deadline a single dropped packet hangs the caller
/// forever, which is exactly what "failure must be explicitly bounded" is for.
///
/// The loop is serial, so this deadline is also how long a lost packet stops
/// us beating. It has to stay well inside the interval, or one drop costs the
/// lease: at a 500 ms interval a five-second timeout meant a single lost packet
/// took the member out of the roster.
/// The closest two beats from one member may be. Only reachable when a watched
/// pool is changing constantly, and it caps that at twenty beats a second
/// rather than as fast as the network will go.
const MIN_GAP: Duration = Duration::from_millis(50);

fn beat_timeout(interval_ms: u64, hold_ms: u64) -> Duration {
    if hold_ms == 0 {
        return Duration::from_millis(interval_ms.clamp(50, 30_000) * 3 / 4);
    }
    // The registry may sit on this for `hold_ms` plus its own jitter, and that
    // is the answer arriving on time rather than late. Giving up before then
    // would turn every quiet interval into a failed beat.
    //
    // Proportional, never a flat margin. A constant put the deadline past the
    // lease at short leases -- 2.6s of waiting against a 2s lease -- so one
    // dropped packet cost the seat, which is exactly what a bounded beat is
    // for. Held at hold * 1.5 it stays at 0.44 of a lease whatever the lease.
    Duration::from_millis(hold_ms + hold_ms / 2 + 200)
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
    let resp = tokio::time::timeout(budget, http.request(req))
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
    let bytes = tokio::time::timeout(budget, resp.into_body().collect())
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
            .build_http();
        let mut cancelled_last = false;
        loop {
            if shared.leaving.load(Ordering::Relaxed) && shared.beats_ok.load(Ordering::Relaxed) > 0
            {
                // One final beat carrying `leaving` was already sent by leave().
                return;
            }
            let started = std::time::Instant::now();
            let mut beat = shared.compose();
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
                // no faster than MIN_GAP: a process publishing flat out would
                // otherwise turn the loop into a request generator bounded
                // only by the round trip. Measured under an unthrottled
                // publisher, this halves it -- 24 to 11 requests a second.
                // What keeps such a publisher *alive* is the unparked
                // replacement above, not this.
                let spent = started.elapsed();
                if spent < MIN_GAP {
                    tokio::time::sleep(MIN_GAP - spent).await;
                }
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
                    let alive = shared.apply(&ack, &beat.watch);
                    shared.beats_ok.fetch_add(1, Ordering::Relaxed);
                    shared.mark_ok();
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
                let spent = started.elapsed();
                if spent < MIN_GAP {
                    let _ = tokio::time::timeout(MIN_GAP - spent, shared.wake.notified()).await;
                }
            }
        }
    });
    rt
}

/// Send one beat synchronously, used by join() and leave() so that arrival and
/// departure are visible immediately instead of at the next tick.
pub fn beat_once(rt: &tokio::runtime::Runtime, shared: &Arc<Shared>) -> bool {
    let s = shared.clone();
    rt.block_on(async move {
        let http: HttpClient = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .http2_only(true)
            .build_http();
        let mut beat = s.compose();
        // One-shot, with a caller waiting: never parked, and given a fixed
        // budget rather than the loop's.
        beat.hold_ms = 0;
        let out = match post(&http, &s.endpoint, &beat, Duration::from_secs(5)).await {
            Ok(ack) => {
                s.interval_ms.store(ack.ttl_ms / 4, Ordering::Relaxed);
                s.hold_ms
                    .store((ack.ttl_ms / 4).clamp(50, 30_000), Ordering::Relaxed);
                s.note_registry(&ack);
                s.apply(&ack, &beat.watch);
                s.beats_ok.fetch_add(1, Ordering::Relaxed);
                s.mark_ok();
                true
            }
            Err(why) => {
                s.beats_failed.fetch_add(1, Ordering::Relaxed);
                *s.last_error.lock().unwrap() = why;
                false
            }
        };
        s.ring();
        out
    })
}
