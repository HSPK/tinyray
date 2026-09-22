use super::*;

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
    /// Rung after every beat outcome. Registration, publication confirmation,
    /// and transport diagnostics wait here without waking discovery callers.
    pub beat_revision: Mutex<u64>,
    pub beat_bell: Condvar,
    pub beat_notify: Notify,
    /// Rung only when cache or lifecycle state changes. Waiters block on this
    /// instead of polling: every wait in the Python layer was a sleep loop,
    /// the tightest of them turning 500 times a second per pool.
    ///
    /// A std condvar rather than a tokio one on purpose -- it is waited on
    /// from Python threads, and it has to keep working after leave() has taken
    /// the runtime away.
    pub revision: Mutex<u64>,
    pub bell: Condvar,
    pub revision_notify: Notify,
    /// How many semantic cache/lifecycle wakeups were broadcast.
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
    /// Registry sockets live on the heartbeat runtime, but fork duplicates
    /// their descriptors into a child whose runtime workers no longer exist.
    pub registry_fds: FdTable,
    /// Physical registry TCP connections opened by this client. The heartbeat
    /// loop should keep this nearly constant while beats_ok keeps rising.
    pub registry_connects: AtomicU64,
    /// Beat requests sent over an already-open registry connection.
    pub registry_reuses: AtomicU64,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: String,
        pool: String,
        id: u64,
        incarnation: u64,
        policy: String,
        slot: Option<u64>,
        size: Option<u64>,
        url: Option<String>,
        methods: Vec<String>,
        exclusive: bool,
        coalesce_ms: u64,
    ) -> Self {
        Self {
            endpoint,
            pool,
            id,
            slot,
            incarnation,
            policy,
            size,
            methods,
            published: Mutex::new(Published {
                state: serde_json::Value::Object(Default::default()),
                ready: false,
                url,
                version: 0,
            }),
            leaving: AtomicBool::new(false),
            exclusive,
            watch: Mutex::new(Vec::new()),
            cache: RwLock::new(HashMap::new()),
            accepted: AtomicBool::new(true),
            beats_ok: AtomicU64::new(0),
            beats_failed: AtomicU64::new(0),
            last_error: Mutex::new(String::new()),
            refused: Mutex::new(String::new()),
            confirmed: AtomicU64::new(0),
            interval_ms: AtomicU64::new(1000),
            coalesce_ms,
            hold_ms: AtomicU64::new(0),
            last_ok_ms: AtomicU64::new(0),
            seen_epoch: AtomicU64::new(0),
            registry_protocol: AtomicU64::new(0),
            registry_version: Mutex::new(String::new()),
            started: Instant::now(),
            wake: Notify::new(),
            acked: Notify::new(),
            beat_revision: Mutex::new(0),
            beat_bell: Condvar::new(),
            beat_notify: Notify::new(),
            revision: Mutex::new(0),
            bell: Condvar::new(),
            revision_notify: Notify::new(),
            wakeups: AtomicU64::new(0),
            short_polls: AtomicU64::new(0),
            wake_fds: Mutex::new(Vec::new()),
            registry_fds: FdTable::new(),
            registry_connects: AtomicU64::new(0),
            registry_reuses: AtomicU64::new(0),
        }
    }

    /// One semantic cache/lifecycle tick, rung after state has been written so
    /// a waiter woken by it sees the new value rather than the old one.
    pub fn ring(&self) {
        *self.revision.lock().unwrap() += 1;
        self.wakeups.fetch_add(1, Ordering::Relaxed);
        self.bell.notify_all();
        self.revision_notify.notify_waiters();
        // One byte is enough: the reader drains and re-checks the revision it
        // actually cares about, so a full pipe is not a lost wakeup.
        // ManuallyDrop so the fds are not closed when these go out of scope:
        // Python owns them. The write ends are non-blocking, so a full pipe
        // fails here rather than stalling the beat loop, and that is the right
        // answer -- a byte already waiting says the same thing.
        #[cfg(unix)]
        {
            use std::os::fd::FromRawFd;
            for fd in self.wake_fds.lock().unwrap().iter() {
                let f = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(*fd) });
                let _ = (&*f).write(&[1u8]);
            }
        }
    }

    pub fn note_beat(&self) {
        *self.beat_revision.lock().unwrap() += 1;
        self.beat_bell.notify_all();
        self.beat_notify.notify_waiters();
    }

    pub fn beat_revision(&self) -> u64 {
        *self.beat_revision.lock().unwrap()
    }

    pub fn wait_beat_revision(&self, since: u64, timeout: Duration) -> u64 {
        let revision = self.beat_revision.lock().unwrap();
        if *revision != since {
            return *revision;
        }
        let (revision, _) = self.beat_bell.wait_timeout(revision, timeout).unwrap();
        *revision
    }

    pub fn wait_registered(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let revision = self.beat_revision.lock().unwrap();
            if self.beats_ok.load(Ordering::Relaxed) > 0 {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            drop(
                self.beat_bell
                    .wait_timeout(revision, deadline - now)
                    .unwrap(),
            );
        }
    }

    pub fn wait_publication(&self, wanted: u64, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let revision = self.beat_revision.lock().unwrap();
            if self.confirmed.load(Ordering::Relaxed) >= wanted
                || !self.accepted.load(Ordering::Relaxed)
            {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            drop(
                self.beat_bell
                    .wait_timeout(revision, deadline - now)
                    .unwrap(),
            );
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

    pub fn epoch_valid(&self, pool: &str, roster: u64) -> bool {
        self.accepted.load(Ordering::Relaxed)
            && self
                .cache
                .read()
                .unwrap()
                .get(pool)
                .is_none_or(|cached| cached.roster == roster)
    }

    pub fn wait_epoch(
        &self,
        pool: &str,
        minimum: Option<i128>,
        timeout: Duration,
    ) -> EpochWaitResult {
        let deadline = Instant::now() + timeout;
        loop {
            let revision = self.revision.lock().unwrap();
            if !self.accepted.load(Ordering::Relaxed) {
                return EpochWaitResult {
                    status: WaitStatus::Fenced,
                    view: None,
                    found: 0,
                    target: minimum.unwrap_or_default(),
                    mismatched: false,
                    seen_pool: false,
                    silence_ms: self.silence_ms(),
                };
            }
            let lease_ms = self
                .interval_ms
                .load(Ordering::Relaxed)
                .saturating_mul(4)
                .max(1000);
            let silence_ms = self.silence_ms();
            if silence_ms > lease_ms {
                return EpochWaitResult {
                    status: WaitStatus::Stale,
                    view: None,
                    found: 0,
                    target: minimum.unwrap_or_default(),
                    mismatched: false,
                    seen_pool: false,
                    silence_ms,
                };
            }

            let mut result = {
                let cache = self.cache.read().unwrap();
                match cache.get(pool) {
                    None => EpochWaitResult {
                        status: WaitStatus::Pending,
                        view: None,
                        found: 0,
                        target: minimum.unwrap_or_default(),
                        mismatched: false,
                        seen_pool: false,
                        silence_ms,
                    },
                    Some(cached) => {
                        let Some(target) = minimum.or(cached.size.map(i128::from)) else {
                            return EpochWaitResult {
                                status: WaitStatus::NoSize,
                                view: None,
                                found: 0,
                                target: 0,
                                mismatched: false,
                                seen_pool: true,
                                silence_ms,
                            };
                        };
                        let view = cached.frozen(true);
                        let found = view.members.len();
                        let mismatched = view.members.roster() != view.roster;
                        let ready = (found as i128) >= target && !mismatched;
                        EpochWaitResult {
                            status: if ready {
                                WaitStatus::Ready
                            } else {
                                WaitStatus::Pending
                            },
                            view: ready.then_some(view),
                            found,
                            target,
                            mismatched,
                            seen_pool: true,
                            silence_ms,
                        }
                    }
                }
            };
            if result.status == WaitStatus::Ready {
                if !self.accepted.load(Ordering::Relaxed) {
                    result.status = WaitStatus::Fenced;
                    result.view = None;
                }
                return result;
            }
            let now = Instant::now();
            if now >= deadline {
                result.status = if result.mismatched && (result.found as i128) >= result.target {
                    WaitStatus::Mismatch
                } else {
                    WaitStatus::Timeout
                };
                return result;
            }
            let stale_in = Duration::from_millis(lease_ms.saturating_sub(silence_ms) + 1);
            let wait_for = (deadline - now).min(stale_in);
            let _ = self.bell.wait_timeout(revision, wait_for).unwrap();
        }
    }

    pub(super) fn compose(&self) -> (Beat, u64) {
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
    pub(super) fn note_registry(&self, ack: &BeatAck) {
        self.registry_protocol
            .store(ack.protocol as u64, Ordering::Relaxed);
        let mut v = self.registry_version.lock().unwrap();
        if *v != ack.version {
            v.clone_from(&ack.version);
        }
    }

    pub(super) fn apply(&self, ack: &BeatAck) -> (bool, bool) {
        // A restarted registry counts from zero again. Keeping a cache built
        // against the old numbering means asking for changes since a version
        // it has never reached, being told there are none, and holding a stale
        // roster forever with nothing to show for it.
        let previous = self.seen_epoch.swap(ack.epoch, Ordering::Relaxed);
        let restarted = previous != 0 && previous != ack.epoch;
        let mut changed = false;
        if restarted {
            let mut cache = self.cache.write().unwrap();
            changed = !cache.is_empty();
            cache.clear();
        }
        if !ack.accepted {
            changed |= self.accepted.swap(false, Ordering::Relaxed);
            if let Some(why) = &ack.refused {
                *self.refused.lock().unwrap() = why.clone();
            }
            return (false, changed);
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
            changed = true;
        }
        // There used to be a loop here recording every watched pool as empty,
        // for fear that a pool we asked about and heard nothing back for would
        // stay missing and make the first lookup wait. It never did anything:
        // the registry creates a pool the moment somebody watches it, and the
        // first delta for a name we have not seen is a full one, so the entry
        // arrives in the answer. Measured with the loop taken out -- the whole
        // suite green, the cache entry still there for a pool nobody ever
        // joined, and the first lookup of one still 41ms against 41ms.
        (true, changed)
    }
}
