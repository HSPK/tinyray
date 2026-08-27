//! Python bindings. Everything crossing this boundary is JSON, which keeps the
//! FFI surface tiny; the members Python actually looks at are small.

// The pyo3 macros generate an error conversion that clippy attributes to our
// return types. Item-level allows do not reach macro-expanded code, so this
// has to sit on the crate. The only `.into()` calls we write ourselves are the
// String keys in stats(), which this cannot hide.
#![allow(clippy::useless_conversion)]

mod beat;

use beat::{beat_once, spawn, CachedPool, Published, Shared};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tinyray_proto::{Member, MAX_WATCH};

#[pyclass]
pub struct Client {
    shared: Arc<Shared>,
    // Interior mutability, so every method can take &self. A &mut self method
    // holds pyo3's borrow for its whole duration, and leave() blocks on a
    // network round trip -- long enough for a watchdog thread reading
    // ep.valid to hit "Already mutably borrowed" on every clean shutdown.
    rt: Mutex<Option<tokio::runtime::Runtime>>,
}

fn pick_from(c: &CachedPool, filter: &serde_json::Value, require_ready: bool) -> Vec<Member> {
    let mut out: Vec<Member> = c
        .members
        .values()
        .filter(|m| (!require_ready || m.ready) && m.matches(filter))
        .cloned()
        .collect();
    out.sort_by_key(|m| m.id);
    out
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (endpoint, pool, id, incarnation, policy, slot=None, size=None, url=None, methods=None, exclusive=false))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: String,
        pool: String,
        id: u64,
        incarnation: u64,
        policy: String,
        slot: Option<u64>,
        size: Option<u64>,
        url: Option<String>,
        methods: Option<Vec<String>>,
        exclusive: bool,
    ) -> PyResult<Self> {
        let shared = Arc::new(Shared {
            endpoint,
            pool,
            id,
            slot,
            incarnation,
            policy,
            size,
            methods: methods.unwrap_or_default(),
            url: Mutex::new(url),
            published: Mutex::new(Published {
                state: serde_json::Value::Object(Default::default()),
                ready: false,
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
            interval_ms: AtomicU64::new(1000),
            hold_ms: AtomicU64::new(0),
            last_ok_ms: AtomicU64::new(0),
            seen_epoch: AtomicU64::new(0),
            registry_protocol: AtomicU64::new(0),
            registry_version: Mutex::new(String::new()),
            started: std::time::Instant::now(),
            wake: tokio::sync::Notify::new(),
            revision: Mutex::new(0),
            bell: std::sync::Condvar::new(),
            wakeups: AtomicU64::new(0),
            short_polls: AtomicU64::new(0),
            wake_fds: Mutex::new(Vec::new()),
        });
        Ok(Self {
            shared,
            rt: Mutex::new(None),
        })
    }

    /// Blocks for one beat so the caller is registered on return, then hands
    /// the loop to the tokio threads.
    #[pyo3(signature = (budget_ms = 5_000))]
    fn start(&self, py: Python<'_>, budget_ms: u64) -> PyResult<bool> {
        let rt = spawn(self.shared.clone());
        let s = self.shared.clone();
        // Release the GIL: this is a network round trip.
        let budget = Duration::from_millis(budget_ms.max(1));
        let ok = py.allow_threads(|| beat_once(&rt, &s, budget));
        *self.rt.lock().unwrap() = Some(rt);
        Ok(ok)
    }

    fn watch(&self, pools: Vec<String>) -> PyResult<()> {
        let mut added = false;
        {
            let mut w = self.shared.watch.lock().unwrap();
            for p in pools {
                if !w.contains(&p) {
                    if w.len() >= MAX_WATCH {
                        // Adding it anyway made the registry refuse the whole
                        // beat, which stopped the loop: measured as a member
                        // frozen at zero beats with accepted false, no error
                        // recorded, and its own stale cache still showing it
                        // present. Refusing here names the pool that did it.
                        return Err(PyRuntimeError::new_err(format!(
                            "cannot watch {p:?}: already subscribed to {} pools, \
                             the limit is {MAX_WATCH} including your own. Look \
                             up fewer pool names, or split the work across \
                             processes.",
                            w.len()
                        )));
                    }
                    w.push(p);
                    added = true;
                }
            }
        }
        if added {
            self.shared.wake.notify_one();
        }
        Ok(())
    }

    /// Returns false when the pair was already exactly this, in which case
    /// nothing is nudged: republishing an unchanged state used to cancel the
    /// held beat and spend a request to tell the registry what it already had,
    /// and the registry would not even raise the pool's version for it.
    fn set_state(&self, state_json: &str, ready: bool) -> PyResult<bool> {
        let state: serde_json::Value =
            serde_json::from_str(state_json).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        {
            let mut cur = self.shared.published.lock().unwrap();
            if cur.state == state && cur.ready == ready {
                return Ok(false);
            }
            cur.state = state;
            cur.ready = ready;
        }
        self.shared.wake.notify_one();
        Ok(true)
    }

    /// Publish `state` without touching readiness.
    ///
    /// `ready()` and `set_ready()` assert both at once, which is right for the
    /// component that owns readiness and wrong for every other one: a progress
    /// report had no way to avoid also declaring the member ready, so it would
    /// silently lift a pause somebody else had just applied.
    fn set_state_only(&self, state_json: &str) -> PyResult<bool> {
        let state: serde_json::Value =
            serde_json::from_str(state_json).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        {
            let mut cur = self.shared.published.lock().unwrap();
            if cur.state == state {
                return Ok(false);
            }
            cur.state = state;
        }
        self.shared.wake.notify_one();
        Ok(true)
    }

    /// What the registry said it can do, and which version said it: the
    /// protocol number to branch on, the version string to put in a message.
    fn registry(&self) -> (u64, String) {
        (
            self.shared.registry_protocol.load(Ordering::Relaxed),
            self.shared.registry_version.lock().unwrap().clone(),
        )
    }

    fn is_ready(&self) -> bool {
        self.shared.published.lock().unwrap().ready
    }

    /// Have the bell also write a byte to `fd`, so an event loop can wait on
    /// the fd rather than parking a thread.
    fn add_wake_fd(&self, fd: i32) {
        self.shared.wake_fds.lock().unwrap().push(fd);
    }

    /// Stop writing to `fd`. Must happen before Python closes it, or the bell
    /// would write into whatever the number is reused for.
    fn drop_wake_fd(&self, fd: i32) {
        self.shared.wake_fds.lock().unwrap().retain(|f| *f != fd);
    }

    /// Ring the bell without anything having changed, so every waiter gets a
    /// chance to notice it has been asked to stop.
    fn wake(&self) {
        self.shared.ring();
    }

    #[pyo3(signature = (url=None))]
    fn set_url(&self, url: Option<String>) {
        *self.shared.url.lock().unwrap() = url;
    }

    /// Members of `pool` matching `filter_json`, as a JSON list.
    #[pyo3(signature = (pool, filter_json="{}", require_ready=false))]
    fn lookup(&self, pool: &str, filter_json: &str, require_ready: bool) -> PyResult<String> {
        let filter: serde_json::Value = serde_json::from_str(filter_json)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let cache = self.shared.cache.read().unwrap();
        let members = match cache.get(pool) {
            Some(c) => pick_from(c, &filter, require_ready),
            None => Vec::new(),
        };
        Ok(serde_json::to_string(&members).unwrap())
    }

    /// The members of `pool` matching `require_ready`, the fingerprint they
    /// add up to, the fingerprint the registry holds for the whole pool, and
    /// the pool's version -- all read under one lock.
    ///
    /// epoch() has to compare the last two, and taking them through separate
    /// calls let the beat loop land in between, so the list could come from
    /// one set of occupants and the fingerprint from another. Computing the
    /// members' own fingerprint here rather than in Python also keeps one
    /// implementation of the hash: a second one would drift silently.
    #[pyo3(signature = (pool, require_ready=true))]
    fn frozen(&self, pool: &str, require_ready: bool) -> Option<(String, u64, u64, u64)> {
        let cache = self.shared.cache.read().unwrap();
        let c = cache.get(pool)?;
        let members = pick_from(c, &serde_json::Value::Null, require_ready);
        let mine = members.iter().fold(0u64, |acc, m| acc ^ m.roster_hash());
        Some((
            serde_json::to_string(&members).unwrap(),
            mine,
            c.roster,
            c.version,
        ))
    }

    /// A hash over only the named fields of every member, plus who is present.
    ///
    /// A watcher that cares about two keys should not pay for a whole snapshot
    /// every time somebody bumps a third. Comparing in Python cannot help: the
    /// predicate needs a `Snapshot` to look at, and by then the work is done --
    /// measured at 5,000 members, `snapshot()` is 8.78ms against 0.40ms here,
    /// so the comparison has to happen against the cache, before anything is
    /// serialised.
    ///
    /// `ready` and `url` name those parts of a member; anything else is looked
    /// up in its published state. Identity is always part of the hash: a seat
    /// changing hands matters even when the new tenure publishes exactly what
    /// the old one did.
    ///
    /// Every member is hashed, ready or not. There used to be a require_ready
    /// argument here, and both call sites always passed false -- readiness is
    /// asked for by name, as `fields=["ready"]`, which is the same question
    /// without a second way to spell it.
    fn field_digest(&self, pool: &str, fields: Vec<String>) -> Option<u64> {
        use std::hash::{Hash, Hasher};
        let cache = self.shared.cache.read().unwrap();
        let c = cache.get(pool)?;
        let mut ids: Vec<&u64> = c.members.keys().collect();
        ids.sort_unstable();
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for id in ids {
            let m = &c.members[id];
            m.id.hash(&mut h);
            m.incarnation.hash(&mut h);
            for f in &fields {
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
        Some(h.finish())
    }

    /// Version and roster fingerprint of a cached pool, or None if unseen.
    fn pool_info(&self, pool: &str) -> Option<(u64, u64, Option<u64>, Vec<String>)> {
        let cache = self.shared.cache.read().unwrap();
        cache
            .get(pool)
            .map(|c| (c.version, c.roster, c.size, c.methods.clone()))
    }

    /// The local cache's revision. It moves once per beat, after the ack has
    /// been applied.
    fn cache_revision(&self) -> u64 {
        *self.shared.revision.lock().unwrap()
    }

    /// Block until the cache has moved past `since`, or `timeout_ms` elapses,
    /// and return the revision now current.
    ///
    /// This is what every wait in the Python layer stands on. They were sleep
    /// loops over the local cache -- the tightest turning 500 times a second
    /// per pool -- which spent CPU to find out nothing had happened and still
    /// added up to half a tick of latency when something had.
    fn wait_revision(&self, py: Python<'_>, since: u64, timeout_ms: u64) -> u64 {
        let s = self.shared.clone();
        // Released, because this blocks: holding it would stop the very
        // threads that might satisfy the caller.
        py.allow_threads(move || {
            let rev = s.revision.lock().unwrap();
            if *rev != since {
                return *rev;
            }
            // Spurious wakeups are fine: the caller re-checks what it wanted.
            let (rev, _) = s
                .bell
                .wait_timeout(rev, std::time::Duration::from_millis(timeout_ms))
                .unwrap();
            *rev
        })
    }

    fn leave(&self, py: Python<'_>) {
        self.shared.leaving.store(true, Ordering::Relaxed);
        let rt = self.rt.lock().unwrap().take();
        if let Some(rt) = rt {
            let s = self.shared.clone();
            // Nothing to say goodbye about if nothing ever got through, and a
            // registry that has not answered will not answer this either. It
            // used to try anyway, for another full budget: join(timeout=0.5)
            // against a registry that accepts and never replies took 10502ms,
            // five of them spent on a farewell for a member that was never
            // there.
            if s.beats_ok.load(Ordering::Relaxed) > 0 {
                py.allow_threads(|| beat_once(&rt, &s, Duration::from_secs(5)));
            }
            rt.shutdown_background();
        }
    }

    /// Let go of the runtime without shutting it down. Only a forked child
    /// should call this.
    ///
    /// fork() keeps just the calling thread, so the runtime's workers do not
    /// exist in the child -- but the inherited handle does, and dropping it at
    /// interpreter shutdown waits for threads that will never answer.
    /// Measured: the child hangs forever with no Python frame to show why, in
    /// native code, and the parent's waitpid hangs with it. Leaking the handle
    /// is the right trade here: it points into a copy-on-write image that is
    /// about to be discarded, and there is nothing left alive to shut down.
    fn abandon(&self) {
        if let Some(rt) = self.rt.lock().unwrap().take() {
            std::mem::forget(rt);
        }
    }

    /// Milliseconds since the last successful beat; the registry is
    /// unreachable when this exceeds the lease.
    #[getter]
    fn silence_ms(&self) -> u64 {
        self.shared.silence_ms()
    }

    #[getter]
    fn accepted(&self) -> bool {
        self.shared.accepted.load(Ordering::Relaxed)
    }

    /// Why the registry refused this member, if it was about the pool's shape.
    fn refused(&self) -> String {
        self.shared.refused.lock().unwrap().clone()
    }

    /// Why the last beat failed, or an empty string if none has.
    fn last_error(&self) -> String {
        self.shared.last_error.lock().unwrap().clone()
    }

    fn stats(&self) -> HashMap<String, u64> {
        HashMap::from([
            (
                "beats_ok".into(),
                self.shared.beats_ok.load(Ordering::Relaxed),
            ),
            (
                "beats_failed".into(),
                self.shared.beats_failed.load(Ordering::Relaxed),
            ),
            (
                "interval_ms".into(),
                self.shared.interval_ms.load(Ordering::Relaxed),
            ),
            ("silence_ms".into(), self.shared.silence_ms()),
            (
                "short_polls".into(),
                self.shared.short_polls.load(Ordering::Relaxed),
            ),
            (
                "watch_wakeups".into(),
                self.shared.wakeups.load(Ordering::Relaxed),
            ),
            (
                "state_bytes".into(),
                self.shared
                    .published
                    .lock()
                    .unwrap()
                    .state
                    .to_string()
                    .len() as u64,
            ),
            (
                "pool_revision".into(),
                self.shared
                    .cache
                    .read()
                    .unwrap()
                    .get(&self.shared.pool)
                    .map(|c| c.version)
                    .unwrap_or(0),
            ),
            (
                "watched_pools".into(),
                self.shared.watch.lock().unwrap().len() as u64,
            ),
        ])
    }
}

/// Run the registry in this process. Shipping it inside the extension module
/// means `pip install tinyray` gives you the server too, with no second
/// artifact to build, version or distribute.
#[pyfunction]
#[pyo3(signature = (listen, ttl_ms))]
fn serve_registry(py: Python<'_>, listen: &str, ttl_ms: u64) -> PyResult<()> {
    py.allow_threads(|| tinyray_registry::run(listen, ttl_ms, |addr| println!("{addr}")))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pymodule]
fn _tinyray(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Lets a wheel say which core it was built against, so a stale extension
    // beside fresh Python code is visible instead of merely strange.
    m.add("version", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Client>()?;
    m.add_function(wrap_pyfunction!(serve_registry, m)?)?;
    Ok(())
}
