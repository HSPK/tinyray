//! Python bindings. Everything crossing this boundary is JSON, which keeps the
//! FFI surface tiny; the members Python actually looks at are small.

mod beat;

use beat::{beat_once, spawn, CachedPool, Shared};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tinyray_proto::Member;

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
            state: Mutex::new(serde_json::Value::Object(Default::default())),
            ready: AtomicBool::new(false),
            leaving: AtomicBool::new(false),
            exclusive,
            watch: Mutex::new(Vec::new()),
            cache: RwLock::new(HashMap::new()),
            accepted: AtomicBool::new(true),
            beats_ok: AtomicU64::new(0),
            beats_failed: AtomicU64::new(0),
            last_error: Mutex::new(String::new()),
            interval_ms: AtomicU64::new(1000),
            last_ok_ms: AtomicU64::new(0),
            seen_epoch: AtomicU64::new(0),
            started: std::time::Instant::now(),
            wake: tokio::sync::Notify::new(),
        });
        Ok(Self { shared, rt: Mutex::new(None) })
    }

    /// Blocks for one beat so the caller is registered on return, then hands
    /// the loop to the tokio threads.
    fn start(&self, py: Python<'_>) -> PyResult<bool> {
        let rt = spawn(self.shared.clone());
        let s = self.shared.clone();
        // Release the GIL: this is a network round trip.
        let ok = py.allow_threads(|| beat_once(&rt, &s));
        *self.rt.lock().unwrap() = Some(rt);
        Ok(ok)
    }

    fn watch(&self, pools: Vec<String>) {
        let mut added = false;
        {
            let mut w = self.shared.watch.lock().unwrap();
            for p in pools {
                if !w.contains(&p) {
                    w.push(p);
                    added = true;
                }
            }
        }
        if added {
            self.shared.wake.notify_one();
        }
    }

    fn set_state(&self, state_json: &str, ready: bool) -> PyResult<()> {
        let v: serde_json::Value = serde_json::from_str(state_json)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        *self.shared.state.lock().unwrap() = v;
        self.shared.ready.store(ready, Ordering::Relaxed);
        self.shared.wake.notify_one();
        Ok(())
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

    /// Version and roster fingerprint of a cached pool, or None if unseen.
    fn pool_info(&self, pool: &str) -> Option<(u64, u64, Option<u64>, Vec<String>)> {
        let cache = self.shared.cache.read().unwrap();
        cache.get(pool).map(|c| (c.version, c.roster, c.size, c.methods.clone()))
    }

    fn leave(&self, py: Python<'_>) {
        self.shared.leaving.store(true, Ordering::Relaxed);
        let rt = self.rt.lock().unwrap().take();
        if let Some(rt) = rt {
            let s = self.shared.clone();
            py.allow_threads(|| beat_once(&rt, &s));
            rt.shutdown_background();
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

    /// Why the last beat failed, or an empty string if none has.
    fn last_error(&self) -> String {
        self.shared.last_error.lock().unwrap().clone()
    }

    fn stats(&self) -> HashMap<String, u64> {
        HashMap::from([
            ("beats_ok".into(), self.shared.beats_ok.load(Ordering::Relaxed)),
            ("beats_failed".into(), self.shared.beats_failed.load(Ordering::Relaxed)),
            ("interval_ms".into(), self.shared.interval_ms.load(Ordering::Relaxed)),
            ("silence_ms".into(), self.shared.silence_ms()),
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
    m.add_class::<Client>()?;
    m.add_function(wrap_pyfunction!(serve_registry, m)?)?;
    Ok(())
}
