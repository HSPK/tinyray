//! Python bindings. Registry control traffic and method RPC both use framed
//! MessagePack; method application payloads remain opaque to Rust.

// The pyo3 macros generate an error conversion that clippy attributes to our
// return types. Item-level allows do not reach macro-expanded code, so this
// has to sit on the crate. The only `.into()` calls we write ourselves are the
// String keys in stats(), which this cannot hide.
#![allow(clippy::useless_conversion)]

mod blob;
mod rpc;

#[doc(hidden)]
pub mod beat {
    pub use tinyray_membership::*;
}

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tinyray_membership::{
    beat_once, spawn, CacheWaiter, EpochWaitResult, FrozenPool, Shared, WaitResult, WaitStatus,
};
use tinyray_proto::wire::decode_message;
use tinyray_proto::{value_within_depth_limit, Member, MAX_WATCH};

fn decode_value(raw: &[u8], what: &str) -> PyResult<serde_json::Value> {
    let value = decode_message(raw)
        .map_err(|e| PyRuntimeError::new_err(format!("cannot decode {what} MessagePack: {e}")))?;
    if !value_within_depth_limit(&value) {
        return Err(PyRuntimeError::new_err(format!(
            "cannot decode {what} MessagePack: recursion limit exceeded"
        )));
    }
    Ok(value)
}

fn decode_filter(raw: Option<&[u8]>) -> PyResult<serde_json::Value> {
    match raw {
        Some(raw) => decode_value(raw, "filter"),
        None => Ok(serde_json::Value::Object(Default::default())),
    }
}

fn encode_bytes<T: serde::Serialize>(py: Python<'_>, value: &T) -> PyResult<Py<PyBytes>> {
    let raw = rmp_serde::to_vec_named(value)
        .map_err(|e| PyRuntimeError::new_err(format!("cannot encode MessagePack: {e}")))?;
    Ok(PyBytes::new_bound(py, &raw).unbind())
}

fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    Ok(match value {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(value) => value.into_py(py),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                value.into_py(py)
            } else if let Some(value) = value.as_u64() {
                value.into_py(py)
            } else {
                value
                    .as_f64()
                    .expect("a JSON number is integer or finite float")
                    .into_py(py)
            }
        }
        serde_json::Value::String(value) => value.into_py(py),
        serde_json::Value::Array(values) => {
            let values = values
                .iter()
                .map(|value| json_to_py(py, value))
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new_bound(py, values).into_any().unbind()
        }
        serde_json::Value::Object(values) => {
            let out = PyDict::new_bound(py);
            for (key, value) in values {
                out.set_item(key, json_to_py(py, value)?)?;
            }
            out.into_any().unbind()
        }
    })
}

#[pyclass(frozen)]
struct NativeMember {
    pool: Arc<str>,
    member: Arc<Member>,
    state_batch: Option<(Py<PyAny>, usize)>,
}

impl NativeMember {
    fn new(pool: Arc<str>, member: Arc<Member>) -> Self {
        Self {
            pool,
            member,
            state_batch: None,
        }
    }

    fn with_state_batch(
        pool: Arc<str>,
        member: Arc<Member>,
        state_batch: Py<PyAny>,
        state_index: usize,
    ) -> Self {
        Self {
            pool,
            member,
            state_batch: Some((state_batch, state_index)),
        }
    }
}

#[pymethods]
impl NativeMember {
    #[getter]
    fn pool(&self) -> &str {
        &self.pool
    }

    #[getter]
    fn id(&self) -> u64 {
        self.member.id
    }

    #[getter]
    fn slot(&self) -> Option<u64> {
        self.member.slot
    }

    #[getter]
    fn incarnation(&self) -> u64 {
        self.member.incarnation
    }

    #[getter]
    fn url(&self) -> Option<&str> {
        self.member.url.as_deref()
    }

    #[getter]
    fn ready(&self) -> bool {
        self.member.ready
    }

    #[getter]
    fn identity(&self) -> String {
        format!(
            "{}/{}#{}",
            self.pool,
            self.member.slot.unwrap_or(self.member.id),
            self.member.incarnation
        )
    }

    #[getter]
    fn label(&self) -> String {
        let seat = self
            .member
            .slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| format!("{:04x}", self.member.id & 0xFFFF));
        format!(
            "{}/{seat}#{:03x}",
            self.pool,
            self.member.incarnation & 0xFFF
        )
    }

    fn materialize_state(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.state_batch {
            Some((state_batch, index)) => state_batch
                .bind(py)
                .call_method1("get", (*index,))
                .map(Bound::unbind),
            None => json_to_py(py, &self.member.state),
        }
    }
}

#[pyclass(frozen)]
struct NativeSnapshot {
    pool: Arc<str>,
    frozen: FrozenPool,
}

#[pyclass(frozen)]
struct NativeStateBatch {
    members: Arc<beat::FrozenMembers>,
}

#[pymethods]
impl NativeStateBatch {
    fn materialize(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let states = self.members.serialized_states();
        Ok(PyBytes::new_bound(py, &states).unbind())
    }
}

impl NativeSnapshot {
    fn new(pool: String, frozen: FrozenPool) -> Self {
        Self {
            pool: Arc::from(pool),
            frozen,
        }
    }

    fn methods<'py>(&self, py: Python<'py>) -> Bound<'py, PyTuple> {
        PyTuple::new_bound(py, self.frozen.methods.iter().map(String::as_str))
    }

    fn handle(
        &self,
        py: Python<'_>,
        factory: &Bound<'_, PyAny>,
        member: Arc<Member>,
        methods: &Bound<'_, PyTuple>,
        state_batch: Option<&Bound<'_, PyAny>>,
        state_index: usize,
    ) -> PyResult<PyObject> {
        let member = match state_batch {
            Some(state_batch) => NativeMember::with_state_batch(
                self.pool.clone(),
                member,
                state_batch.clone().unbind(),
                state_index,
            ),
            None => NativeMember::new(self.pool.clone(), member),
        };
        let member = Py::new(py, member)?;
        factory.call1((member, methods)).map(Bound::unbind)
    }
}

#[pymethods]
impl NativeSnapshot {
    fn __len__(&self) -> usize {
        self.frozen.members.len()
    }

    #[getter]
    fn revision(&self) -> u64 {
        self.frozen.version
    }

    #[getter]
    fn fingerprint(&self) -> u64 {
        self.frozen.members.roster()
    }

    #[getter]
    fn roster(&self) -> u64 {
        self.frozen.roster
    }

    #[getter]
    fn size(&self) -> Option<u64> {
        self.frozen.size
    }

    #[pyo3(signature = (factory, state_factory, immutable=true))]
    fn materialize(
        &self,
        py: Python<'_>,
        factory: &Bound<'_, PyAny>,
        state_factory: &Bound<'_, PyAny>,
        immutable: bool,
    ) -> PyResult<PyObject> {
        let methods = self.methods(py);
        let native_batch = Py::new(
            py,
            NativeStateBatch {
                members: self.frozen.members.clone(),
            },
        )?;
        let state_batch = state_factory.call1((native_batch,))?;
        let handles = self
            .frozen
            .members
            .members()
            .iter()
            .enumerate()
            .map(|(index, member)| {
                self.handle(
                    py,
                    factory,
                    member.clone(),
                    &methods,
                    Some(&state_batch),
                    index,
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(if immutable {
            PyTuple::new_bound(py, handles).into_any().unbind()
        } else {
            PyList::new_bound(py, handles).into_any().unbind()
        })
    }

    fn materialize_ready(
        &self,
        py: Python<'_>,
        factory: &Bound<'_, PyAny>,
        state_factory: &Bound<'_, PyAny>,
    ) -> PyResult<PyObject> {
        let methods = self.methods(py);
        let native_batch = Py::new(
            py,
            NativeStateBatch {
                members: self.frozen.members.clone(),
            },
        )?;
        let state_batch = state_factory.call1((native_batch,))?;
        let handles = self
            .frozen
            .members
            .members()
            .iter()
            .enumerate()
            .filter(|(_, member)| member.ready)
            .map(|(index, member)| {
                self.handle(
                    py,
                    factory,
                    member.clone(),
                    &methods,
                    Some(&state_batch),
                    index,
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyList::new_bound(py, handles).into_any().unbind())
    }

    fn slot(
        &self,
        py: Python<'_>,
        slot: u64,
        factory: &Bound<'_, PyAny>,
    ) -> PyResult<Option<PyObject>> {
        let methods = self.methods(py);
        self.frozen
            .members
            .slot_arc(slot)
            .map(|member| self.handle(py, factory, member.clone(), &methods, None, 0))
            .transpose()
    }

    fn get(
        &self,
        py: Python<'_>,
        identity: &str,
        factory: &Bound<'_, PyAny>,
    ) -> PyResult<Option<PyObject>> {
        let methods = self.methods(py);
        self.frozen
            .members
            .get_arc(&self.pool, identity)
            .map(|member| self.handle(py, factory, member.clone(), &methods, None, 0))
            .transpose()
    }
}

type PyWaitResult = (u8, Option<Py<NativeSnapshot>>, usize, usize, u64);

fn wait_result_to_py(py: Python<'_>, pool: &str, result: WaitResult) -> PyResult<PyWaitResult> {
    let view = result
        .view
        .map(|frozen| Py::new(py, NativeSnapshot::new(pool.to_owned(), frozen)))
        .transpose()?;
    Ok((
        result.status as u8,
        view,
        result.matched,
        result.total,
        result.version,
    ))
}

#[pyclass(weakref)]
struct NativeWait {
    pool: String,
    waiter: CacheWaiter,
}

#[pymethods]
impl NativeWait {
    #[pyo3(signature = (initial=true))]
    fn check(&self, py: Python<'_>, initial: bool) -> PyResult<PyWaitResult> {
        wait_result_to_py(py, &self.pool, self.waiter.check(initial))
    }

    #[pyo3(signature = (timeout_ms=None, initial=false))]
    fn wait(
        &self,
        py: Python<'_>,
        timeout_ms: Option<u64>,
        initial: bool,
    ) -> PyResult<PyWaitResult> {
        let result = py.allow_threads(|| {
            self.waiter
                .wait(timeout_ms.map(Duration::from_millis), initial)
        });
        wait_result_to_py(py, &self.pool, result)
    }

    fn close(&self) {
        self.waiter.close();
    }
}

type PyEpochWaitResult = (u8, Option<Py<NativeSnapshot>>, usize, i128, bool, bool, u64);

fn epoch_result_to_py(
    py: Python<'_>,
    pool: &str,
    result: EpochWaitResult,
) -> PyResult<PyEpochWaitResult> {
    let view = result
        .view
        .map(|frozen| Py::new(py, NativeSnapshot::new(pool.to_owned(), frozen)))
        .transpose()?;
    Ok((
        result.status as u8,
        view,
        result.found,
        result.target,
        result.mismatched,
        result.seen_pool,
        result.silence_ms,
    ))
}

#[pyclass]
pub struct Client {
    shared: Arc<Shared>,
    rng: Mutex<fastrand::Rng>,
    // Interior mutability, so every method can take &self. A &mut self method
    // holds pyo3's borrow for its whole duration, and leave() blocks on a
    // network round trip -- long enough for a watchdog thread reading
    // ep.valid to hit "Already mutably borrowed" on every clean shutdown.
    rt: Mutex<Option<tokio::runtime::Runtime>>,
}

fn selection_rng(id: u64, incarnation: u64) -> fastrand::Rng {
    // fastrand's thread-local generator survives fork unchanged.
    static NONCE: AtomicU64 = AtomicU64::new(0);
    let mut seed = std::collections::hash_map::DefaultHasher::new();
    (
        std::process::id(),
        id,
        incarnation,
        std::time::Instant::now(),
        NONCE.fetch_add(1, Ordering::Relaxed),
    )
        .hash(&mut seed);
    fastrand::Rng::with_seed(seed.finish())
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (endpoint, pool, id, incarnation, policy, slot=None, size=None, url=None, methods=None, exclusive=false, coalesce_ms=50))]
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
        coalesce_ms: u64,
    ) -> PyResult<Self> {
        let shared = Arc::new(Shared::new(
            endpoint,
            pool,
            id,
            incarnation,
            policy,
            slot,
            size,
            url,
            methods.unwrap_or_default(),
            exclusive,
            coalesce_ms,
        ));
        Ok(Self {
            shared,
            rng: Mutex::new(selection_rng(id, incarnation)),
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
        let ok = py.allow_threads(|| beat_once(&rt, &s, budget, true));
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
    fn set_state(&self, state_msgpack: &[u8], ready: bool) -> PyResult<bool> {
        let state = decode_value(state_msgpack, "state")?;
        {
            let mut cur = self.shared.published.lock().unwrap();
            if cur.state == state && cur.ready == ready {
                return Ok(false);
            }
            cur.state = state;
            cur.ready = ready;
            cur.version += 1;
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
    fn set_state_only(&self, state_msgpack: &[u8]) -> PyResult<bool> {
        let state = decode_value(state_msgpack, "state")?;
        {
            let mut cur = self.shared.published.lock().unwrap();
            if cur.state == state {
                return Ok(false);
            }
            cur.state = state;
            cur.version += 1;
        }
        self.shared.wake.notify_one();
        Ok(true)
    }

    /// The version last published locally, and the newest one the registry has
    /// acked. flush() waits for the second to reach the first.
    ///
    /// A count of beats cannot do this. It has to assume the beat in flight
    /// was composed before the change and wait for the one after it, which
    /// costs a whole long-poll hold that the publish already interrupted:
    /// measured at a 2s lease, flush() took 645ms where the ack proving the
    /// registry had the state had arrived in one round trip.
    fn publish_versions(&self) -> (u64, u64) {
        (
            self.shared.published.lock().unwrap().version,
            self.shared.confirmed.load(Ordering::Relaxed),
        )
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
        {
            let mut published = self.shared.published.lock().unwrap();
            if published.url == url {
                return;
            }
            published.url = url;
            published.version += 1;
        }
        self.shared.wake.notify_one();
    }

    /// An immutable Arc-backed view of one cached roster.
    #[pyo3(signature = (pool, filter_msgpack=None, require_ready=false))]
    fn snapshot_view(
        &self,
        py: Python<'_>,
        pool: &str,
        filter_msgpack: Option<&[u8]>,
        require_ready: bool,
    ) -> PyResult<Option<Py<NativeSnapshot>>> {
        let filter = decode_filter(filter_msgpack)?;
        let cache = self.shared.cache.read().unwrap();
        cache
            .get(pool)
            .map(|cached| {
                Py::new(
                    py,
                    NativeSnapshot::new(pool.to_owned(), cached.filtered(&filter, require_ready)),
                )
            })
            .transpose()
    }

    #[pyo3(signature = (pool, filter_msgpack=None, require_ready=true))]
    fn count(
        &self,
        pool: &str,
        filter_msgpack: Option<&[u8]>,
        require_ready: bool,
    ) -> PyResult<usize> {
        let filter = decode_filter(filter_msgpack)?;
        let cache = self.shared.cache.read().unwrap();
        Ok(cache
            .get(pool)
            .map(|cached| cached.count(&filter, require_ready))
            .unwrap_or_default())
    }

    #[pyo3(signature = (pool, count, filter_msgpack=None))]
    fn count_waiter(
        &self,
        py: Python<'_>,
        pool: String,
        count: i128,
        filter_msgpack: Option<&[u8]>,
    ) -> PyResult<Py<NativeWait>> {
        let filter = decode_filter(filter_msgpack)?;
        Py::new(
            py,
            NativeWait {
                waiter: CacheWaiter::count(self.shared.clone(), pool.clone(), filter, count),
                pool,
            },
        )
    }

    fn departure_waiter(
        &self,
        py: Python<'_>,
        pool: String,
        identity: String,
    ) -> PyResult<Py<NativeWait>> {
        Py::new(
            py,
            NativeWait {
                waiter: CacheWaiter::departure(self.shared.clone(), pool.clone(), identity),
                pool,
            },
        )
    }

    #[pyo3(signature = (pool, slot, previous=None, capture=false))]
    fn replacement_waiter(
        &self,
        py: Python<'_>,
        pool: String,
        slot: u64,
        previous: Option<String>,
        capture: bool,
    ) -> PyResult<Py<NativeWait>> {
        Py::new(
            py,
            NativeWait {
                waiter: CacheWaiter::replacement(
                    self.shared.clone(),
                    pool.clone(),
                    slot,
                    previous,
                    capture,
                ),
                pool,
            },
        )
    }

    #[pyo3(signature = (pool, timeout_ms, minimum=None))]
    fn wait_epoch(
        &self,
        py: Python<'_>,
        pool: &str,
        timeout_ms: u64,
        minimum: Option<i128>,
    ) -> PyResult<PyEpochWaitResult> {
        let result = py.allow_threads(|| {
            self.shared
                .wait_epoch(pool, minimum, Duration::from_millis(timeout_ms))
        });
        epoch_result_to_py(py, pool, result)
    }

    fn epoch_valid(&self, pool: &str, roster: u64) -> bool {
        self.shared.epoch_valid(pool, roster)
    }

    /// Members of `pool` matching a MessagePack filter, as MessagePack bytes.
    #[pyo3(signature = (pool, filter_msgpack=None, require_ready=false))]
    fn lookup(
        &self,
        py: Python<'_>,
        pool: &str,
        filter_msgpack: Option<&[u8]>,
        require_ready: bool,
    ) -> PyResult<Py<PyBytes>> {
        let filter = decode_filter(filter_msgpack)?;
        let cache = self.shared.cache.read().unwrap();
        let Some(c) = cache.get(pool) else {
            return encode_bytes(py, &Vec::<Member>::new());
        };
        if filter.as_object().is_none_or(|f| f.is_empty()) {
            let raw = c.serialized(require_ready).0;
            return Ok(PyBytes::new_bound(py, &raw).unbind());
        }
        let filtered = c.filtered(&filter, require_ready);
        let members: Vec<&Member> = filtered.members.members().iter().map(Arc::as_ref).collect();
        encode_bytes(py, &members)
    }

    /// Only the selected member crosses the Python boundary.
    #[pyo3(signature = (pool, filter_msgpack=None, require_ready=true))]
    fn choose(
        &self,
        py: Python<'_>,
        pool: &str,
        filter_msgpack: Option<&[u8]>,
        require_ready: bool,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let filter = decode_filter(filter_msgpack)?;
        let cache = self.shared.cache.read().unwrap();
        cache
            .get(pool)
            .and_then(|c| c.choose(&filter, require_ready, &mut self.rng.lock().unwrap()))
            .map(|member| encode_bytes(py, member))
            .transpose()
    }

    #[pyo3(signature = (pool, filter_msgpack=None, require_ready=true))]
    fn choose_ref(
        &self,
        py: Python<'_>,
        pool: &str,
        filter_msgpack: Option<&[u8]>,
        require_ready: bool,
    ) -> PyResult<Option<Py<NativeMember>>> {
        let filter = decode_filter(filter_msgpack)?;
        let cache = self.shared.cache.read().unwrap();
        cache
            .get(pool)
            .and_then(|cached| {
                cached.choose_arc(&filter, require_ready, &mut self.rng.lock().unwrap())
            })
            .map(|member| Py::new(py, NativeMember::new(Arc::<str>::from(pool), member)))
            .transpose()
    }

    #[pyo3(signature = (pool, slot, require_ready=false))]
    fn lookup_slot(
        &self,
        py: Python<'_>,
        pool: &str,
        slot: u64,
        require_ready: bool,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let cache = self.shared.cache.read().unwrap();
        let Some(cached) = cache.get(pool) else {
            return Ok(None);
        };
        cached
            .slot(slot, require_ready)
            .map(|member| encode_bytes(py, member))
            .transpose()
    }

    #[pyo3(signature = (pool, slot, require_ready=false))]
    fn lookup_slot_ref(
        &self,
        py: Python<'_>,
        pool: &str,
        slot: u64,
        require_ready: bool,
    ) -> PyResult<Option<Py<NativeMember>>> {
        let cache = self.shared.cache.read().unwrap();
        let Some(cached) = cache.get(pool) else {
            return Ok(None);
        };
        cached
            .slot_owned(slot, require_ready)
            .map(|member| Py::new(py, NativeMember::new(Arc::<str>::from(pool), member)))
            .transpose()
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
    fn frozen(
        &self,
        py: Python<'_>,
        pool: &str,
        require_ready: bool,
    ) -> Option<(Py<PyBytes>, u64, u64, u64)> {
        let cache = self.shared.cache.read().unwrap();
        let c = cache.get(pool)?;
        let (raw, mine) = c.serialized(require_ready);
        Some((
            PyBytes::new_bound(py, &raw).unbind(),
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
        let cache = self.shared.cache.read().unwrap();
        let c = cache.get(pool)?;
        Some(c.field_digest(&fields))
    }

    /// Version and roster fingerprint of a cached pool, or None if unseen.
    fn pool_info(&self, pool: &str) -> Option<(u64, u64, Option<u64>, Vec<String>)> {
        let cache = self.shared.cache.read().unwrap();
        cache
            .get(pool)
            .map(|c| (c.version, c.roster, c.size, c.methods.to_vec()))
    }

    /// The local cache/lifecycle revision. Empty heartbeat acknowledgements do
    /// not move it or wake discovery waiters.
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

    fn wait_registered(&self, py: Python<'_>, timeout_ms: u64) -> bool {
        let shared = self.shared.clone();
        py.allow_threads(move || {
            shared.wait_registered(std::time::Duration::from_millis(timeout_ms))
        })
    }

    fn wait_publication(&self, py: Python<'_>, wanted: u64, timeout_ms: u64) -> (u64, bool) {
        let shared = self.shared.clone();
        py.allow_threads(move || {
            shared.wait_publication(wanted, std::time::Duration::from_millis(timeout_ms));
            (
                shared.confirmed.load(Ordering::Relaxed),
                shared.accepted.load(Ordering::Relaxed),
            )
        })
    }

    fn debug_beat_revision(&self) -> u64 {
        self.shared.beat_revision()
    }

    fn debug_wait_beat_revision(&self, py: Python<'_>, since: u64, timeout_ms: u64) -> u64 {
        let shared = self.shared.clone();
        py.allow_threads(move || {
            shared.wait_beat_revision(since, std::time::Duration::from_millis(timeout_ms))
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
                py.allow_threads(|| beat_once(&rt, &s, Duration::from_secs(5), false));
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
        self.shared.registry_fds.close_all();
        if let Some(rt) = self.rt.lock().unwrap().take() {
            std::mem::forget(rt);
        }
    }

    fn debug_registry_fds(&self) -> Vec<i32> {
        self.shared.registry_fds.snapshot()
    }

    fn debug_registry_transport(&self) -> HashMap<String, u64> {
        HashMap::from([
            (
                "connections".into(),
                self.shared.registry_connects.load(Ordering::Relaxed),
            ),
            (
                "reuses".into(),
                self.shared.registry_reuses.load(Ordering::Relaxed),
            ),
        ])
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
            ("coalesce_ms".into(), self.shared.coalesce_ms),
            (
                "effective_coalesce_ms".into(),
                beat::coalesce_gap(
                    self.shared.coalesce_ms,
                    self.shared.interval_ms.load(Ordering::Relaxed),
                )
                .as_millis() as u64,
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

    fn debug_filter_index_stats(&self, pool: &str) -> HashMap<String, u64> {
        self.shared
            .cache
            .read()
            .unwrap()
            .get(pool)
            .map(beat::CachedPool::filter_index_stats)
            .unwrap_or_default()
    }

    fn debug_filter_index_clear(&self, pool: &str) {
        if let Some(cached) = self.shared.cache.read().unwrap().get(pool) {
            cached.clear_filter_index();
        }
    }

    #[pyo3(signature = (pool, filter_msgpack=None, require_ready=true))]
    fn debug_filter_scan_ids(
        &self,
        pool: &str,
        filter_msgpack: Option<&[u8]>,
        require_ready: bool,
    ) -> PyResult<Vec<u64>> {
        let filter = decode_filter(filter_msgpack)?;
        Ok(self
            .shared
            .cache
            .read()
            .unwrap()
            .get(pool)
            .map(|cached| cached.debug_scan_ids(&filter, require_ready))
            .unwrap_or_default())
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
    m.add_class::<NativeMember>()?;
    m.add_class::<NativeSnapshot>()?;
    m.add_class::<NativeStateBatch>()?;
    m.add_class::<NativeWait>()?;
    m.add("WAIT_PENDING", WaitStatus::Pending as u8)?;
    m.add("WAIT_READY", WaitStatus::Ready as u8)?;
    m.add("WAIT_TIMEOUT", WaitStatus::Timeout as u8)?;
    m.add("WAIT_FENCED", WaitStatus::Fenced as u8)?;
    m.add("WAIT_CLOSED", WaitStatus::Closed as u8)?;
    m.add("WAIT_STALE", WaitStatus::Stale as u8)?;
    m.add("WAIT_NO_SIZE", WaitStatus::NoSize as u8)?;
    m.add("WAIT_MISMATCH", WaitStatus::Mismatch as u8)?;
    rpc::install(m)?;
    blob::install(m)?;
    m.add_function(wrap_pyfunction!(serve_registry, m)?)?;
    Ok(())
}
