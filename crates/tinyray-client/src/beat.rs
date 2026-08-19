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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tinyray_proto::{Beat, BeatAck, Member};

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
    pub endpoints: Vec<String>,
    pub pool: String,
    pub id: u64,
    pub slot: Option<u64>,
    pub incarnation: u64,
    pub policy: String,
    pub size: Option<u64>,
    pub methods: Vec<String>,
    pub url: Mutex<Option<String>>,
    pub state: Mutex<serde_json::Value>,
    pub ready: AtomicBool,
    pub leaving: AtomicBool,
    pub watch: Mutex<Vec<String>>,
    pub cache: RwLock<HashMap<String, CachedPool>>,
    pub accepted: AtomicBool,
    pub beats_ok: AtomicU64,
    pub beats_failed: AtomicU64,
    pub interval_ms: AtomicU64,
}

impl Shared {
    fn compose(&self) -> Beat {
        let cache = self.cache.read().unwrap();
        let watch = self.watch.lock().unwrap().clone();
        let seen = watch
            .iter()
            .filter_map(|n| cache.get(n).map(|c| (n.clone(), c.version)))
            .collect();
        Beat {
            pool: self.pool.clone(),
            slot: self.slot,
            id: self.id,
            incarnation: self.incarnation,
            policy: self.policy.clone(),
            size: self.size,
            url: self.url.lock().unwrap().clone(),
            state: self.state.lock().unwrap().clone(),
            ready: self.ready.load(Ordering::Relaxed),
            leaving: self.leaving.load(Ordering::Relaxed),
            methods: self.methods.clone(),
            watch,
            seen,
        }
    }

    fn apply(&self, ack: &BeatAck) {
        if !ack.accepted {
            self.accepted.store(false, Ordering::Relaxed);
        }
        let mut cache = self.cache.write().unwrap();
        for (name, d) in &ack.pools {
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
    }
}

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

async fn post(http: &HttpClient, endpoint: &str, beat: &Beat) -> Option<BeatAck> {
    let body = Full::new(Bytes::from(serde_json::to_vec(beat).ok()?));
    let req = hyper::Request::builder()
        .method("POST")
        .uri(format!("{endpoint}/v1/beat"))
        .header("content-type", "application/json")
        .body(body)
        .ok()?;
    let resp = http.request(req).await.ok()?;
    let bytes = resp.into_body().collect().await.ok()?.to_bytes();
    serde_json::from_slice(&bytes).ok()
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
        let http: HttpClient = Client::builder(TokioExecutor::new())
            .timer(TokioTimer::new())
            .pool_idle_timeout(Duration::from_secs(60))
            .build_http();
        let mut next = 0usize;
        loop {
            if shared.leaving.load(Ordering::Relaxed) && shared.beats_ok.load(Ordering::Relaxed) > 0
            {
                // One final beat carrying `leaving` was already sent by leave().
                return;
            }
            let beat = shared.compose();
            let mut ok = false;
            // Try replicas in order; any one of them holds the whole roster.
            for i in 0..shared.endpoints.len() {
                let ep = &shared.endpoints[(next + i) % shared.endpoints.len()];
                if let Some(ack) = post(&http, ep, &beat).await {
                    shared.interval_ms.store(ack.ttl_ms / 4, Ordering::Relaxed);
                    shared.apply(&ack);
                    shared.beats_ok.fetch_add(1, Ordering::Relaxed);
                    next = (next + i) % shared.endpoints.len();
                    ok = true;
                    break;
                }
            }
            if !ok {
                shared.beats_failed.fetch_add(1, Ordering::Relaxed);
            }
            let ms = shared.interval_ms.load(Ordering::Relaxed).clamp(50, 30_000);
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    });
    rt
}

/// Send one beat synchronously, used by join() and leave() so that arrival and
/// departure are visible immediately instead of at the next tick.
pub fn beat_once(rt: &tokio::runtime::Runtime, shared: &Arc<Shared>) -> bool {
    let s = shared.clone();
    rt.block_on(async move {
        let http: HttpClient =
            Client::builder(TokioExecutor::new()).timer(TokioTimer::new()).build_http();
        let beat = s.compose();
        for ep in &s.endpoints {
            if let Some(ack) = post(&http, ep, &beat).await {
                s.interval_ms.store(ack.ttl_ms / 4, Ordering::Relaxed);
                s.apply(&ack);
                s.beats_ok.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    })
}
