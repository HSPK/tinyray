use crate::discovery::DiscoveryPool;
use crate::membership_core::{beat_once, spawn, Shared};
use crate::{
    CallError, Client, Router, RpcRuntime, Server, ServerConfig, Service, ServiceError,
    ServiceRequest, ServiceResponse,
};
use async_trait::async_trait;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tinyray_proto::{Member as DiscoveredMember, MAX_WATCH};

#[derive(Clone, Debug)]
pub struct MemberStats {
    pub beats_ok: u64,
    pub beats_failed: u64,
    pub interval_ms: u64,
    pub confirmed_publication: u64,
    pub published_version: u64,
    pub accepted: bool,
}

#[derive(Debug)]
pub enum MemberError {
    Invalid(String),
    Registry(String),
    Timeout(String),
    Refused(String),
    Rpc(CallError),
}

impl std::fmt::Display for MemberError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message)
            | Self::Registry(message)
            | Self::Timeout(message)
            | Self::Refused(message) => f.write_str(message),
            Self::Rpc(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for MemberError {}

impl From<CallError> for MemberError {
    fn from(error: CallError) -> Self {
        Self::Rpc(error)
    }
}

pub struct MemberBuilder {
    registry: String,
    pool: String,
    policy: String,
    id: Option<u64>,
    incarnation: Option<u64>,
    slot: Option<u64>,
    size: Option<u64>,
    exclusive: bool,
    coalesce_ms: u64,
    listen: String,
    advertise_host: Option<String>,
    max_concurrency: Option<usize>,
    rpc_worker_threads: usize,
    service: Option<Arc<dyn Service>>,
}

impl MemberBuilder {
    pub fn new(registry: impl Into<String>, pool: impl Into<String>) -> Self {
        Self {
            registry: registry.into(),
            pool: pool.into(),
            policy: "churn".into(),
            id: None,
            incarnation: None,
            slot: None,
            size: None,
            exclusive: false,
            coalesce_ms: 50,
            listen: "127.0.0.1:0".into(),
            advertise_host: None,
            max_concurrency: None,
            rpc_worker_threads: 4,
            service: None,
        }
    }

    pub fn policy(mut self, policy: impl Into<String>) -> Self {
        self.policy = policy.into();
        self
    }

    pub fn id(mut self, id: u64) -> Self {
        self.id = Some(id);
        self
    }

    pub fn incarnation(mut self, incarnation: u64) -> Self {
        self.incarnation = Some(incarnation);
        self
    }

    pub fn slot(mut self, slot: u64) -> Self {
        self.slot = Some(slot);
        self
    }

    pub fn size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    pub fn coalesce_ms(mut self, coalesce_ms: u64) -> Self {
        self.coalesce_ms = coalesce_ms;
        self
    }

    pub fn listen(mut self, listen: impl Into<String>) -> Self {
        self.listen = listen.into();
        self
    }

    pub fn advertise_host(mut self, host: impl Into<String>) -> Self {
        self.advertise_host = Some(host.into());
        self
    }

    pub fn max_concurrency(mut self, limit: usize) -> Self {
        self.max_concurrency = Some(limit);
        self
    }

    pub fn rpc_worker_threads(mut self, worker_threads: usize) -> Self {
        self.rpc_worker_threads = worker_threads.max(1);
        self
    }

    pub fn service(mut self, service: Arc<dyn Service>) -> Self {
        self.service = Some(service);
        self
    }

    pub fn router(self, router: Router) -> Self {
        self.service(Arc::new(router))
    }

    pub fn join(self, timeout: Duration) -> Result<Member, MemberError> {
        if self.pool.is_empty() {
            return Err(MemberError::Invalid("pool cannot be empty".into()));
        }
        if self.size == Some(0) {
            return Err(MemberError::Invalid("size must be positive".into()));
        }
        if self
            .slot
            .zip(self.size)
            .is_some_and(|(slot, size)| slot >= size)
        {
            return Err(MemberError::Invalid(
                "slot is outside the declared size".into(),
            ));
        }
        let id = self
            .id
            .unwrap_or_else(|| self.slot.unwrap_or_else(|| fastrand::u64(..(1 << 63))));
        let incarnation = self.incarnation.unwrap_or_else(|| {
            let millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            (millis << 20) | fastrand::u64(..(1 << 20))
        });
        let methods = self
            .service
            .as_ref()
            .map(|service| service.methods().to_vec())
            .unwrap_or_default();
        let shared = Arc::new(Shared::new(
            self.registry,
            self.pool.clone(),
            id,
            incarnation,
            self.policy,
            self.slot,
            self.size,
            None,
            methods,
            self.exclusive,
            self.coalesce_ms,
        ));
        let identity = identity(&self.pool, self.slot, id, incarnation);
        let rpc_runtime = RpcRuntime::new(self.rpc_worker_threads)?;
        let client = rpc_runtime.client();
        let mut server = if let Some(service) = self.service {
            let owned = Arc::new(OwnedService {
                shared: shared.clone(),
                inner: service,
            });
            let mut config = ServerConfig::new(self.listen, identity.clone());
            config.max_concurrency = self.max_concurrency;
            let server = rpc_runtime.start_server(config, owned)?;
            let endpoint = advertised_endpoint(server.endpoint(), self.advertise_host.as_deref())?;
            {
                let mut publication = shared.published.lock().unwrap();
                publication.url = Some(endpoint);
                publication.version += 1;
            }
            Some(server)
        } else {
            None
        };
        shared.watch.lock().unwrap().push(self.pool.clone());
        let runtime = spawn(shared.clone());
        let first_budget = timeout.min(Duration::from_secs(5));
        let _ = beat_once(&runtime, &shared, first_budget, true);
        if !shared.wait_registered(timeout) {
            if let Some(server) = server.as_mut() {
                server.close();
            }
            runtime.shutdown_background();
            return Err(MemberError::Timeout(format!(
                "no registry answer within {}ms: {}",
                timeout.as_millis(),
                shared.last_error.lock().unwrap()
            )));
        }
        if !shared.accepted.load(Ordering::Relaxed) {
            if let Some(server) = server.as_mut() {
                server.close();
            }
            runtime.shutdown_background();
            return Err(MemberError::Refused(shared.refused.lock().unwrap().clone()));
        }
        Ok(Member {
            identity,
            shared,
            runtime: Mutex::new(Some(runtime)),
            server: Mutex::new(server),
            client,
        })
    }

    pub async fn join_async(self, timeout: Duration) -> Result<Member, MemberError> {
        tokio::task::spawn_blocking(move || self.join(timeout))
            .await
            .map_err(|error| MemberError::Registry(format!("join worker failed: {error}")))?
    }
}

pub struct Member {
    identity: String,
    shared: Arc<Shared>,
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    server: Mutex<Option<Server>>,
    client: Client,
}

impl Member {
    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn endpoint(&self) -> Option<String> {
        self.shared.published.lock().unwrap().url.clone()
    }

    pub fn rpc(&self) -> &Client {
        &self.client
    }

    pub fn ready<T: Serialize>(&self, state: &T) -> Result<bool, MemberError> {
        self.publish(state, Some(true))
    }

    pub fn publish<T: Serialize>(
        &self,
        state: &T,
        ready: Option<bool>,
    ) -> Result<bool, MemberError> {
        let state =
            serde_json::to_value(state).map_err(|error| MemberError::Invalid(error.to_string()))?;
        let mut publication = self.shared.published.lock().unwrap();
        let ready = ready.unwrap_or(publication.ready);
        if publication.state == state && publication.ready == ready {
            return Ok(false);
        }
        publication.state = state;
        publication.ready = ready;
        publication.version += 1;
        drop(publication);
        self.shared.wake.notify_one();
        Ok(true)
    }

    pub fn set_ready(&self, ready: bool) -> bool {
        let mut publication = self.shared.published.lock().unwrap();
        if publication.ready == ready {
            return false;
        }
        publication.ready = ready;
        publication.version += 1;
        drop(publication);
        self.shared.wake.notify_one();
        true
    }

    pub fn watch(&self, pools: impl IntoIterator<Item = String>) -> Result<(), MemberError> {
        let mut watch = self.shared.watch.lock().unwrap();
        for pool in pools {
            if !watch.contains(&pool) {
                if watch.len() >= MAX_WATCH {
                    return Err(MemberError::Invalid(format!(
                        "cannot watch more than {MAX_WATCH} pools"
                    )));
                }
                watch.push(pool);
            }
        }
        drop(watch);
        self.shared.wake.notify_one();
        Ok(())
    }

    pub fn pool(&self, name: impl Into<String>) -> Result<DiscoveryPool, MemberError> {
        let name = name.into();
        if name.is_empty() {
            return Err(MemberError::Invalid("pool cannot be empty".into()));
        }
        self.watch(std::iter::once(name.clone()))?;
        Ok(DiscoveryPool::new(name, self.shared.clone()))
    }

    pub fn members(&self, pool: &str, require_ready: bool) -> Vec<DiscoveredMember> {
        self.shared
            .cache
            .read()
            .unwrap()
            .get(pool)
            .map(|cached| {
                cached
                    .native(require_ready)
                    .members()
                    .iter()
                    .map(|member| member.as_ref().clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn pool_methods(&self, pool: &str) -> Vec<String> {
        self.shared
            .cache
            .read()
            .unwrap()
            .get(pool)
            .map(|cached| cached.methods.to_vec())
            .unwrap_or_default()
    }

    pub fn flush(&self, timeout: Duration) -> Result<(), MemberError> {
        let wanted = self.shared.published.lock().unwrap().version;
        self.shared.wait_publication(wanted, timeout);
        if self.shared.confirmed.load(Ordering::Relaxed) >= wanted {
            return Ok(());
        }
        if !self.shared.accepted.load(Ordering::Relaxed) {
            return Err(MemberError::Refused(
                self.shared.refused.lock().unwrap().clone(),
            ));
        }
        Err(MemberError::Timeout(format!(
            "publication {wanted} was not acknowledged within {}ms",
            timeout.as_millis()
        )))
    }

    pub async fn flush_async(&self, timeout: Duration) -> Result<(), MemberError> {
        let wanted = self.shared.published.lock().unwrap().version;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.shared.beat_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.shared.confirmed.load(Ordering::Relaxed) >= wanted {
                return Ok(());
            }
            if !self.shared.accepted.load(Ordering::Relaxed) {
                return Err(MemberError::Refused(
                    self.shared.refused.lock().unwrap().clone(),
                ));
            }
            if tokio::time::timeout_at(deadline, &mut notified)
                .await
                .is_err()
            {
                if self.shared.confirmed.load(Ordering::Relaxed) >= wanted {
                    return Ok(());
                }
                if !self.shared.accepted.load(Ordering::Relaxed) {
                    return Err(MemberError::Refused(
                        self.shared.refused.lock().unwrap().clone(),
                    ));
                }
                return Err(MemberError::Timeout(format!(
                    "publication {wanted} was not acknowledged within {}ms",
                    timeout.as_millis()
                )));
            }
        }
    }

    pub fn stats(&self) -> MemberStats {
        MemberStats {
            beats_ok: self.shared.beats_ok.load(Ordering::Relaxed),
            beats_failed: self.shared.beats_failed.load(Ordering::Relaxed),
            interval_ms: self.shared.interval_ms.load(Ordering::Relaxed),
            confirmed_publication: self.shared.confirmed.load(Ordering::Relaxed),
            published_version: self.shared.published.lock().unwrap().version,
            accepted: self.shared.accepted.load(Ordering::Relaxed),
        }
    }

    pub fn leave(&self) {
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| self.leave_inner())
                    .join()
                    .expect("tinyray leave worker panicked");
            });
        } else {
            self.leave_inner();
        }
    }

    pub async fn leave_async(self) -> Result<(), MemberError> {
        tokio::task::spawn_blocking(move || self.leave())
            .await
            .map_err(|error| MemberError::Registry(format!("leave worker failed: {error}")))
    }

    fn leave_inner(&self) {
        self.shared.leaving.store(true, Ordering::Relaxed);
        if let Some(runtime) = self.runtime.lock().unwrap().take() {
            if self.shared.beats_ok.load(Ordering::Relaxed) > 0 {
                let _ = beat_once(&runtime, &self.shared, Duration::from_secs(5), false);
            }
            runtime.shutdown_background();
        }
        if let Some(mut server) = self.server.lock().unwrap().take() {
            server.close();
        }
    }
}

impl Drop for Member {
    fn drop(&mut self) {
        self.leave();
    }
}

struct OwnedService {
    shared: Arc<Shared>,
    inner: Arc<dyn Service>,
}

#[async_trait]
impl Service for OwnedService {
    fn methods(&self) -> &[String] {
        self.inner.methods()
    }

    async fn dispatch(&self, request: ServiceRequest) -> ServiceResponse {
        if !self.shared.accepted.load(Ordering::Acquire) {
            return ServiceResponse::error(ServiceError::Fenced(
                "the member is held by a later tenure".into(),
            ));
        }
        self.inner.dispatch(request).await
    }
}

fn identity(pool: &str, slot: Option<u64>, id: u64, incarnation: u64) -> String {
    format!("{pool}/{}#{incarnation}", slot.unwrap_or(id))
}

fn advertised_endpoint(endpoint: &str, host: Option<&str>) -> Result<String, MemberError> {
    let Some(host) = host else {
        return Ok(endpoint.to_owned());
    };
    if host.is_empty() || host.contains(':') && !host.starts_with('[') {
        return Err(MemberError::Invalid(
            "advertise_host must be a hostname, IPv4 address, or bracketed IPv6 address".into(),
        ));
    }
    let (_, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| MemberError::Invalid("listener endpoint has no port".into()))?;
    Ok(format!("{host}:{port}"))
}
