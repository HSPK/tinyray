use crate::member::MemberError;
use crate::membership_core::{CacheWaiter, FrozenPool, Shared, WaitResult, WaitStatus};
use serde::Serialize;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tinyray_proto::Member as DiscoveredMember;

#[derive(Clone, Debug)]
pub struct MemberRef {
    pool: Arc<str>,
    member: Arc<DiscoveredMember>,
}

impl MemberRef {
    fn new(pool: Arc<str>, member: Arc<DiscoveredMember>) -> Self {
        Self { pool, member }
    }

    pub fn pool(&self) -> &str {
        &self.pool
    }

    pub fn identity(&self) -> String {
        format!(
            "{}/{}#{}",
            self.pool,
            self.member.slot.unwrap_or(self.member.id),
            self.member.incarnation
        )
    }
}

impl Deref for MemberRef {
    type Target = DiscoveredMember;

    fn deref(&self) -> &Self::Target {
        &self.member
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pool: Arc<str>,
    frozen: FrozenPool,
}

impl Snapshot {
    fn new(pool: Arc<str>, frozen: FrozenPool) -> Self {
        Self { pool, frozen }
    }

    pub fn pool(&self) -> &str {
        &self.pool
    }

    pub fn len(&self) -> usize {
        self.frozen.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frozen.members.is_empty()
    }

    pub fn revision(&self) -> u64 {
        self.frozen.version
    }

    pub fn fingerprint(&self) -> u64 {
        self.frozen.members.roster()
    }

    pub fn roster(&self) -> u64 {
        self.frozen.roster
    }

    pub fn size(&self) -> Option<u64> {
        self.frozen.size
    }

    pub fn methods(&self) -> &[String] {
        &self.frozen.methods
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = MemberRef> + '_ {
        self.frozen
            .members
            .members()
            .iter()
            .cloned()
            .map(|member| MemberRef::new(self.pool.clone(), member))
    }

    pub fn members(&self) -> Vec<MemberRef> {
        self.iter().collect()
    }

    pub fn slot(&self, slot: u64) -> Option<MemberRef> {
        self.frozen
            .members
            .slot_arc(slot)
            .cloned()
            .map(|member| MemberRef::new(self.pool.clone(), member))
    }

    pub fn get(&self, identity: &str) -> Option<MemberRef> {
        self.frozen
            .members
            .get_arc(&self.pool, identity)
            .cloned()
            .map(|member| MemberRef::new(self.pool.clone(), member))
    }
}

#[derive(Clone)]
pub struct Epoch {
    snapshot: Snapshot,
    shared: Arc<Shared>,
}

impl Epoch {
    pub fn valid(&self) -> bool {
        self.shared
            .epoch_valid(self.snapshot.pool(), self.snapshot.roster())
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
}

impl Deref for Epoch {
    type Target = Snapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

#[derive(Clone)]
pub struct DiscoveryPool {
    name: Arc<str>,
    shared: Arc<Shared>,
    rng: Arc<Mutex<fastrand::Rng>>,
}

impl DiscoveryPool {
    pub(crate) fn new(name: String, shared: Arc<Shared>) -> Self {
        Self {
            name: Arc::from(name),
            shared,
            rng: Arc::new(Mutex::new(fastrand::Rng::new())),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn snapshot(&self, include_unready: bool) -> Option<Snapshot> {
        self.shared
            .cache
            .read()
            .unwrap()
            .get(self.name())
            .map(|pool| Snapshot::new(self.name.clone(), pool.frozen(!include_unready)))
    }

    pub fn snapshot_where<T: Serialize>(
        &self,
        filter: &T,
        require_ready: bool,
    ) -> Result<Option<Snapshot>, MemberError> {
        let filter = discovery_filter(filter)?;
        Ok(self
            .shared
            .cache
            .read()
            .unwrap()
            .get(self.name())
            .map(|pool| Snapshot::new(self.name.clone(), pool.filtered(&filter, require_ready))))
    }

    pub fn count<T: Serialize>(
        &self,
        filter: &T,
        require_ready: bool,
    ) -> Result<usize, MemberError> {
        let filter = discovery_filter(filter)?;
        Ok(self
            .shared
            .cache
            .read()
            .unwrap()
            .get(self.name())
            .map(|pool| pool.count(&filter, require_ready))
            .unwrap_or_default())
    }

    pub fn pick<T: Serialize>(
        &self,
        filter: &T,
        require_ready: bool,
    ) -> Result<Option<MemberRef>, MemberError> {
        let filter = discovery_filter(filter)?;
        Ok(self
            .shared
            .cache
            .read()
            .unwrap()
            .get(self.name())
            .and_then(|pool| pool.choose_arc(&filter, require_ready, &mut self.rng.lock().unwrap()))
            .map(|member| MemberRef::new(self.name.clone(), member)))
    }

    pub fn slot(&self, slot: u64, require_ready: bool) -> Option<MemberRef> {
        self.shared
            .cache
            .read()
            .unwrap()
            .get(self.name())
            .and_then(|pool| pool.slot_owned(slot, require_ready))
            .map(|member| MemberRef::new(self.name.clone(), member))
    }

    pub fn get(&self, identity: &str) -> Option<MemberRef> {
        self.snapshot(true)?.get(identity)
    }

    pub fn wait_count<T: Serialize>(
        &self,
        count: usize,
        filter: &T,
        timeout: Duration,
    ) -> Result<Snapshot, MemberError> {
        let filter = discovery_filter(filter)?;
        let target =
            i128::try_from(count).map_err(|_| MemberError::Invalid("count is too large".into()))?;
        let waiter =
            CacheWaiter::count(self.shared.clone(), self.name().to_owned(), filter, target);
        snapshot_wait_result(self, waiter.wait(Some(timeout), true), timeout)
    }

    pub async fn wait_count_async<T: Serialize>(
        &self,
        count: usize,
        filter: &T,
        timeout: Duration,
    ) -> Result<Snapshot, MemberError> {
        let filter = discovery_filter(filter)?;
        let target =
            i128::try_from(count).map_err(|_| MemberError::Invalid("count is too large".into()))?;
        let waiter =
            CacheWaiter::count(self.shared.clone(), self.name().to_owned(), filter, target);
        snapshot_wait_result(self, waiter.wait_async(Some(timeout), true).await, timeout)
    }

    pub fn wait_departure(&self, identity: &str, timeout: Duration) -> Result<bool, MemberError> {
        let waiter = CacheWaiter::departure(
            self.shared.clone(),
            self.name().to_owned(),
            identity.to_owned(),
        );
        bool_wait_result(self, waiter.wait(Some(timeout), true), timeout)
    }

    pub async fn wait_departure_async(
        &self,
        identity: &str,
        timeout: Duration,
    ) -> Result<bool, MemberError> {
        let waiter = CacheWaiter::departure(
            self.shared.clone(),
            self.name().to_owned(),
            identity.to_owned(),
        );
        bool_wait_result(self, waiter.wait_async(Some(timeout), true).await, timeout)
    }

    pub fn wait_replacement(
        &self,
        slot: u64,
        previous: Option<&str>,
        timeout: Duration,
    ) -> Result<Option<MemberRef>, MemberError> {
        let waiter = CacheWaiter::replacement(
            self.shared.clone(),
            self.name().to_owned(),
            slot,
            previous.map(str::to_owned),
            previous.is_none(),
        );
        replacement_wait_result(self, slot, waiter.wait(Some(timeout), true), timeout)
    }

    pub async fn wait_replacement_async(
        &self,
        slot: u64,
        previous: Option<&str>,
        timeout: Duration,
    ) -> Result<Option<MemberRef>, MemberError> {
        let waiter = CacheWaiter::replacement(
            self.shared.clone(),
            self.name().to_owned(),
            slot,
            previous.map(str::to_owned),
            previous.is_none(),
        );
        replacement_wait_result(
            self,
            slot,
            waiter.wait_async(Some(timeout), true).await,
            timeout,
        )
    }

    pub fn epoch(&self, minimum: Option<usize>, timeout: Duration) -> Result<Epoch, MemberError> {
        let minimum = minimum
            .map(i128::try_from)
            .transpose()
            .map_err(|_| MemberError::Invalid("minimum is too large".into()))?;
        let result = self.shared.wait_epoch(self.name(), minimum, timeout);
        match (result.status, result.view) {
            (WaitStatus::Ready, Some(view)) => Ok(Epoch {
                snapshot: Snapshot::new(self.name.clone(), view),
                shared: self.shared.clone(),
            }),
            (WaitStatus::Fenced, _) => Err(MemberError::Refused(format!(
                "this member lost its seat while waiting for pool {:?}",
                self.name()
            ))),
            (WaitStatus::Stale, _) => Err(MemberError::Registry(format!(
                "registry contact for pool {:?} is stale by {}ms",
                self.name(),
                result.silence_ms
            ))),
            (WaitStatus::NoSize, _) => Err(MemberError::Invalid(format!(
                "pool {:?} declares no size; pass minimum",
                self.name()
            ))),
            (WaitStatus::Mismatch, _) => Err(MemberError::Timeout(format!(
                "pool {:?} has {} ready member(s), but readiness does not match its roster",
                self.name(),
                result.found
            ))),
            _ => Err(MemberError::Timeout(format!(
                "pool {:?} did not reach {} member(s) within {}ms",
                self.name(),
                result.target,
                timeout.as_millis()
            ))),
        }
    }
}

fn discovery_filter<T: Serialize>(filter: &T) -> Result<serde_json::Value, MemberError> {
    let value =
        serde_json::to_value(filter).map_err(|error| MemberError::Invalid(error.to_string()))?;
    if !value.is_object() {
        return Err(MemberError::Invalid(
            "a discovery filter must serialize as an object".into(),
        ));
    }
    Ok(value)
}

fn snapshot_wait_result(
    pool: &DiscoveryPool,
    result: WaitResult,
    timeout: Duration,
) -> Result<Snapshot, MemberError> {
    match (result.status, result.view) {
        (WaitStatus::Ready, Some(view)) => Ok(Snapshot::new(pool.name.clone(), view)),
        (WaitStatus::Fenced, _) => Err(MemberError::Refused(format!(
            "this member lost its seat while waiting for pool {:?}",
            pool.name()
        ))),
        (WaitStatus::Closed, _) => Err(MemberError::Registry(format!(
            "wait for pool {:?} was closed",
            pool.name()
        ))),
        _ => Err(MemberError::Timeout(format!(
            "pool {:?} matched {} of the requested members within {}ms",
            pool.name(),
            result.matched,
            timeout.as_millis()
        ))),
    }
}

fn bool_wait_result(
    pool: &DiscoveryPool,
    result: WaitResult,
    timeout: Duration,
) -> Result<bool, MemberError> {
    match result.status {
        WaitStatus::Ready => Ok(true),
        WaitStatus::Timeout => Ok(false),
        WaitStatus::Fenced => Err(MemberError::Refused(format!(
            "this member lost its seat while waiting for pool {:?}",
            pool.name()
        ))),
        WaitStatus::Closed => Err(MemberError::Registry(format!(
            "wait for pool {:?} was closed",
            pool.name()
        ))),
        _ => Err(MemberError::Timeout(format!(
            "wait for pool {:?} did not complete within {}ms",
            pool.name(),
            timeout.as_millis()
        ))),
    }
}

fn replacement_wait_result(
    pool: &DiscoveryPool,
    slot: u64,
    result: WaitResult,
    timeout: Duration,
) -> Result<Option<MemberRef>, MemberError> {
    match (result.status, result.view) {
        (WaitStatus::Ready, Some(view)) => Ok(Snapshot::new(pool.name.clone(), view).slot(slot)),
        (WaitStatus::Timeout, _) => Ok(None),
        (WaitStatus::Fenced, _) => Err(MemberError::Refused(format!(
            "this member lost its seat while waiting for pool {:?}",
            pool.name()
        ))),
        (WaitStatus::Closed, _) => Err(MemberError::Registry(format!(
            "wait for pool {:?} was closed",
            pool.name()
        ))),
        _ => Err(MemberError::Timeout(format!(
            "seat {slot} of pool {:?} was not replaced within {}ms",
            pool.name(),
            timeout.as_millis()
        ))),
    }
}
