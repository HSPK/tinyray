//! The per-actor result buffer.
//!
//! This is what tinyray has instead of a distributed object store. A result
//! stays in the process that produced it; consumers fetch it directly. That
//! keeps `rollout -> learner` traffic off the driver without any of the
//! machinery of a real object store: no spilling, no lineage, no distributed
//! reference counting.
//!
//! What it does have, because without them a long RL run would exhaust memory:
//!
//! * a byte watermark with LRU eviction,
//! * a TTL sweeper,
//! * tombstones, so a consumer that arrives late gets `ObjectLost` rather than
//!   an ambiguous "never heard of it".

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tinyray_core::proto::{ErrorKind, RemoteError};
use tinyray_core::TaskId;
use tokio::sync::Notify;

/// A completed result: the pickle body plus its out-of-band frames.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredValue {
    pub body: Bytes,
    pub frames: Vec<Bytes>,
}

impl StoredValue {
    pub fn new(body: Bytes, frames: Vec<Bytes>) -> StoredValue {
        StoredValue { body, frames }
    }

    pub fn len(&self) -> u64 {
        self.body.len() as u64 + self.frames.iter().map(|f| f.len() as u64).sum::<u64>()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Outcome of a fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fetched {
    /// The value is ready.
    Ready(StoredValue),
    /// The task failed; the error is the user's or the runtime's.
    Failed(RemoteError),
    /// Still running when the caller's deadline expired.
    NotReady,
    /// Known to have existed, but evicted or expired.
    Lost,
    /// Never seen. Usually a bug, or a result whose tombstone has aged out.
    Unknown,
}

/// Tuning knobs for a [`LocalStore`].
#[derive(Debug, Clone, Copy)]
pub struct StoreConfig {
    /// Evict least-recently-used results once the store exceeds this.
    pub max_bytes: u64,
    /// Drop results nobody has fetched within this window.
    pub ttl: Duration,
    /// How many evicted ids to remember so their fetches report `Lost`.
    pub tombstone_capacity: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            // 32 actors each holding a few 10 MB results adds up fast; 2 GiB is
            // a deliberate ceiling rather than an aspiration.
            max_bytes: 2 << 30,
            ttl: Duration::from_secs(300),
            tombstone_capacity: 65_536,
        }
    }
}

/// Point-in-time view of the store, for `/introspect` and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StoreStats {
    pub pending: usize,
    pub ready: usize,
    pub failed: usize,
    pub bytes: u64,
    pub evictions: u64,
    pub expirations: u64,
}

#[derive(Debug)]
enum SlotState {
    Pending,
    Ready(StoredValue),
    Failed(RemoteError),
}

struct Slot {
    state: SlotState,
    notify: Arc<Notify>,
    created: Instant,
    /// Key into the LRU index; `None` while pending (nothing to evict yet).
    lru_key: Option<u64>,
    bytes: u64,
}

struct Inner {
    slots: HashMap<TaskId, Slot>,
    /// access counter -> task, giving a total order for eviction.
    lru: BTreeMap<u64, TaskId>,
    next_access: u64,
    total_bytes: u64,
    tombstones: VecDeque<TaskId>,
    tombstone_set: HashSet<TaskId>,
    evictions: u64,
    expirations: u64,
}

/// Results produced by one actor, held until consumers fetch them.
pub struct LocalStore {
    config: StoreConfig,
    inner: Mutex<Inner>,
}

impl LocalStore {
    pub fn new(config: StoreConfig) -> LocalStore {
        LocalStore {
            config,
            inner: Mutex::new(Inner {
                slots: HashMap::new(),
                lru: BTreeMap::new(),
                next_access: 0,
                total_bytes: 0,
                tombstones: VecDeque::new(),
                tombstone_set: HashSet::new(),
                evictions: 0,
                expirations: 0,
            }),
        }
    }

    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    /// Register a task that is about to run, so fetches can block on it
    /// instead of racing ahead and getting `Unknown`.
    pub fn declare_pending(&self, task_id: TaskId) {
        let mut inner = self.inner.lock();
        inner.slots.entry(task_id).or_insert_with(|| Slot {
            state: SlotState::Pending,
            notify: Arc::new(Notify::new()),
            created: Instant::now(),
            lru_key: None,
            bytes: 0,
        });
    }

    /// Publish a successful result and wake any waiters.
    pub fn complete(&self, task_id: TaskId, value: StoredValue) {
        let bytes = value.len();
        self.finish(task_id, SlotState::Ready(value), bytes);
    }

    /// Publish a failure and wake any waiters.
    pub fn fail(&self, task_id: TaskId, error: RemoteError) {
        self.finish(task_id, SlotState::Failed(error), 0);
    }

    fn finish(&self, task_id: TaskId, state: SlotState, bytes: u64) {
        let notify = {
            let mut inner = self.inner.lock();
            let access = inner.next_access;
            inner.next_access += 1;

            // Borrowing `inner.slots` through the entry API leaves the other
            // fields free, which is what lets the bookkeeping below run without
            // a second lookup.
            let (notify, replaced) = match inner.slots.entry(task_id) {
                Entry::Occupied(mut occupied) => {
                    let slot = occupied.get_mut();
                    let freed = slot.bytes;
                    let old_key = slot.lru_key.take();
                    slot.state = state;
                    slot.bytes = bytes;
                    slot.lru_key = Some(access);
                    (slot.notify.clone(), Some((freed, old_key)))
                }
                Entry::Vacant(vacant) => {
                    // A result completed for a task nobody declared. Keep it:
                    // the fetch may simply not have arrived yet.
                    let notify = Arc::new(Notify::new());
                    vacant.insert(Slot {
                        state,
                        notify: notify.clone(),
                        created: Instant::now(),
                        lru_key: Some(access),
                        bytes,
                    });
                    (notify, None)
                }
            };
            if let Some((freed, old_key)) = replaced {
                inner.total_bytes -= freed;
                if let Some(old) = old_key {
                    inner.lru.remove(&old);
                }
            }
            inner.lru.insert(access, task_id);
            inner.total_bytes += bytes;
            // Protect what we just produced: something is about to fetch it,
            // and evicting the newest entry is never the right call. Without
            // this, a single result larger than the whole watermark evicts
            // itself before anyone can read it.
            inner.evict_to_watermark(
                self.config.max_bytes,
                self.config.tombstone_capacity,
                Some(task_id),
            );
            notify
        };
        // Wake outside the lock so a woken task never blocks on us.
        notify.notify_waiters();
    }

    /// Non-blocking read.
    pub fn try_get(&self, task_id: TaskId) -> Fetched {
        let mut inner = self.inner.lock();
        let access = inner.next_access;

        let outcome = inner.slots.get_mut(&task_id).map(|slot| match &slot.state {
            SlotState::Pending => (Fetched::NotReady, None),
            SlotState::Ready(value) => {
                // Touch: fetching a result makes it recently used.
                let old = slot.lru_key.replace(access);
                (Fetched::Ready(value.clone()), Some(old))
            }
            SlotState::Failed(err) => (Fetched::Failed(err.clone()), None),
        });

        match outcome {
            Some((fetched, touch)) => {
                if let Some(old) = touch {
                    if let Some(old) = old {
                        inner.lru.remove(&old);
                    }
                    inner.lru.insert(access, task_id);
                    inner.next_access += 1;
                }
                fetched
            }
            None if inner.tombstone_set.contains(&task_id) => Fetched::Lost,
            None => Fetched::Unknown,
        }
    }

    /// Wait for a result, up to `timeout`.
    ///
    /// This is the long-poll behind `GET /task/{id}/result`: a consumer that
    /// asks before the producer has finished parks here rather than spinning.
    pub async fn get(&self, task_id: TaskId, timeout: Duration) -> Fetched {
        let deadline = Instant::now() + timeout;
        loop {
            // Register interest *before* re-reading the state, otherwise a
            // completion landing between the two would be missed.
            let notify = {
                let inner = self.inner.lock();
                inner.slots.get(&task_id).map(|slot| slot.notify.clone())
            };
            let Some(notify) = notify else {
                return self.try_get(task_id);
            };
            let notified = notify.notified();
            tokio::pin!(notified);

            match self.try_get(task_id) {
                Fetched::NotReady => {}
                other => return other,
            }

            let now = Instant::now();
            if now >= deadline {
                return Fetched::NotReady;
            }
            if tokio::time::timeout(deadline - now, notified)
                .await
                .is_err()
            {
                return self.try_get_or_not_ready(task_id);
            }
        }
    }

    fn try_get_or_not_ready(&self, task_id: TaskId) -> Fetched {
        match self.try_get(task_id) {
            Fetched::Unknown => Fetched::NotReady,
            other => other,
        }
    }

    /// Drop a result. Best effort: the consumer may already be gone.
    ///
    /// Leaves a tombstone, so a late fetch reports `ObjectLost` rather than the
    /// much more alarming `NotFound`.
    pub fn release(&self, task_id: TaskId) -> bool {
        let mut inner = self.inner.lock();
        let removed = inner.remove(task_id, false).is_some();
        if removed {
            let capacity = self.config.tombstone_capacity;
            inner.tombstone(task_id, capacity);
        }
        removed
    }

    /// Evict everything past its TTL. Called periodically by the runtime.
    pub fn sweep_expired(&self) -> usize {
        let mut inner = self.inner.lock();
        let ttl = self.config.ttl;
        let now = Instant::now();
        let expired: Vec<TaskId> = inner
            .slots
            .iter()
            .filter(|(_, slot)| {
                !matches!(slot.state, SlotState::Pending) && now.duration_since(slot.created) >= ttl
            })
            .map(|(id, _)| *id)
            .collect();
        let capacity = self.config.tombstone_capacity;
        for task_id in &expired {
            inner.remove(*task_id, true);
            inner.tombstone(*task_id, capacity);
        }
        inner.expirations += expired.len() as u64;
        expired.len()
    }

    pub fn stats(&self) -> StoreStats {
        let inner = self.inner.lock();
        let mut stats = StoreStats {
            bytes: inner.total_bytes,
            evictions: inner.evictions,
            expirations: inner.expirations,
            ..Default::default()
        };
        for slot in inner.slots.values() {
            match slot.state {
                SlotState::Pending => stats.pending += 1,
                SlotState::Ready(_) => stats.ready += 1,
                SlotState::Failed(_) => stats.failed += 1,
            }
        }
        stats
    }

    /// Fail every pending task, e.g. when the actor is shutting down.
    pub fn fail_all_pending(&self, error: RemoteError) {
        let pending: Vec<TaskId> = {
            let inner = self.inner.lock();
            inner
                .slots
                .iter()
                .filter(|(_, slot)| matches!(slot.state, SlotState::Pending))
                .map(|(id, _)| *id)
                .collect()
        };
        for task_id in pending {
            self.fail(task_id, error.clone());
        }
    }
}

impl Inner {
    fn remove(&mut self, task_id: TaskId, _expired: bool) -> Option<Slot> {
        let slot = self.slots.remove(&task_id)?;
        if let Some(key) = slot.lru_key {
            self.lru.remove(&key);
        }
        self.total_bytes -= slot.bytes;
        Some(slot)
    }

    fn tombstone(&mut self, task_id: TaskId, capacity: usize) {
        if capacity == 0 {
            return;
        }
        if self.tombstone_set.insert(task_id) {
            self.tombstones.push_back(task_id);
            while self.tombstones.len() > capacity {
                if let Some(old) = self.tombstones.pop_front() {
                    self.tombstone_set.remove(&old);
                }
            }
        }
    }

    fn evict_to_watermark(
        &mut self,
        max_bytes: u64,
        tombstone_capacity: usize,
        protect: Option<TaskId>,
    ) {
        while self.total_bytes > max_bytes {
            // Walk from least recently used, skipping the protected entry.
            let victim = self
                .lru
                .iter()
                .find(|(_, task_id)| Some(**task_id) != protect)
                .map(|(key, task_id)| (*key, *task_id));
            let Some((key, task_id)) = victim else {
                // Only the protected entry is left; the watermark is a target,
                // not a hard cap, and losing this result helps nobody.
                break;
            };
            self.lru.remove(&key);
            if self.remove(task_id, false).is_some() {
                self.tombstone(task_id, tombstone_capacity);
                self.evictions += 1;
            }
        }
    }
}

/// The error a consumer sees when a result has been evicted or expired.
pub fn object_lost(task_id: TaskId, reason: &str) -> RemoteError {
    RemoteError {
        task_id,
        kind: ErrorKind::ObjectLost,
        message: format!("result {task_id} is no longer available: {reason}"),
        traceback: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(size: usize) -> StoredValue {
        StoredValue::new(
            Bytes::from_static(b"body"),
            vec![Bytes::from(vec![7u8; size])],
        )
    }

    fn err(task_id: TaskId) -> RemoteError {
        RemoteError {
            task_id,
            kind: ErrorKind::UserException,
            message: "boom".into(),
            traceback: Some("Traceback".into()),
        }
    }

    fn store() -> LocalStore {
        LocalStore::new(StoreConfig::default())
    }

    #[test]
    fn unknown_task_is_distinguishable_from_pending() {
        let store = store();
        let task = TaskId::generate();
        assert_eq!(store.try_get(task), Fetched::Unknown);
        store.declare_pending(task);
        assert_eq!(store.try_get(task), Fetched::NotReady);
    }

    #[test]
    fn complete_then_fetch() {
        let store = store();
        let task = TaskId::generate();
        store.declare_pending(task);
        store.complete(task, value(128));
        assert_eq!(store.try_get(task), Fetched::Ready(value(128)));
        // Fetching does not consume: several consumers may want the same result.
        assert_eq!(store.try_get(task), Fetched::Ready(value(128)));
    }

    #[test]
    fn failure_is_reported_to_every_consumer() {
        let store = store();
        let task = TaskId::generate();
        store.declare_pending(task);
        store.fail(task, err(task));
        assert_eq!(store.try_get(task), Fetched::Failed(err(task)));
        assert_eq!(store.try_get(task), Fetched::Failed(err(task)));
    }

    #[test]
    fn completion_without_a_declaration_is_kept() {
        // The fetch may simply not have arrived yet; dropping the result would
        // turn a benign race into a lost object.
        let store = store();
        let task = TaskId::generate();
        store.complete(task, value(16));
        assert_eq!(store.try_get(task), Fetched::Ready(value(16)));
    }

    #[test]
    fn release_frees_bytes() {
        let store = store();
        let task = TaskId::generate();
        store.complete(task, value(1024));
        assert!(store.stats().bytes >= 1024);
        assert!(store.release(task));
        assert_eq!(store.stats().bytes, 0);
        assert!(!store.release(task), "second release is a no-op");
    }

    #[test]
    fn watermark_evicts_least_recently_used() {
        let store = LocalStore::new(StoreConfig {
            max_bytes: 3_000,
            ..Default::default()
        });
        let a = TaskId::from_parts(0, 1);
        let b = TaskId::from_parts(0, 2);
        let c = TaskId::from_parts(0, 3);
        store.complete(a, value(1000));
        store.complete(b, value(1000));
        // Touch `a` so `b` becomes the least recently used.
        assert!(matches!(store.try_get(a), Fetched::Ready(_)));
        store.complete(c, value(1000));

        assert!(store.stats().bytes <= 3_000);
        assert_eq!(store.try_get(b), Fetched::Lost, "LRU victim must be b");
        assert!(matches!(store.try_get(a), Fetched::Ready(_)));
        assert!(matches!(store.try_get(c), Fetched::Ready(_)));
        assert_eq!(store.stats().evictions, 1);
    }

    #[test]
    fn eviction_reports_lost_not_unknown() {
        // The difference matters: `Lost` means "it existed, you were too late",
        // which points at the watermark. `Unknown` points at a bug.
        let store = LocalStore::new(StoreConfig {
            max_bytes: 100,
            ..Default::default()
        });
        let first = TaskId::from_parts(9, 1);
        let second = TaskId::from_parts(9, 2);
        store.complete(first, value(1000));
        store.complete(second, value(1000));
        assert_eq!(store.try_get(first), Fetched::Lost);
        assert_eq!(store.try_get(TaskId::generate()), Fetched::Unknown);
    }

    #[test]
    fn a_result_larger_than_the_whole_store_is_still_readable_once() {
        // The watermark is a target, not a hard cap. Evicting a result before
        // anyone could possibly have fetched it helps nobody, and it turns a
        // misconfigured limit into a baffling ObjectLost.
        let store = LocalStore::new(StoreConfig {
            max_bytes: 100,
            ..Default::default()
        });
        let task = TaskId::generate();
        store.complete(task, value(4096));
        assert!(
            matches!(store.try_get(task), Fetched::Ready(_)),
            "the newest result must never evict itself"
        );
        assert_eq!(store.stats().evictions, 0);

        // The next result does displace it.
        store.complete(TaskId::generate(), value(4096));
        assert_eq!(store.try_get(task), Fetched::Lost);
    }

    #[test]
    fn release_leaves_a_tombstone() {
        // A consumer that fetches after an explicit release should learn that
        // the result was dropped, not that tinyray has never heard of it.
        let store = store();
        let task = TaskId::generate();
        store.complete(task, value(64));
        assert!(store.release(task));
        assert_eq!(store.try_get(task), Fetched::Lost);
    }

    #[test]
    fn tombstones_are_bounded() {
        let store = LocalStore::new(StoreConfig {
            max_bytes: 0,
            tombstone_capacity: 4,
            ..Default::default()
        });
        let ids: Vec<TaskId> = (0..10).map(|i| TaskId::from_parts(0, i)).collect();
        for id in &ids {
            store.complete(*id, value(64));
        }
        // The newest is protected from eviction; the four before it are
        // remembered, and older ones decay to `Unknown` rather than growing
        // without bound.
        assert!(matches!(store.try_get(ids[9]), Fetched::Ready(_)));
        assert_eq!(store.try_get(ids[8]), Fetched::Lost);
        assert_eq!(store.try_get(ids[0]), Fetched::Unknown);
    }

    #[test]
    fn pending_slots_are_never_evicted() {
        let store = LocalStore::new(StoreConfig {
            max_bytes: 0,
            ..Default::default()
        });
        let pending = TaskId::generate();
        store.declare_pending(pending);
        store.complete(TaskId::generate(), value(4096));
        store.complete(TaskId::generate(), value(4096));
        assert_eq!(
            store.try_get(pending),
            Fetched::NotReady,
            "a running task must not be evicted out from under its waiter"
        );
    }

    #[test]
    fn sweep_removes_expired_results() {
        let store = LocalStore::new(StoreConfig {
            ttl: Duration::ZERO,
            ..Default::default()
        });
        let task = TaskId::generate();
        store.complete(task, value(64));
        assert_eq!(store.sweep_expired(), 1);
        assert_eq!(store.try_get(task), Fetched::Lost);
        assert_eq!(store.stats().expirations, 1);
    }

    #[test]
    fn sweep_leaves_pending_tasks_alone() {
        let store = LocalStore::new(StoreConfig {
            ttl: Duration::ZERO,
            ..Default::default()
        });
        let task = TaskId::generate();
        store.declare_pending(task);
        assert_eq!(store.sweep_expired(), 0);
        assert_eq!(store.try_get(task), Fetched::NotReady);
    }

    #[test]
    fn fail_all_pending_wakes_everything() {
        let store = store();
        let a = TaskId::from_parts(1, 1);
        let b = TaskId::from_parts(1, 2);
        let done = TaskId::from_parts(1, 3);
        store.declare_pending(a);
        store.declare_pending(b);
        store.complete(done, value(8));

        store.fail_all_pending(object_lost(TaskId::NIL, "actor is shutting down"));
        assert!(matches!(store.try_get(a), Fetched::Failed(_)));
        assert!(matches!(store.try_get(b), Fetched::Failed(_)));
        assert!(
            matches!(store.try_get(done), Fetched::Ready(_)),
            "completed results survive"
        );
    }

    #[test]
    fn stats_count_each_state() {
        let store = store();
        store.declare_pending(TaskId::from_parts(2, 1));
        store.complete(TaskId::from_parts(2, 2), value(100));
        store.fail(TaskId::from_parts(2, 3), err(TaskId::from_parts(2, 3)));
        let stats = store.stats();
        assert_eq!((stats.pending, stats.ready, stats.failed), (1, 1, 1));
        assert!(stats.bytes >= 100);
    }

    #[tokio::test]
    async fn get_returns_immediately_when_ready() {
        let store = store();
        let task = TaskId::generate();
        store.complete(task, value(32));
        assert!(matches!(
            store.get(task, Duration::from_secs(5)).await,
            Fetched::Ready(_)
        ));
    }

    #[tokio::test]
    async fn get_parks_until_the_producer_finishes() {
        let store = Arc::new(store());
        let task = TaskId::generate();
        store.declare_pending(task);

        let writer = {
            let store = store.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                store.complete(task, value(256));
            })
        };

        let fetched = store.get(task, Duration::from_secs(5)).await;
        writer.await.unwrap();
        assert!(matches!(fetched, Fetched::Ready(_)));
    }

    #[tokio::test]
    async fn get_times_out_on_a_slow_producer() {
        let store = store();
        let task = TaskId::generate();
        store.declare_pending(task);
        let fetched = store.get(task, Duration::from_millis(20)).await;
        assert_eq!(fetched, Fetched::NotReady);
    }

    #[tokio::test]
    async fn many_waiters_are_all_woken() {
        // 32 rollout actors fetching one broadcast-ish result must all wake.
        let store = Arc::new(store());
        let task = TaskId::generate();
        store.declare_pending(task);

        let waiters: Vec<_> = (0..32)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.get(task, Duration::from_secs(5)).await })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(10)).await;
        store.complete(task, value(64));

        for waiter in waiters {
            assert!(matches!(waiter.await.unwrap(), Fetched::Ready(_)));
        }
    }

    #[tokio::test]
    async fn waiter_sees_failure() {
        let store = Arc::new(store());
        let task = TaskId::generate();
        store.declare_pending(task);
        let handle = {
            let store = store.clone();
            tokio::spawn(async move { store.get(task, Duration::from_secs(5)).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        store.fail(task, err(task));
        assert!(matches!(handle.await.unwrap(), Fetched::Failed(_)));
    }

    #[tokio::test]
    async fn get_on_unknown_task_does_not_hang() {
        let store = store();
        assert_eq!(
            store.get(TaskId::generate(), Duration::from_secs(5)).await,
            Fetched::Unknown
        );
    }
}
