use super::cache::{identity_matches, member_identity};
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum WaitStatus {
    Pending = 0,
    Ready = 1,
    Timeout = 2,
    Fenced = 3,
    Closed = 4,
    Stale = 5,
    NoSize = 6,
    Mismatch = 7,
}

#[derive(Debug)]
pub struct WaitResult {
    pub status: WaitStatus,
    pub view: Option<FrozenPool>,
    pub matched: usize,
    pub total: usize,
    pub version: u64,
}

enum WaitCondition {
    Count {
        filter: serde_json::Value,
        target: i128,
    },
    Departure {
        identity: String,
    },
    Replacement {
        slot: u64,
        baseline: Mutex<ReplacementBaseline>,
    },
}

enum ReplacementBaseline {
    Capture,
    Watching(Option<String>),
}

pub struct CacheWaiter {
    shared: Arc<Shared>,
    pool: String,
    condition: WaitCondition,
    closed: AtomicBool,
}

impl CacheWaiter {
    pub fn count(
        shared: Arc<Shared>,
        pool: String,
        filter: serde_json::Value,
        target: i128,
    ) -> Self {
        Self {
            shared,
            pool,
            condition: WaitCondition::Count { filter, target },
            closed: AtomicBool::new(false),
        }
    }

    pub fn departure(shared: Arc<Shared>, pool: String, identity: String) -> Self {
        Self {
            shared,
            pool,
            condition: WaitCondition::Departure { identity },
            closed: AtomicBool::new(false),
        }
    }

    pub fn replacement(
        shared: Arc<Shared>,
        pool: String,
        slot: u64,
        previous: Option<String>,
        capture: bool,
    ) -> Self {
        Self {
            shared,
            pool,
            condition: WaitCondition::Replacement {
                slot,
                baseline: Mutex::new(if capture {
                    ReplacementBaseline::Capture
                } else {
                    ReplacementBaseline::Watching(previous)
                }),
            },
            closed: AtomicBool::new(false),
        }
    }

    fn observe(&self) -> WaitResult {
        let cache = self.shared.cache.read().unwrap();
        let cached = cache.get(&self.pool);
        let (total, version) = cached
            .map(|pool| (pool.ids(false).len(), pool.version))
            .unwrap_or((0, 0));
        match &self.condition {
            WaitCondition::Count { filter, target } => {
                let matched = cached
                    .map(|pool| pool.count(filter, true))
                    .unwrap_or_default();
                let ready = cached.is_some() && (matched as i128) >= *target;
                WaitResult {
                    status: if ready {
                        WaitStatus::Ready
                    } else {
                        WaitStatus::Pending
                    },
                    view: cached
                        .filter(|_| ready)
                        .map(|pool| pool.filtered(filter, true)),
                    matched,
                    total,
                    version,
                }
            }
            WaitCondition::Departure { identity } => {
                let departed = cached.is_some_and(|pool| pool.get(&self.pool, identity).is_none());
                WaitResult {
                    status: if departed {
                        WaitStatus::Ready
                    } else {
                        WaitStatus::Pending
                    },
                    view: None,
                    matched: usize::from(!departed),
                    total,
                    version,
                }
            }
            WaitCondition::Replacement { slot, baseline } => {
                let Some(cached) = cached else {
                    return WaitResult {
                        status: WaitStatus::Pending,
                        view: None,
                        matched: 0,
                        total,
                        version,
                    };
                };
                let current = cached.slot(*slot, false);
                let mut baseline = baseline.lock().unwrap();
                if matches!(*baseline, ReplacementBaseline::Capture) {
                    *baseline = ReplacementBaseline::Watching(
                        current.map(|member| member_identity(&self.pool, member)),
                    );
                    return WaitResult {
                        status: WaitStatus::Pending,
                        view: None,
                        matched: usize::from(current.is_some()),
                        total,
                        version,
                    };
                }
                let ReplacementBaseline::Watching(previous) = &*baseline else {
                    unreachable!();
                };
                let replacement = current.filter(|member| {
                    previous
                        .as_deref()
                        .is_none_or(|identity| !identity_matches(&self.pool, member, identity))
                });
                WaitResult {
                    status: if replacement.is_some() {
                        WaitStatus::Ready
                    } else {
                        WaitStatus::Pending
                    },
                    view: replacement.map(|_| cached.frozen(false)),
                    matched: usize::from(replacement.is_some()),
                    total,
                    version,
                }
            }
        }
    }

    fn with_status(&self, status: WaitStatus) -> WaitResult {
        let cache = self.shared.cache.read().unwrap();
        let (total, version) = cache
            .get(&self.pool)
            .map(|pool| (pool.ids(false).len(), pool.version))
            .unwrap_or((0, 0));
        WaitResult {
            status,
            view: None,
            matched: 0,
            total,
            version,
        }
    }

    pub fn check(&self, initial: bool) -> WaitResult {
        if self.closed.load(Ordering::Relaxed) {
            return self.with_status(WaitStatus::Closed);
        }
        if !initial && !self.shared.accepted.load(Ordering::Relaxed) {
            return self.with_status(WaitStatus::Fenced);
        }
        let result = self.observe();
        if result.status == WaitStatus::Ready {
            return result;
        }
        if initial && !self.shared.accepted.load(Ordering::Relaxed) {
            return self.with_status(WaitStatus::Fenced);
        }
        result
    }

    pub fn wait(&self, timeout: Option<Duration>, initial: bool) -> WaitResult {
        let deadline = timeout.map(|budget| Instant::now() + budget);
        let mut initial = initial;
        loop {
            let revision = self.shared.revision.lock().unwrap();
            if self.closed.load(Ordering::Relaxed) {
                return self.with_status(WaitStatus::Closed);
            }
            if initial {
                let result = self.observe();
                if result.status == WaitStatus::Ready {
                    return result;
                }
                if !self.shared.accepted.load(Ordering::Relaxed) {
                    return self.with_status(WaitStatus::Fenced);
                }
            } else {
                if !self.shared.accepted.load(Ordering::Relaxed) {
                    return self.with_status(WaitStatus::Fenced);
                }
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return self.with_status(WaitStatus::Timeout);
                }
                let result = self.observe();
                if result.status == WaitStatus::Ready {
                    return result;
                }
            }
            initial = false;
            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return self.with_status(WaitStatus::Timeout);
                    }
                    let _ = self
                        .shared
                        .bell
                        .wait_timeout(revision, deadline - now)
                        .unwrap();
                }
                None => {
                    drop(self.shared.bell.wait(revision).unwrap());
                }
            }
        }
    }

    pub async fn wait_async(&self, timeout: Option<Duration>, initial: bool) -> WaitResult {
        let deadline = timeout.map(|budget| tokio::time::Instant::now() + budget);
        let mut initial = initial;
        loop {
            let notified = self.shared.revision_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !initial && deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            {
                return self.with_status(WaitStatus::Timeout);
            }
            let result = self.check(initial);
            if result.status != WaitStatus::Pending {
                return result;
            }
            initial = false;
            match deadline {
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, &mut notified)
                        .await
                        .is_err()
                    {
                        return self.with_status(WaitStatus::Timeout);
                    }
                }
                None => notified.await,
            }
        }
    }

    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::Relaxed) {
            self.shared.ring();
        }
    }
}

#[derive(Debug)]
pub struct EpochWaitResult {
    pub status: WaitStatus,
    pub view: Option<FrozenPool>,
    pub found: usize,
    pub target: i128,
    pub mismatched: bool,
    pub seen_pool: bool,
    pub silence_ms: u64,
}
