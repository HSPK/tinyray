//! Per-caller ordered dispatch.
//!
//! HTTP gives no ordering guarantee: four connections between a caller and an
//! actor will deliver four concurrent calls in whatever order the network
//! feels like. Ray's actor semantics say otherwise, and user code relies on it:
//! `a.set_weights.remote(w); a.step.remote()` must not run backwards.
//!
//! So every call carries a monotonic sequence number per `(caller, actor)`
//! pair, and this queue restores the order, buffering arrivals that overtake
//! their predecessors. Different callers are independent of each other, which
//! matches Ray and keeps one slow caller from blocking the rest.

use std::collections::{BTreeMap, HashMap, VecDeque};

use tinyray_core::ids::{CallerId, TaskId};

/// A call waiting to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedTask {
    pub task_id: TaskId,
    pub caller_id: CallerId,
    pub seq: u64,
    pub method: String,
    pub body: bytes::Bytes,
    pub frames: Vec<bytes::Bytes>,
}

/// Why a call was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Too many calls already queued; the caller should back off and retry.
    Backpressure,
    /// This sequence number was already delivered. Retries of an accepted call
    /// are silently absorbed rather than executed twice.
    DuplicateSeq,
}

#[derive(Debug, Default)]
struct CallerState {
    /// Sequence number the next dispatchable call must carry.
    next_seq: u64,
    /// Arrivals that ran ahead of `next_seq`, keyed by their sequence number.
    ahead: BTreeMap<u64, QueuedTask>,
}

/// Restores submission order across concurrent HTTP arrivals.
#[derive(Debug)]
pub struct OrderedQueue {
    callers: HashMap<CallerId, CallerState>,
    ready: VecDeque<QueuedTask>,
    max_pending: usize,
    accepted: u64,
    reordered: u64,
}

impl OrderedQueue {
    pub fn new(max_pending: usize) -> OrderedQueue {
        OrderedQueue {
            callers: HashMap::new(),
            ready: VecDeque::new(),
            max_pending: max_pending.max(1),
            accepted: 0,
            reordered: 0,
        }
    }

    /// Calls accepted and not yet dispatched, including those held back
    /// waiting for a gap to be filled.
    pub fn pending(&self) -> usize {
        self.ready.len() + self.callers.values().map(|c| c.ahead.len()).sum::<usize>()
    }

    /// Calls ready to run right now.
    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }

    /// Total calls accepted over the queue's lifetime.
    pub fn accepted(&self) -> u64 {
        self.accepted
    }

    /// How many calls arrived out of order and had to be buffered. A useful
    /// signal: a large number means connection concurrency is high relative to
    /// call duration.
    pub fn reordered(&self) -> u64 {
        self.reordered
    }

    pub fn is_full(&self) -> bool {
        self.pending() >= self.max_pending
    }

    /// Accept a call, returning the tasks that became dispatchable.
    ///
    /// Accepting may release more than one task: a call that fills a gap also
    /// unblocks everything queued behind it.
    pub fn push(&mut self, task: QueuedTask) -> Result<usize, RejectReason> {
        if self.is_full() {
            return Err(RejectReason::Backpressure);
        }
        let caller = self.callers.entry(task.caller_id).or_default();
        if task.seq < caller.next_seq || caller.ahead.contains_key(&task.seq) {
            return Err(RejectReason::DuplicateSeq);
        }

        self.accepted += 1;
        if task.seq > caller.next_seq {
            self.reordered += 1;
            caller.ahead.insert(task.seq, task);
            return Ok(0);
        }

        // In order: dispatch it, then drain whatever it unblocked.
        let mut released = 0;
        self.ready.push_back(task);
        caller.next_seq += 1;
        released += 1;
        while let Some(next) = caller.ahead.remove(&caller.next_seq) {
            self.ready.push_back(next);
            caller.next_seq += 1;
            released += 1;
        }
        Ok(released)
    }

    /// Take the next task to execute.
    pub fn pop(&mut self) -> Option<QueuedTask> {
        self.ready.pop_front()
    }

    /// Forget a caller's state, e.g. once its handle is gone.
    pub fn forget_caller(&mut self, caller_id: CallerId) -> usize {
        self.callers
            .remove(&caller_id)
            .map(|state| state.ahead.len())
            .unwrap_or(0)
    }

    /// Tasks buffered because an earlier sequence number has not arrived.
    ///
    /// Persistently non-zero means a call was lost in flight, which would
    /// otherwise stall that caller forever.
    pub fn stuck_callers(&self) -> Vec<(CallerId, u64, usize)> {
        self.callers
            .iter()
            .filter(|(_, state)| !state.ahead.is_empty())
            .map(|(id, state)| (*id, state.next_seq, state.ahead.len()))
            .collect()
    }

    /// Drop everything, returning the abandoned tasks so their futures can be
    /// failed rather than left hanging.
    pub fn drain_all(&mut self) -> Vec<QueuedTask> {
        let mut out: Vec<QueuedTask> = self.ready.drain(..).collect();
        for state in self.callers.values_mut() {
            out.extend(std::mem::take(&mut state.ahead).into_values());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn task(caller: CallerId, seq: u64) -> QueuedTask {
        QueuedTask {
            task_id: TaskId::from_parts(seq, seq),
            caller_id: caller,
            seq,
            method: format!("m{seq}"),
            body: Bytes::from(format!("body{seq}")),
            frames: vec![],
        }
    }

    fn drain(queue: &mut OrderedQueue) -> Vec<u64> {
        std::iter::from_fn(|| queue.pop()).map(|t| t.seq).collect()
    }

    #[test]
    fn in_order_arrivals_pass_straight_through() {
        let caller = CallerId::generate();
        let mut queue = OrderedQueue::new(64);
        for seq in 0..5 {
            assert_eq!(queue.push(task(caller, seq)), Ok(1));
        }
        assert_eq!(drain(&mut queue), vec![0, 1, 2, 3, 4]);
        assert_eq!(queue.reordered(), 0);
    }

    #[test]
    fn out_of_order_arrivals_are_buffered_then_released() {
        let caller = CallerId::generate();
        let mut queue = OrderedQueue::new(64);

        // 2 and 1 overtake 0, as concurrent connections readily do.
        assert_eq!(queue.push(task(caller, 2)), Ok(0));
        assert_eq!(queue.push(task(caller, 1)), Ok(0));
        assert_eq!(queue.pop(), None, "nothing may run before seq 0");

        // Seq 0 arrives and unblocks all three at once.
        assert_eq!(queue.push(task(caller, 0)), Ok(3));
        assert_eq!(drain(&mut queue), vec![0, 1, 2]);
        assert_eq!(queue.reordered(), 2);
    }

    #[test]
    fn reverse_order_arrival_still_executes_forwards() {
        let caller = CallerId::generate();
        let mut queue = OrderedQueue::new(64);
        for seq in (0..8).rev() {
            queue.push(task(caller, seq)).unwrap();
        }
        assert_eq!(drain(&mut queue), (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn callers_are_independent() {
        let a = CallerId::from_parts(1, 1);
        let b = CallerId::from_parts(2, 2);
        let mut queue = OrderedQueue::new(64);

        // `a` is stalled waiting for its seq 0.
        queue.push(task(a, 1)).unwrap();
        // `b` must not be held up by that.
        assert_eq!(queue.push(task(b, 0)), Ok(1));
        let dispatched = queue.pop().unwrap();
        assert_eq!(dispatched.caller_id, b);
    }

    #[test]
    fn duplicate_sequence_numbers_are_refused() {
        let caller = CallerId::generate();
        let mut queue = OrderedQueue::new(64);
        queue.push(task(caller, 0)).unwrap();
        // A retry of an already-accepted call must not run the method twice;
        // actor calls are stateful and replaying them corrupts state.
        assert_eq!(queue.push(task(caller, 0)), Err(RejectReason::DuplicateSeq));
        assert_eq!(queue.push(task(caller, 5)), Ok(0));
        assert_eq!(queue.push(task(caller, 5)), Err(RejectReason::DuplicateSeq));
    }

    #[test]
    fn backpressure_kicks_in_at_the_limit() {
        let caller = CallerId::generate();
        let mut queue = OrderedQueue::new(3);
        for seq in 0..3 {
            queue.push(task(caller, seq)).unwrap();
        }
        assert!(queue.is_full());
        assert_eq!(queue.push(task(caller, 3)), Err(RejectReason::Backpressure));

        // Draining one makes room again.
        queue.pop();
        assert_eq!(queue.push(task(caller, 3)), Ok(1));
    }

    #[test]
    fn buffered_out_of_order_calls_count_towards_backpressure() {
        // Otherwise a caller with a lost seq 0 could buffer without bound.
        let caller = CallerId::generate();
        let mut queue = OrderedQueue::new(2);
        queue.push(task(caller, 1)).unwrap();
        queue.push(task(caller, 2)).unwrap();
        assert_eq!(queue.pending(), 2);
        assert_eq!(queue.push(task(caller, 3)), Err(RejectReason::Backpressure));
    }

    #[test]
    fn stuck_callers_are_visible() {
        let caller = CallerId::generate();
        let mut queue = OrderedQueue::new(64);
        queue.push(task(caller, 3)).unwrap();
        queue.push(task(caller, 4)).unwrap();
        let stuck = queue.stuck_callers();
        assert_eq!(stuck.len(), 1);
        let (id, next_seq, buffered) = stuck[0];
        assert_eq!((id, next_seq, buffered), (caller, 0, 2));
    }

    #[test]
    fn forgetting_a_caller_releases_its_buffer() {
        let caller = CallerId::generate();
        let mut queue = OrderedQueue::new(64);
        queue.push(task(caller, 7)).unwrap();
        assert_eq!(queue.pending(), 1);
        assert_eq!(queue.forget_caller(caller), 1);
        assert_eq!(queue.pending(), 0);
        // A fresh handle starts over at seq 0.
        assert_eq!(queue.push(task(caller, 0)), Ok(1));
    }

    #[test]
    fn drain_all_returns_everything_for_failing() {
        let a = CallerId::from_parts(1, 1);
        let b = CallerId::from_parts(2, 2);
        let mut queue = OrderedQueue::new(64);
        queue.push(task(a, 0)).unwrap();
        queue.push(task(a, 1)).unwrap();
        queue.push(task(b, 4)).unwrap(); // buffered
        let drained = queue.drain_all();
        assert_eq!(
            drained.len(),
            3,
            "queued and buffered tasks must all surface"
        );
        assert_eq!(queue.pending(), 0);
    }

    #[test]
    fn interleaved_shuffle_always_dispatches_in_order() {
        // A deterministic stand-in for network reordering: walk several
        // permutations and assert the queue repairs each one.
        let caller = CallerId::generate();
        for stride in [1usize, 3, 5, 7] {
            let mut queue = OrderedQueue::new(1024);
            let n = 16u64;
            let mut order: Vec<u64> = Vec::new();
            let mut index = 0usize;
            let mut seen = vec![false; n as usize];
            for _ in 0..n {
                while seen[index % n as usize] {
                    index += 1;
                }
                order.push((index % n as usize) as u64);
                seen[index % n as usize] = true;
                index += stride;
            }
            for seq in &order {
                queue.push(task(caller, *seq)).unwrap();
            }
            assert_eq!(
                drain(&mut queue),
                (0..n).collect::<Vec<_>>(),
                "stride {stride} was not repaired"
            );
        }
    }
}
