# Store and queue

## Purpose

The two data structures inside every actor: the queue that decides what runs
next, and the store that holds what has finished.

Together they are the reason there is no central object store. Results live at
the producer; a consumer fetches from the producer directly.

---

## The ordered queue

### Problem

HTTP gives no ordering guarantee, and tinyray keeps four connections per peer.
Four concurrent calls arrive in whatever order the network chooses. But actor
semantics say otherwise, and user code relies on it:

```python
a.set_weights.remote(w)
a.step.remote()          # must not run before set_weights
```

### Solution

Every call carries a monotonic `seq` per `(caller, actor)` pair. The queue
restores order, buffering arrivals that overtake their predecessors.

```
CallerState:
  next_seq  u64                  the sequence that may run next
  ahead     BTreeMap<u64, Task>  arrivals that ran ahead
```

On arrival: if `seq == next_seq`, push to `ready` and drain everything in
`ahead` that now follows. Otherwise park it in `ahead`.

Different callers are independent. One slow caller does not block the rest —
the same choice Ray makes.

### Rejection

| Reason | Meaning |
|---|---|
| `Backpressure` | More than `max_pending_calls` queued. Retryable |
| `DuplicateSeq` | Already delivered. Acknowledged, **not executed** |

A duplicate is absorbed rather than run because a retry that reaches an actor
twice would apply a stateful call twice. Silently absorbing is correct; the
caller already has its acknowledgement.

### Stalls

If a call is genuinely lost in flight, `next_seq` is never satisfied and that
caller stalls forever with calls piling up in `ahead`. This is visible, not
silent:

```
127.0.0.1:40755 is waiting for sequence 7 from caller 3f2a1b9c with
4 call(s) buffered behind it
```

`/introspect` reports `waiting_for`, `caller` and `buffered` so the CLI can say
so. It is a real failure mode with no automatic recovery — being able to see it
in one command is the mitigation.

---

## The result store

### Lifecycle

```
reserve   →  pending      a task_id exists, the result does not
complete  →  ready        the value is there
fetch     →  ready        may be fetched repeatedly
release   →  tombstone    consumer says it is done
evict     →  tombstone    LRU or TTL took it
```

A tombstone matters. Without it, a fetch after eviction looks like a fetch for
a task that never existed, and the caller cannot tell a lost result from a typo.
With it, the answer is `ObjectLost` — an unambiguous statement that the value
existed and is gone. Capacity is 65,536 ids.

### Eviction

Two mechanisms:

**LRU at a watermark.** `store_max_bytes`, default 2 GiB. When exceeded, evict
least-recently-used until under.

**TTL.** `store_ttl_seconds`, default 300, swept every 30 s. Catches results
nobody ever fetched — a `wait` that takes 24 of 32 leaves 8 orphans every round,
and without a TTL those accumulate for the life of the job.

### The newest result is never evicted

A bug worth stating as an invariant. The original LRU walked in
least-recently-used order, and a freshly completed result **is** the
least-recently-used one — nothing has touched it yet. A large result was
therefore evicted the instant it was stored, and its `get` raised `ObjectLost`
before the fetch could arrive.

The store now refuses to evict the most recently completed entry regardless of
the watermark. Exceeding the watermark by one result is better than making a
just-finished computation unfetchable.

### Long-poll

A fetch for a `pending` result does not spin. The fetcher registers a waiter and
the connection is held open until the result lands or the timeout expires, at
which point a `CallAck` says "ask again".

This is what makes `get` cheap on a slow call: one held connection rather than a
poll loop.

### `status_only`

A fetch with `status_only` answers readiness without sending the payload.

The bug this fixed: `wait()` answered readiness by issuing a full fetch and
discarding the result. Answering "are 24 of 32 rollouts done?" moved 240 MB
through the driver to produce a list of booleans. Measured on a settled 200 MB
result: **237 ms → 0.14 ms.**

The lesson is in [05-testing.md](05-testing.md): an invariant verified at one call
site is not an invariant.

---

## Ownership and lifetime

A result lives at the actor that produced it. `ObjectRef` carries
`owner_endpoint`, so a consumer — driver or another actor — fetches straight
from the producer. Data goes rollout → learner without passing through the
driver.

There is no reference counting across the cluster. Three things bound memory
instead:

1. `release()`, explicit and best effort
2. the LRU watermark
3. the TTL

Distributed refcounting was rejected: it needs a protocol for lost decrements
and owner death, which is most of a distributed garbage collector. Three
independent bounds, one of which always fires, was judged sufficient at 32
actors.

The cost is honest: **a result can be evicted while a reference to it is still
live.** That is why `ObjectLost` is a distinct error rather than a generic
failure.

## Pitfalls

**`store_max_bytes` is per actor.** 32 actors at the default can hold 64 GiB.

**The TTL sweep is periodic.** A result can outlive its TTL by up to 30 s.

**`release()` may not arrive.** If the owner died, or the message was lost, the
watermark and TTL clean up instead.

**A stalled queue does not resolve itself.** Watch for `waiting_for` in
`tinyray status`.

## See also

- [02-protocol.md](../03-reference/02-protocol.md) — `Fetch`, `status_only`, `DuplicateSeq`
- [03-transport.md](03-transport.md) — how a call gets here
- [04-configuration.md](../03-reference/04-configuration.md) — the knobs
