# API reference

Written against the implementation, not against a plan. Every signature is the
one in `python/tinyray/`.

*[中文](../api.md)*

---

## Module

| Name | What it is |
|---|---|
| `tinyray.join(...)` | Report in; returns a `Member` |
| `tinyray.pool(name)` | Get a `Pool` |
| `tinyray.apool(name)` | Get an `AsyncPool` (its methods return awaitables) |
| `tinyray.Call(method, args=(), kwargs=None)` | Describe one item in an RPC batch |
| `tinyray.batch(handle, calls, timeout=30.0)` | Execute an ordered batch on one member |
| `tinyray.abatch(handle, calls, timeout=30.0)` | Await the same batch operation |
| `tinyray.__version__` | The installed version |
| `tinyray.MAX_STATE` | The hard cap on state, 16 KB |
| `tinyray.FIRST_BEAT_S` | The default for `join(timeout=)`, 30 seconds |

---

## `join()`

```python
tinyray.join(
    pool: str,
    policy: str = "churn",
    *,
    slot: int | None = None,
    size: int | None = None,
    url: str | None = None,
    serves: Any = None,
    exclusive: bool = False,
    max_concurrency: int | None = None,
    timeout: float = FIRST_BEAT_S,
    registry_url: str | None = None,
    coalesce_ms: int = 50,
) -> Member
```

Blocks until the first beat lands. Raises `Unreachable` if the registry cannot
be reached, `SeatTaken` if a later tenure holds the seat, and `PolicyError` if
the pool's shape disagrees. **One process joins one pool.**

If joining fails, its heartbeat and method server are closed before the error
escapes, so the process can retry without leaving a service behind.

### policy

| policy | Has a seat | Used for |
|---|---|---|
| `churn` | No | Interchangeable workers coming and going |
| `serving` | No | Interchangeable, but serving methods |
| `stateful` | **Yes** | Shard holders; seats are not interchangeable |
| `collective` | **Yes** | A group that has to be counted together (`size=` required) |

Seated policies need `slot=`, or read it from `TINYRAY_SLOT` / `RANK` /
`SLURM_PROCID` / `OMPI_COMM_WORLD_RANK`; `size=` likewise comes from
`TINYRAY_SIZE` / `WORLD_SIZE` and friends.

### The other parameters

- **`serves=`** -- hand over an object; its public methods (not starting with
  `_`) become the interface, and their type hints are the schema. The address
  is registered for you.
- **`exclusive=True`** -- refuse if the seat is taken, raising `SeatTaken`.
  This is what an election wants; the default is the opposite, because a
  restarting rank has to reclaim its seat while the dead one's lease still
  runs.
- **`max_concurrency=`** -- a ceiling on calls running at once. Past it the
  caller gets `NotDelivered` rather than a queue. Unlimited by default.
- **`coalesce_ms=`** -- coalescing delay for rapidly changing membership
  traffic, in milliseconds. The default remains 50; smaller values reduce
  burst notification latency at the cost of more requests. Zero opts out of
  this spacing. Values must be nonnegative integers; the effective spacing
  is capped by the registry's lease/4 so a large setting cannot prevent renewal.
  Local publications may wake a resting client early.
- **`url=`** -- set the advertised native method endpoint (`host:port`) by
  hand. By default the host is probed from the routing table and the native
  listener's chosen port is added; on a multi-homed machine use
  `TINYRAY_ADVERTISE`.
- **`registry_url=`** -- which registry to report in to, overriding
  `TINYRAY_REGISTRY`. **Not to be confused with `url=` above**: that one is
  where others reach you, this one is who you go to. The environment stays the
  normal channel (a launcher sets it once for every rank); this parameter is
  for the caller that cannot use it -- a library inside somebody else's
  process, where assigning to `os.environ` to configure one call is a
  process-wide side effect that outlives the call. It picks a registry, it
  does not add one: one process is one member with one registry, and `pool()`
  and `apool()` follow whatever `join()` used. A list of addresses is
  **refused on the spot**.

    !!! warning "TINYRAY_ADVERTISE is a hostname only"
        The listener's port is added to this value. Schemes, paths, and ports
        are rejected. To advertise a different native TCP endpoint, use
        `join(url="host:port")`. Old `http://...` method addresses are rejected
        explicitly; there is no compatibility fallback.

---

## `Member`

This process's own registration.

### Attributes

| Attribute | What it is |
|---|---|
| `identity` | `"pool/seat#tenure"`, the same string peers hold on a Handle |
| `pool` / `slot` / `incarnation` | The same thing, taken apart |
| `state` | The state currently published (a copy) |
| `is_ready` | The readiness currently asserted |
| `accepted` | `False` means a later tenure took the seat |
| `silence_ms` | Milliseconds since the last successful beat. While it climbs everything still works; only failure detection gets slower |
| `last_error` | Why the last beat failed, kept after recovery |
| `stats()` | Counters; see below |

### `stats()`

| Key | Meaning |
|---|---|
| `beats_ok` / `beats_failed` | Beats answered, and beats not |
| `interval_ms` / `silence_ms` | The current interval; time since the last success |
| `coalesce_ms` / `effective_coalesce_ms` | Requested coalescing delay; the delay capped by the current lease/4 |
| `watch_wakeups` | Times the local cache moved and woke a waiter |
| `short_polls` | Times a beat waited on a **timer** rather than on the registry. Only the path before the first ack should do this; a number that keeps climbing means this client is polling and not getting what long polling buys |
| `state_bytes` | How large this member's published state is |
| `pool_revision` | This member's own pool version, as last heard |
| `watched_pools` | How many pools are subscribed |

**Only a member that passed `serves=`** has these as well:

| Key | Meaning |
|---|---|
| `calls` / `failed` | Calls handled, and how many raised |
| `refused` | Calls turned away at the concurrency limit (503) |
| `in_flight` / `peak_in_flight` | How many are running now, and the high-water mark |
| `busy_ms` | Total time spent inside handlers |
| `concurrency_limit` | The value of `max_concurrency` |

This half exists so that "should long calls get their own channel?" has an
answer rather than an opinion. `max_concurrency` bounds pile-up, **not
isolation**: once the slots are full, a control call gets the same 503 as any
other. Reading `refused` next to `peak_in_flight` tells you whether that is
happening.

### Publishing state

```python
# Assert readiness at the same time -- for the code that decides whether this
# member is usable
me.ready(**state) -> Member          # merge into the existing state, mark ready
me.set_ready(state: dict) -> Member  # replace it whole, mark ready
me.unready() -> Member               # keep the state, mark unusable

# Publish state without touching readiness -- for all the other code
me.update(**state) -> Member         # merge
me.replace(state: dict) -> Member    # replace it whole

me.flush(timeout=10.0) -> Member     # block until the registry really has it
```

`ready()` and `update()` both **merge**, so a key you have published cannot be
taken back -- use `set_ready()` or `replace()` to clear one.

!!! warning "Report progress with `update()`, not `ready()`"
    `ready()` asserts two things at once: this is my state, and I am usable.
    For the code that owns readiness that is exactly right; for anything else
    it is overreach.

    Code that only reports progress but calls `ready(step=n)` silently lifts a
    pause somebody else just applied -- after `unready()`, one `ready(step=1)`
    flips readiness back to `True` for every peer, which the caller never
    meant to say.

    With the two separated, "one readiness owner per Member" stops being a
    convention people have to remember: the rest of the code calls `update()`
    and structurally cannot touch the readiness bit.

Republishing the same thing costs nothing: when **both** the state and the
readiness bit are unchanged, nothing nudges the heartbeat and the pool version
does not move. The comparison is on parsed values, not bytes -- `{"b": 2,
"a": 1}` and `{"a": 1, "b": 2}` are the same thing, and comparing bytes would
spend a round trip calling them two changes. Readiness counts too, so a
`ready()` with the same state after `unready()` always goes out.

!!! note "Ordering under concurrent publishes comes from a lock, not the GIL"
    Every publishing path (`ready` / `set_ready` / `unready` / `update` /
    `replace`) does read-merge-write under the same `Member` lock, and the
    write itself under a lock on the Rust side. So two threads publishing are
    serialised, and whichever takes the lock first takes effect first.

    The GIL cannot promise this: it only stops single bytecodes running in
    parallel, and says nothing about the order sends complete in.

    What goes out is the **current value**, not a log. Protocol 2 attaches a
    publication sequence to state, readiness and URL together. The registry
    ignores older payloads, even when a canceled request arrives late, while
    still renewing the lease. Two publishes closer together than a beat may
    mean the middle value never goes out at all -- that is soft state, not a
    defect. If every step has to leave a trace, that belongs on the data plane.

`flush()` waits for acknowledgment of the latest local publication, not just
for another heartbeat. It raises `TimeoutError` if the registry cannot be
reached, and `SeatTaken` if the seat was taken. Protection against later
rollback requires `me.registry.supports("publication_ordering")`; older
registries cannot enforce the sequence and produce an `OldRegistryWarning`.

!!! note "State has a hard cap"
    16 KB, and over it is an error. It is not the same as the RPC size limit:
    state is copied to **every subscriber**, measured at 6 MB reaching 20
    subscribers as 120 MB. This limit protects other people.

State must be valid JSON: `NaN` and infinities raise `ValueError`. A rejected
publication leaves the previous state and readiness unchanged.

### Learning that you were superseded

```python
me.wait_fenced(timeout: float | None = None) -> bool
await me.await_fenced(timeout=None) -> bool
```

Blocks until a later tenure takes the seat and returns `True`; returns `False`
on timeout. Event-driven, never polling.

**It needs contact with the registry.** During a partition it waits forever --
out of contact there is no way to know you were replaced. So it protects
against being replaced, not against a split brain.

### Leaving

```python
me.leave() -> None
with tinyray.join(...) as me: ...   # the same thing
```

A normal exit calls it for you. The seat is freed at once, without waiting for
the lease.

---

## `Pool` / `AsyncPool`

```python
pool = tinyray.pool("engine")
apool = tinyray.apool("engine")  # the same queries; the methods are awaitable
```

**Every lookup reads the local cache and never touches the network** -- no
timeouts, and no load on the registry. The cache trails the truth by about one
round trip (21 ms measured with the defaults), because the registry sends the
answer when the change happens rather than waiting for your next beat to ask.
The first lookup of a pool waits for the first answer to arrive (constructing
a `Pool` *is* subscribing, so building the pools you will need at startup
removes that wait).

### Queries

```python
pool.all(**filt) -> list[Handle]              # only members that are ready
pool.pick(**filt) -> Handle                   # one ready member at random, else NotFound
pool.slot(k, require_ready=False) -> Handle   # by seat; an empty seat raises NotFound
pool.wait(count=1, timeout=30.0, **filt) -> list[Handle]
len(pool)                                     # how many are ready
```

`**filt` matches keys in the state by **equality**. Numbers compare by value
(`shard=6/2` finds a member publishing `shard=3`), booleans strictly (`free=1`
does not match `free=True`).

The number rule goes all the way down: `cfg={"shard": 6/2}` also finds a member
publishing `cfg={"shard": 3}`, and the same holds inside arrays. Shape stays
exact -- nested objects need the same keys, arrays the same order and length --
and only the numbers themselves are relaxed.
Large integers are never rounded to floats to decide equality.

Repeated filters made only of top-level scalar fields (`null`, booleans,
strings, integers, and finite floats) use a bounded native result index.
Nested objects, arrays, oversized keys, and filters outside the limits fall
back to the same scan and therefore keep identical semantics. The per-pool
cache holds at most 32 filters, 8 fields and 4 KiB of key data per filter,
8,192 matching IDs per entry, and 1 MiB in total. Any member/state/readiness
change, removal, full resync, or registry restart invalidates affected cached
results. The index stores wire IDs only, not Handles or state copies.

`slot()` uses a native slot index and `pick()` selects one eligible member
before serialization; neither constructs a full Python roster just to return
one handle. `all()`, `snapshot()`, `epoch()`, and the built-in waits now take
immutable Rust roster views directly instead of encoding a whole roster as
MessagePack for Python to decode. Each pool caches at most an all-members and
a ready-members view, containing only `Arc<Member>` references and indexes,
not copies of potentially large state. On the first state read across a
roster, Rust encodes one bounded MessagePack state array and Python decodes it
once, then gives each Handle its indexed, independent object. Encodings below
1 MiB are cached with the immutable roster view and naturally disappear when
member data changes. Mutating one result still cannot affect a later lookup
or another snapshot.

### Snapshots and changes

```python
pool.snapshot(include_unready=True) -> Snapshot
pool.changes(since=None, timeout=None) -> Watch         # iterates Snapshots
apool.achanges(since=None, timeout=None) -> AsyncWatch  # async iteration
```

`changes()` blocks on an event and **never polls**. While the pool is still it
does not return.
Its timeout bounds the stream's lifetime, including while changes keep arriving.

A stream ends in three ways, and **one of them raises**:

| Why it ended | What you see |
|---|---|
| `timeout` ran out | The loop exits normally |
| Somebody called `close()` | The loop exits normally |
| **This process was superseded** | Raises `Fenced` |

The first two mean "nothing to see"; the third means "something happened": the
seat is gone and the local cache is **frozen** from here on, so every later
lookup is stale without saying so. If all three ended quietly, losing the seat
would look exactly like "the timeout ran out, all is well" -- leaving you to
check `Member.accepted` afterwards, which is precisely what a caller should
not have to guess.

### Watching only a few fields

```python
pool.changes(fields=["role", "ready"])
apool.achanges(fields=["role", "ready"])
```

With `fields` given, a snapshot is produced only when one of those fields
really moved (or a member came or went, or a seat changed hands). `ready` and
`url` are part of the member itself and can be named; anything else is looked
up in the state.

The comparison happens **in the Rust cache**, before anything is serialised.
Creating a `Snapshot` now only creates a shared native view; Python Handles
are built when `members` is first accessed or iteration starts. Consequently
`len(snapshot)`, repr, `slot()`, `get()`, and field digests do not pay for
whole-roster materialization.

**Identity always counts.** A seat changing hands produces a snapshot even when
the new tenure publishes exactly the fields the old one did -- otherwise you
would go on talking to a dead incarnation.

### `Watch` / `AsyncWatch`

What `changes()` and `achanges()` return: iterable, closeable, and a context
manager:

```python
with pool.changes() as w:  # closed on leaving the with
    for snap in w:
        ...

w = pool.changes()
w.close()  # can also be closed from another thread or task
```

**`close()` is the only way to stop a blocked watcher.** It waits on an event
rather than sitting at a `yield`, so a generator cannot be closed and a flag
cannot be seen. `close()` rings the bell on the way out, which drags it back
to somewhere the flag is visible.

`leave()` closes every watcher still alive -- otherwise a watcher in a
non-daemon thread keeps the process from ever exiting.

!!! note "The async side does not hold a thread"
    `achanges()` waits on a pipe written only for cache/lifecycle changes,
    selected by the event loop, and **borrows no executor thread**. Empty
    heartbeat acknowledgements do not write the pipe, so a quiet pool is not
    repeatedly woken at heartbeat cadence. Cancellation is immediate, and
    cancelling any number of watchers affects nothing else.

    An earlier implementation used `asyncio.to_thread`: cancelling the
    awaitable does not stop the thread underneath. On a 24-core machine, after
    cancelling 40 watchers the next `asyncio.to_thread` waited 3,092 ms --
    all 28 workers of the default executor were stuck inside them.

!!! info "Why a stream of snapshots and not a stream of events"
    The client samples at its heartbeat rate, and the registry folds several
    changes within one interval into "that member's current state". So
    promising "no lost events" is something the protocol cannot deliver;
    "no lost state" it can deliver honestly. An event is one diff away --
    every record carries its `incarnation` and `ready`.

### Waiting for a condition

```python
pool.until(predicate, since=None, timeout=None, describe="") -> Snapshot
await apool.auntil(predicate, since=None, timeout=None, describe="")
```

Blocks until `predicate(snapshot)` is true and returns that snapshot; raises
`TimeoutError` on expiry, with `describe` as the phrase in the error saying
what was being waited for.

**Every hand-written wait loop has to get the same four things right**, so it
is written once: check whether it already holds, hand the revision over with
no gap, stop when `close()`d, and let `Fenced` out instead of treating it as
"not satisfied yet". The second is the hardest to notice: if the pool moves
between the first look and the subscription, the wait burns the whole timeout
on a condition that was already true.

Arbitrary Python predicates still use this generic path; they cannot move into
Rust. The built-in conditions below no longer run as Python predicate loops:
`wait()`, `wait_departure()`, `wait_replacement()`, and `epoch()` hand count,
identity, fingerprint, and timeout checks to Rust on the same
revision/condition-variable handoff. Their async forms reuse the event-loop
pipe, with no executor worker and no sleep/poll loop.

Heartbeat outcomes, publication acknowledgements, and discovery revisions use
separate notification domains. A successful renewal with no roster change
wakes registration/publication waiters only, not discovery. First registration
also waits directly to one absolute deadline instead of waking every 100 ms.

### Waiting for members to be ready (async)

```python
await apool.await_ready(count=1, timeout=30.0, **filt) -> list[Handle]
```

The event-loop form of `Pool.wait()`.

!!! warning "Do not call the inherited `wait()` on an event loop"
    `AsyncPool` inherits the synchronous `wait()`, and on a loop that is not
    merely inelegant, it **stops the whole loop**: a one-second
    `apool.wait()` let through 5 ticks of a 10 ms ticker where there should
    have been a hundred.

    Wrapping it in `asyncio.to_thread` is not right either -- cancelling that
    does not stop the thread underneath, which holds a worker of the default
    executor for the whole wait.

### Waiting for a tenure to leave

```python
pool.wait_departure(identity, timeout=None) -> bool
await apool.await_departure(identity, timeout=None) -> bool
```

Blocks until that **tenure** is no longer in the pool and returns `True`;
returns `False` on timeout. Leaving, lease expiry and the seat changing hands
all count.

It is a different question from `wait_replacement()`: that one only answers
when somebody takes over, so if the previous holder merely left it burns the
timeout and returns `None`. Whoever is picking up the work usually only needs
to know the previous holder is gone.

### Waiting for a seat to change hands

```python
pool.wait_replacement(slot=None, identity=None, timeout=None) -> Handle | None
await apool.await_replacement(slot=None, identity=None, timeout=None)
```

Blocks until the seat is taken over by **another tenure** and returns the
successor's `Handle`; returns `None` on timeout. Pass exactly one of `slot=`
or `identity=`.

`Member.wait_fenced()` is the same question from the inside, for the process
that has to let go; this is the outside view, for whoever was talking to it.
A seat standing empty, a seat changing hands, and a member merely no longer
being ready are three different things, and only the incarnation tells them
apart.

### Roll call

```python
pool.epoch(min=None, timeout=60.0) -> Epoch
```

Waits for everyone (the pool's `size` by default, or `min=`) and then freezes.
**A round is handed out only when the fingerprint is the one computed from
that very list** -- so matching fingerprints across ranks mean matching lists.
It raises `Stale` if the registry cannot be reached: better no round than a
round nobody can trust.

Once this process is superseded, existing epochs report `valid=False` and
opening another raises `Fenced`, even if the cached roster never changed.
Losing contact alone still does not invalidate an established epoch.

---

## `Snapshot`

One pool as it stood at a revision, **unready members included**.

| Member | What it is |
|---|---|
| `revision` | Monotonic. Pass it to `changes(since=)` to carry on |
| `members` | Everyone holding a seat |
| `ready()` | The ones that are ready |
| `slot(k)` | Who holds seat k, or `None` |
| `get(identity)` | That exact tenure, or `None` |
| `len()` / iteration | By member |

`members` remains an immutable tuple, but it is lazy: constructing a snapshot
creates no Handles. The first `members` access or iteration materializes and
caches them once; `ready()` materializes only ready members. The view owns the
old `Arc<Member>` references, so later publications, departures, and registry
restarts cannot rewrite it.

`get()` lives on the snapshot and not on the pool on purpose: "is that
incarnation still there?" asks about **one moment**, and asking a live pool
twice may reach two.

---

## `Handle`

A reference to one member. Attribute access proxies to its methods.

| Attribute | What it is |
|---|---|
| `identity` | `"pool/seat#tenure"`, and also the fencing token |
| `label` | The short form, for humans |
| `pool` / `id` / `slot` / `incarnation` / `url` / `state` / `ready` | The record itself |

Handles returned by Pool/Snapshot/Epoch retain a native `Arc<Member>`
reference. Constructing thousands of Handles does not copy state; pool,
identity, slot, incarnation, URL, readiness, identity/label, and method
proxying read the native reference directly. The first `state` access creates
one isolated Python dictionary and installs it in a real slot. Handles from
one roster share a single batch decode; later reads avoid both a property and
a native call. Mutating the value cannot affect another Handle, the cache, or
a frozen snapshot. The manual
`Handle(pool, raw, methods)` constructor and writable-field compatibility
remain unchanged.

```python
h.assign("task")  # call it
h.assign.timeout(5.0)("task")  # this call's timeout; 30 seconds by default
h.pull_job.returns(AgentJob)()  # restore the MessagePack result as an AgentJob
```

`url` is a bare `host:port`. Method connections are persistent and
multiplexed: one connection has one serialized frame writer, one reply reader,
and up to 128 correlated requests in flight. Replies may arrive in any order.
An endpoint admits at most 256 local calls across at most four connections;
the process admits 512 calls and 256 client connections in total, with at most
64 idle connections retained. Selection is least-loaded, and another endpoint
connection is opened when every existing connection already has two reserved
calls, until the four-connection cap. This keeps shallow writer/reader queues
at ordinary concurrency without reverting to one socket per call. Every frame
is a big-endian `u32` length followed by one MessagePack map:

Python and embedded Rust calls use this same public `tinyray::Client`
implementation. The PyO3 layer only adapts Python values, completion delivery,
and cancellation; it does not maintain a second connection pool, socket
reader/writer, or reply-routing state machine.

```text
call  = {v: 1, id, from, to, op: "call", method, body: bytes}
batch = {v: 1, id, from, to, op: "batch", batch: count, body: bytes}
reply = {v: 1, id, status, body: bytes, error?, batch_index?, completed?}
```

`body` is opaque to Rust and contains one separately encoded application
value. Reply statuses are `success`, `method_not_found`, `fenced`,
`caller_fault`, `concurrency_refused`, `remote_error`,
`malformed_protocol`, and `internal`. Malformed, truncated, oversized,
duplicate, or unknown reply IDs poison that connection and fail every
remaining request according to whether its frame completed. A timed-out or
cancelled request whose complete frame was written removes only its waiter;
its known late reply is discarded without poisoning unrelated calls. There
is no HTTP/JSON listener or compatibility fallback. Method frames have a hard
32 MiB cap, checked from the length prefix before body allocation. Connection
count, in-flight frame count, and bulk-frame bytes have both process-global
and per-server admission budgets; control frames up to 1 MiB use a separate
byte reserve so a few maximum frames cannot crowd them out.
`max_concurrency` still gates each request before Python invocation; a batch
is one admission unit and remains sequential internally.

Client pools reuse a connection for at most 10 idle seconds. The server closes
a connection after 15 idle seconds when no calls are active, and gives a
started frame one absolute 15-second prefix/body deadline. The five-second
margin lets the client discard stale pool entries first while ensuring a
silent peer never owns a server task forever.

Standard dataclasses can be passed and returned directly:

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Request:
    prompt: str
    max_tokens: int

@dataclass(frozen=True)
class Reply:
    text: str
    tokens: int

class Worker:
    def infer(self, request: Request) -> Reply:
        return Reply("done", request.max_tokens)

reply = h.infer.returns(Reply)(Request(prompt="hello", max_tokens=32))
```

The caller turns a dataclass into a MessagePack map and the callee restores it
directly from the method parameter annotation. A method may return a dataclass
directly. Without `.returns(T)` the caller receives ordinary dictionaries and
lists; with it, the result is restored as the locally declared type.
Dataclasses may nest and may appear inside `list[T]`, `dict[K, V]`,
Optional/Union, tuples, and sets. `NamedTuple`, `TypedDict`, Enum, datetime,
date/time/timedelta, UUID, Decimal, and those containers restore recursively.

0.18 **intentionally removes Pydantic integration**. There is no
`tinyray[pydantic]` extra, no special BaseModel/Pydantic-dataclass encoding,
and no alias, validator, serializer, or strict-shape marker machinery.
Passing a Pydantic object raises `TypeError` before sending;
`.returns(PydanticType)` is also rejected before the call. Convert explicitly
to a standard dataclass, TypedDict, or ordinary MessagePack value instead.

Every successful native reply already carries the result as its opaque
application payload; there is no `/_result/` path, HTTP status, or fallback
request. Model arguments are constructed directly from `msgspec.Raw`
MessagePack slices where the target type permits, avoiding a generic
intermediate object graph.

MessagePack arrays do not retain whether an ordinary input was a list, tuple,
or set. `.returns(T)` declares the type to restore on the calling side:

Untyped values follow `msgspec.msgpack`: bytes and datetime stay native; UUID
and Enum become their wire values; tuple and set become arrays; integer and
tuple map keys are retained; NaN and infinities remain floats. Python integers
outside MessagePack's signed/unsigned 64-bit range use reserved extension code
121. Codes 122, 123, 125, and 126 preserve large-integer mapping/set shapes
and composite tuple/frozenset keys. These codes carry no Python class name;
applications should not use raw `msgspec.msgpack.Ext` values with those codes.

```python
class AgentJob(NamedTuple):
    attempt: AttemptKey
    proxy_url: str

job = h.pull_job.returns(AgentJob)()
jobs = await ah.pull_jobs.returns(list[AgentJob])()
```

A conversion failure raises a local `TypeError` naming the remote member,
method, and failing path. The remote method has already completed successfully
at that point; only result restoration failed. The protocol never sends Python
class names. Standard dataclasses and the typed containers above are the
explicit model boundary; other arbitrary Python objects are rejected.

`.returns()` and `.timeout()` are per-call modifiers and compose in either
order:

```python
h.pull_job.timeout(5).returns(AgentJob)()
h.pull_job.returns(AgentJob).timeout(5)()
```

Modifiers are not keyword arguments so that they cannot collide with a
parameter of the same name on the far side.

`AsyncHandle` is its async twin: produced by `apool()`, its methods return
awaitables, and it is otherwise identical.

`hasattr(h, "assign")` is **accurate** -- a handle only proxies method names
the pool really offers.

---

## `Epoch`

A frozen round.

| Member | What it is |
|---|---|
| `members` | The list, as of the freeze |
| `roster` | The fingerprint. Equal across ranks means equal lists |
| `valid` | `False` as soon as an occupant changes |
| `slot(k)` | Seat k in this round |

Like `Snapshot`, `members` is a lazily materialized, cached immutable tuple;
`len()`, `slot()`, `valid`, and repr read the frozen native view directly.

Checking `valid` inside the training loop is useless: a stuck rank never
reaches that line. Use a background thread -- NCCL releases the GIL while it
blocks.

---

## `RegistryInfo`

The registry is **another process** and can be upgraded separately from the
Python package. `tinyray.__version__` describes this side; what the other side
can do has to be asked:

```python
me.registry  # -> RegistryInfo
me.registry.protocol  # an integer that only goes up; too old to say reads as 0
me.registry.version  # the far side's version, to put in a log line
me.registry.supports("long_poll") -> bool
me.registry.supports("publication_ordering") -> bool
me.registry.supports("native_registry") -> bool
```

`RegistryInfo.FEATURES` maps a feature name to the protocol it needs, and it
lives on the side that depends on it, so an old client does not need to know
about future features. A misspelled feature name raises `ValueError` rather
than returning `False` -- the latter would let a typo walk quietly into the
degraded branch.

Deployment probes can look without joining by connecting to `host:port` and
sending a `health` frame. Each frame is a big-endian `u32` length followed by
one MessagePack map:

```text
request  = {request_id: 7, operation: "health", payload: nil}
response = {request_id: 7, operation: "health_ack",
            payload: {status: "ok", version: "0.18.0", protocol: 3}}
```

This is not an HTTP endpoint and there is no curl compatibility listener.

| protocol | Meaning |
|---|---|
| 0 | Before long polling (earlier than 0.7.0) |
| 1 | Understands `hold_ms`: park the answer while there is nothing to say, and return the moment a watched pool moves |
| 2 | Understands `publication`: older payloads cannot undo newer state, readiness or URL |
| 3 | Uses the native length-prefixed MessagePack registry transport |

0.18 is a hard cutover for both transports: neither the registry nor method
RPC listens for HTTP/JSON, and clients do not fall back to the old wire.

!!! warning "Missing features affect performance or consistency"
    An old registry answers a long-poll request **quickly and correctly**, it
    just does not park it -- so "parked and nothing happened" and "does not
    park at all" look identical from the client, and no property can be probed
    to tell them apart.

    Measured against a 0.6.1 registry: **14.5** requests a second against
    **0.12** for the current one, and discovery latency falling back from one
    round trip to one heartbeat interval. Everything goes on working and
    nothing raises.

So `join()` emits an `OldRegistryWarning` in that case. Silence it as usual:

```python
warnings.filterwarnings("ignore", category=tinyray.OldRegistryWarning)
```

---

## `CallContext`

The caller's identity, on the serving side. Annotate a parameter with the type
and the library fills it in:

```python
def pull_job(self, ctx: tinyray.CallContext) -> dict:
    ctx.identity  # "worker/3#1874..."
    ctx.pool  # "worker"
    ctx.slot  # 3, or None without a seat
    ctx.incarnation  # the tenure number
    ctx.request_id  # what the caller named this attempt
```

Context can be a regular, positional-only or keyword-only parameter. The
other parameters retain Python's binding rules, including `*args`, `**kwargs`,
defaults and rejection of duplicate arguments.

**Self-declared identity, not authentication.** In this system a member picks
its own tenure number anyway. What it buys is that the caller cannot forget to
send it or send the wrong one, and nothing more.

`request_id` differs per call by default, so logs on both sides can point at
the same attempt. If the caller identity is long, TinyRay keeps a readable
prefix, adds the identity SHA-256 and exact sequence, and remains within 200
bytes. Explicit `request_id()` values retain their existing length validation.

To make **a retry share one name** (which idempotence needs), wrap the whole
retry loop:

```python
with tinyray.request_id(f"commit-{batch}"):
    for _ in range(3):
        try:
            return h.commit(rows)
        except tinyray.NotDelivered:
            continue
```

A block rather than a per-call argument, because a retry is already shaped
like a block, and because a keyword would collide with the callee's own
parameter names. It is a ContextVar, so it follows `await` into tasks started
inside the block and does not leak into the one next door.

!!! note "tinyray does not do idempotence caching"
    It gives you a name; it does not deduplicate by it. The callee cannot know
    whether replaying a call is safe -- how long to keep a result, and what
    counts as "the same call", are application questions. That decision
    belongs to the caller, and the split between `NotDelivered` (definitely
    not delivered, safe to retry) and `OutcomeUnknown` (it may have run) is
    what makes it possible. If you want idempotence, use this id as the key
    and implement it.

---

## RPC batches

```python
calls = [
    tinyray.Call("assign", args=("task-1",)),
    tinyray.Call("assign", kwargs={"task": "task-2"}),
]
results = tinyray.batch(handle, calls)
results = await tinyray.abatch(handle, calls)
```

A batch contains at most 128 calls to one member. Calls run sequentially and
stop at the first failure. An empty batch is a local no-op. This amortizes one
framed TCP exchange across several operations; it is **not a transaction**.

`BatchError` contains `failed_index` (zero-based), `completed_results`, and
`cause` (`RemoteError`, `TypeError`, `AttributeError`, or `Fenced`). Earlier
items completed; the failing method may already have had side effects before
raising; later items were not invoked. Each return value is serialized before
the next item runs. Fencing is rechecked between items.

The transport timeout applies to the batch exchange, not separately to every
item. `OutcomeUnknown` means an unknown prefix, or the whole batch, may have
run. Async cancellation stops waiting, not remote execution. Nothing is
automatically retried. Old HTTP peers are rejected as incompatible endpoints;
the batch is never replayed as individual requests.

Each item receives a distinct, stable `CallContext.request_id` derived from
the batch ID and its index. Short IDs use `<batch-id>:<index>`; long IDs retain
a deterministic hash and stay within 200 characters. Pin the batch ID with
`request_id()` when the application needs to reconcile retries. Deduplication
remains the application's responsibility.

A batch occupies one concurrency slot and counts as one request in `calls`;
it is counted as failed if any item fails.

## Rust SDK

The public workspace crate `tinyray` has no Python/PyO3 dependency. Its main
types are:

- `MemberBuilder` / `Member`: join, publish state/readiness, watch, flush, and
  leave.
- `DiscoveryPool` / `Snapshot` / `MemberRef` / `Epoch`: read the Arc-backed
  roster directly, with filter/count/pick/slot/get, frozen views, validity
  checks, and synchronous/asynchronous count, departure, and replacement
  waits without copying member state.
- `Service` / `Router`: advertise named methods and dispatch opaque
  MessagePack payloads.
- `CallContext`: caller identity, request ID, fencing target, and a
  cancellation token that fires when the connection or server closes.
- `Client` / `Target`: raw and serde-typed synchronous/asynchronous calls with
  caller-controlled request IDs and no automatic retry.
- `RpcRuntime`: lets multiple `Client` and `Server` values share one Tokio
  worker pool. `MemberBuilder` places its client and server on the same
  four-worker runtime by default.
- `ReceivedRawReply` / `ReceivedRpcReply`: own the response's BlobRef
  acknowledgement while raw bytes are inspected; `decode<T>()` maps BlobRefs
  before releasing it.
- `Server` / `ServerConfig`: standalone listener lifecycle, admission limits,
  counters, and explicit shutdown.

`Router::raw` does not deserialize application payloads. `typed` decodes the
whole payload, while `typed_arg` and `typed_no_args` use the Python-compatible
argument envelope. Batches remain one admitted request and run items
sequentially, stopping at the first failure with `batch_index`, `completed`,
and the encoded successful prefix.

The generic listener is shared with Python. A Rust `Service` future runs
directly on Tokio and never acquires the GIL or enters `spawn_blocking`.
Python `serves=` installs a `PythonService` adapter on the same listener; only
that adapter enters `spawn_blocking` and acquires the GIL.

## BlobRef

```python
with tinyray.blob(data) as blob:
    result = handle.consume(blob)
    view = blob.view()       # read-only memoryview, no payload copy
    copied = bytes(blob)     # explicit copy
```

`BlobRef` is explicit same-host transport, not a cross-host fallback. On Linux
the creator copies the input once into a `memfd`, sets mode 0600/CLOEXEC,
seals it against write/grow/shrink/further seal changes, and maps it read-only.
`MAX_BLOB_BYTES` is the default 256 MiB creation and receive cap; Rust exposes
the same limit as `DEFAULT_MAX_BLOB_BYTES`, and both APIs accept a lower
per-operation cap. One decoded MessagePack value may contain at most
`MAX_BLOB_REFS_PER_MESSAGE` (64) BlobRefs and
`MAX_BLOB_MAPPED_BYTES_PER_MESSAGE` (512 MiB) of distinct mapped data.
Identical descriptors in that value share one mapping. Native fallback
admission also limits directly decoded/public `from_descriptor` handles to
128, live decoded mappings to 64, and their aggregate mapped bytes to 512 MiB.
MessagePack extension **124** carries only a bounded descriptor: protocol,
Linux boot fingerprint, owner pid/fd, payload size, device/inode, and a random
token from Linux `getrandom`, also stored in the sealed header.

The receiver first checks the size limit (256 MiB by default), boot identity,
numeric pid/fd, then opens only `/proc/<pid>/fd/<fd>` read-only. It verifies
device, inode, exact file length, all required seals, header magic, token and
size before mapping. A stale, reused, cross-boot, unsealed, oversized, or
malformed descriptor raises `BlobError` before the method is invoked. Unknown
MessagePack extensions remain ordinary `msgspec.msgpack.Ext` values and cannot
forge a BlobRef.

Outgoing call owners move into native pending state and remain there after a
fully written timeout or cancellation until the late reply or connection
teardown. Response owners remain on the server until the caller finishes
decoding and sends a correlated BlobRef acknowledgement. Each reply is capped
at 64 unique owners and 512 MiB. Outstanding replies are additionally bounded
per connection (128 refs/512 MiB), server (512 refs/1 GiB), and process
(2,048 refs/2 GiB); duplicate owners are charged once. Closing a connection
releases all its unacknowledged permits and owners. A live raw-reply guard
keeps both endpoints out of their normal 10/15-second idle expiry; an
unacknowledged response still has a hard 60-second server deadline.

Every public serialization uses the current process PID and local fd, so an
inherited mapping that remains valid in a child never advertises the parent's
descriptor. Once mapped, a `BlobRef` remains valid after its sender closes or
exits, including SIGKILL. The kernel deletes the anonymous object when the
final fd/mapping closes. Exported memoryviews prevent normal close; forked
Python children close all native-registered BlobRefs and native-only RPC owner
sets before inherited runtimes are forgotten.

`BlobRef.from_descriptor(...)` is intentionally public for explicit protocol
integration and testing. It does not accept paths and does not bypass identity,
seal, strict full-consumption parsing, size, count, or mapped-byte admission.
Rust `BlobRef::from_file` uses positional reads, leaving the caller's file
cursor unchanged and rejecting truncation or growth during the snapshot.

There is no non-Linux, missing-`/proc`, permission, or cross-host bytes
fallback. Ordinary `bytes` encoding and transport are unchanged.

## Exceptions

```text
TinyrayError
├── Unreachable          no answer came back
│   ├── NotDelivered     definitely not delivered -- it did not run, retry it
│   └── OutcomeUnknown   it may have run -- retry with a request id, or be idempotent
├── Fenced               delivered, but that seat changed hands
├── RemoteError          delivered, and the method raised (.type/.message/.traceback)
├── BatchError           an item failed; exposes its index, earlier results, and cause
├── Stale                out of contact with the registry; the roster is not trustworthy
└── SeatTaken            the seat is held (exclusive) or was taken by a later tenure

NotFound(LookupError)    nobody matched
TypeError                the arguments do not fit the callee's signature -- it did not
                         run, and the caller got it wrong
PolicyError(ValueError)  the policy, seat and size do not add up
OversizeWarning(UserWarning)     past the 1 MB advisory line (advisory only; it is sent)
OldRegistryWarning(UserWarning)  the registry is older than this package
```

**Only `NotDelivered` may be retried blindly.** `OutcomeUnknown` means the far
side may already have done it -- the one case that needs a request id. tinyray
never retries `RemoteError` for you; only you know whether it can be redone.

Which class you get depends on **whether the callee read the request whole**,
not on the status code it returned. Every path where it gives up before
finishing the read -- an unparsable content-length, a body that stops halfway,
the concurrency limit -- had not called the method yet, so all of them are
`NotDelivered`.

Arguments that do not fit the signature (too many positional, a required one
missing, an unknown keyword, the same parameter twice, a type mismatch) did
not run either, but that is **the caller's mistake**, so it is `TypeError`
rather than `Unreachable`: retrying the same call cannot end differently.

`NotDelivered` and `OutcomeUnknown` are both subclasses of `Unreachable`, so
existing `except Unreachable` is unaffected.

---

## Environment variables

| Variable | What it does |
|---|---|
| `TINYRAY_REGISTRY` | The registry address, `127.0.0.1:8760` by default. Exactly **one** address; `join(registry_url=)` overrides it |
| `TINYRAY_ADVERTISE` | The advertised **hostname or IP**, and nothing else. Required on a multi-homed machine, or the wrong interface may be registered |
| `TINYRAY_SLOT` / `RANK` / `SLURM_PROCID` / `OMPI_COMM_WORLD_RANK` | The seat number |
| `TINYRAY_SIZE` / `WORLD_SIZE` / `SLURM_NTASKS` / `OMPI_COMM_WORLD_SIZE` | The size |

---

## The registry

```bash
tinyray --listen 127.0.0.1:8760 --ttl-ms 20000
```

`--ttl-ms` is the lease length, with a floor of 200 ms (clients beat at ttl/4,
and anything shorter expires between two beats).

**It decides two things only: how long before a member is declared gone, and
how much heartbeat traffic there is. It does not decide how quickly a change
becomes visible.** Those three used to be tied to one number -- the interval
was `ttl/4`, and that was also the upper bound on discovery latency, so "do
not declare people dead too early" fought directly with "notice things
quickly".

Now the registry **parks the answer** while it has nothing to say and returns
the moment the pool moves. A client asks to be parked for exactly the interval
it would otherwise have slept, so the number of requests is unchanged while
the answer goes from "returns on a timer" to "returns when something happens":

| ttl | Old upper bound | Measured discovery latency | Beats/s |
|---|---|---|---|
| 2 s | 500 ms | 21 ms | 2.06 |
| 8 s | 2,000 ms | 19 ms | 0.75 |
| 20 s (default) | 5,000 ms | 21 ms | 0.44 |

Publishing your own state is unaffected and always immediate (0.6 ms
measured) -- when there is something to send, the parked request in flight is
cancelled and replaced.

A caller that does not ask to be parked still gets an answer at once.

Production heartbeat connections are persistent but strictly serial: exactly
one `beat` is in flight, and the next is sent only after a complete,
correlated `beat_ack`. Publication cancellation, timeout, EOF, malformed or
mis-correlated replies, registry restart, and refusal discard the connection;
the following beat connects lazily, so a late reply cannot be consumed by a
new request. The server closes a connection idle for 35 seconds. `health` and
`debug_pools` remain one-request probes.

Requests are capped at 512 KiB and replies at 64 MiB, with the declared length
checked before allocating the body. Every reply echoes `request_id`. Health
also reports cumulative accepts, active connections, and received frames for
benchmarks and diagnosis.

| operation | reply | What it is for |
|---|---|---|
| `health` | `health_ack` | Liveness, version and protocol probe |
| `debug_pools` | `debug_pools_ack` | Each pool's version / roster / member count |
| `beat` | `beat_ack` | Heartbeats, long polls and `leaving=true` goodbye |

Unknown operations and malformed frames return a structured
`operation="error"` reply. Seat or pool-shape refusal remains a normal
`beat_ack` with `accepted=false`.
