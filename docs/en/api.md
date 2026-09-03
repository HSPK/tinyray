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
) -> Member
```

Blocks until the first beat lands. Raises `Unreachable` if the registry cannot
be reached, `SeatTaken` if a later tenure holds the seat, and `PolicyError` if
the pool's shape disagrees. **One process joins one pool.**

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
- **`url=`** -- set the advertised address by hand. By default it is probed
  from the routing table; on a multi-homed machine use `TINYRAY_ADVERTISE`.
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

    !!! warning "A hostname only -- no scheme, no port"
        `http://` and the port are composed around it, so
        `TINYRAY_ADVERTISE=http://10.0.0.5` becomes
        `http://http://10.0.0.5:33097`. That now **fails immediately** and
        says why; it used to register happily and blow up when somebody called.

        To advertise a genuinely different address -- behind a reverse proxy,
        say -- use `join(url=...)`.

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

    What goes out is the **current value**, not a log. There is one heartbeat
    loop and it reads that register, so nothing arrives out of order; but two
    publishes closer together than a beat may mean the middle value never goes
    out at all -- that is the definition of soft state, not a defect. If every
    step has to leave a trace, that belongs on the data plane.

`flush()` costs at most one extra beat: if a beat was in flight when you
called, it was composed before the change, so confirmation waits for the one
after. It raises `TimeoutError` if the registry cannot be reached, and
`SeatTaken` if the seat was taken.

!!! note "State has a hard cap"
    16 KB, and over it is an error. It is not the same as the RPC size limit:
    state is copied to **every subscriber**, measured at 6 MB reaching 20
    subscribers as 120 MB. This limit protects other people.

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

### Snapshots and changes

```python
pool.snapshot(include_unready=True) -> Snapshot
pool.changes(since=None, timeout=None) -> Watch         # iterates Snapshots
apool.achanges(since=None, timeout=None) -> AsyncWatch  # async iteration
```

`changes()` blocks on an event and **never polls**. While the pool is still it
does not return.

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
Measured at 5,000 members:

| | |
|---|---|
| `pool.snapshot()` | 8.78 ms |
| `field_digest(["role", "ready"])` | **0.40 ms** |

A predicate in Python saves none of that -- it needs the `Snapshot` before it
can decide, and by then the money is spent.

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
    `achanges()` waits on a pipe the heartbeat writes to, selected by the event
    loop, and **borrows no executor thread**. Cancellation is therefore
    immediate, and cancelling any number of them affects nothing else.

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

Everything below is a special case of it. So is `pool.wait()` -- it used to
write its own loop, which made it the one wait in the library that, once
superseded, burned the whole timeout and then blamed the pool for being empty.

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

```python
h.assign("task")  # call it
h.assign.timeout(5.0)("task")  # this call's timeout; 30 seconds by default
h.pull_job.returns(AgentJob)()  # restore the JSON result as an AgentJob
```

JSON does not retain Python types: a `NamedTuple` crosses the wire as an array,
and a dataclass-shaped value as an object. `.returns(T)` declares the type to
restore on the calling side. It recursively handles `NamedTuple`, dataclass,
`TypedDict`, Enum, `T | None`, and containers such as `list[T]`,
`dict[K, V]`, tuple, and set:

```python
class AgentJob(NamedTuple):
    attempt: AttemptKey
    proxy_url: str

job = h.pull_job.returns(AgentJob)()
jobs = await ah.pull_jobs.returns(list[AgentJob])()
```

A conversion failure raises a local `TypeError` naming the remote member,
method, and failing JSON path. The remote method has already completed
successfully at that point; only result restoration failed. The protocol
remains plain JSON and never sends Python class names. The value returned by
the server must itself be JSON-compatible -- `.returns()` restores types, it
does not teach the server to serialize arbitrary objects.

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
```

`RegistryInfo.FEATURES` maps a feature name to the protocol it needs, and it
lives on the side that depends on it, so an old client does not need to know
about future features. A misspelled feature name raises `ValueError` rather
than returning `False` -- the latter would let a typo walk quietly into the
degraded branch.

You can look without joining:

```console
$ curl -s http://registry:7000/health
{"status":"ok","version":"0.9.0","protocol":1}
```

| protocol | Meaning |
|---|---|
| 0 | Before long polling (earlier than 0.7.0) |
| 1 | Understands `hold_ms`: park the answer while there is nothing to say, and return the moment a watched pool moves |

!!! warning "A version mismatch is a performance cliff, not an error"
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

**Self-declared identity, not authentication.** In this system a member picks
its own tenure number anyway. What it buys is that the caller cannot forget to
send it or send the wrong one, and nothing more.

`request_id` differs per call by default, so logs on both sides can point at
the same attempt.

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

## Exceptions

```text
TinyrayError
├── Unreachable          no answer came back
│   ├── NotDelivered     definitely not delivered -- it did not run, retry it
│   └── OutcomeUnknown   it may have run -- retry with a request id, or be idempotent
├── Fenced               delivered, but that seat changed hands
├── RemoteError          delivered, and the method raised (.type/.message/.traceback)
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

A caller that does not ask to be parked -- including any client older than the
field -- still gets an answer at once.

| Endpoint | What it is for |
|---|---|
| `GET /health` | A liveness probe |
| `GET /v1/pools` | Each pool's version / roster / member count |
| `POST /v1/beat` | The heartbeat (used by clients) |
