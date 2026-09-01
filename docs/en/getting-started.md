# Getting started

Ten minutes, from install to two processes finding and calling each other.

*[中文](../getting-started.md)*

## Install

```bash
pip install tinyray
```

The registry ships in the wheel; there is no second thing to install.

```bash
tinyray --listen 127.0.0.1:8760 --ttl-ms 20000
```

It has no config file, no data directory and never writes to disk. Every piece
of state is re-asserted by its owner on each heartbeat, so **if it dies, just
start it again** -- it refills itself within one heartbeat interval.

!!! tip "In a cluster"
    There is **one** registry, started by a supervisor. Not replicating it is
    deliberate: its state is soft, so losing it is a gap in discovery, not
    lost data.

## Three concepts

| Concept | In one line |
|---|---|
| **member** | A process. After `join()` it is on the roster; when its heartbeats stop it is out. |
| **pool** | A group of like processes. Found by name. |
| **incarnation** | One process lifetime. When a seat changes hands, the tenure number changes. |

One process joins one pool. If you need several roles, start several
processes; do not register logical components as members.

## Reporting in

```python
import tinyray

me = tinyray.join("collector", "stateful", slot=0)
me.ready(model_version=17)
```

`join()` **blocks until the first beat lands**, so returning means the registry
really has you. If it cannot be reached you get `Unreachable`; it does not
pretend to have succeeded.

`ready()` hangs out the sign: what you pass goes into your state, where others
can see it.

## Letting others call you

Hand an ordinary object to `serves=` and its public methods become the
interface -- no decorators, no IDL.

```python
import tinyray


class Collector:
    def assign(self, task: str) -> dict:
        return {"took": task}


with tinyray.join(
    "collector", "stateful", slot=0, serves=Collector(), max_concurrency=64
) as me:
    me.ready(model_version=17)
    ...
```

`max_concurrency` caps concurrency. Past it callers are refused rather than
queued -- a refusal is bounded, a queue is not.

Underneath it is ordinary HTTP, so nothing is lost for debugging with `curl`:

```bash
curl -X POST http://host:port/call/assign -d '{"task":"t"}'
curl http://host:port/_methods
```

## Finding someone, then calling them

```python
import tinyray

me = tinyray.join("driver", "churn")
me.ready()

pool = tinyray.pool("collector")
engine = pool.wait(count=1, timeout=20)[0]

engine.assign("task-7")  # {'took': 'task-7'}
pool.pick(model_version=17)  # filter on state, pick one at random
pool.slot(0)  # by seat number; an empty seat raises NotFound
```

**A lookup reads the local cache and never touches the network.** So looking a
thousand times adds no load to the registry and cannot time out. The cache
trails the truth by about one round trip (21 ms measured with the defaults) --
the registry sends the answer when the change happens, rather than waiting for
your next beat to ask.

## Who is calling me

Annotate a parameter as `CallContext` and the library fills it in:

```python
class Collector:
    def assign(self, task: str, ctx: tinyray.CallContext) -> dict:
        # ctx.identity / ctx.pool / ctx.slot / ctx.incarnation
        return {"took": task, "for": ctx.identity}
```

The caller writes nothing.

!!! warning "This is not authentication"
    Identity is **self-declared** -- in this system a member picks its own
    tenure number too. What it buys is that the caller cannot forget to send
    it or send the wrong one, not that it cannot be forged. Do not use it as a
    permission boundary.

## The call failed -- can I retry?

This is the one that matters most. **Only "definitely not delivered" may be
retried as-is.**

```python
try:
    engine.assign("task-7")
except tinyray.NotDelivered:
    # It never reached the far side, so the method certainly did not run.
    # Retry against someone else; no request id needed.
    ...
except tinyray.OutcomeUnknown:
    # It may have run. To retry, carry the same request id, or be idempotent.
    ...
except tinyray.Fenced:
    # That seat changed hands. Look the address up again.
    ...
except tinyray.RemoteError as e:
    # It arrived and the method itself raised. tinyray never retries this.
    print(e.type, e.message, e.traceback)
```

The first two are subclasses of `Unreachable`, so existing
`except Unreachable` keeps working.

| Exception | Did it arrive? | What to do |
|---|---|---|
| `NotDelivered` | **Definitely not** | Retry directly |
| `OutcomeUnknown` | **Unknown** | Retry with a request id, or be idempotent |
| `Fenced` | Yes | Look the address up again |
| `RemoteError` | Yes | Your own problem to decide |

## Wait for changes; do not poll

```python
snap = pool.snapshot(include_unready=True)

for snap in pool.changes(since=snap.revision):
    print(snap.revision, len(snap), len(snap.ready()))
```

`changes()` blocks on an event. While the pool is still it does not return and
**burns no CPU**. The async form is `AsyncPool.achanges()`.

The loop ending quietly means the timeout ran out or somebody called
`close()` -- both fine. But if **this process is superseded** it raises
`Fenced`: that is not "you have seen everything", it is the seat being gone
and the cache frozen from here on. Stop what you are holding and `join()`
again.

```python
try:
    for snap in pool.changes():
        ...
except tinyray.Fenced:
    ...  # the seat is gone
```

The difference between `snapshot()` and `all()` is worth remembering:

- `all()` answers "**who can I use**" -- only members that are ready
- `snapshot()` answers "**who holds a seat**" -- including one that has taken a
  seat and not yet said it is ready

The usual trap is during setup: a member has taken the seat (so nobody else
may have it) but has not said it is ready, and `all()` reports it missing.
`snapshot()` is the answer to that question. Every entry carries its own
`incarnation` and `ready`, so comparing two snapshots tells apart "still there
but not ready", "the one that was there left" and "the same seat changed
hands".

## Being superseded

Seats are last-writer-wins: a restarting process has to be able to reclaim its
seat even while the old lease is still running. The superseded side needs to
find out on its own, because it may still be holding a GPU, an inference
server and a port:

```python
if me.wait_fenced(timeout=60):
    shutdown_my_inference_server()  # tinyray only stops calls that go through it
```

The async form is `await me.await_fenced()`.

!!! warning "It does not protect against a split brain"
    Learning that you were replaced needs **contact with the registry**.
    During a partition this waits forever -- out of contact there is no way to
    know. That is the same trade as "losing the registry does not stop a
    training run".

## Publishing state, and confirming it

```python
me.set_ready({"weights": "v9"})  # replaces the whole map, does not merge
me.flush(timeout=10)  # wait until the registry really has it
```

`ready(**kw)` **merges**, so a key you have published cannot be taken back.
Use `set_ready()` to replace the whole map. `flush()` saves you from
publishing and then looking yourself up to find out.

## Roll call together

When every rank must be handed the **same list** -- building a communication
group, say -- use `epoch()`:

```python
me = tinyray.join("trainer", "collective", slot=RANK, size=WORLD_SIZE)
me.ready()

ep = tinyray.pool("trainer").epoch()  # wait for everyone, then freeze
build_process_group(ep.members)  # every rank is handed the same list
```

A frozen round carries a fingerprint, and matching fingerprints across ranks
mean matching lists -- because a round is only handed out when the fingerprint
is the one computed from that very list.

```python
def watchdog():  # checking inside the training loop is useless: a stuck rank
    while ep.valid:  # never reaches that line. A background thread works,
        time.sleep(0.5)  # because NCCL releases the GIL while it blocks.
    pg._abort()
```

## Before you go

```python
me.leave()
```

A normal exit calls `leave()` for you, which frees the seat at once instead of
waiting for the lease to expire. A SIGKILL falls back to lease expiry. Both
work; they differ only in how fast.

## Next

- [API reference](api.md) -- the whole surface
- The design notes are Chinese only: [为什么](../01-why.md) (the problem and
  why existing tools do not fit) and [是什么](../02-design.md) (policies,
  boundaries, scale and guarantees)
