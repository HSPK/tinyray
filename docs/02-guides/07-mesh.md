# Peer mesh

## Purpose

Making workers talk to each other, so the driver stops being a relay.

Use this when your workload is a **pipeline** — a fleet producing for another
fleet — rather than a fan-out. A dataloader fleet feeding a trainer fleet, a
rollout fleet feeding a learner, a prefill tier feeding a decode tier.

## The shape

One sidecar beside each framework process, sidecars connected to each other:

```
loader process 0..N                     trainer process 0..M
┌──────────────────────────┐            ┌──────────────────────────┐
│ torch DataLoader         │            │ model + optimiser        │
│          ▲ local         │            │          ▲ local         │
│  ┌───────┴────────┐      │            │  ┌───────┴────────┐      │
│  │ tinyray sidecar│◄─────┼── direct ──┼─►│ tinyray sidecar│      │
│  └────────────────┘      │            │  └────────────────┘      │
└──────────────────────────┘            └──────────────────────────┘

            driver: places them, introduces them, goes quiet
```

The driver still does what only it can do — placement, supervision, restart. It
stops doing what it should never have been doing: forwarding every message.

## Setting one up

### 1. Serve each side

Nothing new. Each process ends in `serve`, wrapping the object the framework
built:

```python
# loader.py
loader = DataLoader(dataset, num_workers=4)   # yours
tinyray.serve(LoaderSidecar(loader))          # the only tinyray line
```

### 2. Launch the fleets

```python
loaders = tr.launch_workers([sys.executable, "loader.py"], size=8, name="loader")
trainers = tr.launch_workers([sys.executable, "train.py"], size=4, name="trainer")
```

### 3. Introduce them

```python
tr.link(loader=loaders, trainer=trainers)
```

Every member of every named group now knows every other member.

This is a **push after startup**, not an environment variable, because
endpoints do not exist until every worker has bound a port. It is also why
`link` is a separate call rather than something `launch_workers` does: the
groups have to exist before they can be introduced.

### 4. Talk

Inside any worker:

```python
import tinyray

tinyray.my_group()        # "trainer"
tinyray.my_rank()         # 2
tinyray.group_size("loader")   # 8

loaders = tinyray.peers("loader")        # list, indexed by rank
mine = loaders[tinyray.my_rank()::4]     # this rank's share

ref = mine[0].next_batch.remote()        # non-blocking, as in the driver
batch = tinyray.get(ref)                 # fetched from the loader, direct
```

`peers` returns `RemoteWorker` handles, so calling a peer looks exactly like
calling a worker from the driver.

## Discovery, not configuration

The alternative is passing endpoints down command lines, which fails for a
reason worth stating: **a restarted worker comes back on a new port.** A command
line is fixed at exec time; a roster can be pushed again.

```python
tr.link(loader=loaders, trainer=trainers)   # after a restart, survivors relearn
```

`link` is idempotent and replaces the previous roster entirely.

## Passing peers around

Handles are picklable, so a peer reference can travel:

```python
def hand_off(self, group, rank):
    target = tinyray.peer(group, rank)
    return tinyray.get(tinyray.peer("helper", 0).use.remote(target))
```

An `ActorHandle` sent to a worker arrives as a `RemoteWorker`. It can still be
called; it can no longer be managed. That asymmetry is deliberate — placement
and restart belong to whoever owns the head, and a worker does not.

## Both directions

A mesh is not a one-way pipe. In the
[dataloader example](08-examples.md#dataloader_sidecarspy-the-mesh):

- **trainer → loader**: `set_epoch`, prefetch depth, and the pull itself
- **loader → trainer**: the batches

Which side drives the loop is your choice. Having the *consumer* drive is
usually simpler, because it needs no backpressure protocol — it asks for the
next batch when it wants one.

## What it costs

**A worker's executor is single-threaded.** While a sidecar runs a long method,
incoming peer calls queue behind it. A push-based design where the producer
calls into a busy consumer will stall; a pull-based one will not.

**No cycle detection.** If A calls B and B calls A synchronously, both block.
tinyray will not warn you.

**The roster is a snapshot.** A worker that restarts after `link` has a stale
entry until you link again. Nothing pushes it automatically — see
[roadmap](../05-project/03-roadmap.md).

## Pitfalls

**`link` before use, or `NotLinked`.** `peers`, `my_rank` and `my_group` raise
if the driver never pushed a roster. The message says so.

**`hasattr` is meaningless on a handle.** Handles proxy every public name to a
remote method, so `hasattr(worker, "anything")` is `True`. This already caused a
bug here: a dispatch on `hasattr(x, "world_size")` treated a single actor as a
worker group. Check types, not attributes.

**Group names are yours.** `launch_workers(name=...)` and the keyword in
`tr.link(...)` are independent. Keeping them the same avoids confusion.

**Peer calls do not go through the driver, so the driver cannot see them.**
`transport_stats()` in the driver shows driver traffic only. Use
`tinyray status` against the workers.

## See also

- [02-native-frameworks.md](02-native-frameworks.md) — `serve`, `launch_workers`, `connect`
- [08-examples.md](08-examples.md) — a runnable mesh
- [02-decisions.md](../05-project/02-decisions.md#reversals) — why this was missing at first
