# Actors

## Purpose

The actor API, for code written for tinyray: pure-Python rollouts, evaluation
harnesses, hyperparameter trials. If your code is a native framework script,
[attach to it](02-native-frameworks.md) instead — this API is the most invasive of
the three.

## Concepts

An actor is a class whose instance lives in its own OS process. tinyray starts
the process, ships the class over the wire and constructs it remotely. Calls are
dispatched over HTTP and executed one at a time, in submission order.

## Defining and creating

```python
import tinyray as tr

tr.init()


@tr.remote(num_gpus=1, num_cpus=4, max_restarts=3)
class Rollout:
    def __init__(self, cfg):
        self.env = make_env(cfg)

    def step(self):
        return self.env.rollout()          # ~10 MB


actor = Rollout.remote(cfg)                # starts a process, runs __init__
```

`.options()` overrides the decorator for one instance:

```python
trial = Rollout.options(num_gpus=0.25, name="trial-7").remote(cfg)
```

### Options

| Option | Default | Meaning |
|---|---|---|
| `num_cpus` | 1.0 | Fractional allowed |
| `num_gpus` | 0.0 | `>= 1` reserves whole devices; fractional shares one |
| `memory_bytes` | 0 | Checked against the node |
| `max_restarts` | 0 | See [fault tolerance](05-fault-tolerance.md) |
| `max_pending_calls` | 1000 | Backpressure threshold |
| `store_max_bytes` | 2 GiB | Result store watermark |
| `store_ttl_seconds` | 300 | Result lifetime |
| `strategy` | `"SPREAD"` | `PACK`, `SPREAD` or `STRICT_SPREAD` |
| `name` | — | Makes the actor findable with `get_actor` |

## Calling

```python
ref = actor.step.remote()        # returns immediately, never raises your error
value = tr.get(ref)              # blocks, raises the remote exception
```

`.remote()` does not wait for the method to run. Failures surface at `get()`,
carrying the remote traceback:

```python
try:
    tr.get(actor.might_fail.remote())
except tr.UserCodeError as exc:
    print(exc.remote_traceback)   # the actor's stack, not the driver's
```

### Ordering

Calls from one caller run in submission order, even though HTTP delivers them
concurrently over a pool of connections. This is what makes the obvious thing
correct:

```python
actor.set_weights.remote(w)      # always runs first
actor.step.remote()              # always runs second
```

Different callers are independent of each other.

## Waiting on many calls

```python
refs = [a.step.remote() for a in actors]
ready, pending = tr.wait(refs, num_returns=24, timeout=60)
batch = tr.get(ready)
```

`wait` returns two lists of `ObjectRef`, never values. `ready` have settled —
successfully or not; `pending` are still running.

**Dropping a straggler drops its result, not its work.** Those actors keep
running and their outputs occupy their stores until the watermark or TTL
reclaims them. If they are also in a collective group, they must still attend
the next barrier.

`wait` is a readiness probe: it moves a few hundred bytes regardless of payload
size. Measured on a settled 200 MB result: 0.14 ms.

## Passing references between actors

The property that keeps large payloads off the driver:

```python
refs = [r.step.remote() for r in rollouts]    # 10 MB each, still in the rollouts
learner.update.remote(refs)                   # the driver moves ~40 bytes each
```

Inside the learner, fetch them:

```python
@tr.remote
class Learner:
    def update(self, refs):
        import tinyray as tr

        for batch in tr.get(refs):            # fetched from the rollouts directly
            ...
```

**Top-level reference arguments are resolved automatically**, matching Ray:

```python
learner.update.remote(one_ref)     # arrives as the value
learner.update.remote([one_ref])   # arrives as a reference, you call get()
```

The nested form is deliberate: an actor receiving a batch of references decides
when, or whether, to fetch each one.

## Result lifetime

A result lives in the actor that produced it until one of:

- you call `tr.release(ref)`;
- the store passes `store_max_bytes` and evicts it (least recently used);
- `store_ttl_seconds` expires;
- the actor restarts, taking its whole store with it.

Afterwards, `get()` raises `ObjectLost`. That is deliberately distinct from
`NotFound`: `ObjectLost` means it existed and you were late, which points at the
watermark; `NotFound` points at a bug.

## ActorPool

For many items across a fixed set of actors:

```python
pool = tr.ActorPool(actors)
for result in pool.map_unordered(lambda a, x: a.run.remote(x), hparam_grid):
    ...                                      # yields in completion order
```

`map_unordered` yields as results arrive, so one slow trial does not hold up
everything behind it. `map` preserves input order. Both bound how far ahead they
submit, so the queue does not simply move into the actors' memory.

## Named actors

```python
ps = ParamServer.options(name="ps").remote()
...
ps = tr.get_actor("ps")           # from anywhere in the same driver
```

Naming and lifetime are independent. `lifetime="detached"` is **refused** with
an explanation: it needs a standalone head process to own the actor, which does
not exist yet, so accepting it would silently leak a process at shutdown.

## Collective groups

> **Do not use this with Megatron, SGLang or vLLM.** It calls
> `init_process_group` and takes the process's one default group. Use
> [`create_worker_group`](02-native-frameworks.md) instead.

For pure-tinyray actors that need NCCL among themselves:

```python
group = tr.collective.create_group([learner, *rollouts])
group.run("sync_weights", src_rank=0)
```

Admission is strict, because every rule corresponds to a NCCL failure that hangs
rather than errors: at least two members, each owning a whole GPU, no two
sharing a device.

Groups carry an epoch. Any membership change marks the group `BROKEN` and
requires `tinyray.collective.rebuild(group)` — a seconds-scale operation, so
groups must be long-lived and never rebuilt per iteration. **Rebuild is not
automatic**; a restarted member leaves the group broken until you call it.

## Killing

```python
tr.kill(actor)                    # immediate, for early stopping a bad trial
```

## Contract

| Call | Blocks | Raises |
|---|---|---|
| `Class.remote(...)` | until `__init__` returns | `PlacementFailed`, `UserCodeError` |
| `method.remote(...)` | no | only on backpressure or a dead actor |
| `get(ref)` | yes | the remote exception |
| `wait(refs, num_returns)` | until N settle or timeout | no |
| `release(refs)` | no | no (best effort) |
| `kill(handle)` | until the process exits | no |

## Pitfalls

**Do not call methods without `.remote()`.** `actor.inc(1)` raises a `TypeError`
telling you so, rather than silently running in the driver.

**Results from `get()` are read-only.** One buffer serves many consumers.
`.copy()` if you need to write.

**Private methods are not remotely callable.** Anything starting with `_` is
refused — a guard against accidents, not a security boundary.

**A restarted actor loses its store.** Every reference into it becomes
`ObjectLost`. The constructor is replayed, so state rebuilds, but results do
not come back.

**Backpressure only engages when the actor is slower than its caller.** With a
fast method the queue never fills. That is the intended behaviour, not a bug.

## See also

- [02-native-frameworks.md](02-native-frameworks.md) — the less invasive path
- [05-fault-tolerance.md](05-fault-tolerance.md) — restart semantics in detail
- [04-placement.md](04-placement.md) — how `num_gpus` is interpreted
- [01-api-python.md](../03-reference/01-api-python.md) — exact signatures
