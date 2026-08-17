# Placement

## Purpose

How tinyray decides where a process runs, how GPUs are reserved, and why gang
placement is atomic.

## Concepts

One scheduler covers actors, served workers and managed processes alike. That is
the only way to guarantee a trainer actor and an inference server are never
handed the same GPU.

Placement happens **once**, when something is created. There are no stateless
tasks, so there is no high-frequency scheduling and the head stays simple.

## Resources

| Resource | Fractional | Notes |
|---|---|---|
| `num_cpus` | yes | Advisory; not enforced with cgroups |
| `num_gpus` | yes, with a caveat | See below |
| `memory_bytes` | — | Checked against the node's reported total |

### The GPU rule

```
num_gpus >= 1     reserves that many whole physical devices, exclusively
num_gpus < 1      shares a device, reserves none exclusively
```

Whole devices are what a collective member or a tensor-parallel engine needs.
Two NCCL ranks on one device deadlock rather than erroring, so exclusivity is a
correctness requirement.

Fractional requests exist for hyperparameter trials, which pack many small jobs
onto one card. **A fractional actor cannot join a collective group** — it does
not own a device — and it does not need to, having no weights to broadcast.

Whichever it gets, the process sees its devices as `CUDA_VISIBLE_DEVICES`, so
user code can assume it owns device 0.

## Strategies

| Strategy | Picks | Use for |
|---|---|---|
| `PACK` | the busiest node that still fits | Tensor-parallel groups; fractional trials |
| `SPREAD` | the emptiest node | Rollout actors wanting independent CPUs and NICs |
| `STRICT_SPREAD` | the emptiest, refusing to co-locate | Fault isolation |

`PACK` is the default for worker groups: ranks on one node share NVLink, and
splitting a tensor-parallel group across machines is usually an accident.
`SPREAD` is the default for actors.

Both orderings are stable, so placement does not depend on hash iteration order.

## Gang placement

```python
workers = tr.launch_workers(["python", "train.py"], size=8, gpus_per_worker=1)
rollouts = tr.create_actors(Rollout, cfg, count=32)
```

Both are **atomic: all or nothing.** This is a requirement rather than an
optimisation. A group that comes up halfway cannot complete a rendezvous, and
the framework inside blocks forever waiting for ranks that will never arrive —
far harder to diagnose than a clean refusal.

Ask what would fit before committing:

```python
from tinyray._tinyray import ClusterState   # via the head in practice

capacity = tr.api._require_context().head.state.gang_capacity(
    num_cpus=4.0, num_gpus=1.0, strategy="SPREAD",
)
```

## When it does not fit

The error carries the arithmetic, because "infeasible" at 3am is useless:

```
PlacementFailed: no node can satisfy the request: requested 4.00 CPUs and
2 whole GPUs; the best node has 12.00 CPUs and 1 free GPUs across 2 node(s)
```

For a gang:

```
PlacementFailed: cluster has 4 of 8 bundles free; gang placement is all or nothing
```

A failed gang leaves nothing reserved. A failed single placement returns its
reservation before raising.

## Resource lifecycle

Resources are returned **immediately** when a process exits, not at the next
heartbeat. A hyperparameter sweep starts and stops actors constantly, and
waiting for a heartbeat would idle the cluster.

Reservations have exactly one owner. Releasing one twice does not lose a GPU —
it invents one, after which the scheduler places two processes on hardware that
fits one. `ClusterState::release` clamps to the node total so that a future
caller bug is a no-op rather than a fiction.

## Inspecting

```python
for node in tr.nodes():
    print(node["hostname"], node["available_cpus"], node["free_gpu_ids"])

for actor in tr.actors():
    print(actor["name"], actor["state"], actor["gpu_ids"])

for process in tr.processes():
    print(process.name, process.gpu_ids, process.endpoint)
```

## Contract

**`create_actors(remote_class, *args, count, strategy="SPREAD", **kwargs)`** —
atomic. Returns handles, or raises `PlacementFailed`.

**`launch_workers(command, *, size, gpus_per_worker, strategy="PACK", ...)`** —
atomic. Returns a `WorkerGroup`.

**`launch_process(command, *, num_cpus, num_gpus, strategy="PACK", ...)`** —
single placement.

## Pitfalls

**CPU limits are bookkeeping, not enforcement.** tinyray will not start more
than the node claims to have, but nothing stops a process using more. There are
no cgroups.

**Tests must not assume a core per actor.** CI runners have four. A workload
test that creates five actors at the default `num_cpus=1` fails there for
reasons unrelated to the code under test. Use `num_cpus=0.1` when testing shape
rather than resource accounting.

**There is no affinity API.** You cannot say "put this actor on the same node as
that one" or "share these GPUs with the trainer". Colocating a trainer and an
inference engine — standard in RL post-training — is not currently expressible.
See [roadmap](../05-project/roadmap.md).

**A single node is all you get today.** The resource table and placement are
multi-node capable and tested, but there is no head daemon or remote node agent,
so every placement lands locally.

## See also

- [scheduler.md](../04-internals/scheduler.md) — the algorithm and its tests
- [fault-tolerance.md](fault-tolerance.md) — what happens when a placement dies
- [native-frameworks.md](native-frameworks.md) — placement in the main line
