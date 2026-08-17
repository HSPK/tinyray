# The problem

> Proposal; not the current implementation.

> A control plane at ten thousand workers fails from quadratic relationships and
> hand-written liveness, not from slow code. The previous tinyray design had
> both, and neither can be patched.

## 1. Scope

This document records why the previous tinyray design cannot reach the target
scale, using measurements from this repository. It is the input to
[02-positioning.md](02-positioning.md).

Every number below is labelled **measured**, **derived** or **to be measured**,
per [00-conventions.md §9](../00-conventions.md#9-numbers).

## 2. What the previous design assumed

Three assumptions, all made when the target was 32 actors on one machine:

| Assumption | Consequence |
|---|---|
| tinyray starts the processes | A launcher, a placement engine and a resource table |
| tinyray assigns the GPUs | `num_gpus` on every API, a device ledger, gang placement |
| The driver is at the centre | Every message relayed; `link()` pushes the whole roster |

At the original scale all three were reasonable. At ten thousand they are all
false: the job is started by Slurm, Kubernetes or `torchrun`, the GPUs were
allocated before tinyray was imported, and a single driver cannot be on the path
of a loop that runs across the cluster.

## 3. The roster push is quadratic

`link()` sent every member the endpoints of every member.

**Measured**, `tinyray.serde.serialize` on a two-group roster of `N` entries
(32-hex actor id, `host:port`, integer rank):

| N | Roster per push | Total pushed |
|---:|---:|---:|
| 128 | 4.2 KB | 0.001 GB |
| 1,024 | 34.0 KB | 0.035 GB |
| 8,192 | 277.7 KB | **2.275 GB** |

The push is O(N) calls carrying O(N) bytes each, from one process. At 8,192
workers a single introduction moves 2.3 GB out of the driver; at 10,000 it is
**derived** as 3.4 GB.

Nothing about this is fixable by making the driver faster. The roster itself is
the wrong shape: a worker is being told about 9,999 peers to reach the four it
needs.

## 4. Fan-out from one process is linear and serial

**Measured**, 16 real worker processes on one machine, warm connections:

| Operation | 16 workers | Per worker |
|---|---:|---:|
| `link()` | 3.7 ms | 233 µs |
| `run()` fan-out | 3.5 ms | 221 µs |

**Derived** at 10,000 workers: 2.3 s for an introduction, 2.2 s for every
broadcast. For a control operation that is survivable once and fatal per
iteration.

## 5. Liveness cannot come from a supervisor that did not start anything

The previous design detected death by supervising child processes. When the
launcher is Slurm or Kubernetes, tinyray has no children, so this mechanism does
not merely scale badly — it does not exist.

The replacement must be a lease: the worker asserts its own liveness, and
absence is the signal.

**Measured**, single registry replica, a single sequential client:

| Operation | Throughput |
|---|---:|
| `register` | 4,168 ops/s |
| `heartbeat` | 4,295 ops/s |
| `lookup` (8 ranks) | 1,933 ops/s |

**Derived**: 10,000 workers at a 10 s heartbeat is 1,000 ops/s, so one replica
holds 4.3x headroom. The measurement is a lower bound because the client was
sequential; the server was not saturated.

## 6. Leases must not go to a consensus store

The natural instinct is to put every worker's lease in etcd. Public data says
otherwise:

| Fact | Source |
|---|---|
| Kubernetes officially supports up to 5,000 nodes | [Considerations for large clusters](https://kubernetes.io/docs/setup/best-practices/cluster-large/) |
| Node leases refresh every 10 s and are a documented source of etcd pressure | same |
| Every lease renewal commits through Raft, bounded by the slowest member's disk | [etcd performance](https://etcd.io/docs/v3.4/op-guide/performance/) |
| Standard mitigation is a longer interval or a separate etcd | same |

Ten thousand worker leases is twice the entire node budget of a supported
Kubernetes cluster, on top of whatever that cluster is already doing. Leases
must therefore be **hierarchical**: workers lease against their cell, cells
lease against consensus. Consensus then sees O(cells), not O(workers). See
[02-architecture/02-topology.md](../02-architecture/02-topology.md).

## 7. Failure is continuous at this scale

**To be measured** on the target cluster, but the public data is unambiguous:

| System | Scale | Reliability |
|---|---:|---|
| Llama 3 405B | 16,384 H100 | 419 unplanned interruptions in 54 days — **derived** 3.09 h mean interval; 78% hardware |
| Meta research clusters | 16,384 GPU | Predicted MTTF **1.8 hours** |
| MegaScale | 12,288 GPU | Over 100 automatic repairs in one production run |

Sources: [Llama 3](https://arxiv.org/abs/2407.21783),
[Reliability in large-scale ML clusters](https://arxiv.org/abs/2410.21680),
[MegaScale](https://arxiv.org/abs/2402.15627).

A control plane whose recovery unit is "the job" will spend most of its life
recovering. The unit must be smaller than the job, and the design must assume a
failure is in progress at all times.

## 8. Global operations degrade superlinearly in success probability

**Derived**, for a control operation repeated `n` times where each attempt's
final success probability after retries is `p`:

| p | n = 5,000,000 all succeed |
|---:|---:|
| 99.999% | 1.9e-22 |
| 99.9999% | 0.67% |
| 99.99999% | 60.7% |
| 99.999999% | 95.1% |

Any design that requires every member to complete an operation, repeated per
iteration, needs a per-operation reliability that no distributed system
achieves. The conclusion is not "retry harder"; it is that **no global operation
may require all members**.

## 9. What follows

| Broken assumption | Replacement |
|---|---|
| Full roster push | Scoped lookup, bounded by the request — [03-modules/05-discovery.md](../03-modules/05-discovery.md) |
| Driver fan-out | Hierarchy; the global tier addresses cells — [02-architecture/02-topology.md](../02-architecture/02-topology.md) |
| Supervision by parenthood | Leases — [03-modules/02-membership.md](../03-modules/02-membership.md) |
| Leases in consensus | Hierarchical leases — [02-architecture/03-state-model.md](../02-architecture/03-state-model.md) |
| Job-sized failure unit | Cell-sized — [05-operations/02-failure-model.md](../05-operations/02-failure-model.md) |
| All-members operations | Quorum of healthy membership — [03-modules/03-reconciliation.md](../03-modules/03-reconciliation.md) |
| tinyray allocates and launches | It does neither — [02-positioning.md](02-positioning.md) |

## 10. Limitations of this analysis

- The fan-out and registry throughput figures are single-machine, loopback, and
  one client. Real networks add latency and real clusters add concurrency; the
  first makes these numbers optimistic, the second makes them pessimistic.
- No figure here was taken on the target cluster. The list of what must be
  measured before the design is frozen is in
  [06-testing/02-fake-cluster.md](../06-testing/02-fake-cluster.md).
- The reliability data is from pre-training runs. RL adds inference engines and
  sandboxes, so the failure rate should be treated as a floor.

## 11. Source mapping

Measurements were produced against the current implementation:

- `python/tinyray/mesh.py` — the roster push being replaced
- `python/tinyray/registry.py`, `python/tinyray/cluster.py` — the lease prototype
- `benchmarks/` — the fan-out harness
