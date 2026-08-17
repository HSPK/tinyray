# Status

> Proposal; not the current implementation.

> An honest inventory: what exists today, what this proposal changes, and what
> has never been run.

## 1. Reusable from the current implementation

Proven, and carried forward unchanged or nearly so:

| Component | State | Evidence |
|---|---|---|
| Wire framing, limits, out-of-band frames | Complete | 117 Rust tests |
| Native serving path, no GIL | Complete | **Measured** 1.04x under contention against 49x |
| Per-caller ordering, duplicate absorption | Complete | Unit tested |
| Admission with explicit rejection | Complete | Unit tested |
| Result store: watermark, TTL, tombstones | Complete | Unit tested |
| Byte accounting per peer | Complete | `tests/test_driver_byte_budget.py` |
| Process-group supervision and cleanup | Complete | Integration tested |
| Readiness observation predicates | Complete | Integration tested |
| Registry with leases, prototype | Working | **Measured** 4,295 heartbeat/s |
| Multi-replica failover and cache fallback | Working | Chaos tested |
| Testing standard, mutation harness | Complete | 21/21 mutants caught |

## 2. To be built

| Component | Proposal | Depends on |
|---|---|---|
| `Slot` / `Incarnation` / fencing | [03-modules/01-identity.md](../03-modules/01-identity.md) | — |
| Fencing enforcement in the transport | same | Identity |
| Hierarchical membership, cell tier | [03-modules/02-membership.md](../03-modules/02-membership.md) | Identity |
| Cell summary aggregation | same | Membership |
| Reconciler and leadership adapter | [03-modules/03-reconciliation.md](../03-modules/03-reconciliation.md) | Consensus adapter |
| Consensus adapter over etcd | [02-architecture/03-state-model.md](../02-architecture/03-state-model.md) | — |
| Readiness composition and publication | [03-modules/04-readiness.md](../03-modules/04-readiness.md) | Membership |
| Scoped discovery with version watch | [03-modules/05-discovery.md](../03-modules/05-discovery.md) | Membership |
| Fake cluster harness | [06-testing/02-fake-cluster.md](../06-testing/02-fake-cluster.md) | Membership |
| Chaos harness | [06-testing/03-chaos.md](../06-testing/03-chaos.md) | — |

## 3. To be removed

Each removal deletes a capability that the layering says tinyray must not have.

| Removed | Lines, approx | Reason |
|---|---:|---|
| Placement and resource table | 800 Rust | The scheduler allocated before tinyray was imported |
| Gang placement | 200 Rust | tinyray places nothing |
| Actor launcher | 300 Python | The launcher starts the job |
| Driver-side head and supervision loop | 700 Python | Nothing supervises what it did not start |
| `link()` roster push | 200 Python | **Measured** 2.3 GB at 8,192 workers |
| Worker-group abstraction | 200 Python | `torchrun` owns this |
| Prewarm pool | 150 Python | Tied to tinyray starting processes |
| Collective registry | 550 Rust | Never ran on a GPU; the epoch concept moves to the reconciler |

**Derived** total: roughly 3,100 lines removed against roughly 1,500 added.

## 4. Never run against the real thing

The most important section. Everything below is designed and untested against
its target.

| Claim | Actually verified |
|---|---|
| **10,000 worker scale** | Nothing above 16 real workers. Every large number in these documents is derived or extrapolated |
| **Hierarchical membership** | The single-tier prototype only |
| **Consensus adapter** | Not written; no etcd has been contacted |
| **Cell controller failover** | Designed; not built |
| **NCCL interaction** | Never run on a GPU. The previous collective code was admission rules and a state machine, tested against gloo |
| **Real frameworks** | SGLang, vLLM and Megatron have never been launched. Stand-in scripts with the same launch shape were used |
| **Multi-node** | Never run end to end. Placement across nodes was unit tested; a second machine has never been involved |

The fake cluster exists to move the first three rows out of this table before
any GPU is booked.

## 5. Known gaps in the proposal itself

Recorded rather than discovered later:

| Gap | Impact |
|---|---|
| No authentication or encryption | Unsuitable for a shared cluster without network isolation |
| No push-based watch | Change latency is one poll interval |
| Admission bound is a count, not bytes or time | A thousand cheap and a thousand expensive calls occupy the same room |
| No per-producer fairness | A loud producer can consume the whole allowance |
| No log persistence | A 200-line ring per process; when it dies, that is what remains |
| Wire format unversioned | Mixed releases in one cluster are unsupported |
| Cell sizing has no default | An operator decision with no guidance beyond the forces |

## 6. Fit

**Good fit**: a cluster launched by Slurm, Kubernetes or `torchrun`, needing
membership, discovery, fencing and readiness across thousands of processes,
where the application owns its own semantics.

**Bad fit**: a job wanting to be placed and started for it; anything needing a
data plane; a shared cluster with untrusted tenants; anyone needing a supported
system today rather than a design.

## 7. Version

The current published package implements the previous design. This proposal is
not implemented. The migration is in
[03-roadmap.md](03-roadmap.md).
