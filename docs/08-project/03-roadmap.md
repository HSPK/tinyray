# Roadmap

> Proposal; not the current implementation.

> Ordered by what each phase unblocks. Every phase is independently useful, and
> none requires the next to be worth doing.

## Phase 0 — Baseline and harness

**Why first.** Every number in this proposal is derived or extrapolated. The
harness that fixes that is also the cheapest thing to build, because a simulated
worker is just a worker without an application.

- Fake cluster harness, threaded and async modes
- Chaos harness with a recorded injection timeline
- Establish the "flat in worker count" assertions:
  consensus writes, lookup bytes, summary bytes, metric cardinality
- Collect real distributions: control latency, churn rate, failure rate

**Exit criterion.** 100,000 simulated workers in steady state, with the four
flat metrics asserted rather than plotted.

## Phase 1 — Identity and fencing

- `Slot`, `Incarnation`, fencing tokens
- Enforcement in the transport, not at call sites
- Supersession reporting and the callback
- Chaos: restart while the old process is alive and still writing

**Exit criterion.** Split brain is fenced with both processes running.

## Phase 2 — Hierarchical membership

- Cell tier with worker leases terminating there
- Fixed-size cell summary
- Cell lease against consensus
- Readiness composition and publication
- Scoped discovery with version-based change detection

**Exit criterion.** At 10,000 simulated workers, consensus write rate is flat and
lookup size tracks scope rather than cluster size.

## Phase 3 — Reconciliation

- Consensus adapter over etcd
- Leadership with fencing tokens
- Desired/observed convergence loop
- Membership epochs for operations needing a fixed set
- Chaos: leader failover, old leader returning alive

**Exit criterion.** A leader can be killed repeatedly with cells continuing to
run and no stale write accepted.

## Phase 4 — Removal

Only after the replacement is proven, because deleting first leaves a period
where neither works.

- Placement, resource table, gang placement
- Actor launcher and prewarm pool
- Driver-side head and supervision loop
- `link()` roster push
- Worker-group abstraction
- Collective registry

**Derived**: roughly 3,100 lines removed.

**Exit criterion.** No public API accepts a resource quantity, asserted
structurally.

## Phase 5 — Production hardening

- Authentication, or a documented network-isolation requirement
- Push-based watch
- Log persistence beyond the ring buffer
- Wire format versioning

## Phase 6 — Real hardware

Everything simulation cannot cover:

- Multi-node, end to end
- Real GPU device assignment reporting
- A real framework: SGLang, vLLM or Megatron, unmodified
- NCCL behaviour under a cell rebuild

**Exit criterion.** A real job runs under this control plane with one line added
to its worker script.

## Not planned

| Item | Why |
|---|---|
| Data plane | L0's |
| Resource allocation | L1's |
| Task, sample or version semantics | L3's |
| A distributed object store | Out of scope, permanently |
| Elastic reshaping of a collective | The framework's |
| Cross-cluster federation | No use case |

## Sequencing note

Phase 0 before Phase 1 is the load-bearing choice. Building the mechanisms first
and measuring afterwards is how the previous design reached 0.2.1 before
discovering that its central operation was quadratic.
