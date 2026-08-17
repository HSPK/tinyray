# Design principles

> Proposal; not the current implementation.

> Seven principles. Each was written after something broke, and each is stated
> so that a review can cite it.

## 1. Scope

These principles govern every design decision in `03-modules/` and
`04-protocols/`. Where a module deviates, its "Limitations and trade-offs"
section must say which principle it breaks and why.

## 2. The principles

### P1 — The control plane never carries bulk data

Control messages are kilobytes. Anything larger belongs to L0.

**Origin.** `wait()` answered a readiness question by fetching the payload and
discarding it. **Measured**: 237 ms for a settled 200 MB result, against 0.14 ms
once it asked for status only. Every functional test passed, because the answer
was correct.

**Enforcement.** Every control operation has a byte budget asserted by a test,
and a meta-test requires that every wire-touching operation has one. See
[06-testing/01-standard.md](../06-testing/01-standard.md).

### P2 — Never claim a resource the process already owns

tinyray does not allocate GPUs, does not take the default process group, does
not set `CUDA_VISIBLE_DEVICES`, and does not decide where anything runs. It
reads what the launcher assigned and reports it.

**Origin.** An earlier collective module claimed the default process group,
which meant Megatron could not initialise its own.

**Consequence.** `num_gpus`, `cpus_per_worker` and placement leave the API
entirely. Two ledgers for one resource is one ledger too many.

### P3 — Present the launcher's interface; do not invent one

Rank, world size and local rank come from `RANK`, `SLURM_PROCID`,
`OMPI_COMM_WORLD_RANK` — whatever the launcher set. tinyray adds no numbering
scheme of its own.

**Origin.** Any framework integration begins by asking "what is my rank?", and
a second answer to that question is where integrations break.

### P4 — Every identity carries a generation, and receivers fence

A logical name identifies a slot. An incarnation identifies the process
currently filling it. Every cross-process write carries its incarnation, and the
receiver rejects a stale one.

**Origin.** Without it, a restarted rank and its predecessor both heartbeat the
same lease, alternately resurrecting a dead address.

**Enforcement.** Fencing is applied by the transport, not by each caller. A
mechanism that fifteen call sites must remember is a mechanism that fourteen of
them will.

### P5 — No operation may require every member

Global operations act on healthy membership at a frozen epoch, with an explicit
minimum. One unreachable worker must not block the rest.

**Origin.** **Derived**: five million control operations complete only 0.67% of
the time even at 99.9999% per-operation success. The arithmetic is in
[01-problem.md §8](01-problem.md#8-global-operations-degrade-superlinearly-in-success-probability).

**Consequence.** Degrade capacity, do not stop. 98% is a result; 0% is an
outage.

### P6 — Failures are explicit and bounded; never a hang

Every wait has a deadline. Every failure names what was expected, from whom, and
for how long. A distributed job that hangs yields no information at all.

**Origin.** Three separate deadlocks, all of the same shape: a group operation
awaited one member before dispatching to the others. Also, and repeatedly, a
method containing a collective invoked on one rank.

**Consequence.** Group operations dispatch to all members before awaiting any.
This applies to starting a group, not only to calling one.

### P7 — Prefer soft state; use consensus only where nothing can be rebuilt

State that its owner re-asserts on a timer needs no consensus: a replica that
loses everything is correct again one lease later. Consensus is reserved for
state with no owner to re-assert it — leadership, desired configuration,
ownership of a partition.

**Origin.** **Measured**: with every registry replica killed, workers continued
to address each other from cache and training was unaffected. A stale endpoint
is worth far more than a stopped job.

**Consequence.** Replication of membership needs no log, no leader and no
agreement. The full split is in
[02-architecture/03-state-model.md](../02-architecture/03-state-model.md).

## 3. Applying them

The principles conflict in two places, and the resolution is fixed here.

**P5 against strict synchronisation.** Some operations genuinely need every
participant — a NCCL communicator, for one. The resolution: P5 applies to
*membership*, not to *collectives*. Freeze the healthy membership into an epoch,
require all members of that epoch, and fence the rest out. A member that returns
joins the next epoch. tinyray provides the epoch; the collective belongs to the
framework.

**P7 against P4.** Fencing tokens must be monotonic, and a soft-state store
cannot guarantee monotonicity across a total loss. The resolution: incarnations
are derived from a source that survives — a consensus counter for cell-level
identity, and for worker-level identity a value that is monotonic per slot
without coordination (see
[03-modules/01-identity.md](../03-modules/01-identity.md)). Worker incarnations
never need global uniqueness, only per-slot ordering.

## 4. What these principles rule out

Stated so that the design cannot drift back:

- A driver on the path of every iteration.
- A roster whose size grows with the cluster.
- Liveness inferred from process parenthood.
- Per-worker leases in a consensus store.
- Any API that accepts a resource quantity.
- Any operation whose success requires all members.
- Retrying an operation that is not idempotent.

## 5. Limitations and trade-offs

- **P7 admits stale reads.** A worker may address an endpoint that has moved.
  Fencing (P4) makes that safe but not free: the call fails and is retried
  against a fresh lookup.
- **P5 admits partial progress.** A broadcast may reach 98% of members. The
  application must be able to say what that means, and tinyray cannot say it for
  them.
- **P2 gives up the ability to prevent conflicts.** With no ledger, tinyray
  cannot stop two processes claiming the same GPU. That protection moves to the
  scheduler, where it belongs and where it is stronger.

## 6. Source mapping

Each principle is asserted by a structural test in
`tests/test_suite_quality.py`; the mapping is in
[06-testing/01-standard.md](../06-testing/01-standard.md).
