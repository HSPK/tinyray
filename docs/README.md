# tinyray

> Proposal; not the current implementation. tinyray is the generic control-plane
> fabric for clusters it does not own: identity, membership, reconciliation and
> discovery. It allocates nothing, launches nothing, and never touches a tensor.

| Attribute | Value |
|---|---|
| Status | Proposal. Supersedes all prior tinyray design |
| Target scale | 10,000+ GPU, a single experiment or a shared cluster |
| Position | L2 in the [layering](02-architecture/01-layering.md) — between the scheduler and the application |
| Written against | [rl-bridge cell runtime proposal](../../rl-bridge/docs/08-proposals/02-cell-based-high-availability-runtime.md) |
| Language | English body text; identifiers keep their source spelling |

## Why this document exists

The previous tinyray design was wrong in a way that could not be patched. It
assumed tinyray starts the processes, assigns the GPUs and sits at the centre of
every message. All three collapse above a few hundred workers, and the
measurements are in [01-overview/01-problem.md](01-overview/01-problem.md).

The redesign moves tinyray to the one layer nobody supplies: the mechanics that
every large control plane re-implements by hand — logical slots with
generations, leases that expire, desired state that converges, discovery that
does not grow with the cluster.

## Reading order

Directories and files are numbered. The numbers are the reading order, and no
document depends on a later one.

**Start here:** [01-overview](01-overview/) then
[02-architecture](02-architecture/). That is the proposal proper, and about
thirty minutes.

**If you are evaluating the boundary**, read
[01-overview/02-positioning.md](01-overview/02-positioning.md) and
[02-architecture/01-layering.md](02-architecture/01-layering.md) alone. They
contain the whole argument about what tinyray owns.

**If you are implementing**, [03-modules](03-modules/) is the specification and
[04-protocols](04-protocols/) is the wire contract.

## Authoritative locations

One fact is defined completely in one place. Everything else links to it.

| Information | Location |
|---|---|
| Why the design is what it is | [02-architecture](02-architecture/) |
| A module's responsibility and internal state | [03-modules](03-modules/) |
| Cross-process message order and schema | [04-protocols](04-protocols/) |
| Running and debugging | [05-operations](05-operations/) |
| Test evidence | [06-testing](06-testing/) |
| Exhaustive lists: API, config, metrics | [07-reference](07-reference/) |
| What is built, decided, and planned | [08-project](08-project/) |

## Contents

### 00 Conventions

- [00-conventions.md](00-conventions.md) — document structure and writing rules

### 01 Overview

- [01-problem.md](01-overview/01-problem.md) — what broke, with measurements
- [02-positioning.md](01-overview/02-positioning.md) — tinyray is L2, and why that is the valuable layer
- [03-principles.md](01-overview/03-principles.md) — seven principles, each traced to a failure

### 02 Architecture

- [01-layering.md](02-architecture/01-layering.md) — L0 to L4, and the ownership boundary
- [02-topology.md](02-architecture/02-topology.md) — worker, cell, global
- [03-state-model.md](02-architecture/03-state-model.md) — what needs consensus and what does not
- [04-planes.md](02-architecture/04-planes.md) — control plane, data plane, and the rule between them

### 03 Modules

- [01-identity.md](03-modules/01-identity.md) — slots, incarnations, fencing
- [02-membership.md](03-modules/02-membership.md) — hierarchical leases
- [03-reconciliation.md](03-modules/03-reconciliation.md) — desired and observed state
- [04-readiness.md](03-modules/04-readiness.md) — composable readiness
- [05-discovery.md](03-modules/05-discovery.md) — scoped lookup
- [06-admission.md](03-modules/06-admission.md) — backpressure primitives
- [07-transport.md](03-modules/07-transport.md) — the Rust core and the GIL boundary
- [08-supervision.md](03-modules/08-supervision.md) — node-local process supervision

### 04 Protocols

- [01-wire-format.md](04-protocols/01-wire-format.md) — framing
- [02-membership-protocol.md](04-protocols/02-membership-protocol.md) — register, heartbeat, expire
- [03-control-rpc.md](04-protocols/03-control-rpc.md) — calls, results, errors

### 05 Operations

- [01-deployment.md](05-operations/01-deployment.md) — deployment shapes
- [02-failure-model.md](05-operations/02-failure-model.md) — failure and recovery matrix
- [03-observability.md](05-operations/03-observability.md) — metrics and diagnosis

### 06 Testing

- [01-standard.md](06-testing/01-standard.md) — the testing standard and its origin
- [02-fake-cluster.md](06-testing/02-fake-cluster.md) — 10,000 to 100,000 simulated workers
- [03-chaos.md](06-testing/03-chaos.md) — fault injection matrix

### 07 Reference

- [01-api.md](07-reference/01-api.md) — the Python API
- [02-configuration.md](07-reference/02-configuration.md) — every knob and default

### 08 Project

- [01-status.md](08-project/01-status.md) — what exists, what is proposed
- [02-decisions.md](08-project/02-decisions.md) — decisions and reversals
- [03-roadmap.md](08-project/03-roadmap.md) — implementation phases

## Summary of the proposal

**tinyray owns**: logical identity with generations and fencing; hierarchical
lease membership; desired/observed reconciliation; composable readiness; scoped
discovery; admission and backpressure primitives; the control RPC transport;
node-local process supervision.

**tinyray does not own**: GPU or CPU allocation; job launching; any tensor;
consensus storage; and every domain concept — tasks, samples, model versions,
checkpoints belong to the application.

**The load-bearing claim**: a control plane at 10,000 workers fails from
quadratic relationships and hand-written liveness, not from slow code. Removing
both is a small, generic, testable library — and one that can be validated with
100,000 fake workers before a single GPU is booked.
