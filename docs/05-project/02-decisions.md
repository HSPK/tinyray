# Decisions

## Purpose

The choices that shaped tinyray, each with the reasoning and the cost. Reversals
are included — a design that never changed its mind was not tested against
reality.

Format: what was decided, why, what it costs.

---

## Scope

### Actors only, no tasks

**Why.** Every workload in view — RL rollouts, hyperparameter trials, a training
loop — is stateful. An actor holds a model; a task would reload it. Dropping
tasks removes the dynamic task graph, the global scheduler, lineage
re-execution and distributed reference counting: the majority of Ray's
complexity, for none of the target use cases.

**Cost.** No `ray.remote` on a function. Map-style work needs `ActorPool`.

### HTTP only

**Why.** Debuggable with `curl`, works through any network, and hyper is
excellent. gRPC would add a protobuf toolchain and a code generator for point-
to-point traffic that does not need either.

**Cost.** HTTP/1.1 head-of-line blocking, mitigated with four connections per
peer. No streaming.

### No object store

**Why.** A result lives at the actor that produced it, and consumers fetch from
the owner. A central store would add a copy in each direction and a service to
keep alive.

**Cost.** No `ray.put`. A result dies with its producer. Memory is bounded by
watermark and TTL rather than by reference counting, so a result can be evicted
while a reference is live — hence `ObjectLost` as a distinct error.

### Control plane only

**Why.** The decisive reframing. Megatron, SGLang, vLLM and torchrun each have
years of work in their data paths. Replacing them is not the goal; coordinating
them is. tinyray moves kilobytes of control traffic while the frameworks move
gigabytes their own way.

**Cost.** tinyray cannot optimise anything inside a framework. It offers no
tensor transport of its own.

---

## The six principles

Each was written after a specific failure.

**1. The control plane never moves tensors.**
After `wait()` was found fetching 200 MB to answer a boolean.

**2. Never claim process-exclusive resources.**
`tinyray.collective` claimed the default process group, so Megatron could not
initialise its own.

**3. Present the `torchrun` interface, do not invent one.**
`RANK`, `WORLD_SIZE`, `MASTER_ADDR` — anything else means every framework needs
an adapter.

**4. Supervise arbitrary processes, not only tinyray actors.**
SGLang is a server, not a class. `launch_process` came from this.

**4b. Minimise intrusion, in three levels.** From
[positioning](../01-concepts/01-positioning.md).

**5. Group operations are collective-safe by default.**
After the same deadlock appeared three times.

**6. Failures must be explicit, never a hang.**
A hung distributed job gives no information at all.

---

## Implementation

### Rust core

**Why.** One measurement: decoding 10 MB under GIL contention costs 1.04x from a
native thread and 49x from a Python thread. The serving path must be
tokio-driven.

**Cost.** 2-3x the development time, a Rust toolchain for contributors, a wheel
matrix in CI. Confined to byte-moving and concurrency; everything semantic
stayed in Python.

### Rust does not link CUDA

**Why.** Avoids the single worst class of build problem. NCCL is reached through
PyTorch.

**Cost.** Collectives cannot be driven from Rust.

### Per-caller ordering, not global

**Why.** Matches Ray, and one slow caller must not block others.

**Cost.** No total order across callers. Two callers' interleaving is
unspecified.

### Only backpressure is retried

**Why.** It is the only failure where resending the identical request is safe.
A user exception, a lost object and a dead actor are facts about state.

**Cost.** No `max_task_retries`. Application-level retry is the caller's.

### Whole GPUs at or above 1

**Why.** "One and a half devices" has no meaning to NCCL. Refusing at placement
converts a deadlock into an error message.

**Cost.** No `num_gpus=1.5`.

### Gang placement is atomic

**Why.** Half a collective group hangs at the first barrier and holds resources
that make the retry fail too.

**Cost.** A large group fails outright on a fragmented cluster.

### `release` clamps to node totals

**Why.** A double release without a clamp *invents* a GPU, and the next
placement succeeds against a device that does not exist. Losing a GPU is
recoverable; inventing one is not.

**Cost.** A genuine accounting bug is masked rather than reported.

---

## Reversals

The ones that were wrong first.

### Bidirectional zero-copy → copy on the way in

**Originally:** zero-copy in both directions.

**Reversed because** `.remote()` returns immediately and the caller may mutate
the array on the next line. Borrowing would make the bytes on the wire depend on
when the transport happened to read them.

**Now:** Python → Rust copies once at submit; Rust → Python borrows. Results are
read-only.

### abi3 → one wheel per Python version

**Originally:** abi3, for one wheel per platform.

**Reversed because** the limited API does not expose `Py_buffer` — which is the
zero-copy mechanism Rust was introduced for.

**Now:** a 15-job matrix. Also switched macOS to universal2 after macos-13 Intel
runners queued for 29 minutes.

### tinyray owns the process → tinyray supervises the process

**Originally:** actor classes, Ray-style, tinyray owning the interpreter.

**Reversed because** every native framework already owns its process. An actor
class means rewriting a Megatron entry point as a class, which nobody will do.

**Now:** three intrusion levels, the lightest being one line inside an
unmodified script.

### Serial construction → `construct_all`

**Originally:** actors constructed one at a time.

**Reversed because** a framework that rendezvous in `__init__` deadlocks: rank 0
blocks waiting for rank 1, which has not been created. The same pattern appeared
three times, and is now principle 5.

**Now:** all members are dispatched before any is awaited.

### Star topology → peer mesh

**Originally:** the driver at the centre, workers as leaves, every message
relayed through the middle. Workers had no way to address each other, and none
of the API admitted that they might want to.

**Reversed because** that is the shape of a *fan-out*, not a *pipeline*. It fits
32 rollouts and a learner with one loop in the driver. It does not fit a
dataloader fleet feeding a trainer fleet, where the driver has nothing to
contribute between steps and routing through it makes the controller the
bottleneck at exactly the point where it adds no value.

The assumption came from the original scope — "32 actors, one learner, the
driver runs the loop" — and was never revisited when the project repositioned
as a control plane for native frameworks. Every real disaggregated stack is a
pipeline.

Three symptoms, one cause:

| Symptom | Detail |
|---|---|
| `get_actor(name)` failed inside a worker | The registry lives in the driver's head, and a worker has no client to it |
| Handles could not be pickled | They hold a `Context`, so a peer reference could not be sent anywhere |
| `connect(endpoint)` "worked" | By falling through to `init()` and building a **second head** inside the worker — a phantom cluster with its own supervision loop, believing it owned the machine |

The third is the telling one. Peer-to-peer was never designed; it only appeared
to function because a driver would silently bootstrap itself inside any worker
that tried.

**Now:** `tr.link(...)` pushes a roster after startup, `tinyray.peers(group)`
resolves it, handles are picklable, and a worker uses a client-only
`PeerContext` with no head at all. Measured on the dataloader example: **868
bytes** through the driver for an entire training loop that moved 13.6 MB
between sidecars.

**Cost.** One more concept, and a roster that is a snapshot rather than a live
view — a worker that restarts after `link` is stale until you link again.

### Independent processes → process groups

**Originally:** plain `subprocess`.

**Reversed because** killing `torchrun` left its workers alive holding GPU
memory, so the next placement succeeded against a full device.

**Now:** `start_new_session=True` plus `killpg`.

---

## Deletions

### `shm.rs`

A same-host shared-memory fast path. 11 passing unit tests. Never called by
transport or actor. Deleted.

It is the clearest example of blind spot 2 in
[testing](../04-internals/05-testing.md#2-dead-code-with-passing-unit-tests):
coverage of a module says nothing about whether anything uses it. The cost of
keeping it — a maintained, tested, unreachable code path — exceeded the cost of
losing it.

Same-host 10 MB transfers still use loopback: ~9 ms against a design target
of <5 ms.

### `DESIGN.md`

The original design document, written in Chinese and never part of the
repository. Its content is now dissolved into `docs/`, and it is no longer
maintained.

Kept out of the repository rather than kept alongside these pages, because two
overlapping documents diverge immediately — and the one nobody reads is the one
that goes stale.

---

## See also

- [01-positioning.md](../01-concepts/01-positioning.md) — the stance in full
- [03-tradeoffs.md](../01-concepts/03-tradeoffs.md) — each choice with its measured cost
- [01-status.md](01-status.md) — what these decisions left unbuilt
