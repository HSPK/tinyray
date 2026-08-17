# Tradeoffs

## Purpose

Every significant choice, what it buys and what it costs. A tradeoff recorded
without its cost is marketing.

## HTTP for the control plane

**Chosen because** every message is inspectable with `curl` and `tcpdump`, there
is no protobuf toolchain, and the libraries are mature.

**Cost:** roughly 200 µs per round trip against gRPC's 60–100 µs.

**Why that is acceptable:** an ML call takes ~200 ms, so the overhead is 0.1%.
Measured p50 for submit-plus-fetch against a real listener is 198 µs, p99 295 µs.

**When it would not be:** a workload with sub-millisecond calls. tinyray is not
for that, and no amount of protocol tuning would change the answer.

Explicitly rejected: raw sockets, a bespoke binary protocol, HTTP/2. All would
save microseconds that do not matter. **The Rust core exists to escape the GIL,
not to save microseconds** — do not read it as licence to micro-optimise.

## No distributed object store

**Chosen because** plasma, spilling, distributed reference counting and lineage
reconstruction are the most complex parts of Ray, and a framework-owned workload
needs none of them.

**Cost:** a result lives only in the actor that produced it. If that actor
restarts, every reference into it becomes `ObjectLost`. There is no
reconstruction and no spilling; when the store hits its watermark, results are
evicted and consumers are told.

**What was kept:** the property that actually matters — `rollout → learner` data
never passes through the driver. An `ObjectRef` names `(task_id, owner_endpoint)`,
so the consumer fetches from the producer directly.

**Why it is demoted:** when Megatron or SGLang owns the data, this machinery is
nearly unused. It remains useful for pure-Python rollouts.

## Rust core with a Python API

**Chosen because** an actor running a 200 ms training step holds the GIL. If the
data path were Python, it could not serve result fetches during that window, and
32 actors fetching from each other would degrade to serial execution.

**Measured**, decoding 10 MB with four GIL-bound Python threads running:

| Initiated from | Idle | Contended | Slowdown |
|---|---|---|---|
| A native thread (the real serving path) | 0.37 ms | 0.38 ms | **1.04x** |
| Python | 0.75 ms | 36.7 ms | **49x** |

**The lesson is not "Rust is fast".** Both rows are mostly Rust. The second one
is slow because *Python initiated it*, so it queues for the GIL on the way in
and out regardless of how little interpreter work it does. Hence the
architectural rule: the serving path must be driven by tokio, never by a
Python-side loop.

**Cost:** roughly 2–3x the development effort, a Rust toolchain for
contributors, and a language boundary where mistakes are memory-unsafe rather
than merely wrong. That boundary is confined to
`crates/tinyray-py/src/buffers.rs`, which is the only place `unsafe` appears.

## Copy on the way in, share on the way out

**Python → Rust copies once.** `.remote()` does not block, so the send happens
after the call returns and the caller is free to mutate the array it just
passed. Borrowing that memory would be a data race.

**Rust → Python does not copy.** Results are immutable `Bytes` exposed through
the buffer protocol, so `pickle.loads(..., buffers=...)` builds numpy arrays
that view Rust memory directly.

**Cost:** one 10 MB memcpy per call argument, well under a millisecond against a
200 ms task. The original design specified zero copy in both directions; it was
abandoned because the saving is under 0.5% and the price is use-after-free in
the hot path.

**Consequence you will notice:** arrays returned by `get()` are **read-only**.
One buffer may be served to many consumers, so letting any of them write would
corrupt the rest. Call `.copy()` if you need to mutate.

## NCCL for weights, never a bespoke broadcast

**Chosen because** NCCL is a solved problem and tinyray has nothing to add. It
supplies only what NCCL leaves to the caller: rank assignment, rendezvous and
what to do when a member dies.

**Cost, and it is a real one:** a collective is a barrier. `wait(num_returns=24)`
lets you drop the *results* of eight slow rollouts, but all 32 ranks must still
attend the next broadcast. Skipping a straggler's participation hangs the group,
so the barrier reintroduces exactly the latency you avoided.

**Second cost:** a NCCL communicator is not fault tolerant. One dead rank makes
every subsequent collective hang. Groups therefore carry an epoch and are
rebuilt on any membership change — a seconds-scale operation, which is why
groups must be long-lived and never rebuilt per iteration.

**Third cost:** NCCL is GPU-only and two ranks sharing a device deadlock, so
every group member must own at least one whole GPU. CPU-only rollout actors
cannot join.

## Actors only, no stateless tasks

**Chosen because** ML workers hold models and CUDA contexts. A stateless task
model would mean reloading them.

**Cost:** no dynamic task graphs, no `ray.put`, no automatic parallelism for
embarrassingly parallel work. [`ActorPool`](../02-guides/actors.md#actorpool)
covers the common case.

**Bonus:** no tasks means no high-frequency scheduling, so the head is a
single-threaded piece of bookkeeping rather than a distributed scheduler.

## Version-specific wheels, no abi3

**Chosen because** the limited API omits `Py_buffer` on older interpreters, and
the buffer protocol is exactly how results reach numpy without a copy.

**Cost:** a wheel per Python version — 3.9 through 3.13, times three platforms.
Fifteen build jobs instead of three.

## Synchronous driver API

**Chosen because** it matches Ray's shape, which is the vocabulary the target
users have. `.remote()` returns immediately; `get()` blocks in Rust with the GIL
released.

**Cost:** no `async def` actors and `max_concurrency > 1` is unimplemented, so
the executor is strictly single-threaded. For an async-first engine like SGLang
this would serialise requests and destroy its throughput — which is why SGLang
belongs behind [`launch_process`](../02-guides/native-frameworks.md#supervising-a-server)
as its own server, not inside an actor.

## Two round trips for a small result

`get(f.remote())` costs a submit and a fetch. The protocol carries a
`want_inline` flag for returning small results with the acknowledgement, but the
actor always answers `inline: false`.

**Why:** the acknowledgement happens when the call is *queued*, before the method
has run. The result does not exist yet.

**Cost:** small calls pay ~400 µs instead of ~200 µs. Closing the gap needs an
optional blocking submit, which is not implemented.

## See also

- [positioning.md](positioning.md) — the stance these choices serve
- [decisions.md](../05-project/decisions.md) — the decision log, including reversals
- [testing.md](../04-internals/testing.md) — how these claims are kept honest
