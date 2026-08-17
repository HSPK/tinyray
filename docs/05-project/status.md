# Status

## Purpose

An honest inventory: what works, what is missing, and what has never been run on
real hardware. Written so a reader can decide whether tinyray fits before
finding out the hard way.

Current release: **0.2.1**.

## Works

| Area | State |
|---|---|
| Actors: create, call, restart, name, kill | complete |
| Ordered per-caller dispatch | complete |
| Backpressure with automatic retry | complete |
| Hybrid `ObjectRef`, fetch from owner | complete, see [inline](#inline-results) |
| Result store: LRU, TTL, tombstones | complete |
| `get` / `wait` / `release` | complete |
| Placement: CPU, GPU by device id, PACK/SPREAD | complete |
| Gang placement, all-or-nothing | complete |
| Managed processes with readiness detection | complete |
| Process-group cleanup | complete |
| `launch_workers` / `serve` / `connect` | complete |
| `torchrun` environment injection | complete |
| Supervision, heartbeats, restart with `__init__` replay | complete |
| Prewarm pool | complete |
| `/introspect`, `tinyray status`, byte counters | complete |
| Type stubs, `py.typed`, mypy clean | complete |

## Not implemented

| Feature | Why it matters | Notes |
|---|---|---|
| **Multi-node deployment** | tinyray is single-node in practice | The state machine — resource table, placement, heartbeats, failure — is implemented and tested. What is missing is packaging it as a daemon: there is no `tinyray start --head`, no node-agent executable, and `NodeHandle.agent` is always a local object |
| **`lifetime="detached"`** | Actors die with the driver | Explicitly refused with a reason rather than silently ignored. Needs a standalone head to be meaningful |
| **`max_concurrency > 1`** | One method at a time per actor | The executor is single-threaded. Concurrent actors would need a different GIL story |
| **`max_task_retries`** | Only backpressure retries | Deliberate: replaying a stateful call is unsafe without idempotence the framework cannot know about |
| **Worker group restart** | A dead rank stops the group | Detection works; recovery is the caller's. Restarting one rank without rebuilding the communicator hangs the rest |
| **Automatic collective rebuild** | After a rank dies | The epoch state machine exists; nothing drives it automatically |
| **`/metrics` (Prometheus)** | No standard scraping | `/introspect` carries the same data in custom JSON |
| **Log aggregation** | Logs are per process | The driver prefixes forwarded output with `[name:pid]`; there is no central collection |
| **Adaptive compression** | Bandwidth on large results | Never implemented |
| **`run_id` / seed propagation** | Reproducibility | Never implemented |
| **Same-host fast path** | 10 MB same-host still goes over loopback | `shm.rs` was written, had 11 passing tests, was never wired in, and was deleted. See [decisions](decisions.md#deletions) |

### Inline results

The protocol has `want_inline` and `inline` fields, and the actor **always
answers `inline: false`**.

The acknowledgement is sent when a call is queued, not when it completes, so at
that moment the result does not exist. Honouring the threshold would mean
holding the acknowledgement until the method returns — which makes `.remote()`
blocking.

Effect: `get(f.remote())` costs **two round trips** (submit, then fetch), not
one. For a small result on loopback that is tens of microseconds. Getting to one
would need an optional blocking submit.

## Never run on real hardware

The most important section. Everything below is implemented, has tests, and has
**never executed against the thing it targets**.

| Feature | What is actually verified |
|---|---|
| **NCCL collectives** | Admission rules, the epoch state machine and barrier semantics — 22 tests. `init_process_group` and `broadcast` have never been called on a GPU |
| **GPU placement, `CUDA_VISIBLE_DEVICES`** | The assignment logic. Never run on a real multi-GPU machine |
| **Cross-node fetch** | The code path is identical to same-node (both HTTP), but no second machine has been involved |
| **SGLang, vLLM, Megatron** | Stand-in scripts with the same launch shape, readiness signature and rendezvous behaviour. The real frameworks have never been launched |

Collective tests run against **gloo** on CPU, which exercises the group
management but not NCCL itself.

## Non-goals

Not oversights. These will not be added.

- Distributed object store, plasma, spilling, global reference counting,
  lineage re-execution
- Stateless tasks, `ray.put`, dynamic task graphs
- **Any self-written broadcast or collective transport.** Weights go through
  NCCL; tinyray does rendezvous only
- `allreduce` / `allgather` / gradient synchronisation / DDP integration
- CPU collectives as a supported path
- Autoscaling, multi-tenancy, cross-cloud
- Anything resembling Ray Data, Tune or Serve
- Strict exactly-once semantics

## Known risks

| Risk | Mitigation |
|---|---|
| A collective barrier reintroduces straggler latency — you can discard a slow actor's *result* but not let it miss the broadcast | `run(timeout=)` plus group rebuild, or asynchronous off-policy weight updates |
| NCCL is not fault tolerant: one dead rank hangs the group | Epoch state machine, `NCCL_ASYNC_ERROR_HANDLING=1`, `pg._abort()`. Groups must be long-lived, not rebuilt per round |
| PyO3 buffer lifetimes: a wrong guard is a use-after-free | All `unsafe` confined to `buffers.rs`, 188 lines |
| A collective blocking the actor's serving path | Rust data plane is decoupled from Python; collectives run on a separate thread |
| `LocalStore` exhausting memory | Byte watermark, LRU, TTL, 429 backpressure |
| Prewarmed processes and CUDA: an initialised process cannot change `CUDA_VISIBLE_DEVICES` | Prewarmed processes import torch but never touch CUDA; pools are keyed by device assignment |

## Fit

**Good fit:** single node, 8-64 workers, RL rollout loops, hyperparameter
sweeps, coordinating a native training stack, anything where you want processes
supervised without giving up `torchrun`.

**Bad fit:** multi-node production, jobs that must outlive the driver, anything
needing stateless tasks or a shared object store, anything where an untested
NCCL path is unacceptable.

## See also

- [decisions.md](decisions.md) — why, including the reversals
- [roadmap.md](roadmap.md) — what is next
- [testing.md](../04-internals/testing.md) — how the gaps were found
