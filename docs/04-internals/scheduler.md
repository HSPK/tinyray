# Scheduler and supervision

## Purpose

How placement decisions are made, how the head keeps track, and how death is
detected and handled.

## The head is not on the data path

With no stateless tasks, there is no high-frequency scheduling. The head
participates when a worker is **created, looked up, or dies** — never when a
call is made or a result is fetched.

That is why it is single-threaded bookkeeping inside the driver process rather
than a service. There is no `tinyray start`, no daemon, and the head has no
throughput requirement worth engineering for.

The cost: the driver is a single point of failure, and there is no
`lifetime="detached"`. See [status](../05-project/status.md).

## Resource model

```rust
Resources {
    num_cpus: f64,
    num_gpus: f64,
    memory_bytes: u64,
    custom: HashMap<String, f64>,
}
```

Comparison uses `f64::EPSILON` tolerance, so `0.1 × 10` does not fail to fit a
whole CPU.

`memory_bytes` is bookkeeping. Nothing enforces it.

### GPU identity

GPUs are not a count; they are **identified devices**. A node tracks
`free_gpu_ids`, and an actor asking for two whole GPUs is assigned two specific
device indices, exported as `CUDA_VISIBLE_DEVICES`.

Necessary because NCCL cares which physical device a rank holds. A count would
allow two ranks on the same device, which deadlocks rather than failing.

### The whole-number rule

`num_gpus` may be 0, a fraction below 1, or a whole number at or above 1.
`1.5` is refused at placement time.

Below 1 means sharing a device — reasonable for hyperparameter trials. At or
above 1 means owning devices. "One and a half devices" has no meaning to NCCL,
and refusing early converts a runtime hang into an immediate error.

## Strategies

**`PACK`** — fill one node before using the next. The default, because a
collective on one node uses NVLink instead of the network.

**`SPREAD`** — distribute across nodes. For rollout actors, where the failure
domain matters more than bandwidth and 32 actors on one node contend on the same
PCIe bus.

Neither is a constraint solver. Both are greedy passes over nodes sorted by
current load.

## Gang placement

`create_actors`, `launch_workers` and `create_worker_group` place **atomically**.

Either all `count` members fit, or `PlacementFailed` is raised and **nothing is
reserved**.

Partial placement is worse than failure here. Half a collective group is not
half useful — it hangs at the first barrier waiting for ranks that were never
created, and the resources it holds make the retry fail too.

Implementation: compute the whole assignment against a trial copy of the
resource table, commit only if complete.

## Release is clamped

`release` clamps to the node's totals.

This looks like defensive programming and is not. Without the clamp, a double
release *invents* a GPU: available goes above total, and the next placement
succeeds against a device that does not exist. Then two processes get the same
device and NCCL deadlocks — a failure that looks nothing like its cause.

Losing a GPU to a missed release is recoverable. Inventing one is not.

## Supervision

A pass every `supervise_interval` (1 s):

1. Reap nodes silent longer than `heartbeat_timeout` (30 s)
2. Check managed process liveness
3. Restart or fail actors whose processes died

Detection bounds:

| Failure | Detected within |
|---|---|
| Actor process death | ~1 s |
| Managed process death | ~1 s |
| Node death | ~31 s |

### Heartbeats

`LocalNodeAgent` sends one on a timer.

It originally did not. Nothing sent a heartbeat at all, so after 30 s every node
was declared dead and every actor reaped. Every test passed, because no test ran
for 30 s.

Two things came from that. The timing constants became environment-overridable
so a test can reach the deadline in seconds. And the test that should have
caught it — `assert dead_nodes() == []` — turned out to be a false positive:
it is true when the node is healthy *and* when it has already been reaped. It
was the only survivor of the first mutation run. See [testing.md](testing.md).

### Restart

On actor death with `max_restarts` remaining:

1. Release the old resources (clamped)
2. Place again
3. Start a new process
4. **Replay `__init__` with the original arguments**
5. Update the endpoint in the registry

Step 4 is the one that was missing. A restarted actor came back with no
constructor state — an empty model, silently. The handle stayed valid, calls
succeeded, and the results were wrong.

`ActorHandle.endpoint` is looked up rather than cached, so a handle held across
a restart addresses the new process.

**In-flight calls are lost.** They fail with `ActorDied` and are not replayed:
tinyray cannot know whether the method was idempotent.

### Worker groups do not restart

`launch_workers` has no `max_restarts`. Restarting one rank of a collective
without rebuilding the communicator leaves the other ranks blocked in a
barrier forever — a hang, not an error.

Detection works; recovery is the caller's. See
[fault-tolerance](../02-guides/fault-tolerance.md).

### Process trees

Managed processes are started with `start_new_session=True` and stopped with
`killpg`.

Without it, killing `torchrun` leaves its workers running. They hold GPU memory,
so the next placement succeeds against a device that is actually full, and the
job fails with OOM somewhere unrelated. The same is true of SGLang, which forks
a scheduler and a detokeniser.

## Pitfalls

**Node death takes 31 s to notice.** Deliberate: a shorter timeout reaps a node
that was merely paged out.

**There is no rescheduling on failed placement.** `PlacementFailed` is raised;
tinyray does not queue and wait.

**`num_cpus` is advisory.** Nothing constrains a worker to its allocation.

**Placement is greedy.** It can fail on a fragmented cluster where a smarter
packing would have fitted.

## See also

- [placement.md](../02-guides/placement.md) — the user-facing view
- [fault-tolerance.md](../02-guides/fault-tolerance.md) — what to do about failure
- [configuration.md](../03-reference/configuration.md) — the timing constants
