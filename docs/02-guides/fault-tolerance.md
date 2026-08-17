# Fault tolerance

## Purpose

What happens when something dies: what restarts, what does not, and what you are
told. The governing rule is [principle 6](../01-concepts/positioning.md#6-fail-loudly-never-hang):
fail loudly, never hang.

## Failure taxonomy

Distinct exceptions, because collapsing them hides bugs.

| Exception | Means | Usually indicates |
|---|---|---|
| `UserCodeError` | Your method raised | Your bug. `remote_traceback` has the stack |
| `ObjectLost` | The result existed and is gone | Eviction, TTL, release, or an actor restart |
| `ActorDied` | The target is gone and will not return | Restart budget exhausted, or a crash |
| `NotFound` | No such actor, task or method | A tinyray bug, or a typo |
| `Backpressure` | Over the queue watermark | Retried automatically |
| `ProcessStartupError` | A managed process died or never became ready | Its log is attached |
| `PlacementFailed` | The cluster cannot host the request | The arithmetic is in the message |

`ObjectLost` versus `NotFound` is the distinction that matters most in practice.
The first says you were late; the second says something is broken.

## Actor restart

```python
@tr.remote(max_restarts=3)
class Trainer:
    def __init__(self, cfg):
        self.model = build_model(cfg)
```

On death within budget, tinyray:

1. restarts the process, keeping the same `actor_id`, so handles stay valid;
2. **replays the constructor** with the original arguments;
3. blocks calls until reconstruction finishes, so nothing overtakes `__init__`;
4. updates its routing to the new endpoint.

Constructor replay is necessary because a new process contains no user object.
The head deliberately knows nothing about user code, so the driver replays it.

What does **not** survive:

- **The result store.** Every reference into that actor becomes `ObjectLost`.
- **Sequence numbering.** It restarts at zero; the new process never ran the
  earlier calls.
- **Collective membership.** The group is marked `BROKEN` and is **not** rebuilt
  automatically.

Past the budget, the actor becomes `ActorDied` and its resources are returned.

### Calls are not retried

A stateful call is not idempotent, so replaying it can corrupt state. Only
backpressure is retried automatically, being the one failure where retrying the
identical request is safe. `max_task_retries` and `retry_exceptions` are not
implemented.

## Managed process restart

```python
server = tr.launch_process(
    ["python", "-m", "sglang.launch_server", "--port", "{port}"],
    name="rollout", num_gpus=4, ready_when="http:/health", max_restarts=3,
)
```

The supervisor reaps exits and restarts within budget, re-running the same
readiness check. Past the budget the process is removed and its GPUs returned.

**The whole process tree is stopped.** `torchrun` spawns a child per GPU and
SGLang spawns schedulers and workers; signalling only the parent would leave
those alive holding GPU memory, with another set stranded on every restart. The
process gets its own session and the group is signalled.

`launch_workers` does **not** accept `max_restarts` — see
[pitfalls](#pitfalls).

## Node failure

Node agents heartbeat every few seconds; the head declares a node dead after
`heartbeat_timeout` (30 s by default) and marks its actors `ActorDied`.

The interval is derived from the deadline rather than fixed, so it cannot drift
above it. A fixed interval larger than the timeout would declare every healthy
node dead — and the first version of that test passed anyway, because it
asserted `dead_nodes() == []` and the supervisor had already reaped the node.
Both readings are an empty list. The test now asserts end to end that an actor
still answers.

## Readiness

A process that is running is not a process that is ready. An inference server
binds its port minutes before it can answer.

| Check | Ready when |
|---|---|
| `"http:/health"` | The endpoint returns 200 or 204 |
| `"port"` | Something accepts connections |
| `"log:pattern"` | A matching line appears |
| `"alive"` | The process exists. Honest, weak |

Startup failure carries the child's output:

```
ProcessStartupError: rollout exited with code 1 before it was ready.
--- last output ---
torch.OutOfMemoryError: CUDA out of memory
```

## Backpressure

An actor accepts `max_pending_calls` (1000 by default) before answering `429`.
The client backs off and retries, so the visible effect is that everything still
completes — with the actor's memory bounded.

The result store has its own watermark: `store_max_bytes` (2 GiB) with LRU
eviction, and `store_ttl_seconds` (300 s). The newest result is never the
eviction victim, so a result larger than the whole store is still readable once.

## Shutdown

`tr.shutdown()` stops actors, workers and managed processes, including their
children, and is registered with `atexit`.

Clean shutdown depends on an implementation detail worth knowing: the executor
polls with a short timeout so it returns to the interpreter several times a
second. Python only runs signal handlers while the main thread executes
bytecode, so a thread parked in Rust indefinitely would ignore `SIGTERM` and
every shutdown would fall back to `SIGKILL` after the timeout. Measured before
the fix: 10.00 s for three actors. After: 0.24 s.

## Pitfalls

**Collective groups are not rebuilt automatically.** A restarted member leaves
the group `BROKEN`, and the next `run()` raises `GroupRebuilding`. You must call
`tinyray.collective.rebuild(group)`. For an RL loop this means fault tolerance
is not closed: one dead rank stops training until someone intervenes.

**`launch_workers` has no restart.** `launch_process` does;
the worker-group path does not. Even with it, a restarted rank would not rejoin
its process group.

**A `kill -9` on your driver leaves everything running.** `atexit` does not run.
There is no external reaper.

**`ObjectLost` after a restart is expected, not a bug.** The store went with the
process.

**Long-loading servers need a longer `startup_timeout`.** The default is 600
seconds and a large model can exceed it.

## See also

- [observability.md](observability.md) — diagnosing before it dies
- [placement.md](placement.md) — how resources come back
- [status.md](../05-project/status.md) — which of these gaps are open
