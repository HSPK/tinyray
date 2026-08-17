# Roadmap

## Purpose

What is next, in the order it matters. Ordered by how much it unblocks, not by
how interesting it is.

No dates. This is a research tool.

---

## Next

### Verify on real GPUs

The largest gap. NCCL, GPU placement and `CUDA_VISIBLE_DEVICES` are implemented
and tested against their state machines, and **have never run on a GPU**.

Needed: a multi-GPU machine, a real `init_process_group`, a real `broadcast`,
and a rank killed mid-barrier to see whether the epoch machine behaves as
designed.

Until this happens, every collective claim is a claim about code, not about
behaviour.

### Real framework integration

Stand-in scripts reproduce the launch shape of SGLang, vLLM and Megatron. The
real ones have never been launched.

Expected to surface: readiness signatures that do not match, environment
variables not accounted for, shutdown paths that leave children behind.

### Multi-node

The state machine is done — resource table, placement, heartbeats, failure
detection, all tested. What is missing is packaging:

- `tinyray start --head` and a node-agent executable
- `NodeHandle.agent` as an HTTP proxy rather than always a local object
- `launch_workers` computing `LOCAL_RANK` across nodes instead of assuming one

The third is a real bug on any multi-node run, not merely an absence.

---

## After that

### Worker group restart

Detection works. Recovery does not: restarting one rank without rebuilding the
communicator leaves the others blocked in a barrier.

Needs: detect the death, abort the group on every surviving rank, restart the
dead one, rebuild at a new epoch. The state machine exists; nothing drives it.

### Automatic roster refresh

`link` pushes a snapshot. When a worker restarts it comes back on a new port,
and every peer holding the old endpoint is stale until the driver links again.

The driver already knows — it performed the restart. It should re-push without
being asked.

### Automatic collective rebuild

Follows from the above. Today `GroupRebuilding` is raised and the caller decides.

### Detached lifetime

Needs a head process that outlives the driver — which is the multi-node work.
Once there is a daemon, `lifetime="detached"` becomes possible rather than
refused.

### Log persistence

Output is forwarded to the driver with a `[name:pid]` prefix and a 200-line
ring buffer per process. When a process dies, the ring is what remains.

Wanted: per-process files, and `tinyray logs NAME`.

---

## Considered, not scheduled

### Inline results

Would remove one round trip from `get(f.remote())`. Needs an optional blocking
submit, which complicates the common path to save tens of microseconds on
loopback. Worth it only if a real workload is dominated by small-result latency.

### Same-host fast path

`shm.rs` was written and deleted. Same-host 10 MB still costs ~9 ms on loopback
against a design target of <5 ms.

Would only be reconsidered with a workload where same-host result transfer is
the bottleneck — and under the control-plane principle, large same-host
transfers should not be going through tinyray at all.

### Prometheus `/metrics`

`/introspect` has the same data. This is a format change, worth doing when
something needs to scrape it.

### Placement affinity

Colocating an actor with a specific process — a rollout actor on the node
running its inference server. Currently expressible only through `host=`.

### `max_concurrency > 1`

The executor is single-threaded. Concurrency would need a different GIL story
and would break the ordering guarantee that makes actor semantics predictable.

---

## Will not be done

See the non-goals in [status](01-status.md#non-goals). The short version: no object
store, no stateless tasks, no self-written collective transport, no autoscaler,
nothing resembling Ray Data, Tune or Serve.

## See also

- [01-status.md](01-status.md) — what is missing today
- [02-decisions.md](02-decisions.md) — why the boundaries are where they are
