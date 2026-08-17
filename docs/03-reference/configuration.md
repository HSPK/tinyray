# Configuration

## Purpose

Every knob, its default, and why the default is what it is. Defaults are read
from the source; if one disagrees, the source is right.

## Runtime

Set on `tr.init(...)`.

| Knob | Default | Notes |
|---|---|---|
| `num_cpus` | `os.cpu_count()` | What this node advertises, not a cgroup limit |
| `num_gpus` | detected | From `CUDA_VISIBLE_DEVICES`, else `nvidia-smi`, else 0 |
| `prewarm` | `0` | Warm interpreters per device assignment |
| `heartbeat_timeout` | `30.0` s | A node silent this long is declared dead and its actors are reaped |
| `supervise_interval` | `1.0` s | Between supervision passes. Bounds detection latency |

`heartbeat_timeout` must exceed `supervise_interval` by a wide margin, or a slow
pass reaps a healthy node. Detection takes up to
`heartbeat_timeout + supervise_interval`.

## Actor

Set on `@tr.remote(...)` or `.options(...)`.

| Knob | Default | Notes |
|---|---|---|
| `num_cpus` | `1.0` | Fractional allowed |
| `num_gpus` | `0.0` | **Must be a whole number if ≥ 1.** See below |
| `memory_bytes` | `0` | Advisory; not enforced |
| `max_restarts` | `0` | Automatic restarts after a crash |
| `max_pending_calls` | `1000` | Queue depth before `Backpressure` |
| `store_max_bytes` | `2 GiB` | LRU watermark for held results |
| `store_ttl_seconds` | `300.0` | Unfetched results expire |
| `strategy` | `"PACK"` | `"PACK"` or `"SPREAD"` |
| `name` | `None` | Cluster-unique; enables `get_actor` |

### The GPU rule

`num_gpus` may be `0`, a fraction below `1`, or a **whole number** at or above
`1`. `1.5` is refused.

A fractional GPU means sharing one device. A value above one means several
devices. `1.5` would mean "one and a half devices", which has no meaning for
NCCL — a rank owns whole devices or the group cannot be built. Refusing at
placement time turns a confusing runtime hang into an immediate error.

### `max_pending_calls`

1000 is high enough that a normal burst does not trip it and low enough that a
runaway producer is caught before memory is gone. Lower it if you want
backpressure to be felt early; raise it for a genuinely bursty producer.

### `store_max_bytes` and `store_ttl_seconds`

32 actors each holding a few 10 MB results adds up fast, so 2 GiB is a ceiling,
not an aspiration. The TTL catches results nobody ever fetched — a `wait` that
took 24 of 32 leaves 8 orphans every round.

Eviction never touches the newest result, whatever the watermark says.

## Managed process

Set on `launch_process(...)`.

| Knob | Default | Notes |
|---|---|---|
| `num_cpus` | `1.0` | |
| `num_gpus` | `0.0` | Same whole-number rule |
| `ready_when` | `"alive"` | Weak. Pass something stricter for a server |
| `allocate_port` | `True` | Substituted into `{port}` |
| `startup_timeout` | `600.0` s | Model loading is slow |
| `strategy` | `"PACK"` | |
| `max_restarts` | `0` | |
| `env`, `cwd`, `host` | `None` | |

## Worker group

`launch_workers(...)` and `create_worker_group(...)`.

| Knob | Default | Notes |
|---|---|---|
| `size` | required | World size |
| `gpus_per_worker` | `1.0` | |
| `cpus_per_worker` | `1.0` | |
| `master_addr` | first worker's host | Rendezvous |
| `master_port` | allocated | Rendezvous |
| `strategy` | `"PACK"` | Keeps ranks on one node for NVLink |
| `startup_timeout` | `900.0` s | Higher than a single process: every rank must reach the rendezvous |
| `name` | `"workers"` | |

`launch_workers` has no `max_restarts`. Restarting one rank of a collective
without rebuilding the group leaves the rest deadlocked.

## Serve

`serve(...)`, called inside the worker.

| Knob | Default | Notes |
|---|---|---|
| `background` | `False` | `True` returns immediately |
| `bind` | `TINYRAY_CONTROL_PORT`, else `127.0.0.1:0` | |
| `actor_id` | `TINYRAY_ACTOR_ID`, else generated | |
| `max_pending_calls` | `1000` | |

## Transport

Not exposed in Python. Listed because they explain latency.

| Knob | Default | Why |
|---|---|---|
| `connections_per_peer` | `4` | HTTP/1.1 head-of-line blocking: a 10 MB response would stall small control messages behind it |
| `request_timeout` | `300` s | |
| `backoff` | `25` ms | Linear, multiplied by attempt, capped at 8 steps |
| `max_retries` | `16` | Backpressure only |
| `pool_idle_timeout` | `90` s | |
| `TCP_NODELAY` | on | Nagle would add up to 40 ms to a small message |

## Framing limits

| Limit | Default |
|---|---|
| `max_header_len` | 1 MiB |
| `max_frames` | 4096 |
| `max_frame_len` | 4 GiB |
| `max_message_len` | 8 GiB |

Sized for ~10 MB payloads with headroom. Exceeding one is fatal to the
connection: see [protocol](protocol.md#limits).

## Environment variables

Listed in [cli.md](cli.md#environment-variables). Two are meant for you rather
than for tinyray:

| Variable | Default | Purpose |
|---|---|---|
| `TINYRAY_STARTUP_TIMEOUT` | `60` s | Actor startup deadline |
| `TINYRAY_SWEEP_INTERVAL` | `30` s | TTL sweep period |

Both exist so tests can reach the deadline in seconds. A timing constant that
only ever runs at its production value is a constant nobody tests.

## Timing summary

| Event | Bound |
|---|---|
| Node death detected | `heartbeat_timeout + supervise_interval` ≈ 31 s |
| Process death detected | `supervise_interval` ≈ 1 s |
| Actor startup timeout | 60 s |
| Process startup timeout | 600 s |
| Worker group startup timeout | 900 s |
| Result expiry | `store_ttl_seconds` + up to `TINYRAY_SWEEP_INTERVAL` |
| Default `get` / `wait` timeout | 300 s |
| Backpressure retry ceiling | ~1.8 s over 16 attempts |

## Pitfalls

**`memory_bytes` is not enforced.** It is bookkeeping. An actor that exceeds it
is killed by the OS, not by tinyray.

**`num_cpus` is not a cgroup limit.** Nothing stops an actor using more.

**Store settings are per actor, not per cluster.** 32 actors at the default can
hold 64 GiB between them.

**`ready_when="alive"` proves almost nothing.** The process existed for a
moment. For anything that serves, use `"http"` or `"log:"`.

## See also

- [placement.md](../02-guides/placement.md) — resources in practice
- [fault-tolerance.md](../02-guides/fault-tolerance.md) — the timing constants in context
