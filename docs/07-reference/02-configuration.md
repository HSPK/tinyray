# Configuration

> Proposal; not the current implementation.

Every value is **derived** from the design or **to be measured** per deployment.
None is a production default until the fake cluster has run
([06-testing/02-fake-cluster.md](../06-testing/02-fake-cluster.md)).

## 1. Membership

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `lease_ttl` | s | 30 | > 3 x heartbeat | Time to eviction |
| `heartbeat_interval` | s | `ttl/3` | > 0 | Assertion rate |
| `sweep_interval` | s | 5 | > 0 | Eviction granularity |
| `startup_window` | s | 300 | > 0 | Wait for a registry that has not started |
| `registry` | addresses | `TINYRAY_REGISTRY` | Non-empty | Replica list, comma separated |

`lease_ttl` must exceed a plausible garbage-collection pause. Too short evicts
healthy workers; too long leaves dead addresses in lookups.

## 2. Discovery

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `cache_ttl` | s | 5.0 | >= 0 | Lookup freshness |
| `watch_interval` | s | 2.0 | > 0 | Change detection latency |
| `lookup_timeout` | s | 10.0 | > 0 | Per replica |
| `max_scope` | int | 1024 | > 0 | Refuses an oversized lookup |

## 3. Readiness

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `interval` | s | 1.0 | > 0 | Evaluation rate |
| `predicate_timeout` | s | 1.0 | > 0 | Per predicate |
| `verdict_max_age` | s | 3 x interval | > interval | Stale verdict is not ready |

Initial state is not-ready and is not configurable.

## 4. Admission

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `max_pending` | int | 1000 | > 0 | The bound |
| `high_watermark` | ratio | 0.8 | 0..1 | Enter pressured |
| `low_watermark` | ratio | 0.6 | < high | Leave pressured |
| `retry_after_base` | s | 0.025 | > 0 | Linear backoff step |
| `retry_after_max` | s | 1.0 | > base | Ceiling |
| `max_retries` | int | 16 | >= 0 | Backpressure retries |

## 5. Reconciliation

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `interval` | s | 2.0 | > 0 | Convergence rate |
| `leader_ttl` | s | 15 | > 3 x renew | Failover window |
| `leader_renew` | s | `ttl/3` | > 0 | Renewal rate |
| `min_ready_fraction` | ratio | 0.9 | 0..1 | Refuses to freeze an epoch below this |
| `consensus` | addresses | `TINYRAY_CONSENSUS` | Non-empty when used | Store location |

## 6. Transport

| Field | Type | Default | Effect |
|---|---|---|---|
| `connections_per_peer` | int | 4 | Head-of-line mitigation |
| `request_timeout` | s | 300 | Per request |
| `max_pending_calls` | int | 1000 | Server admission bound |
| `max_header_len` | bytes | 1 MiB | Allocation guard |
| `max_frames` | int | 4096 | Allocation guard |
| `max_frame_len` | bytes | 4 GiB | Allocation guard |
| `max_message_len` | bytes | 8 GiB | Allocation guard |
| `result_ttl` | s | 300 | Unfetched results |

## 7. Supervision

| Field | Type | Default | Effect |
|---|---|---|---|
| `ready_when` | predicate | `alive` | How readiness is observed |
| `startup_timeout` | s | 600 | Model loading is slow |
| `stop_timeout` | s | 30 | Grace before the group is killed |
| `log_lines` | int | 200 | Ring buffer depth |

`ready_when="alive"` proves almost nothing. Anything that serves should use
`port`, `http` or `log:`.

## 8. Environment variables

| Variable | Read by | Written by tinyray |
|---|---|---|
| `TINYRAY_REGISTRY` | Client | No |
| `TINYRAY_CONSENSUS` | Reconciler | No |
| `TINYRAY_CONTROL_PORT` | `join` | Only for a process it supervises |
| `RANK`, `SLURM_PROCID`, `OMPI_COMM_WORLD_RANK` | `join` | **Never** |
| `WORLD_SIZE`, `SLURM_NTASKS` | `join` | **Never** |
| `LOCAL_RANK`, `SLURM_LOCALID` | `join` | **Never** |
| `CUDA_VISIBLE_DEVICES` | `join`, reported in meta | **Never** |

The "never" rows are asserted by `tests/test_suite_quality.py`. Writing them
would give the cluster two answers to "what is my rank".

## 9. Timing summary

**Derived** from the defaults above:

| Event | Bound |
|---|---|
| Worker death detected at its cell | `lease_ttl + sweep` ~35 s |
| Worker unready detected | `readiness interval` ~1 s |
| Cell death detected at global | `cell_ttl` ~15 s |
| Leader failover | `leader_ttl` ~15 s |
| Membership change visible to a reader | `lease + cache_ttl` ~35 s |
| Convergence latency | `interval` ~2 s |

Every timing constant is environment-overridable, so tests reach the deadline in
seconds. A constant that only ever runs at its production value is a constant
nobody tests.
