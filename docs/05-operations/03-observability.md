# Observability

> Proposal; not the current implementation.

> One question dominates distributed ML debugging: which worker is stuck, and on
> what. Everything here exists to answer it.

## 1. Problem

At ten thousand workers, aggregate metrics hide the failure. Mean latency is
healthy while one cell is dead, and a job that has stopped making progress looks
identical to one that is merely slow.

## 2. Goals

- Answer "which worker is stuck, and on what" in one command.
- Report a worker's own belief, not a controller's guess.
- Keep metric cardinality independent of worker count at the global tier.

## 3. Non-goals

- Storing metrics. tinyray exposes; a TSDB stores.
- Aggregating another layer's metrics.
- Log storage. Output goes to object storage asynchronously, not through any
  controller.

## 4. Design

### 4.1 Endpoints

| Path | Content | Format |
|---|---|---|
| `/health` | Alive, identity, draining | JSON |
| `/introspect` | Queues, readiness reasons, inflight method and duration, admission depth, incarnation | JSON |
| `/metrics` | Counters and gauges | Prometheus |

`/health` and `/introspect` are plain JSON so `curl` works with no tinyray
client. They are served by the native transport, so a worker whose Python is
blocked still answers — which is exactly when the answer is needed.

### 4.2 Hierarchical reduction

| Tier | Cardinality | Reports |
|---|---|---|
| Worker | Per worker, scraped locally | Readiness, queues, inflight, admission |
| Cell | Per cell | Ready capacity, evictions, churn, control latency |
| Global | Per cluster | Live cells, unavailable capacity, leader changes, consensus writes |

Per-worker series never reach the global tier. Ten thousand workers times a
dozen series is a cardinality problem in the monitoring system, which then fails
at the same time as the cluster.

### 4.3 The diagnostic command

```
tinyray status <cell-or-endpoint>...
```

One line per worker, then anything that looks wrong:

```
ENDPOINT              READY  INFLIGHT        SECS  QUEUED  DEPTH  INCARNATION
10.0.3.7:41234        yes    train_step       0.5       0     12  @1739..a1
10.0.3.8:41234        no     -                0.0       0      0  @1739..b2

Problems:
  - 10.0.3.8 not ready: model_version_in_window failed
  - 10.0.4.1 waiting for seq 7 from caller 3f2a with 4 buffered (a call was lost)
  - 10.0.4.2 refused 340 calls for backpressure; slower than its callers
  - 10.0.5.9 in train_step for 94.2s against a median of 12.1s: likely straggler
```

Exit status 0 when clean, 1 when a problem was found. Straggler detection needs
at least three running workers to have a median worth comparing against.

### 4.4 Metric groups

| Group | Key series |
|---|---|
| Membership | `membership_live`, `membership_evictions_total`, `membership_version` |
| Identity | `fencing_rejections_total`, `identity_superseded_total` |
| Readiness | `readiness_current`, `readiness_failures_by_reason`, `readiness_transitions_total` |
| Discovery | `discovery_response_bytes`, `discovery_served_from_stale_total` |
| Admission | `admission_depth`, `admission_rejections_total`, `admission_pressured_seconds` |
| Transport | `control_bytes_sent`, `control_retries_total`, `queue_waiting_for` |
| Reconciliation | `reconcile_iterations_total`, `leader_changes_total`, `epoch_current` |
| Consensus | `consensus_writes_total` |

### 4.5 The four that matter most

| Series | Why |
|---|---|
| `control_bytes_*` growing with workload | A payload has entered the control plane |
| `consensus_writes_total` growing with worker count | The state split has been violated |
| `discovery_response_bytes` growing with cluster size | Scoping has been bypassed |
| `queue_waiting_for` non-empty | A caller is permanently stalled |

Each is a design invariant expressed as a metric, so a regression is visible in
production and not only in tests.

## 5. Normal flow

Scrape workers locally, reduce at the cell, expose cluster-level series at
global. Diagnosis goes the other way: global says which cell, the cell says
which worker, the worker says which method.

## 6. State and ownership

All observability state is soft and process-local. Nothing is persisted by
tinyray.

## 7. Correctness invariants

- `/health` and `/introspect` answer while the worker's Python is blocked.
- A worker reports its own belief; no tier fabricates a verdict for a tier below.
- Global-tier cardinality is independent of worker count.
- A negative readiness verdict always carries a reason.
- Logs never pass through a controller.

## 8. Failure and recovery

| Failure | Effect |
|---|---|
| Scrape fails | Series go stale; the worker is unaffected |
| TSDB down | No history; the cluster is unaffected |
| `/introspect` unreachable | Reported as `UNREACHABLE`, distinct from unhealthy |

## 9. Observability of the observability

`tinyray status` reports which endpoints did not answer, rather than omitting
them. A missing row is worse than an error row.

## 10. Trade-offs

- **No storage.** A deployment without a TSDB keeps only what `tinyray status`
  shows now.
- **No cluster-wide discovery in the CLI.** `status` takes endpoints or a cell.
  Getting them is a lookup.
- **Straggler detection needs three workers** and is a heuristic.
- **Log persistence is missing.** Output is a 200-line ring buffer per process,
  and when a process dies the ring is what remains. On the
  [roadmap](../08-project/03-roadmap.md).

## 11. Implementation and testing

| Behaviour | Test |
|---|---|
| `/introspect` answers while Python is blocked | `tests/test_observability.py` |
| Global cardinality does not track worker count | `tests/test_fake_cluster.py` |
| `status` exits non-zero on a problem | `tests/test_observability.py` |
| A stalled queue appears in `status` | `tests/test_transport.py` |
| Every negative verdict has a reason | `tests/test_readiness.py` |
