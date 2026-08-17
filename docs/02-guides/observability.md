# Observability

## Purpose

Answering the question that dominates distributed ML debugging: **which worker
is stuck, and on what?**

## Concepts

Every actor and served process reports its own state over HTTP. What you see is
what the worker believes, not a driver-side guess — which matters, because the
two disagree exactly when something is wrong.

## `tinyray status`

One line per worker, plus anything that looks wrong:

```
$ tinyray status 127.0.0.1:40755 127.0.0.1:34005
ENDPOINT                 INFLIGHT              SECS  QUEUED    DONE  FAILED     STORE
-------------------------------------------------------------------------------------
127.0.0.1:40755          slow                   0.5       0       2       0       23B
127.0.0.1:34005          -                      0.0       0       1       0        4B

No problems detected.
```

`INFLIGHT` is the method running right now and `SECS` how long it has been
running. That pair usually answers the question on its own.

Problems are called out explicitly, and the exit status is 1 when any are found:

```
Problems:
  - 127.0.0.1:40755 is waiting for sequence 7 from caller 3f2a1b9c with 4 call(s)
    buffered behind it (a call was lost in flight; this caller is stalled)
  - 127.0.0.1:34005 has evicted 12 result(s); consumers may see ObjectLost.
    Raise store_max_bytes or fetch sooner.
  - 127.0.0.1:34005 refused 340 call(s) for backpressure; it is slower than its callers.
  - 127.0.0.1:41002 has been in train_step for 94.2s, more than 3x the median
    of 12.1s: likely straggler
```

Straggler detection needs at least three running workers to have a median worth
comparing against.

### Other commands

```bash
tinyray introspect 127.0.0.1:40755    # the raw JSON report
tinyray health 127.0.0.1:40755        # liveness only
```

## Introspection from code

```python
import json

report = json.loads(actor.introspect())
```

```json
{
  "actor": "762b745ae205ef5b0000000000000003",
  "accepted": 3,
  "completed": 2,
  "failed": 0,
  "rejected_backpressure": 0,
  "rejected_duplicate": 0,
  "reordered": 0,
  "queued": 0,
  "ready": 0,
  "inflight": "slow",
  "inflight_seconds": 0.5,
  "store": {
    "pending": 1, "ready": 2, "failed": 0,
    "bytes": 23, "evictions": 0, "expirations": 0
  },
  "stuck_callers": []
}
```

### Reading it

| Field | Rising means |
|---|---|
| `inflight`, `inflight_seconds` | What it is doing and for how long. Start here |
| `queued` | Callers are outrunning it |
| `rejected_backpressure` | It hit the queue limit and refused |
| `reordered` | Calls arrived out of order and were buffered. Normal with a connection pool |
| `rejected_duplicate` | Retransmits absorbed rather than executed twice |
| `store.bytes` | Results are accumulating. Compare with `store_max_bytes` |
| `store.evictions` | Results were dropped. Consumers will see `ObjectLost` |
| `store.expirations` | Results aged out |
| `stuck_callers` | **A call was lost in flight and that caller is stalled** |

`stuck_callers` deserves attention. Each entry names a caller, the sequence
number the actor is waiting for, and how many later calls are buffered behind
it. Persistently non-empty means that caller will never make progress.

## Driver byte counters

Answering "is my driver relaying data it should not be?":

```python
for endpoint, stats in tr.transport_stats().items():
    print(endpoint, stats["bytes_received"], stats["requests"], stats["retries"])
```

In a healthy run the driver's byte counts stay small: it moves references while
the workers move payloads between themselves. `examples/native_stack.py` reports
2,163 bytes for three iterations of a four-rank trainer plus an inference
server.

These counters exist because a code path that quietly moves a payload looks
identical to a correct one from the outside. Only a byte count distinguishes
them — and one did slip through: `wait()` fetched whole results to answer a
readiness question, and no functional test could see it.

## Logs

Output from every worker is prefixed and forwarded to the driver:

```
[trainer-0:1920268] iteration 12 loss=2.31
[rollout:1920412] INFO: sglang server started
```

Without the prefix, eight workers writing to one terminal are indistinguishable.

A managed process also keeps its recent output for diagnostics:

```python
print(process.tail(20))         # last 20 lines
print(process.recent_log())     # the ring buffer, up to 200 lines
```

That buffer is what makes a startup failure legible: the error message carries
it.

**There is no log persistence.** Output goes to the driver's stdout and a
200-line ring buffer. Redirect the driver if you need a record.

## Hung workers

An actor process installs a `faulthandler` on `SIGUSR1`:

```bash
kill -USR1 <pid>       # dumps every thread's stack to stderr
```

Useful when `inflight_seconds` is climbing and you need to know where inside the
method it is.

## Contract

| Endpoint | Returns |
|---|---|
| `GET /health` | `{"status":"ok","actor":"...","shutting_down":false}` |
| `GET /introspect` | The JSON above |

Both are plain HTTP, so `curl` works.

## Pitfalls

**`reordered` rising is not a problem.** With four connections per peer, calls
routinely arrive out of order and the queue repairs it. `stuck_callers` is the
field that indicates trouble.

**`store.bytes` near the watermark is a warning.** Consumers are about to start
seeing `ObjectLost`. Raise `store_max_bytes`, fetch sooner, or release earlier.

**There is no `/metrics`.** No Prometheus endpoint exists; `/introspect` carries
the same data in a bespoke JSON shape.

**`tinyray status` takes endpoints, not names.** There is no cluster-wide
discovery command yet. Get endpoints from `tr.actors()` or `tr.processes()`.

## See also

- [fault-tolerance.md](fault-tolerance.md) — what the failures mean
- [cli.md](../03-reference/cli.md) — full command reference
- [protocol.md](../03-reference/protocol.md) — the endpoints
