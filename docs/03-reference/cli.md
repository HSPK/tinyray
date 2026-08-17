# CLI

## Purpose

The `tinyray` command. It exists to answer one question: **which worker is
stuck, and on what?**

All commands read the same `/introspect` endpoint the runtime serves, so what
you see is what the worker believes — not a driver-side guess.

## Synopsis

```
tinyray [--timeout SECONDS] {status,introspect,health} ...
```

`--timeout` sets the per-request deadline, default 10 seconds.

## `status`

```
tinyray status HOST:PORT [HOST:PORT ...]
```

One line per worker, then anything that looks wrong.

```
ENDPOINT                 INFLIGHT              SECS  QUEUED    DONE  FAILED     STORE
-------------------------------------------------------------------------------------
127.0.0.1:40755          slow                   0.5       0       2       0       23B
127.0.0.1:34005          -                      0.0       0       1       0        4B

No problems detected.
```

| Column | Meaning |
|---|---|
| `INFLIGHT` | The method running now, or `-` if idle |
| `SECS` | How long it has been running |
| `QUEUED` | Calls accepted but not yet dispatched |
| `DONE` / `FAILED` | Completed and failed calls |
| `STORE` | Bytes held in the result store |

### Problems

Reported explicitly, with the remedy where there is one:

```
Problems:
  - 127.0.0.1:40755 is waiting for sequence 7 from caller 3f2a1b9c with 4 call(s)
    buffered behind it (a call was lost in flight; this caller is stalled)
  - 127.0.0.1:34005 has evicted 12 result(s); consumers may see ObjectLost.
    Raise store_max_bytes or fetch sooner.
  - 127.0.0.1:34005 refused 340 call(s) for backpressure; it is slower than its callers.
  - 127.0.0.1:41002 has been in train_step for 94.2s, more than 3x the median
    of 12.1s: likely straggler
  - 127.0.0.1:41003 did not answer /introspect
```

Straggler detection needs at least three running workers, so there is a median
worth comparing against.

**Exit status:** 0 if clean, 1 if any problem was found, 2 on usage error.
Usable in a health check.

## `introspect`

```
tinyray introspect HOST:PORT
```

The raw JSON report, pretty-printed. Field meanings are in
[observability](../02-guides/observability.md#reading-it).

Exit 1 if the endpoint does not answer.

## `health`

```
tinyray health HOST:PORT [HOST:PORT ...]
```

Liveness only:

```
127.0.0.1:40755: {"status":"ok","actor":"762b745a...","shutting_down":false}
```

Exit 1 if any endpoint is unreachable.

## Finding endpoints

There is no discovery command yet. Get them from the driver:

```python
[a["endpoint"] for a in tr.actors()]
[p.endpoint for p in tr.processes()]
```

## Environment variables

Read by tinyray processes rather than passed as flags.

| Variable | Set by | Purpose |
|---|---|---|
| `TINYRAY_CONTROL_PORT` | tinyray | Port `serve()` should bind |
| `TINYRAY_ACTOR_ID` | tinyray | Identity the driver will address |
| `TINYRAY_ANNOUNCE_FD` | tinyray | Where to write the endpoint once bound |
| `TINYRAY_ACTOR_NAME` | tinyray | Marks a process as an actor; used for log prefixes |
| `TINYRAY_PROCESS_NAME` | tinyray | Name of a managed process |
| `TINYRAY_GROUP` | tinyray | Worker group name |
| `TINYRAY_PREIMPORT` | prewarm pool | Comma-separated modules to import ahead of time |
| `TINYRAY_PREWARM` | prewarm pool | Marks a warm interpreter |
| `TINYRAY_STARTUP_TIMEOUT` | you | Override actor startup timeout, default 60 s |
| `TINYRAY_SWEEP_INTERVAL` | you | Override the TTL sweep interval, default 30 s |

The last two exist so tests can reach a deadline in seconds rather than assume
the production value works. A constant that only ever runs at its default is a
constant nobody tests.

## Pitfalls

**`status` takes endpoints, not names.** No cluster-wide discovery yet.

**`UNREACHABLE` may be correct.** A worker that has just been killed, or is
still starting, will not answer.

**There is no `tinyray start`.** The head runs inside the driver process; there
is no daemon to start. See [status](../05-project/status.md).

## See also

- [observability.md](../02-guides/observability.md) — using this in anger
- [configuration.md](configuration.md) — every knob
