# Chaos

> Proposal; not the current implementation.

> Every row of the failure model has an injection. A failure mode with no
> injection test is an assumption.

## 1. Problem

Failure behaviour is the part of a design most often described and least often
executed. Descriptions are free and wrong at no cost.

## 2. Goals

- One injection per row of
  [05-operations/02-failure-model.md](../05-operations/02-failure-model.md).
- Assert the bound, not only that recovery eventually happened.
- Run the cases that single-instance testing structurally cannot cover.

## 3. Non-goals

- Injecting application failures.
- Hardware fault simulation.

## 4. Injection matrix

| Injection | Expected | Bound asserted |
|---|---|---|
| Kill a worker | Lease expires; removed from lookups | `ttl + sweep` |
| Pause a worker with `SIGSTOP` | Same as death; readiness degrades first | readiness interval |
| Block a worker's main thread | `/introspect` still answers | Immediate |
| Kill a supervised child | Reported; grandchildren also gone | ~1 s |
| Kill a node agent | Its processes reclaimed | node ttl |
| Kill one registry replica | Failover; no interruption | One request |
| Kill **every** registry replica | Lookups from cache; peers still reachable | Unbounded, reported |
| Restart a registry replica empty | Workers re-register | One heartbeat interval |
| Kill a cell controller | Standby takes over with a new generation | cell ttl |
| Partition a cell from global | Cell finishes valid work, requests none | cell ttl |
| Kill the global leader | Election; cells continue | `leader_ttl` |
| **Old leader returns while alive** | Fenced out | Immediate |
| **Restart a worker while the old one lives** | Old one superseded and fenced | One heartbeat |
| Fill an admission queue | Explicit rejection, not stalling | Immediate |
| Clock step on a worker | Fencing unaffected | Immediate |

The three bold rows are the ones that only fail when done properly.

## 5. The cases that must be done properly

### 5.1 Split brain requires both processes alive

A restart test that kills the old process first proves nothing about fencing. The
old process must be running, still heartbeating, and still trying to write, when
the new one registers. The assertions are: the registry serves the new endpoint,
the old process learns it was superseded, and a write from the old process is
rejected by a third party.

### 5.2 Availability requires at least two replicas, and killing one

The previous implementation passed every single-replica test while two replicas
were permanently broken by a shared identity — calls were submitted to one and
fetched from the other. A high-availability test with one instance is a
correctness test wearing the wrong label.

### 5.3 Total loss must be tested, not reasoned about

"Reads fall back to cache" is a claim about code that runs only when everything
is down. It must be executed: kill every replica, then assert that a lookup
returns, that it is reported as stale, and that a peer call still succeeds.

## 6. Method

Injections are applied by the harness against real processes, with the timeline
recorded so a metric change can be attributed to a step. Each case asserts:

1. The failure was detected.
2. Within the stated bound.
3. By the stated detector.
4. With the stated blast radius and no more.

Point four is the one usually omitted, and it is what distinguishes a contained
failure from a lucky one.

## 7. Flakiness

Chaos tests are timing-sensitive and will occasionally fail spuriously. The
policy: quarantine, never delete. A quarantined chaos test is a known gap; a
deleted one is an unknown gap.

Timing constants are environment-overridable so cases run in seconds rather than
at production TTLs.

## 8. Correctness invariants

- Every failure-model row has at least one injection.
- Every injection asserts a bound, not merely eventual recovery.
- Availability injections use two or more instances and kill one.
- Split-brain injections keep the superseded process alive.
- No injection is satisfied by "no exception was raised".

## 9. Implementation

Proposed: `tests/test_chaos.py`, with the process harness shared with
[02-fake-cluster.md](02-fake-cluster.md).

Runs on every merge for the fast cases and nightly for the slow ones.
