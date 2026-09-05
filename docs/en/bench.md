# Benchmarks

`bench.py` measures numbers; the tests assert bounds. They are not the same
thing: a test only fires when a bound is crossed, so something can get twice
as slow and stay green -- and that has happened. Handing the whole budget to
the first beat took the lossy-link test from 7:22 to 17:06, **passing all the
way**.

*[中文](../bench.md)*

```bash
python bench.py                       # a table
python bench.py --json out.json       # machine readable
python bench.py --only rpc_latency    # one scenario
python bench.py --check               # compare against bench-baseline.json,
                                      # non-zero exit on a regression
python bench.py --only point_lookup rpc_batch
python bench.py --only discovery watch_wakeup --coalesce-ms 1
```

Every scenario feature-detects: this one script has to run against old wheels,
reporting `n/a` for what it cannot do rather than crashing, or cross-version
comparison is impossible.

Known unsupported features are labeled `unsupported`; execution failures are
errors and exit non-zero. `--check` also refuses missing metrics, empty
comparisons, obsolete baseline formats and different workload settings. It
never treats a failed measurement as an improvement. Workloads and cleanup
still execute under `python -O`.

Format 2 records the Python/native versions, a library fingerprint, benchmark
revision, workload settings and host details. Capture a new baseline with
`--json` after changing scenarios or calibration hardware. Do not compare an
explicit low-latency policy against a default-policy baseline.

`rpc_latency` and `rpc_throughput` retain the historical same-process topology.
Use `rpc_latency_separate` and `rpc_concurrency` for a callee with its own GIL.
`rpc_batch` compares the same 32 logical calls individually and in one request,
not 32 requests against one logical operation. `point_lookup` uses stable
seated rosters up to 5,000 members. `all_filtered_ms` and `pick_filtered_ms`
now measure the operations their names describe.

The 50 ms default coalescing budget is a traffic/latency choice, not a network
floor. `discovery` measures a burst; `discovery_spaced` spaces changes by
150 ms. Lower `coalesce_ms` explicitly to measure the latency/load tradeoff.

For registry-only work, build and run the portable
`crates/tinyray-registry/examples/perf_registry.rs` example. Its output
distinguishes owned acknowledgment assembly from HTTP/shared-response costs;
do not report the former as end-to-end throughput.

## The baseline

The current format-2 baseline is the per-metric median of three independent
optimized-build runs, followed by two independent verification runs. All 23
watched metrics passed both verification runs. Relative tolerance is 20%,
combined with calibrated absolute floors; the sub-millisecond `flush()` floor
is now 0.1 ms, not the historical 50 ms allowance.

### Measured optimization results

Same benchmark script, same Python 3.11.15 environment, same 24-vCPU AMD EPYC
host, sequential runs with no tests/builds running alongside. The before build
is the saved v0.15.0 wheel; the after build is the optimized source released
in v0.16.0, measured before its version metadata was bumped.
Lookup figures below are warm-cache medians across three runs.

| Operation | Before | After |
|---|---:|---:|
| `slot()` at 5,000 members | 9.211 ms | 0.002034 ms |
| Unfiltered `pick()` at 5,000 | 9.133 ms | 0.003671 ms |
| `all()` at 5,000 | 9.271 ms | 4.445 ms |
| Repeated field digest at 1,000 | 0.0990 ms | 0.000360 ms |
| 64 KiB RPC echo p50 | 1.269 ms | 0.961 ms |
| Burst discovery, default policy | 50.85 ms | 50.73 ms |

One opt-in `coalesce_ms=1` run reduced burst discovery to 1.22 ms and watched
notification to 2.24 ms. The default remains unchanged. A separate-process
batch of 32 no-op calls cost 1.12 ms versus about 20.7 ms individually
(18.5x per logical operation). Single-call RPC latency stayed near 0.69 ms.

The portable registry benchmark reduced quiet owned-ack assembly from
39.4 us to 0.58 us and fresh one-change history replay from 2.29 us to
0.73 us. A separate HTTP probe with a shared roughly 1 MiB roster improved
from 3.27 ms to 2.84 ms: assembly gains are not network-throughput multipliers.

Native caches trade bounded memory for repeated-read speed: clients retain
at most two 1 MiB serialized snapshots and one bounded field digest per pool;
registry delta caches retain at most eight entries with a 2 MiB conservative
serialized-payload budget per pool. These are not process-wide RSS limits,
and cold reads or changing pools must still build their new snapshots.

### Historical noise rationale

`--check` compares a small set of metrics, and **the threshold was measured,
not chosen**. Five consecutive runs on an idle machine, taking each metric's
worst deviation from its median (the throughput row was measured later):

| Watched | Noise | Not watched | Noise |
|---|---|---|---|
| `watch_wakeup` p50 | 0.2% | `join_cold_start` p50 | **2306%** |
| `discovery` p50 | 0.4% | `async_call` p99 | 129% |
| `rpc_latency` p50 | 0.7% | every `max_ms` | 24-34% |
| lookup @1000 | 0.6-1.1% | `update_changed_us` | 33% |
| `async_call` p50 | 2.6% | `idle_beat_rate` | 9% |
| `flush` p50 | 2.9% | lookup @10 | quantised to 0.001 ms |
| 64 KiB p50 | 4.4% | throughput (below) | 22% |

The threshold is **20%**, about four times the noisiest watched metric, plus
an absolute floor so 0.001 ms quantisation noise cannot cry wolf.
**Throughput was later removed**: its 4.3% came from five consecutive runs of
a single build, which is optimistic -- the same build later spread 815-958
over six runs, and v0.10.0 produced one run of 662 between neighbours of 899
and 901, 22% below its own median. Eight GIL-bound threads sharing one client
for five seconds was never going to be a steady quantity. Wide enough not to
cry wolf is wide enough to let a real regression through.
`join_cold_start`'s median is excluded because it is bimodal (below), so which
peak a run lands on is luck.

Verified in both directions: four further independent runs against the
baseline were **16 of 16 silent**; regressions of realistic size were all
reported -- RPC p50 25% slower reported 1, lookup twice as slow reported 7,
discovery back to its pre-long-poll behaviour reported 1 (+960%), throughput
down 30% reported 1 -- while 10% slower passed through.

**Only something that fires gets read.** A baseline that goes red on its own
is ignored, and then it is worse than none.

## Historical cross-version measurements

The tables below retain earlier measurements; `HEAD` means the checkout used
at that time, not today's build. In particular, their old `flush()` values are
not the current sub-millisecond path.

### Group one: the core paths

One machine, one script, a wheel built per version and installed into the same
clean venv. A 2000 ms lease (500 ms heartbeat interval), all over loopback.

| Metric | v0.6.1 | v0.7.1 | v0.9.1 | v0.11.0 | v0.12.0 | HEAD |
|---|---|---|---|---|---|---|
| RPC round trip p50 (ms) | 0.735 | 0.733 | 0.744 | 0.762 | 0.697 | **0.675** |
| RPC round trip p99 (ms) | 1.136 | 1.167 | 1.094 | 1.077 | 0.958 | **0.955** |
| RPC throughput, 8 threads (/s) | 887 | 872 | 817 | 848 | 872 | 907 |
| 64 KiB echo p50 (ms) | 1.390 | 1.409 | 1.459 | 1.429 | 1.349 | 1.360 |
| `all()` over 200 members p50 (ms) | 0.310 | 0.306 | 0.306 | 0.313 | 0.308 | 0.316 |
| `snapshot()` p50 (ms) | 0.313 | 0.310 | 0.311 | 0.312 | 0.313 | 0.312 |
| `field_digest` p50 (ms) | n/a | n/a | n/a | 0.023 | 0.024 | **0.023** |
| **Discovery latency p50 (ms)** | **541.8** | **100.9** | **51.1** | 51.6 | 51.2 | **51.0** |
| Idle heartbeats (/s) | 1.83 | 2.0 | 1.83 | 2.0 | 2.0 | 1.83 |

### Discovery latency is the only thing that really moved

541.8 ms to 51.0 ms, **10.6x**, all of it from the long polling introduced in
0.7.x and finished in 0.9.x. The 541.8 ms of 0.6.1 is exactly what the design
notes call "structurally one heartbeat interval", and the measurement bears it
out: a 500 ms interval, a 541.8 ms median. The four versions after 0.9.1 read
51.1 / 51.6 / 51.2 / 51.0, inside the noise. Those bursts exercised the
50 ms coalescing budget; they did not establish a network latency floor.

### Everything else is flat, and that is the finding

RPC round trip p50 went 0.735 to 0.675 over six versions (8% faster) and p99
went 1.136 to 0.955 (14% better). The direction is consistent but the size is
not enough to call an optimisation; it is enough to say nothing got slower.
The lookup paths (`all()` and `snapshot()` over 200 members) stayed between
0.306 and 0.316 ms across all six -- **no regression**. The value of these
numbers is not that they are pretty; it is that the next time somebody touches
the cache or the delivery path, they will say by how much.

`field_digest` exists from 0.11.0: 0.023 ms against `snapshot()`'s 0.312 ms,
**13x** -- which is the whole reason it exists. A watcher that cares about two
keys should not pay for a full snapshot when a third key changes.

### Three traps that have to be said out loud

**907 calls a second is not the 327,000 a second in the design notes.** That
one is the registry's heartbeat throughput under `loadgen`; this one is the
Python RPC path (8 threads, one httpx client, the GIL). The two numbers
measure two different things, and the notes did not say so, which makes it
easy to read as a regression along one line. The 907 here also includes the
callee sharing the caller's GIL; it is not an isolated caller ceiling.

**Idle heartbeats are 2 a second, the same in all six versions.** That is not
what long polling saves. The "14.5 a second against 0.12" in the notes is
about **a watcher waiting for changes**, a different scenario that this script
does not yet cover. Do not use this row as evidence for long polling.

**Cold start is bimodal, and identically so in every version.** A start either
gets its answer at once (about 1 ms) or waits for the next beat (about 42 ms),
with nothing in between. So where the median lands is purely the ratio: at
n=10 it jumped between 1.0, 21.2 and 41.6 ms, which looked like a 40x
difference. Re-measured at n=40, v0.12.0 is 25 of 40 fast and HEAD is 24 of 40
-- **identical** -- while v0.6.1 is 20 of 40. The script therefore reports
`under_5ms` and `over_15ms` rather than a median alone. This one is a note to
self: suspect the method before suspecting the code.

### Group two: the rest of the API

| Metric | v0.6.1 | v0.9.1 | v0.11.0 | v0.12.0 | HEAD |
|---|---|---|---|---|---|
| RPC sync p50 (ms) | 0.733 | 0.748 | 0.763 | 0.695 | **0.671** |
| **RPC async p50 (ms)** | 1.415 | 1.424 | 1.441 | 1.390 | **1.331** |
| A call that raises, p50 (ms) | 0.912 | 0.911 | 0.925 | 0.860 | 0.919 |
| `update()` (µs) | 3.0 | 3.0 | 3.0 | 4.0 | 3.0 |
| `flush()` p50 (ms) | 584 | 660 | 689 | 645 | 702 |
| `all()` @1000 members (ms) | 1.543 | 1.521 | 1.566 | 1.562 | 1.557 |
| `epoch()` @1000 (ms) | 1.567 | 1.572 | 1.670 | 1.607 | 1.615 |
| `field_digest` @1000 (ms) | n/a | n/a | 0.102 | 0.108 | **0.097** |
| `changes()` wakeup p50 (ms) | n/a | 50.99 | 51.18 | 51.02 | 50.98 |
| `fields=` suppressed 10 unrelated changes | n/a | n/a | 0 | 0 | 0 |

`n/a` is the result of feature detection, not a failure: 0.6.1's `changes()`
returned a bare generator (not a context manager, no `fields=`), and
`field_digest` arrived in 0.11.0.

**An async call costs twice a sync one, in all five versions.** 1.33 ms
against 0.67 ms. This is not a regression -- it has always been so, and nobody
had measured it. The difference comes from the `httpx.AsyncClient` path plus
event-loop scheduling rather than from tinyray's own code, but a caller needs
to know: **on loopback the async API buys threads, not time.**

**The scale curve (HEAD):**

| Members | `all()` | `snapshot()` | `all(shard=)` | `field_digest` | `epoch()` |
|---|---|---|---|---|---|
| 10 | 0.018 | 0.017 | 0.006 | 0.001 | 0.020 |
| 100 | 0.140 | 0.142 | 0.026 | 0.009 | 0.148 |
| 1000 | 1.496 | 1.513 | 0.207 | 0.102 | 1.548 |

These historical bulk paths grew with N. Two ratios are worth remembering: at 1000 members `field_digest`
is **15x** cheaper than a full snapshot (0.102 against 1.513), and a filter
matching one in eight is **7x** cheaper than `all()` (0.207 against 1.496) --
the latter because the match happens before serialisation, on the Rust side.
`epoch()` costs barely more than `snapshot()`: freezing a round is the price
of taking the list once.

The `changes()` wakeup at 51.0 ms equals the polled discovery latency of
50.9 ms, which says all the time on this path goes into getting the change
into the local cache; being woken is free. `fields=` suppressed all ten
unrelated changes in every version that has it.

## Method

- Run back to back on one idle machine. **Do not run the test suite while
  benchmarking** -- tried once, and two timing-sensitive tests went red on the
  spot; one of them was the fencing window, where 59 calls landed on a process
  that should already have been fenced.
- Each version is checked out with `git worktree`, built with
  `maturin build --release`, and installed into `/tmp/bv`. `bench.py` always
  comes from the working tree, so the script is a constant.
- Share `CARGO_TARGET_DIR`, or every version pays for a cold compile.
