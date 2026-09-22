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
python bench.py --only rpc_models     # dict, dataclass, typed containers, manual codecs
python bench.py --only rust_service   # Rust handler versus Python/GIL handler
python bench.py --only blobref        # bytes versus same-host sealed memfd references
python bench.py --only filter_index   # cold/warm scalar-filter indexing at 100/1,000/5,000
python bench.py --only registry_connections  # persistent vs per-beat reconnect, 1,000 members
python bench.py --only idle_waiters    # whether heartbeats wake 1,000 quiet watchers
python bench.py --only discovery watch_wakeup --coalesce-ms 1
```

Every scenario feature-detects within one wire generation, reporting `n/a`
for what it cannot do rather than crashing. The 0.18 native registry and
method transports are hard cutovers with no HTTP fallback; older wheels need
their matching historical benchmark script.

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
`rpc_concurrency` measures 1/4/8/32/128 callers, p50/p99 and physical client
connections; it therefore distinguishes multiplexing from merely opening more
sockets.
`rpc_batch` compares the same 32 logical calls individually and in one request,
not 32 requests against one logical operation. `point_lookup` uses stable
seated rosters up to 5,000 members and separates `snapshot_ms` (creating only
the frozen native view) from `snapshot_materialize_ms` (first creation of all
Handles). It also reports frozen-view len/slot/get, an already-satisfied
`wait(count)`, epoch creation, and epoch materialization. State measurements
separate the first batched decode (`all_state_ms` / `snapshot_state_ms`) from
direct slot reads on the same Handles (`cached_all_state_ms` /
`cached_snapshot_state_ms`). At 5,000 members, five-run medians are
4.8658/5.0067 ms and 0.1648/0.2499 ms respectively; Handle-only all/snapshot
materialization is 1.3480/1.3778 ms.
`all_filtered_ms` and `pick_filtered_ms` now measure their named operations.
`filter_index` reports cold construction and warm hits for count, pick, all,
and wait at 100/1,000/5,000 members, plus live entry/byte counters and every
configured bound. At 5,000 members the three-run medians are 0.000561 ms for
warm count and 0.001783 ms for warm pick, versus 0.444125/0.447807 ms to build
the first result. The one cached 625-member result is estimated at 5,143
bytes; warm all/wait still materialize those matches and measure
0.160748/0.162572 ms.
`rpc_models` interleaves plain
dicts, automatic dataclass/typed-container conversion, and equivalent hand-written
conversion. It reports codec microseconds and complete RPC round trips
separately so network cost is not mislabeled serialization cost.

The 50 ms default coalescing budget is a traffic/latency choice, not a network
floor. `discovery` measures a burst; `discovery_spaced` spaces changes by
150 ms. Lower `coalesce_ms` explicitly to measure the latency/load tradeoff.
`idle_beat_rate` also reports new connections and reused requests during the
window. `registry_connections` has 1,000 members send six beats each:
persistent mode accepts 1,000 sockets for 6,000 frames, while reconnecting
each beat accepts 6,000—an **83.3%** reduction with the same 6,000 successful
beats. The scenario also requires all 1,000 leases to remain present, reports
zero failed beats, and compares p50/p99/max latency for persistent and
reconnecting modes; the benchmark gate watches their median-latency ratio.
`idle_waiters` parks 1,000 quiet watchers on one event loop and waits through
four lease renewals. Its five-run median is **0 rechecks, 0 discovery wakes,
and 1.684 ms CPU**. The former per-beat broadcast caused 4,000 rechecks and
about 55 ms CPU, so both rechecks and wakeups are hard gates.

With least-loaded scaling, the three-run separate-process medians are
12,552 calls/s at 8 callers, 12,253 at 32 and 11,543 at 128. Four and eight
callers use 2 and 4 client connections; 32 and 128 remain capped at 4, a
96.875% socket reduction at 128 callers. Eight-caller p50/p99 are
0.602158/1.832874 ms, and all three calibration p99s stayed at or below
2.734274 ms.

`rust_service` uses the same standalone Rust SDK client and native transport
against both handlers. Three-run medians:

| Workload | Python handler | Rust handler |
|---|---:|---:|
| Raw no-op p50 | 0.174334 ms | **0.097581 ms** |
| 64 KiB p50 | 0.339040 ms | **0.216983 ms** |
| Typed payload p50 | 0.181216 ms | **0.098363 ms** |
| 32-item batch | 0.364817 ms | **0.114883 ms** |
| 8 callers | 14,233/s | **56,090/s** |
| 32 callers | 13,703/s | **114,551/s** |
| 128 callers | 13,184/s | **129,489/s** |

Both sides use four client connections at 8/32/128 callers. Rust handlers run
directly on Tokio; Python handlers use the compatibility adapter on the same
listener and transport.

`rust_discovery` compares the public Rust SDK's Arc-backed views with the old
owned `Member::members()` clone. Across 100/1,000/5,000 members, the three-run
median snapshot creation time stays near 0.00007 ms. Materializing 5,000
`MemberRef` values takes about 0.0778 ms versus 2.518 ms for complete
member/state clones, a **34.1x** speedup. Warm filtered count is about
0.00027 ms and pick about 0.00033 ms. Build it with
`cargo build --release -p tinyray --example rust_discovery_bench`.

`rpc_copy_profile` separates payload cloning, envelope encoding, and decoding.
After the server switched to a borrowed MessagePack request, decoding a 64 KiB
payload and making its single Arc-owned copy takes 1.693 us versus 81.782 us
for the old owned decode, about **48.5x** faster. At 1 MiB the figures are
30.136 and 59.911 us, about **2.0x**. In three end-to-end runs, ordinary
64 KiB RPC moved from the 0.4443 ms baseline to 0.4104 ms, while the Rust
handler's 64 KiB path moved from 0.2170 to 0.1786 ms. Wire and public APIs are
unchanged.

`rust_runtime` counts the native workers of a serving Rust member directly:
membership keeps two workers, while RPC client and server share four, for
**six total**. Before sharing this was 2 + 4 + 4, or ten. Three cold joins
ranged from 0.94 to 1.18 ms with a 1.02 ms median. The public `RpcRuntime`
also lets applications explicitly share a worker pool across clients and
servers.

`blobref` reports creation separately from reused-reference calls and direct
mapped access. Three-run medians:

| 16 MiB topology | Ordinary bytes call | Reused BlobRef call | Blob wire |
|---|---:|---:|---:|
| Python same process | 24.446 ms | **0.329 ms** | 166 B |
| Python separate process | 24.630 ms | **0.311 ms** | 167 B |
| Rust same process | 160.679 ms | **0.150 ms** | 165 B |
| Rust separate process | 63.360 ms | **0.179 ms** | 168 B |

The ordinary MessagePack argument is 16,777,236 bytes. BlobRef creation is a
separate one-time copy (about 7 ms in Python and 12.8 ms in the Rust
same-process calibration); reused calls and mapped access are nearly
size-independent.

For registry-only work, build and run the portable
`crates/tinyray-registry/examples/perf_registry.rs` example. Its output
distinguishes owned acknowledgment assembly from native framed
MessagePack/shared-response costs; do not report the former as end-to-end
throughput.

### Native method RPC hard-cutover measurements (0.18)

Python 3.11.15 on a 24-vCPU AMD EPYC host, comparing three release-mode runs
of the published 0.17.0 wheel (HTTP/JSON) with three runs of 0.18 native
TCP/MessagePack. Runs were sequential on the same host. The machine was shared,
with one-minute load averages between 12.7 and 16.5, so these numbers are
recorded and reproducible but not presented as idle-machine calibration.

| Scenario (p50 unless noted) | 0.17 HTTP | 0.18 native | Change |
|---|---:|---:|---:|
| Same-process call | 0.7735 ms | **0.2326 ms** | -69.9% |
| Separate-process call | 0.6982 ms | **0.2705 ms** | -61.3% |
| Async call | 1.4525 ms | **0.3675 ms** | -74.7% |
| Raising call | 0.9130 ms | **0.4289 ms** | -53.0% |
| 32-call batch total | 1.3109 ms | **0.4841 ms** | -63.1% |
| 1 / 4 / 8 concurrent callers | 1,342 / 1,204 / 1,019 per s | **3,175 / 6,588 / 6,383 per s** | +137% / +447% / +526% |
| Plain model-shaped dict | 0.8211 ms | **0.3417 ms** | -58.4% |
| Dataclass | 0.8406 ms | **0.2719 ms** | -67.7% |
| 64 KiB string echo | 1.0201 ms | **0.4470 ms** | -56.2% |

The first hard-cutover measurement's 64 KiB +105.3% regression came from
extracting Python bytes directly into `Vec<u8>` at the Python/Rust boundary.
After all three extraction boundaries moved to `PyBackedBytes`, the three-run
median is 0.3758 ms. Three isolated runs after the resource/protocol fixes had
a 0.4470 ms median with the final MemberRef/Pydantic-free build, still 56.2%
faster than 0.17 and within the existing regression
gate. The 1 MiB warning remains advisory;
data-plane payloads should still travel by reference.

0.18 no longer has a Pydantic benchmark or extra. The refreshed three-run
`rpc_models` medians cover plain values, standard dataclasses,
TypedDict/NamedTuple containers, and a 100-item dataclass batch. Encoding
plain/dataclass/typed-container values took 0.641/0.742/0.871 us; restoring a
dataclass and typed container took 0.772/1.052 us. Full RPC p50 was
0.2675/0.2719/0.2763 ms, and the 100-item batch was 0.4934 ms. The removed
Pydantic workload and the new dataclass batch do not have directly comparable
payload sizes.

A later full audit found that BlobRef hardening made ordinary values with no
BlobRef unconditionally create thread-local encode/decode scopes. The codec
now takes an extension-free fast path and retries under the bounded scope only
when it encounters a MessagePack extension. Five-run medians are
0.681/0.781/0.911 us for plain/dataclass/typed encoding and 0.862/1.142 us for
dataclass/typed restoration; complete RPC p50 is
0.2535/0.2605/0.2629 ms. All five fixed-cost codec metrics are now benchmark
gates.

### Native Snapshot/Epoch view measurements

Measurements on the same shared 24-vCPU host with the same 0.18 native RPC
code. The original architecture results are three-run medians; the final
state-path results are five-run medians. A 5,000-member `snapshot()` now
creates only the cached Arc-backed native view; `snapshot().members` measures
first Handle materialization separately.

| Operation | Before | After | Change |
|---|---:|---:|---:|
| Create 5,000-member `snapshot()` | 4.0072 ms | **0.001032 ms** | -99.97% |
| Materialize all 5,000 Handles | 4.0072 ms | **1.3778 ms** | -65.6% |
| 5,000-member `all()` | 4.0622 ms | **1.3480 ms** | -66.8% |
| Frozen-view `len()` at 5,000 | - | **0.000230 ms** | new metric |
| Frozen-view `slot()` / `get()` at 5,000 | - | **0.000712 / 0.000611 ms** | new metrics |
| Create 5,000-member `epoch()` | - | **0.001664 ms** | new metric |
| Create 1,000-member `epoch()` | 0.7953 ms | **0.001648 ms** | -99.8% |
| 5,000-member `pool.slot()` / `pick()` | 0.002144 / 0.002214 ms | **0.001353 / 0.001373 ms** | faster |

Handle materialization does not copy state. On first access to all 5,000 state
values, roster-wide encoding and one decode bring total `all()` / snapshot
time to 4.8658 / 5.0067 ms; reading the real state slots again takes only
0.1648 / 0.2499 ms. Against the user's five-run 0.17 medians, those four paths
are 4.9%, 4.8%, 5.8%, and 3.9% faster, while Handle-only paths remain about
67% faster. The bounded scalar index reduces an already-satisfied
`wait(count=1, idx=...)` at 5,000 members from 0.4823 to 0.003246 ms; nested
or container filters still use the exact O(n) scan. After RPC multiplexing,
ordinary p50/p99 are 0.263066/0.399071 ms, 64 KiB p50/p99
0.444255/0.731057 ms, a 32-item batch 0.564224 ms, and separate-process
eight-caller throughput 12,552 calls/s on four connections.

## The baseline

The current format-2 baseline is the per-metric median of three 0.18 release
runs on the shared host described above. Its calibration metadata records
`host_idle: false` and the observed load range. A final independent run passed
all 60 watched metrics. Relative tolerance remains 20%, combined with
calibrated absolute floors; the sub-millisecond `flush()` floor is 0.1 ms.

### Earlier measured optimization results (0.16)

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
0.73 us. A separate native-wire probe with a shared roughly 1 MiB roster improved
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
