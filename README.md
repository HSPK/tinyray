# tinyray

A minimal, actor-only Ray for ML experiments. Rust core, Python API.

```python
import tinyray as tr

@tr.remote(num_gpus=1, max_restarts=3)
class Rollout:
    def __init__(self, cfg):
        self.env = make_env(cfg)
    def step(self):
        return self.env.rollout()          # ~10 MB

rollouts = tr.create_actors(Rollout, cfg, count=32)   # atomic, all or nothing
refs = [r.step.remote() for r in rollouts]            # returns immediately
ready, pending = tr.wait(refs, num_returns=24)        # 24 finished; 8 still running
learner.update.remote(ready)                          # data goes rollout -> learner
```

`wait` returns two lists of `ObjectRef`, never values: `ready` are the ones that
have settled, `pending` the ones still running. Both are just names -- a task id
and the address of the actor holding the result -- so the driver moves a few
dozen bytes per reference and never the payload. Passing `ready` to the learner
hands over those names, and the learner fetches each 10 MB batch straight from
the rollout that produced it.

Dropping the eight slow rollouts drops their *results*, not their work: they are
still running, and their outputs still occupy the producers' stores until the
watermark or TTL reclaims them. And if the group runs NCCL collectives, those
same eight actors must still attend the next barrier -- see `tinyray.collective`.

## What it is, and is not

**tinyray is an HTTP control plane. The data plane belongs to the framework.**

SGLang, vLLM, Megatron and `torchrun` already do distributed compute well. What
they want from a cluster manager is placement, rank assignment, supervision and
restart -- and then to be left alone.

* **Actors only.** No stateless tasks. ML workers hold models and CUDA contexts.
* **HTTP for control.** At 200 ms per call a ~200 us round trip is 0.1%
  overhead, and every message is inspectable with ordinary tools.
* **It never claims a process-global resource.** A process has one default
  `torch.distributed` group, one CUDA context, one set of signal handlers. Those
  belong to your framework. tinyray assigns ranks and injects the `torchrun`
  environment; you call `init_process_group` yourself.
* **It supervises processes it did not write.** An SGLang server is an ordinary
  process: give it GPUs, wait until it actually serves, label its logs, restart
  it when it dies.
* **No object store.** For pure-Python rollouts a result stays in the actor that
  produced it and consumers fetch it directly. When a framework owns the data,
  tinyray moves references and nothing else.

```python
@tr.remote(num_gpus=1)
class Trainer:
    def __init__(self):
        import torch.distributed as dist
        dist.init_process_group(backend="nccl")   # yours, not tinyray's

group = tr.create_worker_group(Trainer, size=8, name="trainer")
group.run("train_step", batch)          # dispatched to all ranks, then awaited

server = tr.launch_process(              # not a tinyray actor at all
    ["python", "-m", "sglang.launch_server", "--port", "{port}"],
    name="rollout", num_gpus=4, ready_when="http:/health",
)
```

Two details that are load-bearing rather than cosmetic:

* **Constructors run concurrently.** A framework that rendezvous in `__init__`
  blocks rank 0 until the last rank arrives, so constructing a group serially
  deadlocks on the first worker.
* **Readiness is observed, not assumed.** An inference server binds its port
  minutes before it can answer; `ready_when="http:/health"` waits for the
  second event, not the first.

`tinyray.collective` still exists for pure-tinyray NCCL groups, but it takes the
default process group and therefore **cannot coexist with Megatron or SGLang**.
Use `create_worker_group` unless nothing else in the process wants that group.

## Why Rust

An actor running a 200 ms training step holds the GIL. If the data path were
Python, it could not serve result fetches during that time, and 32 actors
fetching from each other would degrade to serial. The Rust core removes that
coupling entirely:

| 10 MB decode | idle | 4 GIL-bound threads | slowdown |
|---|---|---|---|
| native thread (the real serving path) | 0.37 ms | 0.38 ms | **1.04x** |
| initiated from Python | 0.75 ms | 36.7 ms | **49x** |

Which also yields an architectural rule: the serving path must be driven by
tokio, never by a Python-side loop. Work *initiated from Python* inherits GIL
scheduling latency no matter how little of it runs in the interpreter, so
`/task/fetch` never enters the interpreter at all.

## Install

```bash
pip install tinyray
```

Wheels are built per interpreter version (3.9-3.13) for Linux and macOS on both
x86_64 and aarch64. There is no abi3 build: the buffer protocol that makes
results zero-copy is absent from the limited API on older interpreters, and
version-specific wheels are a cheap price for it.

The package ships `py.typed` and hand-written stubs for the Rust extension, so
type checkers see the full API.

### From source

Requires a Rust toolchain.

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/maturin develop --release
pre-commit install
```

## Tests

```bash
./scripts/test.sh                    # everything
cargo test --workspace               # 120 Rust tests
pytest tests/ -q                     # 216 Python tests
pytest benchmarks/ -q -s -m bench
python scripts/mutate.py             # 19 mutants: do the tests actually work?
```

### Testing the tests

A green suite proves nothing about the tests. Three mechanisms keep them honest:

* `tests/test_suite_quality.py` -- structural checks encoding the six blind
  spots that let real bugs through: unwired modules, timing constants that only
  ever run at their production value, options accepted but ignored, collapsed
  error taxonomies, untested lock branches, and design claims with no test.
* `tests/test_driver_byte_budget.py` -- parks a 32 MB result in an actor,
  exercises every driver-side operation and asserts what crossed the driver's
  wire. The design's central claim is that payloads move between actors and
  never through the driver; this is where that claim is enforced for *every*
  operation rather than one.
* `scripts/mutate.py` -- deliberately breaks 21 invariants and reports which
  ones the suite catches. It has already found three tests that passed while
  the behaviour they claimed to check was disabled.

Each check exists because a real bug got through in that exact shape.

## Development

`pre-commit install` wires up the gates that CI also runs:

| hook | what it guards |
|---|---|
| `ruff check` / `ruff format` | Python lint and formatting |
| `cargo fmt` / `cargo clippy -D warnings` | Rust formatting and lint |
| `mypy` | the public API's annotations, checked against the stubs |

Releases are cut by pushing a `v*` tag; `.github/workflows/release.yml` runs the
full suite, builds the wheel matrix and an sdist, and publishes to PyPI through
a trusted publisher. `workflow_dispatch` targets TestPyPI by default so a dry
run cannot reach the real index by accident.

## Operations

```bash
tinyray status 127.0.0.1:41234 127.0.0.1:41235   # what is each actor doing?
tinyray introspect 127.0.0.1:41234               # raw report
tinyray health 127.0.0.1:41234
```

`status` calls out stalled callers, evictions, backpressure and stragglers,
because "which actor is stuck, and on what?" is the question that dominates
distributed ML debugging.

## Layout

```
crates/tinyray-core/      wire protocol, framing, identifiers
crates/tinyray-runtime/   store, ordered queue, transport, cluster, collective, shm
crates/tinyray-py/        PyO3 bindings (all unsafe lives in buffers.rs)
python/tinyray/           API, serde, launcher, head, collective, pool, CLI
```
