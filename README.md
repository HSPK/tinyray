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

## Documentation

Full documentation is in [`docs/`](docs/).

| Section | For |
|---|---|
| [Concepts](docs/01-concepts/) | What tinyray is, and why it stops where it does |
| [Guides](docs/02-guides/) | Getting started, native frameworks, actors, placement, fault tolerance, observability |
| [Reference](docs/03-reference/) | Python API, protocol, CLI, every configuration default |
| [Internals](docs/04-internals/) | Rust core, store and queue, transport, scheduler, testing |
| [Project](docs/05-project/) | Honest status, decisions and reversals, roadmap |

New here? Start with [getting started](docs/02-guides/01-getting-started.md), then
[native frameworks](docs/02-guides/02-native-frameworks.md).

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

### Minimal intrusion: your script keeps its own process

The least invasive option, and the one to reach for with Megatron, DeepSpeed or
anything else that expects to own its entrypoint. The script is unchanged apart
from one line:

```python
# train.py -- your ordinary training script
import torch.distributed as dist

class Trainer:                                # not decorated, not subclassed
    def __init__(self):
        dist.init_process_group(backend="nccl")   # yours, not tinyray's
        self.model = build_model()                # yours
    def train_step(self, batch): ...

if __name__ == "__main__":
    import tinyray
    tinyray.serve(Trainer())                  # the only tinyray line
```

```python
# controller.py
workers = tr.launch_workers(["python", "train.py"], size=8, gpus_per_worker=1)
workers.run("train_step", batch)              # all ranks, then awaited

server = tr.launch_process(                   # a process tinyray never imports
    ["python", "-m", "sglang.launch_server", "--port", "{port}", "--tp", "4"],
    name="rollout", num_gpus=4, ready_when="http:/health",
)
```

Nothing is pickled to the worker, no class is shipped over the wire, and
`__main__` stays yours. `tinyray.connect(endpoint)` will even drive a process
that something else started.

Three runnable stacks live in [`examples/`](examples/), all on CPU with gloo so
they work without a GPU:

| Example | What it shows |
|---|---|
| `native_stack.py` | A four-rank trainer and an inference server, end to end |
| `dataloader_sidecars.py` | A real torch DataLoader and a real DDP trainer, connected by sidecars: 13.6 MB peer to peer against 868 bytes through the driver |
| `rl_control_plane.py` | Actor-learner RL: stragglers dropped without being abandoned, weights swapped without touching tinyray |

They are executed by the test suite, which parses their output and asserts the
numbers they print. See [the guide](docs/02-guides/08-examples.md).

### The actor API

Still available, and the right choice for code written for tinyray -- pure
Python rollouts, evaluation harnesses, hyperparameter trials:

```python
@tr.remote(num_gpus=1)
class Rollout:
    def step(self): return self.env.rollout()

rollouts = tr.create_actors(Rollout, count=32)
```

Here tinyray owns the process and constructs your class remotely, which is
convenient and unavoidably more invasive.

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
crates/tinyray-runtime/   store, ordered queue, transport, cluster, collective
crates/tinyray-py/        PyO3 bindings (all unsafe lives in buffers.rs)
python/tinyray/           API, serde, launcher, head, collective, pool, CLI
docs/                     concepts, guides, reference, internals, project status
```
