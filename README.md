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
ready, stragglers = tr.wait(refs, num_returns=24)
learner.update.remote(ready)                          # data goes rollout -> learner
```

## What it is, and is not

Built for one workload: RL rollouts and hyperparameter sweeps, ~32 actors,
~10 MB payloads, ~200 ms per call.

* **Actors only.** No stateless tasks. ML workers hold models and CUDA contexts.
* **HTTP for control and results.** At 200 ms per call a ~200 µs round trip is
  0.1% overhead, and every message is inspectable with ordinary tools.
* **No object store.** A result stays in the actor that produced it; consumers
  fetch it directly. No plasma, no spilling, no distributed refcounting.
* **NCCL for weights.** tinyray implements no collective transport at all. It
  assigns ranks and manages the epoch state machine; `torch.distributed` moves
  the bytes.

Design notes, measurements and the decisions the implementation forced us to
revise are kept with the source rather than published here.

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

A green suite proves nothing about the tests. Two mechanisms keep them honest:

* `tests/test_suite_quality.py` -- structural checks encoding the six blind
  spots that let real bugs through: unwired modules, timing constants that only
  ever run at their production value, options accepted but ignored, collapsed
  error taxonomies, untested lock branches, and design claims with no test.
* `scripts/mutate.py` -- deliberately breaks 19 invariants and reports which
  ones the suite catches. It has already found two tests that passed while the
  behaviour they claimed to check was disabled.

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
