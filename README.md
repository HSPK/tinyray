# tinyray

*[中文](README.zh-CN.md)*

**A phone book and a roll call.** A membership layer for asynchronous ML jobs.

It starts no processes, allocates no GPUs and moves no tensors. It answers
three questions: **who is here, are they still alive, and who should I talk
to?**

```python
import tinyray

me = tinyray.join("engine", "serving")
me.ready(model_version=17)

engine = tinyray.pool("engine").pick(model_version=17)
print(engine.url)
```

## What it does

Reporting in, leases, a local roster cache and lookup -- plus a call layer:

```python
class Collector:
    def assign(self, task: str) -> dict:
        return {"took": task}


me = tinyray.join("collector", "stateful", slot=0, serves=Collector())
me.ready()

# in another process
tinyray.pool("collector").slot(0).assign("task-7")
await tinyray.apool("collector").slot(0).assign("task-7")
```

Underneath it is ordinary HTTP, so nothing is lost for debugging with `curl`:

```bash
curl -X POST http://host:port/call/assign -d '{"task":"t"}'
curl http://host:port/_methods
```

Seats, tenures and a frozen roster:

```python
me = tinyray.join("trainer", "collective", slot=RANK, size=WORLD_SIZE)
me.ready()

ep = tinyray.pool("trainer").epoch()  # wait for everyone, then freeze
build_process_group(ep.members)       # every rank is handed the same list


def watchdog():  # checking inside the training loop is useless: a stuck rank
    while ep.valid:  # never reaches that line. A background thread works,
        time.sleep(0.5)  # because NCCL releases the GIL while it blocks.
    pg._abort()
```

## Documentation

**<https://hspk.github.io/tinyray/>**

| Document | What is in it |
|---|---|
| [Getting started](docs/en/getting-started.md) | Ten minutes, from install to two processes calling each other |
| [API reference](docs/en/api.md) | The whole surface, written against the implementation |
| [Benchmarks](docs/en/bench.md) | What it costs, measured, and the traps in measuring it |

The design notes -- the problem, why existing tools do not fit, and the
reasoning behind the API -- are in Chinese only:
[为什么](docs/01-why.md) and [是什么](docs/02-design.md).

## Install

```bash
pip install tinyray                    # the registry ships in the wheel
tinyray --listen 127.0.0.1:8760
```

## Development

```bash
cargo build --release          # needs rustup's rustc; the system one is too old
maturin develop --release
pytest tests/ -q               # the default set
pytest tests/ -q -m examples   # the examples, a few minutes
cargo test --workspace         # the registry and the wire types
python bench.py                # the benchmark suite
python bench.py --check        # compare against the recorded baseline
python mutation_check.py       # put each bug back and prove a test goes red
mkdocs serve                   # the docs site, needs pip install mkdocs-material
```

Python tests are grouped by subsystem:

| Directory | Coverage |
|---|---|
| `tests/membership/` | Joining, readiness, seats, identity, cleanup and fork ownership |
| `tests/discovery/` | Cached lookups, filters, subscriptions and waiting |
| `tests/collectives/` | Epochs and roster fingerprints |
| `tests/registry/` | Wire contracts, admission, leases, ordering and network recovery |
| `tests/rpc/` | Calling, validation, HTTP, payloads, concurrency and call statistics |
| `tests/examples/` | Example programs and their domain logic |
| `tests/project/` | Public API, documentation and CI contracts |

Shared fixtures live in `tests/conftest.py`; registry processes and network
proxies live in `tests/support/`. Regression cases belong beside the feature
they protect, not in milestone or review-specific files. Select a subsystem
with, for example, `pytest tests/rpc/ -q`. Rust tests remain in their crates.

## How it is built

Around 2,900 lines: a Rust registry and client (`crates/`) behind a Python API
(`python/tinyray/`), wired together with pyo3 and maturin.

Every behaviour in here was measured before it was changed, and every one of
them has an entry in `mutation_check.py` -- put the bug back, and a named test
goes red. A test that cannot fail is not a test.
