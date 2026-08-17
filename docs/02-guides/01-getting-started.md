# Getting started

## Purpose

Install tinyray and run something, in both of its modes.

## Install

```bash
pip install tinyray
```

Wheels cover Python 3.9–3.13 on Linux (x86_64, aarch64) and macOS
(universal2). There is no abi3 build — the buffer protocol that makes results
zero-copy is missing from the limited API on older interpreters.

The package ships `py.typed` and hand-written stubs, so type checkers see the
full API.

### From source

Requires a Rust toolchain.

```bash
git clone https://github.com/HSPK/tinyray
cd tinyray
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/maturin develop --release
pre-commit install
```

## Your first actor

```python
import tinyray as tr

tr.init()


@tr.remote(num_cpus=1)
class Counter:
    def __init__(self, start=0):
        self.n = start

    def inc(self, by=1):
        self.n += by
        return self.n


counter = Counter.remote(10)
print(tr.get(counter.inc.remote(5)))   # 15
print(tr.get(counter.inc.remote()))    # 16

tr.shutdown()
```

Three things worth noticing:

- `Counter.remote(10)` starts a **separate OS process** and runs the constructor
  there.
- `.remote()` returns immediately with an `ObjectRef`; it does not wait for the
  method.
- `get()` blocks and raises whatever the method raised, with the remote
  traceback attached.

## Your first native worker

The mode most users want. The script keeps its own entrypoint:

```python
# worker.py
class Worker:
    def __init__(self):
        self.calls = 0

    def work(self, x):
        self.calls += 1
        return x * 2


if __name__ == "__main__":
    import tinyray

    tinyray.serve(Worker())
```

```python
# controller.py
import tinyray as tr

tr.init()

workers = tr.launch_workers(["python", "worker.py"], size=4, gpus_per_worker=0)
print(workers.run("work", 21))         # [42, 42, 42, 42]

tr.shutdown()
```

`Worker` is not decorated and is never pickled. tinyray started four processes,
gave each one a rank and a control port, and waited until they answered.

## Running a server

```python
server = tr.launch_process(
    ["python", "-m", "http.server", "{port}"],
    name="web",
    ready_when="port",
)
print(server.endpoint)
```

That process contains no tinyray code at all.

## Which mode do I want?

| If your code is | Use |
|---|---|
| A server you run as a subprocess | [`launch_process`](02-native-frameworks.md#supervising-a-server) |
| A training script you can edit | [`serve`](02-native-frameworks.md#attaching-to-a-training-script) |
| Written for tinyray from the start | [`@remote`](03-actors.md) |

When in doubt, choose the first row that applies. Later rows are more invasive.

## Verifying an install

```python
import tinyray as tr

tr.init()
print("cluster:", tr.nodes())
tr.shutdown()
```

You should see one node with your machine's CPU count and any GPUs
`nvidia-smi` reports.

## Pitfalls

**`tr.init()` is implicit but explicit is better.** Any API call starts the
runtime, but calling `init()` yourself makes the resource limits visible.

**`tr.shutdown()` stops everything tinyray started** — actors, workers and
managed processes, including their children. It is registered with `atexit`, so
a normal exit cleans up; a `kill -9` on your driver does not.

**Fractional CPUs are for tests, not honesty.** `num_cpus=0.1` lets a small
machine host many actors. It does not make them cheaper.

## See also

- [02-native-frameworks.md](02-native-frameworks.md) — the main line
- [03-actors.md](03-actors.md) — the actor API in full
- [04-placement.md](04-placement.md) — resources and gangs
