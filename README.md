# tinyray

**A control-plane fabric for clusters you did not launch.**

> The design in `docs/` is a **proposal**. The published package implements an
> earlier design that does not reach the scale this one targets. See
> [docs/08-project/01-status.md](docs/08-project/01-status.md).

tinyray provides the mechanics that every large control plane ends up writing by
hand: logical slots with generations, leases that expire, desired state that
converges, discovery that does not grow with the cluster.

It allocates nothing, launches nothing, and never touches a tensor.

```python
# trainer.py -- launched by torchrun, srun, or a Kubernetes Job
import torch.distributed as dist
import tinyray

dist.init_process_group("nccl")          # yours
trainer = build_trainer()                # yours
tinyray.join(trainer, group="trainer")   # the only tinyray line; returns immediately
```

```python
# a controller, anywhere -- a login node, a notebook, another worker
cluster = tinyray.attach()
cluster.group("trainer").wait_ready(size=1024)

# a peer, inside a worker
tinyray.group("ingest").shard(my_dp, num_dp)[0].accept.remote(reference)
```

There is no `num_gpus`, no placement and no launcher. The scheduler did all of
that before tinyray was imported.

## Where it sits

| Layer | Owner |
|---|---|
| Domain: agents, trajectories, rewards | Your application |
| Application control: tasks, samples, model versions, checkpoints | Your application |
| **Control-plane mechanics: identity, membership, reconciliation, discovery** | **tinyray** |
| Resources and process lifecycle | Slurm, Kubernetes, Volcano, `torchrun` |
| Bulk transport: weights, samples | NCCL, UCX, NIXL, object storage |

The layer above is your product. The layers below are solved. The one in the
middle is written by hand, over and over — one design reviewed for this work
contained **fifteen** identity types, each needing its own generation and
fencing check.

## Why the redesign

The previous design assumed tinyray started the processes, assigned the GPUs and
sat at the centre of every message. Measured on this repository:

| Problem | Measurement |
|---|---|
| Roster push is quadratic | 2.3 GB out of one process at 8,192 workers |
| Fan-out is serial | 233 µs per worker — 2.3 s at 10,000 |
| Liveness by supervision | Impossible when Slurm started the job |
| Per-worker leases in consensus | Kubernetes tops out at 5,000 nodes doing exactly this |

None is fixable by making the code faster. The full analysis is in
[docs/01-overview/01-problem.md](docs/01-overview/01-problem.md).

## Documentation

Start with [docs/](docs/). Sections and files are numbered in reading order.

| Section | Contents |
|---|---|
| [01-overview](docs/01-overview/) | What broke, where tinyray sits, seven principles |
| [02-architecture](docs/02-architecture/) | Layering, topology, state model, plane split |
| [03-modules](docs/03-modules/) | Identity, membership, reconciliation, readiness, discovery, admission, transport, supervision |
| [04-protocols](docs/04-protocols/) | Wire format, membership, control RPC |
| [05-operations](docs/05-operations/) | Deployment, failure model, observability |
| [06-testing](docs/06-testing/) | Testing standard, fake cluster, chaos |
| [07-reference](docs/07-reference/) | API, configuration |
| [08-project](docs/08-project/) | Status, decisions, roadmap |

The shortest useful path is three pages:
[positioning](docs/01-overview/02-positioning.md) →
[layering](docs/02-architecture/01-layering.md) →
[status](docs/08-project/01-status.md).

## Why Rust

One measurement decided it. Decoding 10 MB while four GIL-bound Python threads
run costs **1.04x** from a native thread and **49x** when initiated from Python.
Same code; the difference is who holds the GIL when the work starts.

So the serving path is tokio-driven and never needs the GIL: a worker saturated
by its own framework still answers control messages, which is exactly when you
need it to.

## Development

```bash
export PATH="$HOME/.cargo/bin:$PATH"
python3.11 -m venv .venv
.venv/bin/pip install -e ".[test,dev]"
.venv/bin/maturin develop --release
scripts/test.sh          # exactly what CI runs
```

## Testing

The suite is built around seven failure categories that each produced a real
bug — see [docs/06-testing/01-standard.md](docs/06-testing/01-standard.md). Two
rules matter more than the rest:

- **Assert the cost, not only the result.** A readiness check that fetched the
  payload took 237 ms instead of 0.14 ms, and every functional test passed.
- **Availability claims need two instances and a kill.** A registry with a
  shared identity passed every single-replica test while two replicas were
  permanently broken.

```bash
cargo test --workspace --release
.venv/bin/python -m pytest tests/ -q
.venv/bin/python scripts/mutate.py
```

## Layout

```
crates/tinyray-core/      wire protocol, framing, identifiers
crates/tinyray-runtime/   transport, queue, store
crates/tinyray-py/        PyO3 bindings (all unsafe lives in buffers.rs)
python/tinyray/           membership, discovery, readiness, admission, supervision
docs/                     the proposal
```

## Status

Not production software. It is a design with a working transport underneath it
and an honest inventory of what has never been run —
[docs/08-project/01-status.md](docs/08-project/01-status.md).
