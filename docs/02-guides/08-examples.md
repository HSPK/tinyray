# Examples

## Purpose

Three runnable programs in [`examples/`](../../examples/). Each one is a
complete stack — native workers, real collectives, real payloads — with tinyray
doing only the coordinating.

They run on CPU with gloo and numpy, so they work on a laptop. Turning them into
the real thing means changing command lines and `num_gpus`, not the control-plane
code.

All three are executed by `tests/test_examples.py`, which parses their output
and asserts the numbers they print. An example here cannot quietly stop working,
and cannot quietly stop being true.

```bash
python examples/native_stack.py
python examples/dataloader_sidecars.py
python examples/rl_control_plane.py
```

---

## `native_stack.py` — a trainer and an inference server

The smallest complete stack: a four-rank DDP trainer and an HTTP inference
server, neither of which tinyray wrote.

- the trainer calls its own `init_process_group`
- the server is a process tinyray only supervises — readiness is *observed*
  with `ready_when="http:/health"`, not assumed
- weights move between them however the frameworks prefer

Read it for the shape. It is the SGLang + Megatron pattern with the command
lines swapped for toys, and the substitutions are written in the comments.

**Prints:** control traffic through the driver, which is a few kilobytes.

---

## `dataloader_sidecars.py` — the mesh

A real `torch.utils.data.DataLoader` — with its own worker processes — feeding a
real DDP trainer, connected by sidecars.

```
loader process 0..3                     trainer process 0..1
┌──────────────────────────┐            ┌──────────────────────────┐
│ torch DataLoader         │            │ model + optimiser        │
│          ▲ local         │            │          ▲ local         │
│  ┌───────┴────────┐      │            │  ┌───────┴────────┐      │
│  │ tinyray sidecar│◄─────┼── direct ──┼─►│ tinyray sidecar│      │
│  └────────────────┘      │            │  └────────────────┘      │
└──────────────────────────┘            └──────────────────────────┘
```

tinyray is neither the dataloader nor the trainer. Each is the framework's own
object, in a process the framework owns. tinyray is the connector.

What it demonstrates:

**The driver leaves.** It places two fleets, calls `tr.link` once, and says
"go". Measured over the whole training loop: **868 bytes** through the driver
while 13.6 MB moved between sidecars.

**Discovery.** The trainer sidecar asks `tinyray.peers("loader")` who its
loaders are and takes `[rank::world_size]`. No endpoints on any command line,
and nothing in the worker knows the driver's address.

**Both directions.** Trainer → loader: `set_epoch` at an epoch boundary, and
the pull itself. Loader → trainer: the batches.

**The DataLoader is real.** `num_workers=2, persistent_workers=True`, so each
loader process forks its own workers — eight in total, none of them tinyray's.

**Prints:** batches served, epoch boundaries pushed, forked worker processes,
MB peer-to-peer against bytes through the driver.

### Why the loss is reported in halves

Each rank alternates between its shards, and the shards sit at different
offsets, so a single batch's loss says more about which shard it came from than
about the model. The first version of this example reported first-batch versus
last-batch and appeared to diverge; the numbers turned out to be exactly
`1 + shard²`. Averaging over halves removes the confound.

A reminder that a metric which moves is not the same as a metric that means
something.

## `rl_control_plane.py` — actor-learner RL

Eight rollout workers and a two-rank learner, running the loop every on-policy
algorithm runs.

```
rollout 0..7 ──── trajectories (direct) ────► learner rank 0..1
      ◄────────── weights (NCCL / CUDA IPC / disk) ──────┘

      driver: a few hundred bytes, and the word "go"
```

What it demonstrates:

**Stragglers.** `wait(num_returns=6)` takes the first six of eight. The rollout
workers are deliberately heterogeneous — cost grows with rank — so ranks 6 and 7
straggle every iteration, reproducibly.

Dropping a straggler drops its *result*, not its work. It is still running, and
its output still occupies memory on the producer until released.

**Stragglers still attend the weight sync.** The reload is sent to
`rollouts[0..7]`, not to the six that were fast. Dropping a result is free;
dropping a rank from a collective is a hang.

**The weights never touch tinyray.** Here they go through a file, because that
runs anywhere. In a real stack it is an NCCL broadcast or a CUDA IPC handle.
tinyray's part is identical either way: it says *when*, not *how*.

**Prints:** trajectories collected, who straggled, the policy version, and the
reward climbing as the published policy reaches the fleet.

### The bug this example was written with

`publish` writes the checkpoint on rank 0 and ends with `dist.barrier()` — the
barrier is what makes "the checkpoint is complete" true rather than hopeful.

It was first called with `run_on(0, "publish", ...)`. Rank 0 entered the barrier
and waited forever for rank 1, which had never been asked. The whole example
hung with no error.

That is design principle 5 exactly, in code written by someone who had just
finished documenting design principle 5. **Any method containing a collective
goes through `run`.** The comment in the source now says why.

---

## Turning these into a real stack

| In the example | In production |
|---|---|
| `[sys.executable, str(trainer_path)]` | `[sys.executable, "pretrain_gpt.py", "--tensor-model-parallel-size", "8", ...]` |
| `[sys.executable, str(server_path), "{port}"]` | `["python", "-m", "sglang.launch_server", "--model-path", MODEL, "--port", "{port}", "--tp", "4"]` |
| `gpus_per_worker=0.0` | `gpus_per_worker=1.0` |
| gloo | nccl |
| `np.save` / `np.load` | NCCL broadcast, or CUDA IPC |
| a synthetic `Dataset` | webdataset, a memmapped corpus, an index file |

The controller code — placement, dispatch, `wait`, `release`, the reload signal
— does not change. That is the claim these examples exist to support.

## Pitfalls

**These are not benchmarks.** Timings are CPU, gloo and one machine. They
demonstrate a shape, and the ratios hold; the absolute numbers do not transfer.

**The generated `_generated_*.py` files are temporary.** They are written at
startup and removed in a `finally` block. A test asserts none survive.

**`run_on` is not safe for a method with a collective in it.** See above. It
exists for genuinely single-rank work, such as reading a metric.

**A worker's executor is single-threaded.** While a sidecar runs a long method,
peer calls queue behind it. The mesh example has the consumer drive the loop for
that reason: pulling needs no backpressure protocol.

## See also

- [02-native-frameworks.md](02-native-frameworks.md) — the API these examples use
- [03-actors.md](03-actors.md) — `create_actors`, `wait`, `release`
- [05-fault-tolerance.md](05-fault-tolerance.md) — what happens when a worker dies
- [07-mesh.md](07-mesh.md) — the peer API the dataloader example uses
- [05-testing.md](../04-internals/05-testing.md) — why the examples are executed, not just linted
