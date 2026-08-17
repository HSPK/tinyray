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
python examples/dataloader_to_trainer.py
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

## `dataloader_to_trainer.py` — the classic pipeline

Four loader actors feeding a four-rank DDP trainer.

```
loader 0..3 ──── batches (megabytes, direct) ────► rank 0..3
                          │
                       driver: references only
```

What it demonstrates:

**Prefetch.** `.remote()` returns immediately, so batch *k+1* is built while the
trainer works on *k*. The example runs the epoch twice, once serially and once
pipelined, and reports the speedup **against the theoretical ceiling** — because
"1.3x" means nothing until you know the ceiling was 1.36x.

**References, not values.** The driver never calls `get` on a batch. It hands
the trainer a list of references; each rank fetches its own shard. The list is
nested, so tinyray passes it through untouched — only top-level reference
arguments are resolved, which is exactly what stops all four ranks pulling all
four shards.

**`wait`, not `get`, for timing.** The example times the loader by waiting for
readiness rather than fetching. Using `get` there would drag 8 MB into the
driver to run a stopwatch — the precise anti-pattern the whole design is built
against.

**Release.** Consumed batches are dropped explicitly, and so are the ones
prefetching left over. A loader holding stale batches is memory paid for twice.

**Prints:** ~150 MB loader → trainer against ~90 KB through the driver.

### The asymmetry worth noticing

The trainer ranks read `RANK` from the environment, because torchrun's contract
supplies it. The loader actors cannot: `create_actors` is atomic, and atomicity
means one set of constructor arguments for the whole gang. Shard identity is a
second, cheap call.

---

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
| `time.sleep` in the loader | a real dataset |

The controller code — placement, dispatch, `wait`, `release`, the reload signal
— does not change. That is the claim these examples exist to support.

## Pitfalls

**These are not benchmarks.** Timings are CPU, gloo and one machine. They
demonstrate a shape, and the ratios hold; the absolute numbers do not transfer.

**The generated `_generated_*.py` files are temporary.** They are written at
startup and removed in a `finally` block. A test asserts none survive.

**`run_on` is not safe for a method with a collective in it.** See above. It
exists for genuinely single-rank work, such as reading a metric.

**One loader per rank is for clarity.** Real jobs usually have more loaders than
ranks and pull from a pool.

## See also

- [02-native-frameworks.md](02-native-frameworks.md) — the API these examples use
- [03-actors.md](03-actors.md) — `create_actors`, `wait`, `release`
- [05-fault-tolerance.md](05-fault-tolerance.md) — what happens when a worker dies
- [05-testing.md](../04-internals/05-testing.md) — why the examples are executed, not just linted
