# Native frameworks

## Purpose

Running Megatron, DeepSpeed, SGLang, vLLM or `torchrun` under tinyray, with the
framework keeping everything it expects to own. This is the main line.

## Concepts

Three ways to attach, in increasing order of intrusion. Pick the first one that
fits.

| Your workload | Use | You change |
|---|---|---|
| A server: SGLang, vLLM | [`launch_process`](#supervising-a-server) | nothing |
| A `torchrun` job | [`launch_process`](#supervising-a-torchrun-job) | nothing |
| A training script you can edit | [`serve`](#attaching-to-a-training-script) | one line |
| Something already running | [`connect`](#connecting-to-something-already-running) | nothing |

What tinyray provides in every case: placement against a single resource table,
the `torchrun` environment, readiness detection, labelled logs, restart, and
resource reclamation.

What tinyray never does: call `init_process_group`, import your model code, or
touch a tensor.

## Supervising a server

The zero-intrusion case. The process does not know tinyray exists.

```python
import tinyray as tr

tr.init()

server = tr.launch_process(
    ["python", "-m", "sglang.launch_server",
     "--model-path", "meta-llama/Llama-3-8B",
     "--port", "{port}",
     "--tp", "4"],
    name="rollout",
    num_gpus=4,
    ready_when="http:/health",
    startup_timeout=900,
)

print(server.endpoint)   # 127.0.0.1:41234 — now send it requests
```

`{port}` in the command, or in any environment value, is replaced with a free
port tinyray allocated. You do not have to find one yourself.

### Readiness is observed, not assumed

An inference server binds its port minutes before it can answer, because it is
loading weights. A controller that starts sending requests in that window sees
connection refused and concludes the cluster is broken.

| `ready_when` | Ready when | Use for |
|---|---|---|
| `"http:/health"` | The endpoint returns 200 or 204 | Inference servers |
| `"port"` | Something accepts connections | Servers with no health route |
| `"log:pattern"` | A line matches the regex | Anything that announces itself |
| `"alive"` | The process is running | Batch jobs. Honest, but weak |

The default is `"alive"`. For anything that serves requests, pass something
stricter.

### When startup fails

The error carries the child's output, because that is the only artefact worth
having:

```
ProcessStartupError: rollout exited with code 1 before it was ready.
command: python -m sglang.launch_server --model-path ... --port 41234
--- last output ---
torch.OutOfMemoryError: CUDA out of memory. Tried to allocate 2.00 GiB
```

## Supervising a torchrun job

`torchrun` is an ordinary process, so it needs nothing special:

```python
job = tr.launch_process(
    ["python", "-m", "torch.distributed.run",
     "--nproc_per_node", "8",
     "--master_port", "{port}",
     "train.py"],
    name="pretrain",
    num_gpus=8,
    ready_when="log:training started",
    startup_timeout=1800,
)
```

`torchrun` spawns one child per GPU. tinyray starts it in its own process group
and signals the whole group on stop, so nothing is orphaned. Without that, every
restart would strand eight processes holding GPU memory.

## Attaching to a training script

For a script you can edit, one line buys an RPC channel. The script keeps its
entrypoint, its imports, its `init_process_group` and its model.

```python
# train.py — your ordinary training script
import torch.distributed as dist


class Trainer:                                  # not decorated, not subclassed
    def __init__(self):
        dist.init_process_group(backend="nccl")   # yours, not tinyray's
        self.model = build_model()                # yours
        self.optimizer = build_optimizer(self.model)

    def train_step(self, batch):
        loss = self.model(batch).backward()
        self.optimizer.step()
        return {"loss": float(loss)}

    def save_checkpoint(self, path):
        torch.save(self.model.state_dict(), path)


if __name__ == "__main__":
    import tinyray

    tinyray.serve(Trainer())                    # the only tinyray line
```

Then from the controller:

```python
workers = tr.launch_workers(
    ["python", "train.py"],
    size=8,
    name="trainer",
    gpus_per_worker=1,
)

for step in range(1000):
    losses = workers.run("train_step", batch)   # all ranks, then awaited
    if step % 100 == 0:
        workers.run_on(0, "save_checkpoint", f"ckpt-{step}.pt")
```

`run` dispatches to every rank before awaiting any of them. That is mandatory,
not an optimisation: a collective inside `train_step` only returns once all
ranks have entered it, so awaiting rank 0 first would deadlock.

`run_on` targets one rank, and is safe only for methods that stay out of
collectives.

### What tinyray injects

Exactly what `torchrun` would set, so your script cannot tell the difference:

```
RANK, WORLD_SIZE, LOCAL_RANK, LOCAL_WORLD_SIZE,
GROUP_RANK, GROUP_WORLD_SIZE, MASTER_ADDR, MASTER_PORT
CUDA_VISIBLE_DEVICES            (the devices placement assigned)
TINYRAY_CONTROL_PORT            (where serve() binds)
```

`LOCAL_RANK` is derived from where the gang actually landed, not guessed.

### Readiness for a served script

The control port answering means the script reached `serve()`, which is after
its own setup. For a trainer that means the model is built and
`init_process_group` has returned — the moment it is genuinely ready to be
commanded.

## Connecting to something already running

For a worker started by a scheduler, an existing shell script, or a person:

```python
# somewhere else, by any means
# $ python train.py            → prints "[tinyray] serving control on 127.0.0.1:41234"

worker = tr.connect("127.0.0.1:41234")
result = tr.get(worker.train_step.remote(batch))
```

tinyray takes no responsibility for the lifecycle here. It just calls in.

## Putting it together

An RL post-training loop with a native trainer and a native inference server:

```python
trainer = tr.launch_workers(
    ["python", "train.py"], size=8, name="trainer", gpus_per_worker=1,
)

rollout = tr.launch_process(
    ["python", "-m", "sglang.launch_server", "--port", "{port}", "--tp", "4"],
    name="rollout", num_gpus=4, ready_when="http:/health",
)

for iteration in range(n):
    prompts = sample_prompts()
    completions = http_post(f"http://{rollout.endpoint}/generate", prompts)
    trainer.run("train_step", completions)
    trainer.run_on(0, "export_weights", "/shared/ckpt")
    http_post(f"http://{rollout.endpoint}/update_weights", "/shared/ckpt")
```

Weights move between trainer and server however the frameworks prefer — NCCL,
CUDA IPC, a checkpoint on shared storage. tinyray says *when*; the frameworks
decide *how*.

A runnable version is in [`examples/native_stack.py`](../../examples/native_stack.py).
It uses gloo and a toy HTTP server so it works without a GPU, and it reports how
many bytes crossed the driver: 2,163 for three iterations.

## Contract

**`launch_process(command, *, name, num_cpus, num_gpus, ready_when, env, allocate_port, startup_timeout, max_restarts, strategy, cwd, host)`**

Places, starts and waits for readiness. Returns a `ManagedProcess` with
`.endpoint`, `.pid`, `.port`, `.recent_log()`, `.is_alive()`.
Raises `PlacementFailed` if the cluster cannot host it, `ProcessStartupError` if
it dies or never becomes ready.

**`launch_workers(command, *, size, name, gpus_per_worker, cpus_per_worker, env, master_addr, master_port, strategy, startup_timeout, cwd)`**

Places a gang atomically, starts every rank before awaiting any, returns a
`WorkerGroup`. Raises `PlacementFailed` if the whole gang does not fit.

**`serve(target, *, background=False, bind=None, actor_id=None, max_pending_calls=1000)`**

Adds a control port to the current process and dispatches to `target`'s methods.
`target` may be an object, a dict of callables, or a module. **Blocks by
default.** Returns a `Server`.

**`connect(endpoint, actor_id=None)`** — returns a `RemoteWorker` for a process
that is already serving.

## Pitfalls

**A method that enters a collective must be called with `run`, not `run_on`.**
`run_on` reaches one rank; the others never enter the collective and the group
hangs.

**`serve()` blocks by default.** Pass `background=True` to keep your own loop —
but then control calls execute on a background thread while your loop runs on
another, and anything they share needs to be safe against that.

**`launch_workers` assumes a single node.** `LOCAL_RANK` is computed as if all
ranks are local. Multi-node needs node agents that report a routable address,
which is not built. See [status](../05-project/01-status.md).

**`launch_workers` does not restart ranks.** `launch_process` takes
`max_restarts`; the worker-group path does not yet, and a restarted rank would
not rejoin its process group anyway.

**Give inference servers a long `startup_timeout`.** Loading a large model takes
minutes; the default of 600 seconds is not always enough.

**Do not put SGLang inside an actor.** The executor is single-threaded, which
serialises requests and defeats continuous batching. Run it as its own server
behind `launch_process`.

## See also

- [04-placement.md](04-placement.md) — how resources and gangs are allocated
- [05-fault-tolerance.md](05-fault-tolerance.md) — restart and failure semantics
- [06-observability.md](06-observability.md) — when something is stuck
- [01-positioning.md](../01-concepts/01-positioning.md) — why the boundary is here
