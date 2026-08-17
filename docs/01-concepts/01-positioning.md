# Positioning

## Purpose

What tinyray is for, what it refuses to do, and the six principles that decide
every design question. Read this before anything else; the rest of the
documentation only makes sense against it.

## The stance

> **tinyray is an HTTP control plane. The data plane belongs to the framework.**

SGLang, vLLM, Megatron and `torchrun` already do distributed compute well. They
do not need a competing implementation. What they need from a cluster manager
is placement, rank assignment, supervision and restart — and then to be left
alone.

This is a narrower claim than Ray's. Ray takes over the process: you decorate
the class, Ray pickles it to the worker and constructs it remotely. That works
for code written for Ray. It does not work for a Megatron script, which expects
to own its own entrypoint, its own imports and its own process group.

tinyray takes a port instead.

## Three levels of intrusion

Reach for the least invasive option that works.

| Level | What you give up | Use for |
|---|---|---|
| [`launch_process`](../02-guides/02-native-frameworks.md#supervising-a-server) | **Nothing.** The process never learns tinyray exists. | SGLang, vLLM, any server |
| [`serve`](../02-guides/02-native-frameworks.md#attaching-to-a-training-script) | **One line** at the bottom of your script. | Megatron, DeepSpeed, training scripts |
| [`@remote`](../02-guides/03-actors.md) | The class is decorated and shipped to the worker; tinyray owns `__main__`. | Code written for tinyray |

The actor API is genuinely convenient, and for a pure-Python rollout or an
evaluation harness it is the right choice. But it requires the class to be
picklable, the driver to be able to import it, and tinyray to own the process
entrypoint. A native framework satisfies none of those.

## The six principles

Each of these was learned from a specific failure, and each is enforced by a
test rather than by good intentions.

### 1. The control plane never carries tensors

tinyray moves call signalling, references and status. Anything larger than a
few tens of kilobytes crossing the driver is a design smell.

*Enforced by* [`tests/test_driver_byte_budget.py`](../04-internals/05-testing.md#the-driver-byte-budget),
which parks a 32 MB result in an actor and asserts what every driver operation
moves.

*Learned from* `wait()`, which answered "has this settled?" by fetching the
whole result and discarding it. Thirty-two rollouts of 10 MB became 320 MB
pulled through the driver and thrown away. Every functional test passed, because
the answers were correct.

### 2. Never claim a process-global resource

A process has exactly one default `torch.distributed` group, one CUDA context
and one set of signal handlers. They belong to the user's framework. If tinyray
takes any of them, the framework has nowhere to go.

*Consequence:* [`tinyray.collective`](../02-guides/03-actors.md#collective-groups)
calls `init_process_group` on your behalf and therefore **cannot coexist with
Megatron or SGLang**. It is documented as mutually exclusive and
[`create_worker_group`](../02-guides/02-native-frameworks.md) exists to replace it.

*Same principle, different resource:* the [prewarm pool](../04-internals/01-rust-core.md#prewarming)
imports torch but never touches CUDA. A process that has initialised a CUDA
context has its `CUDA_VISIBLE_DEVICES` frozen and can never be reused for a
different device assignment.

### 3. Speak `torchrun`, do not invent an interface

Rank assignment must come out as `RANK`, `WORLD_SIZE`, `LOCAL_RANK`,
`LOCAL_WORLD_SIZE`, `MASTER_ADDR` and `MASTER_PORT`. Every framework already
reads these. A bespoke interface would mean asking users to patch their
framework, which is exactly the intrusion this project exists to avoid.

`LOCAL_RANK` is **derived from where the gang actually landed**, never guessed:
it is a property of the placement.

### 4. Supervise any process, not only tinyray actors

An SGLang server, a vLLM server, a `torchrun` job — all ordinary processes. The
control plane must give them GPUs, inject an environment, detect readiness,
label their logs and restart them. Without this, "control plane" is a slogan.

### 5. Group operations are collective-safe by default

Anything addressing a group of workers must **dispatch to all of them before
awaiting any**. A framework collective only returns once every rank has entered
it, so waiting for rank 0 first deadlocks.

This applies to *starting* a group as well as calling one. Launching rank by
rank and waiting for each to become ready hangs, because rank 0 blocks inside
its rendezvous until the last rank exists — and the resulting error blames
readiness rather than launch order.

### 6. Fail loudly, never hang

The most expensive failure in a distributed system is being stuck without
knowing where. So: an impossible placement is refused rather than partially
satisfied; a missing result reports `ObjectLost` rather than `NotFound`; a dead
actor reports `ActorDied` rather than timing out.

## What follows from this

| Component | Status |
|---|---|
| Process placement, gangs, resource accounting | **Core** |
| Rank assignment and `torchrun` environment | **Core** |
| Supervising arbitrary processes, readiness | **Core** |
| Health, introspection, straggler detection, logs | **Core** |
| Restart and constructor replay | **Core** |
| RPC and small result passing | Kept, for control data only |
| `LocalStore`, `ObjectRef`, direct fetch | Demoted — useful for pure-Python rollouts, irrelevant when a framework owns the data |
| `tinyray.collective` (managed NCCL) | Optional, mutually exclusive with frameworks |
| Same-node shared memory | **Deleted.** The data plane is not ours; that path would never have been wired up |

That last row is worth dwelling on. `shm.rs` had eleven passing unit tests and
no callers. Under this positioning it would never acquire any, so it was
removed rather than kept. Dead code with a justification is still dead code.

## Pitfalls

**Do not use `tinyray.collective` with a framework.** It takes the default
process group. The symptom is a second `init_process_group` raising, or worse,
a hang. Use [`create_worker_group`](../02-guides/02-native-frameworks.md) instead.

**Do not reach for `@remote` by default.** It is the most invasive option. If
your code is a training script, [attach to it](../02-guides/02-native-frameworks.md#attaching-to-a-training-script)
instead.

**Do not assume `ObjectRef` is the point.** For a framework-owned workload it
is nearly unused: the tensors move over NCCL and tinyray moves commands.

## See also

- [02-architecture.md](02-architecture.md) — how the pieces fit
- [03-tradeoffs.md](03-tradeoffs.md) — what each choice costs
- [02-native-frameworks.md](../02-guides/02-native-frameworks.md) — the main line, in code
- [01-status.md](../05-project/01-status.md) — what is actually built
