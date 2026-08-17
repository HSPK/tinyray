# Architecture

## Purpose

The components, how processes are laid out, and where the boundary between the
control plane and the data plane falls.

## Components

```
                    ┌──────────────────────────┐
                    │  Head                    │
                    │  - actor + process registry
                    │  - resource table, placement
                    │  - collective group state machine
                    │  - heartbeats, supervision
                    └───────┬─────────┬────────┘
                            │         │
              ┌─────────────┘         └─────────────┐
     ┌────────▼──────────┐                ┌─────────▼─────────┐
     │ Node agent        │                │ Node agent        │
     │ - reports CPU/GPU │                │                   │
     │ - starts actors   │                │                   │
     │ - supervises      │                │                   │
     │ - prewarm pool    │                │                   │
     └───┬───────────┬───┘                └───┬───────────────┘
         │           │                        │
   ┌─────▼─────┐ ┌───▼────────────┐    ┌──────▼──────────────┐
   │ Actor     │ │ Native script  │    │ Managed process     │
   │ (tinyray  │ │ + tinyray.serve│    │ (SGLang, torchrun)  │
   │  owns it) │ │ (you own it)   │    │ (tinyray never      │
   │           │ │                │    │  imports it)        │
   │ Rust: HTTP│ │ Rust: HTTP     │    │ no tinyray code     │
   │ Py: your  │ │ Py: your loop  │    │ at all              │
   │  class    │ │                │    │                     │
   └───────────┘ └────────────────┘    └─────────────────────┘
         ▲              ▲                        ▲
         └──────────────┴────────────────────────┘
                        │ HTTP control only
              ┌─────────┴──────────┐
              │ Driver             │
              │ your script        │
              └────────────────────┘
```

Two properties hold throughout:

1. **The head is never on the data path.** It participates when something is
   created, looked up, dies, or joins a group. Never when a result moves.
2. **An actor's HTTP service is not blocked by user compute.** The server runs
   on Rust threads that never take the GIL. See [rust-core](../04-internals/01-rust-core.md).

## The three kinds of worker

tinyray addresses three things that all answer HTTP, in decreasing order of how
much it owns them.

### Actors

tinyray owns the process. It starts `python -m tinyray.worker_main`, ships your
decorated class over the wire and constructs it remotely. Full lifecycle
control, maximum intrusion.

### Served scripts

You own the process. Your script keeps its entrypoint, imports and
`init_process_group`; `tinyray.serve(obj)` adds a control port and dispatches
calls to an object you already built. Nothing is pickled, nothing is decorated.

### Managed processes

Nobody attaches anything. tinyray starts a command, gives it GPUs and an
environment, waits until it is observably ready, labels its logs and restarts
it. The process contains no tinyray code.

## Process model inside an actor

An actor process runs three lines of execution that must not block each other.

| Thread | Language | Job | If it blocks |
|---|---|---|---|
| tokio pool | Rust | Accept calls, serve `/task/fetch` | Nothing — it never takes the GIL |
| executor | Python | Run user methods | That actor queues; others unaffected |
| collective | Python | NCCL calls only | Waits at the barrier, as intended |

This split is why an actor grinding through a 200 ms training step still serves
result fetches at full speed. Measured: 1.04x slowdown when four Python threads
saturate the GIL, against 49x for the same work initiated from Python. The
[rust-core](../04-internals/01-rust-core.md#the-gil-boundary) page explains why the
second number is the interesting one.

## Control plane and data plane

| Channel | Carries | Implementation |
|---|---|---|
| **Control** | Calls, references, rank assignment, health | HTTP, tinyray's own |
| **Data — results** | Rollout output, for pure-Python workloads | HTTP lazy pull from the producing actor |
| **Data — framework** | Gradients, weights, KV cache | NCCL, CUDA IPC, whatever the framework uses. **tinyray is absent.** |

For a framework-owned workload only the first row is active. `examples/native_stack.py`
runs a four-rank trainer plus an inference server through three iterations and
moves 2,163 bytes through the driver.

## Placement

One scheduler covers all three worker kinds, which is the only way to guarantee
that a trainer actor and an inference server are not handed the same GPU.

- Fractional `num_gpus` shares a device and reserves none exclusively — for
  hyperparameter trials.
- `num_gpus >= 1` reserves whole physical devices, which is what a collective
  member or a tensor-parallel engine requires.
- Gang placement is atomic: all or nothing. A group that comes up halfway
  cannot complete a rendezvous, and the framework inside blocks forever on
  ranks that will never arrive.

See [placement](../02-guides/04-placement.md) and [scheduler](../04-internals/04-scheduler.md).

## Implementation split

| Layer | Language | Why |
|---|---|---|
| HTTP server and client, framing | Rust | Must not take the GIL |
| Result store, ordered queue, backpressure | Rust | Hot path, and correctness through the type system |
| Resource table, placement, gangs | Rust | Shared by head and bindings |
| Serialisation (cloudpickle) | Python | Only Python can pickle Python |
| User code, `torch.distributed` calls | Python | Obviously |
| Driver API, process supervision | Python | Policy, not bytes |

The rule: **things that move bytes or manage concurrency go in Rust; things
that touch Python object semantics stay in Python.**

## Pitfalls

**The head is a library, not yet a daemon.** In the current release it runs
inside the driver process. The state machine is multi-node capable and tested,
but there is no `tinyray start --head` binary, so a cluster is single-machine in
practice. See [status](../05-project/01-status.md).

**A "node" is currently always local.** `LocalNodeAgent` is an in-process
object. Multi-node needs it wrapped behind HTTP, which is not done.

**Actor restart is not group-aware.** A restarted actor rejoins as a process,
but any collective group it belonged to stays `BROKEN` until you call
`tinyray.collective.rebuild()` yourself.

## See also

- [01-positioning.md](01-positioning.md) — why the boundary is drawn here
- [01-rust-core.md](../04-internals/01-rust-core.md) — the language boundary in detail
- [02-protocol.md](../03-reference/02-protocol.md) — what actually crosses the wire
