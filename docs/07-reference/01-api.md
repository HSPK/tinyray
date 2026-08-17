# API

> Proposal; not the current implementation. Signatures describe the target
> design, not the installed package.

## 1. Joining

```python
join(target, *, group, slot=None, rank=None, registry=None,
     readiness=None, meta=None, bind=None) -> Membership
```

Serves `target` on a control port and registers it. **Does not block** — the
control port runs on its own thread, because `__main__` belongs to the framework.

Rank comes from the launcher (`RANK`, `SLURM_PROCID`, `OMPI_COMM_WORLD_RANK`)
unless overridden. **No resource arguments exist.**

| Member | Meaning |
|---|---|
| `Membership.slot` | The logical slot |
| `Membership.incarnation` | This process's incarnation |
| `Membership.state` | `Current` / `Superseded` / `Expired` |
| `Membership.endpoint` | Where it is serving |
| `Membership.leave()` | Deregister; never raises |

## 2. Identity

```python
Slot(kind, **coords) -> Slot
Slot.incarnate() -> Incarnation
on_superseded(callback) -> None
```

```python
slot = tinyray.Slot("collector", cell="c07", index=3)
str(slot)          # "collector/c07/3"
```

A slot name never encodes placement.

## 3. Discovery

```python
group(name) -> GroupView
```

| Member | Returns | Note |
|---|---|---|
| `.ranks(list)` | `GroupView` | Narrower scope |
| `.shard(i, n)` | `GroupView` | Every n-th member from i |
| `.ready()` | `GroupView` | Ready members only |
| `.members(fresh=False)` | Entries | Cached unless `fresh` |
| `[rank]` | Handle | Callable, fenced |
| `len()`, iteration | | |
| `.wait_ready(size, timeout)` | Self | Replaces gang placement |
| `.watch(callback)` | Watcher | Polls the membership version |

Response size is bounded by the scope, never by the cluster.

## 4. Calling

```python
handle.method.remote(*args, **kwargs) -> Reference
get(refs, *, timeout=300.0) -> Any
wait(refs, *, num_returns=1, timeout=300.0) -> (ready, pending)
release(refs) -> None
```

`.remote()` returns before the call runs. `wait` asks for status and transfers no
payload. Handles are picklable, so a peer reference can be sent to a third
process.

## 5. Readiness

```python
readiness(*predicates) -> Readiness
```

Built-in predicates: `ProcessAlive`, `PortOpen`, `HttpOk`, `LogMatch`,
`QueueBelow`, `EventLoopLagBelow`.

Domain predicates are the application's. A predicate returns `bool` or
`(bool, reason)`; a negative verdict must carry a reason.

## 6. Admission

```python
admission(max_pending=1000, high_watermark=0.8, low_watermark=0.6) -> Admission
```

| Member | Returns |
|---|---|
| `.try_admit()` | `Ticket` or rejection, **never blocks** |
| `.credits()` | Remaining capacity |
| `.depth()` | Current depth |

## 7. Reconciliation

```python
reconciler(desired, observed, fn, interval=2.0) -> Reconciler
leadership(name) -> context manager
Reconciler.publish(state) -> version
Reconciler.epoch(min_ready) -> Epoch
```

The convergence function must be idempotent; it is called repeatedly.

## 8. Supervision

```python
supervise(command, *, ready_when="alive", env=None, cwd=None,
          startup_timeout=600.0, stop_timeout=30.0) -> Process
```

No `num_gpus`, no `num_cpus`. `ready_when` accepts `"alive"`, `"port"`,
`"http[:/path]"`, `"log:regex"`, a predicate or a callable.

| Member | Meaning |
|---|---|
| `.is_alive()`, `.exit_code()` | |
| `.tail(n)` | Ring buffer, 200 lines |
| `.stop(timeout)` | Signals the process **group** |

## 9. Registry

```python
serve_registry(bind, *, ttl=30.0, background=False) -> Server
```

Stateless. Run several; they do not talk to each other.

## 10. Exceptions

```
TinyrayError
├── ProtocolError
│   └── MessageTooLarge
├── RegistryUnavailable
├── NotLinked
└── RemoteCallError          .kind, .remote_traceback
    ├── UserCodeError
    ├── ObjectLost
    ├── NotFound
    ├── Fenced
    └── Backpressure

InsufficientCapacity
StartupError
NotLeader
ConsensusUnavailable
```

## 11. Removed from the previous API

| Removed | Replacement |
|---|---|
| `init(num_cpus=, num_gpus=)` | Nothing; there is no resource table |
| `remote(num_gpus=...)` | `join()`; the launcher assigned the devices |
| `create_actors(count=...)` | The launcher starts them; `wait_ready(size)` |
| `launch_workers(gpus_per_worker=, cpus_per_worker=)` | The launcher starts them and already sized them |
| `link(**groups)` | Self-registration and scoped `group()` |
| `nodes()` | The scheduler knows |
| `PlacementFailed` | Nothing places |

The rationale is in [08-project/02-decisions.md](../08-project/02-decisions.md).
