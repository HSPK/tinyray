# Python API reference

## Purpose

Exact signatures and contracts for the main-line API. Low-level symbols
(`Frame`, `Decoder`, `Limits`, `Id`, `OwnerRef`, `encode_message`,
`decode_message`) are covered by the type stubs in `python/tinyray/_tinyray.pyi`
and are not repeated here.

Signatures are extracted from the installed package; if one disagrees with the
code, the code is right and this page is a bug.

---

## Runtime

### `init`

```python
init(*, num_cpus=None, num_gpus=None, prewarm=0,
     heartbeat_timeout=30.0, supervise_interval=1.0) -> Context
```

Starts the local runtime. Idempotent — any API call starts it implicitly, but
calling it explicitly makes the limits visible.

| Argument | Default | Meaning |
|---|---|---|
| `num_cpus` | detected | Override the node's reported CPUs |
| `num_gpus` | detected | Override the GPU count |
| `prewarm` | 0 | Warm interpreters per device assignment. See [pitfalls](#pitfalls) |
| `heartbeat_timeout` | 30.0 | Seconds before a silent node is declared dead |
| `supervise_interval` | 1.0 | Seconds between supervision passes |

### `shutdown`

```python
shutdown() -> None
```

Stops every actor, worker and managed process this driver started, including
their children. Registered with `atexit`.

---

## Actors

### `remote`

```python
remote(*decorator_args, **options) -> RemoteClass | Callable[[type], RemoteClass]
```

Marks a class as an actor. Usable bare or with options:

```python
@tr.remote
class A: ...

@tr.remote(num_gpus=1, max_restarts=3)
class B: ...
```

Options: `num_cpus`, `num_gpus`, `memory_bytes`, `max_restarts`,
`max_pending_calls`, `store_max_bytes`, `store_ttl_seconds`, `strategy`, `name`.
See [configuration](04-configuration.md).

Raises `TypeError` on a function — tinyray has no tasks.

### `RemoteClass`

| Member | Signature | Notes |
|---|---|---|
| `.remote(*args, **kwargs)` | `-> ActorHandle` | Places, starts, runs `__init__`. Blocks until the constructor returns |
| `.options(**overrides)` | `-> RemoteClass` | A copy with different options |

Calling the class directly raises `TypeError`.

### `ActorHandle`

| Member | Type | Notes |
|---|---|---|
| `.actor_id` | `str` | Stable across restarts |
| `.endpoint` | `str` | Looked up, so it follows a restart |
| `.name` | `str` | |
| `.pid` | `int` | Of the original process |
| `.gpu_ids` | `list[int]` | Physical devices assigned |
| `.is_alive()` | `bool` | |
| `.introspect()` | `str` | JSON; see [observability](../02-guides/06-observability.md) |
| `.<method>` | `ActorMethod` | Any public method |

`ActorMethod.remote(*args, **kwargs) -> ObjectRef`. Calling without `.remote()`
raises `TypeError`.

### `create_actors`

```python
create_actors(remote_class, *args, count, strategy="SPREAD", **kwargs) -> list[ActorHandle]
```

Starts `count` actors **atomically**. Raises `PlacementFailed` if the whole gang
does not fit; nothing is left reserved.

### `get_actor`

```python
get_actor(name) -> ActorHandle
```

Raises `NotFound` if no actor holds that name.

### `kill`

```python
kill(handle, *, no_restart=True) -> None
```

Terminates immediately and returns the resources.

---

## References

### `get`

```python
get(refs, *, timeout=300.0) -> Any
```

Fetches one reference or a sequence of them. Blocks. Raises the remote
exception with `remote_traceback` attached.

Works in the driver and inside an actor: a reference carries its owner's
endpoint, so an actor fetches straight from the producer.

**Returned arrays are read-only.** One buffer may serve many consumers.

### `wait`

```python
wait(refs, *, num_returns=1, timeout=300.0) -> tuple[list[ObjectRef], list[ObjectRef]]
```

Returns `(ready, pending)`, both lists of `ObjectRef` — never values. A failed
reference counts as ready; `get` will raise.

A readiness probe: it moves a few hundred bytes regardless of payload size.

### `release`

```python
release(refs) -> None
```

Tells owners the results are no longer needed. Best effort; the watermark and
TTL are the real safety net. A later `get` raises `ObjectLost`.

### `ObjectRef`

| Member | Notes |
|---|---|
| `.task_id` | `str` |
| `.owner_endpoint` | `host:port` of the actor holding the value |

Picklable, so it can be passed to another actor. Top-level reference arguments
are resolved automatically; nested ones are passed through.

---

## Native processes

### `launch_process`

```python
launch_process(command, *, name=None, num_cpus=1.0, num_gpus=0.0,
               ready_when=None, env=None, allocate_port=True,
               startup_timeout=600.0, strategy="PACK", cwd=None,
               host=None, max_restarts=0) -> ManagedProcess
```

Places, starts and waits for readiness. `{port}` in the command or in any
environment value is replaced with an allocated port.

`ready_when` accepts `"port"`, `"http"`, `"http:/path"`, `"log:regex"`,
`"alive"`, a `Readiness`, or a callable. Default `"alive"` — weak; pass
something stricter for a server.

Raises `PlacementFailed` or `ProcessStartupError` (which carries the child's
last output).

### `ManagedProcess`

| Member | Notes |
|---|---|
| `.name`, `.pid`, `.port`, `.endpoint` | `endpoint` is `None` without a port |
| `.gpu_ids` | Assigned devices |
| `.restarts` | Count so far |
| `.is_alive()`, `.exit_code()` | |
| `.recent_log()`, `.tail(n)` | Ring buffer, up to 200 lines |
| `.terminate(timeout)`, `.kill()` | Signals the whole process group |

### `stop_process` / `processes`

```python
stop_process(name) -> None          # stops the tree, returns the resources
processes() -> list[ManagedProcess]
```

---

## Worker groups

### `serve`

```python
serve(target, *, background=False, bind=None, actor_id=None,
      max_pending_calls=1000) -> Server
```

Called **inside** the worker. Adds a control port and dispatches to `target`'s
methods. `target` may be an object, a dict of callables, or a module.

**Blocks by default.** `background=True` returns immediately and serves on
another thread.

Reads `TINYRAY_CONTROL_PORT` and `TINYRAY_ACTOR_ID` when tinyray started the
process; otherwise binds its own port and prints the endpoint.

### `launch_workers`

```python
launch_workers(command, *, size, name="workers", gpus_per_worker=1.0,
               cpus_per_worker=1.0, env=None, master_addr=None,
               master_port=None, strategy="PACK", startup_timeout=900.0,
               cwd=None, context=None) -> WorkerGroup
```

Starts `size` copies of a native script with the `torchrun` environment plus a
control port. Placement is atomic and **every rank is spawned before any is
awaited**.

### `create_worker_group`

```python
create_worker_group(remote_class, *args, size, name="workers",
                    gpus_per_worker=1.0, cpus_per_worker=1.0,
                    master_addr=None, master_port=None, strategy="PACK",
                    extra_env=None, context=None, **kwargs) -> WorkerGroup
```

The same, for a tinyray actor class rather than a script. The actor calls
`init_process_group` itself.

### `WorkerGroup`

| Member | Notes |
|---|---|
| `.world_size`, `len()`, iteration, `[rank]` | |
| `.master_addr`, `.master_port` | The rendezvous |
| `.run(method, *args, timeout=600.0, **kwargs)` | **All ranks**, dispatched then awaited. Required for anything collective |
| `.run_on(rank, method, ...)` | One rank. Not safe for collectives |
| `.shutdown()` | |

### `connect`

```python
connect(endpoint, actor_id=None) -> RemoteWorker
```

Drives a process that is already serving. tinyray takes no responsibility for
its lifecycle.

### `torchrun_env`

```python
torchrun_env(*, rank, world_size, local_rank, local_world_size,
             master_addr, master_port) -> dict[str, str]
```

The environment `torchrun` sets. Exposed for callers building their own launch
path.

---

## Cluster

```python
nodes() -> list[dict]        # node_id, hostname, total/available cpus and gpus, free_gpu_ids
actors() -> list[dict]       # actor_id, name, state, endpoint, gpu_ids, restarts
processes() -> list[ManagedProcess]
transport_stats() -> dict[str, dict[str, int]]   # per peer: requests, retries, failures, bytes
```

---

## Pool

```python
ActorPool(actors, *, max_in_flight_per_actor=2)
  .map_unordered(fn, items, timeout=300.0) -> Iterator   # completion order
  .map(fn, items, timeout=300.0) -> list                 # input order
```

`fn` receives `(actor, item)` and must return the `ObjectRef` from a `.remote()`
call.

---

## Exceptions

```
TinyrayError
├── ProtocolError
│   └── MessageTooLarge
└── RemoteCallError          .kind, .remote_traceback
    ├── UserCodeError
    ├── ObjectLost
    ├── ActorDied
    ├── NotFound
    └── Backpressure

ActorStartupError            an actor process failed to start
ProcessStartupError          a managed process failed to start
PlacementFailed              the cluster cannot host the request
CollectiveError
└── GroupRebuilding
```

---

## Pitfalls

**`prewarm` is off by default.** Worth setting for a hyperparameter sweep, where
`import torch` in every short trial dominates the run; pointless for a handful
of long-lived actors. Measured with `prewarm=2`: actor creation drops from
49.5 ms to 2.6 ms.

**`lifetime="detached"` is refused.** It needs a standalone head process to own
the actor, which does not exist. Naming does not require it.

**`launch_workers` has no `max_restarts`.** `launch_process` does.

**`context=` is an escape hatch**, not for normal use.

## See also

- [04-configuration.md](04-configuration.md) — every default in one table
- [02-protocol.md](02-protocol.md) — what crosses the wire
- [03-actors.md](../02-guides/03-actors.md) — the actor API in prose
