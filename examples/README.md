# Examples

Every file runs on its own with no arguments. Each starts a registry on a free
port, spawns the processes it needs, asserts what it claims, and cleans up.

```bash
maturin develop --release      # the registry ships in the wheel
python examples/01_hello_world.py
```

## Start here

| | What it shows |
|---|---|
| [01_hello_world](01_hello_world.py) | The smallest thing that works: one process serves a method, another finds it and calls it |
| [09_error_taxonomy](09_error_taxonomy.py) | Six different failures told apart, and the retry rule for each |
| [15_plain_http](15_plain_http.py) | It is still HTTP, so `curl` still works — methods, calls, pools, health |

## The four kinds of member

| | What it shows |
|---|---|
| [02_weight_rollout](02_weight_rollout.py) | `serving`: an engine loading weights is present but must not be picked |
| [03_shard_router](03_shard_router.py) | `stateful`: reaching the wrong seat corrupts data, so an empty seat raises |
| [04_singleton_leader](04_singleton_leader.py) | Election with `exclusive=True`, and why the default is the opposite |
| [17_two_layer_dp](17_two_layer_dp.py) | `collective` inside `stateful`: losing a rank ends one group, not the job |

## Failure, and what it costs

| | What it shows |
|---|---|
| [06_fencing](06_fencing.py) | The process that came back, still listening, and why tenure numbers exist |
| [07_registry_restart](07_registry_restart.py) | The registry killed: lookups and calls carry on, the roster refills itself |
| [08_leaving_vs_dying](08_leaving_vs_dying.py) | 456 ms to notice a goodbye, 1,664 ms to notice a kill |
| [14_gil_heartbeat](14_gil_heartbeat.py) | What actually starves a Python heartbeat, measured — the one reason for Rust |
| [20_restart_supervisor](20_restart_supervisor.py) | Restart policy is per pool, because the three groups die differently |

## Calling

| | What it shows |
|---|---|
| [11_async_collector](11_async_collector.py) | asyncio on both sides, coroutines on the loop that was already there |
| [12_typed_payloads](12_typed_payloads.py) | Annotations are the schema, nested containers included |
| [13_timeouts](13_timeouts.py) | Per-call budgets, and why the modifier is not a keyword argument |

## Rounds

| | What it shows |
|---|---|
| [05_elastic_dp](05_elastic_dp.py) | Rebuilding with `epoch(min=)`, and why the first round must be strict |
| [dataloader_to_trainer](dataloader_to_trainer.py) | A 4 MB batch and a 103-byte handover note |

## Shape and scale

| | What it shows |
|---|---|
| [10_backpressure](10_backpressure.py) | Refusing work instead of queueing it into staleness |
| [16_cross_node](16_cross_node.py) | Advertising an address peers can reach, with no loopback fallback |
| [18_observability](18_observability.py) | Everything worth looking at when something is wrong |
| [19_pool_size_guard](19_pool_size_guard.py) | Memory tracks members × subscribers, measured |

## Whole systems

| | What it shows |
|---|---|
| [async_rl_fleet](async_rl_fleet.py) | Four kinds of member at once, with one killed mid-flight |
| [agent_pool/](agent_pool/) | An agent tier from a real framework, ported over: 532 lines of membership plumbing gone |

## Notes

`_harness.py` is shared plumbing so no example repeats forty lines of process
bookkeeping. It is not part of the API.

Several examples deliberately keep a hazard visible rather than tidying it
away — a round that must not be opened with `min=`, an empty pool that means
two opposite things, a lease that also sets how fast a change propagates. Those
comments are the point of the file.
