"""Worker groups: rank assignment for frameworks that run their own collectives.

Megatron, SGLang, vLLM and DeepSpeed all build their own process groups. What
they need from a cluster manager is to be told where they sit -- rank, world
size, and where the rendezvous is -- and then to be left alone.

That is the entire job here. tinyray places the processes, injects the
environment variables ``torchrun`` would have set, and **never calls**
``init_process_group`` itself. The framework initialises its own groups and
keeps full control of its topology: tensor, pipeline and expert parallel are
its business, not tinyray's.

This is deliberately *less* than :mod:`tinyray.collective`. That module drives
NCCL on the caller's behalf, which means it takes the default process group --
and a process only gets one, so a Megatron worker cannot share it. A worker
group takes the opposite position: tinyray stays out of the data plane and
confines itself to lifecycle, placement, health and restart.

Rule of thumb: if the payload moves inside a framework, use a worker group. Use
:mod:`tinyray.collective` only when tinyray actors need to broadcast among
themselves and nothing else in the process wants the default group.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Any, ClassVar, Optional

from .api import ActorHandle, Context, _require_context, construct_all, get


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return int(sock.getsockname()[1])


def torchrun_env(
    *,
    rank: int,
    world_size: int,
    local_rank: int,
    local_world_size: int,
    master_addr: str,
    master_port: int,
) -> dict[str, str]:
    """The environment `torchrun` sets, which every framework reads.

    ``GROUP_RANK`` and ``GROUP_WORLD_SIZE`` are set alongside the usual names
    because different stacks read different ones, and discovering which is a
    poor use of anyone's afternoon.
    """
    return {
        "RANK": str(rank),
        "WORLD_SIZE": str(world_size),
        "LOCAL_RANK": str(local_rank),
        "LOCAL_WORLD_SIZE": str(local_world_size),
        "GROUP_RANK": str(rank),
        "GROUP_WORLD_SIZE": str(world_size),
        "MASTER_ADDR": master_addr,
        "MASTER_PORT": str(master_port),
    }


@dataclass
class WorkerGroup:
    """A set of actors forming one SPMD group.

    They are ordinary tinyray actors -- supervised, restartable, callable. What
    makes them a group is that each was started knowing its rank, so whatever
    runs inside can call ``torch.distributed.init_process_group()`` and find
    its peers.
    """

    name: str
    handles: list[ActorHandle]
    master_addr: str
    master_port: int

    #: Marker for duck-typing that survives handle proxying. Handles forward
    #: every public attribute to a remote method, so `hasattr(x, "world_size")`
    #: is true for things that are not groups; underscore names are the only
    #: ones they refuse.
    _tinyray_group: ClassVar[bool] = True

    @property
    def world_size(self) -> int:
        return len(self.handles)

    def __len__(self) -> int:
        return len(self.handles)

    def __iter__(self):
        return iter(self.handles)

    def __getitem__(self, index: int) -> ActorHandle:
        return self.handles[index]

    def __repr__(self) -> str:
        return (
            f"WorkerGroup({self.name}, world_size={self.world_size}, "
            f"master={self.master_addr}:{self.master_port})"
        )

    def run(self, method: str, *args: Any, timeout: float = 600.0, **kwargs: Any) -> list[Any]:
        """Call `method` on every rank and collect the results.

        Every rank is dispatched before any result is awaited. A framework
        collective inside the method only returns once all ranks have entered
        it, so awaiting rank 0 first would deadlock the group.
        """
        refs = [handle._submit(method, args, kwargs) for handle in self.handles]
        return get(refs, timeout=timeout)

    def run_on(
        self, rank: int, method: str, *args: Any, timeout: float = 600.0, **kwargs: Any
    ) -> Any:
        """Call `method` on a single rank.

        Only safe for methods that stay out of collectives; anything collective
        must go through :meth:`run`.
        """
        return get(self.handles[rank]._submit(method, args, kwargs), timeout=timeout)

    def shutdown(self) -> None:
        from . import api

        for handle in self.handles:
            api.kill(handle)


def create_worker_group(
    remote_class: Any,
    *args: Any,
    size: int,
    name: str = "workers",
    gpus_per_worker: float = 1.0,
    cpus_per_worker: float = 1.0,
    master_addr: Optional[str] = None,
    master_port: Optional[int] = None,
    strategy: str = "PACK",
    extra_env: Optional[dict[str, str]] = None,
    context: Optional[Context] = None,
    **kwargs: Any,
) -> WorkerGroup:
    """Start `size` workers that know their ranks.

    Placement is atomic. A group that comes up halfway cannot complete a
    rendezvous, and the framework inside blocks forever waiting for ranks that
    will never arrive -- far harder to diagnose than a clean refusal.

    `gpus_per_worker` above one hands a worker several devices, which is what a
    single-process tensor-parallel engine such as SGLang or vLLM expects.

    `strategy` defaults to ``PACK``: ranks on one node share NVLink, and
    splitting a tensor-parallel group across machines is usually an accident.
    """
    context = context or _require_context()
    resolved_port = master_port or _free_port()
    requested_addr = master_addr

    # Filled in from the actual placement: LOCAL_RANK and the master's hostname
    # are properties of where the gang landed, not of what was requested.
    chosen: dict[str, str] = {}

    def build_env(placement: dict[str, Any]) -> dict[str, str]:
        addr = requested_addr or placement["master_hostname"]
        # A single-node cluster reports its own hostname, which need not
        # resolve to a bindable address; loopback always does.
        if placement["local_world_size"] == placement["world_size"] and not requested_addr:
            addr = "127.0.0.1"
        chosen["addr"] = addr
        return {
            **torchrun_env(
                rank=placement["rank"],
                world_size=placement["world_size"],
                local_rank=placement["local_rank"],
                local_world_size=placement["local_world_size"],
                master_addr=addr,
                master_port=resolved_port,
            ),
            "TINYRAY_GROUP": name,
            **(extra_env or {}),
        }

    options = dict(remote_class._options)
    entries = context.head.create_actors(
        size,
        name=name,
        num_cpus=options.get("num_cpus", cpus_per_worker),
        num_gpus=options.get("num_gpus", gpus_per_worker),
        memory_bytes=options.get("memory_bytes", 0),
        strategy=strategy,
        max_restarts=options.get("max_restarts", 0),
        max_pending_calls=options.get("max_pending_calls", 1000),
        env_builder=build_env,
    )

    # All constructors are dispatched before any is awaited: a framework that
    # rendezvous in __init__ blocks rank 0 until the last rank arrives, so
    # serial construction would deadlock on the first worker.
    handles = construct_all(context, entries, remote_class._cls, args, kwargs)
    return WorkerGroup(
        name=name,
        handles=handles,
        master_addr=chosen.get("addr", "127.0.0.1"),
        master_port=resolved_port,
    )
