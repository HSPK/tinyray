"""Collective groups for weight broadcast.

**tinyray implements no collective transport.** Weights move through NCCL via
``torch.distributed``; this module supplies the parts NCCL leaves to the caller:
rank assignment, rendezvous, and what to do when a member dies.

.. warning::
   This module calls ``init_process_group`` on your behalf, which claims the
   process's one and only default group. A worker that also runs Megatron,
   SGLang, vLLM or DeepSpeed cannot use it: those frameworks need that group for
   their own topology. Use :mod:`tinyray.worker_group` instead -- it assigns
   ranks and injects the ``torchrun`` environment, then leaves initialisation
   entirely to the framework.

Three consequences of using NCCL are worth understanding before you build a
group, because each one turns into a hang rather than an error if ignored.

**Every member needs a whole GPU of its own.** NCCL is GPU-only, and two ranks
sharing one device deadlock. Fractional-GPU trials cannot join a group -- which
is fine, since they have no weights to broadcast.

**A broadcast is a barrier.** ``wait(num_returns=24)`` lets you drop the results
of eight slow rollouts, but all 32 ranks must still show up for the next
broadcast. Skipping a straggler's *result* is allowed; skipping its
*participation* hangs the group. In effect the barrier reintroduces the
straggler latency you just avoided, and that is the price of using NCCL.

**A communicator is not fault tolerant.** One dead rank makes every subsequent
collective hang forever, so a group carries an epoch: any membership change
aborts the old communicator and rebuilds. Rebuilding takes seconds, which is why
groups must be long-lived. Never build one per iteration.
"""

from __future__ import annotations

import contextlib
import os
import socket
import threading
from collections.abc import Sequence
from typing import Any, Optional

from ._tinyray import new_id

#: Environment tinyray injects into every collective member.
NCCL_ENV = {
    # Without this, a dead peer leaves surviving ranks blocked inside NCCL with
    # no way to abort, and the epoch state machine cannot do its job.
    "NCCL_ASYNC_ERROR_HANDLING": "1",
    "TORCH_NCCL_ASYNC_ERROR_HANDLING": "1",
}


class CollectiveError(RuntimeError):
    """A collective group could not be created, joined or used."""


class GroupRebuilding(CollectiveError):
    """The group is being rebuilt after a membership change; retry shortly."""


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


class CollectiveGroup:
    """A handle to a group of actors that can run collectives together."""

    def __init__(self, context: Any, group_id: str, handles: Sequence[Any]) -> None:
        self._context = context
        self.group_id = group_id
        self._handles = list(handles)
        self._lock = threading.Lock()

    @property
    def world_size(self) -> int:
        return len(self._handles)

    @property
    def members(self) -> list[Any]:
        return list(self._handles)

    def info(self) -> Optional[dict[str, Any]]:
        return self._context.collective.info(self.group_id)

    @property
    def state(self) -> str:
        info = self.info()
        return info["state"] if info else "DESTROYED"

    @property
    def epoch(self) -> int:
        info = self.info()
        return info["epoch"] if info else -1

    def __repr__(self) -> str:
        return (
            f"CollectiveGroup({self.group_id}, world_size={self.world_size}, "
            f"state={self.state}, epoch={self.epoch})"
        )

    def run(
        self,
        method: str,
        *args: Any,
        src_rank: int = 0,
        timeout: float = 300.0,
        **kwargs: Any,
    ) -> list[Any]:
        """Invoke `method` on every member as one collective round.

        The call is delivered to all ranks and awaited together, because a
        collective in which only some ranks participate hangs the rest.

        The method receives ``rank``, ``world_size`` and ``src_rank`` keyword
        arguments so user code can call ``torch.distributed`` directly.
        """
        from . import api

        state = self.state
        if state == "BROKEN":
            raise GroupRebuilding(
                f"group {self.group_id} is broken and must be rebuilt before it can be used"
            )
        if state != "READY":
            raise CollectiveError(
                f"group {self.group_id} is {state}, not READY; "
                "every member must finish joining first"
            )

        with self._lock:
            refs = [
                handle._submit(
                    method,
                    args,
                    {
                        **kwargs,
                        "rank": rank,
                        "world_size": self.world_size,
                        "src_rank": src_rank,
                    },
                )
                for rank, handle in enumerate(self._handles)
            ]
            # Every rank is awaited: a barrier that only some ranks reach is a
            # hang, so partial success is not a meaningful outcome here.
            return api.get(refs, timeout=timeout)

    def destroy(self) -> None:
        self._context.collective.destroy(self.group_id)


def create_group(
    handles: Sequence[Any],
    *,
    backend: str = "nccl",
    store_host: Optional[str] = None,
    store_port: Optional[int] = None,
    timeout: float = 120.0,
) -> CollectiveGroup:
    """Build a collective group from actor handles.

    Admission is strict on purpose: every rule checked here corresponds to a
    NCCL failure mode that manifests as a hang rather than an error.
    """
    from . import api

    context = api._require_context()
    group_id = str(new_id())
    store_host = store_host or "127.0.0.1"
    store_port = store_port or _free_port()

    members = []
    for handle in handles:
        entry = context.head.get_actor(handle.actor_id)
        if entry is None:
            raise CollectiveError(f"actor {handle.actor_id[:8]} is not registered")
        members.append(
            (
                entry["actor_id"],
                float(entry["num_gpus"]),
                entry["node_id"],
                list(entry["gpu_ids"]),
                entry["state"] == "ALIVE",
            )
        )

    try:
        context.collective.create(
            group_id, members, backend=backend, store_host=store_host, store_port=store_port
        )
    except Exception as exc:
        raise CollectiveError(str(exc)) from exc

    group = CollectiveGroup(context, group_id, handles)
    _join_all(context, group, timeout=timeout)
    context.register_group(group)
    return group


def _join_all(context: Any, group: CollectiveGroup, *, timeout: float) -> None:
    """Tell every member to join, and wait until all of them have."""
    from . import api

    refs = []
    for handle in group.members:
        rendezvous = context.collective.rendezvous_for(group.group_id, handle.actor_id)
        if rendezvous is None:
            raise CollectiveError(
                f"actor {handle.actor_id[:8]} has no rank in group {group.group_id}"
            )
        refs.append(handle._submit("__tinyray_join_collective__", (rendezvous,), {}))

    try:
        api.get(refs, timeout=timeout)
    except Exception as exc:
        context.collective.break_group(group.group_id, f"join failed: {exc}")
        raise CollectiveError(
            f"group {group.group_id} failed to form: {exc}. Every member must reach "
            "init_process_group; a single missing rank blocks the rest."
        ) from exc

    epoch = context.collective.info(group.group_id)["epoch"]
    for handle in group.members:
        context.collective.acknowledge(group.group_id, handle.actor_id, epoch)

    state = context.collective.info(group.group_id)["state"]
    if state != "READY":
        raise CollectiveError(f"group {group.group_id} is {state} after joining, expected READY")


def rebuild(group: CollectiveGroup, *, timeout: float = 120.0) -> None:
    """Abort the old communicator and form a new epoch.

    Called after a member dies and is replaced. Seconds-scale, which is exactly
    why groups are long-lived and not rebuilt per iteration.
    """
    from . import api

    context = api._require_context()
    for handle in group.members:
        with contextlib.suppress(Exception):
            api.get(
                handle._submit("__tinyray_abort_collective__", (group.group_id,), {}),
                timeout=30.0,
            )

    if context.collective.begin_rebuild(group.group_id) is None:
        raise CollectiveError(f"group {group.group_id} has been destroyed")
    _join_all(context, group, timeout=timeout)


# --------------------------------------------------------------------------
# Actor-side helpers. These run inside the actor process.
# --------------------------------------------------------------------------


class _ActorCollectiveState:
    """Per-actor collective state, owned by the actor process.

    Every NCCL call is issued from a single dedicated thread. NCCL requires the
    collective order to be identical on every rank, and pinning them to one
    thread is the simplest way to guarantee that. It also keeps the blocking
    wait off the executor thread -- though notably *not* off the data path,
    which lives in Rust and never touches the interpreter at all.
    """

    def __init__(self) -> None:
        self.groups: dict[str, Any] = {}
        self.rendezvous: dict[str, dict] = {}
        self._lock = threading.Lock()

    def join(self, rendezvous: dict[str, Any]) -> str:
        import torch.distributed as dist  # imported lazily: torch is optional

        group_id = rendezvous["group_id"]
        for key, value in NCCL_ENV.items():
            os.environ.setdefault(key, value)

        with self._lock:
            store = dist.TCPStore(
                rendezvous["store_host"],
                int(rendezvous["store_port"]),
                int(rendezvous["world_size"]),
                rendezvous["rank"] == 0,
            )
            dist.init_process_group(
                backend=rendezvous["backend"],
                store=store,
                rank=int(rendezvous["rank"]),
                world_size=int(rendezvous["world_size"]),
            )
            self.groups[group_id] = dist.group.WORLD
            self.rendezvous[group_id] = rendezvous
        return group_id

    def abort(self, group_id: str) -> None:
        import torch.distributed as dist

        with self._lock:
            self.groups.pop(group_id, None)
            self.rendezvous.pop(group_id, None)
            try:
                if dist.is_initialized():
                    dist.destroy_process_group()
            except Exception:
                pass

    def process_group(self, group_id: str) -> Any:
        return self.groups.get(group_id)


#: The actor process's collective state, created on first use.
_actor_state: Optional[_ActorCollectiveState] = None
_actor_state_lock = threading.Lock()


def actor_state() -> _ActorCollectiveState:
    global _actor_state
    with _actor_state_lock:
        if _actor_state is None:
            _actor_state = _ActorCollectiveState()
        return _actor_state
