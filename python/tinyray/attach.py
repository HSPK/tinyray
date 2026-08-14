"""Launching native scripts and talking to them.

The driver half of :mod:`tinyray.serve`. tinyray places the processes, hands
each one a rank and a control port, waits until it is answering, and gives back
handles. The script itself is whatever the user already had.

This is what replaces `torchrun` for a controlled job: same environment, same
unmodified script, but with placement, supervision, restart and an RPC channel
that `torchrun` does not offer.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Optional

from . import serde
from ._tinyray import new_id
from .api import Context, ObjectRef, _require_context, get
from .process import HttpOk, ManagedProcess, free_port
from .serve import ACTOR_ID_ENV, CONTROL_PORT_ENV
from .worker_group import WorkerGroup, torchrun_env


class RemoteWorker:
    """A handle to a process running :func:`tinyray.serve`.

    Deliberately the same shape as an ``ActorHandle`` -- ``worker.step.remote()``
    -- so a :class:`~tinyray.WorkerGroup` does not care which kind it holds.
    """

    def __init__(
        self,
        context: Context,
        actor_id: str,
        endpoint: str,
        process: Optional[ManagedProcess] = None,
        rank: int = 0,
    ) -> None:
        self._context = context
        self._actor_id = actor_id
        self._endpoint = endpoint
        self._process = process
        self._rank = rank
        context.register_endpoint(actor_id, endpoint)

    @property
    def actor_id(self) -> str:
        return self._actor_id

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def rank(self) -> int:
        return self._rank

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid if self._process else None

    def is_alive(self) -> bool:
        return self._process.is_alive() if self._process else True

    def __repr__(self) -> str:
        return f"RemoteWorker(rank={self._rank}, {self._endpoint})"

    def __getattr__(self, name: str) -> RemoteMethod:
        if name.startswith("_"):
            raise AttributeError(name)
        return RemoteMethod(self, name)

    def _submit(self, method: str, args: tuple, kwargs: dict) -> ObjectRef:
        body, frames = serde.serialize((args, kwargs))
        owner = self._context.client.submit(self._actor_id, method, body, frames)
        return ObjectRef(owner, self._context)

    def introspect(self) -> str:
        return self._context.client.get_text(self._endpoint, "/introspect")


class RemoteMethod:
    """One remotely callable method on a served process."""

    def __init__(self, worker: RemoteWorker, method: str) -> None:
        self._worker = worker
        self._method = method

    def remote(self, *args: Any, **kwargs: Any) -> ObjectRef:
        return self._worker._submit(self._method, args, kwargs)

    def __call__(self, *args: Any, **kwargs: Any):
        raise TypeError(
            f"use {self._method}.remote(...) to call a worker method; "
            "calling it directly would run it in the driver"
        )


def launch_workers(
    command: Sequence[str],
    *,
    size: int,
    name: str = "workers",
    gpus_per_worker: float = 1.0,
    cpus_per_worker: float = 1.0,
    env: Optional[dict[str, str]] = None,
    master_addr: Optional[str] = None,
    master_port: Optional[int] = None,
    strategy: str = "PACK",
    startup_timeout: float = 900.0,
    cwd: Optional[str] = None,
    context: Optional[Context] = None,
) -> WorkerGroup:
    """Start `size` copies of a native script, each with a rank and a control port.

    The script is unmodified apart from calling :func:`tinyray.serve`. It keeps
    its own entrypoint, imports and distributed initialisation; tinyray supplies
    the environment `torchrun` would have, plus one extra variable telling it
    where to bind its control port.

    Readiness is the control endpoint answering, which for a trainer means the
    model is built and ``init_process_group`` has returned. A process that is
    merely running is not ready, and a controller that assumes otherwise sends
    its first command into a connection refused.

    Placement is atomic: a group that comes up halfway leaves the framework
    blocked in a rendezvous that will never complete.
    """
    context = context or _require_context()
    resolved_master_port = master_port or free_port()
    resolved_master = master_addr or "127.0.0.1"

    processes: list[ManagedProcess] = []
    workers: list[RemoteWorker] = []
    try:
        for rank in range(size):
            actor_id = str(new_id())
            control_port = free_port()
            worker_env = {
                **torchrun_env(
                    rank=rank,
                    world_size=size,
                    # Single node for now; multi-node needs the agent to report
                    # a routable address before this can be anything else.
                    local_rank=rank,
                    local_world_size=size,
                    master_addr=resolved_master,
                    master_port=resolved_master_port,
                ),
                CONTROL_PORT_ENV: str(control_port),
                ACTOR_ID_ENV: actor_id,
                "TINYRAY_GROUP": name,
                **(env or {}),
            }

            process = context.head.launch_process(
                list(command),
                name=f"{name}-{rank}",
                num_cpus=cpus_per_worker,
                num_gpus=gpus_per_worker,
                strategy=strategy,
                env=worker_env,
                allocate_port=False,
                # The control port answering means the script got as far as
                # calling serve(), which is after its own setup.
                ready_when=HttpOk(path="/health", port=control_port),
                startup_timeout=startup_timeout,
                cwd=cwd,
                # Every rank is spawned before any is awaited. A script that
                # calls init_process_group during setup blocks rank 0 until the
                # last rank exists, so waiting rank by rank deadlocks the launch.
                wait_ready=False,
            )
            process.port = control_port
            processes.append(process)
            workers.append(
                RemoteWorker(
                    context,
                    actor_id=actor_id,
                    endpoint=f"127.0.0.1:{control_port}",
                    process=process,
                    rank=rank,
                )
            )
        # Now that the whole group exists, its members can complete their
        # rendezvous and start answering.
        for process in processes:
            context.head.await_process_ready(process, startup_timeout)
    except Exception:
        # A partly started group is worse than none: the survivors sit in a
        # rendezvous waiting for ranks that will never arrive.
        for started in processes:
            context.head.stop_process(started.name)
        raise

    return WorkerGroup(
        name=name,
        handles=workers,  # type: ignore[arg-type]
        master_addr=resolved_master,
        master_port=resolved_master_port,
    )


def connect(endpoint: str, actor_id: Optional[str] = None) -> RemoteWorker:
    """Talk to a process that is already serving.

    For a worker started outside tinyray -- by hand, by a scheduler, by an
    existing launch script. tinyray takes no responsibility for its lifecycle
    and simply calls into it.
    """
    context = _require_context()
    if actor_id is None:
        import json

        health = json.loads(context.client.get_text(endpoint, "/health"))
        actor_id = health["actor"]
    return RemoteWorker(context, actor_id=actor_id, endpoint=endpoint)


def get_all(refs: Sequence[ObjectRef], timeout: float = 600.0) -> list[Any]:
    """Await a batch of calls that were dispatched together."""
    return get(list(refs), timeout=timeout)
