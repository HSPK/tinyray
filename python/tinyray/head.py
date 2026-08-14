"""The head: cluster registry, placement and supervision.

Runs inside the driver for a single-machine cluster, and as its own process
(``tinyray start --head``) for a multi-node one. Both paths use the same
:class:`Head` object, so local development exercises the real code.

The head is deliberately absent from the data path. It is consulted when an
actor is created, looked up, or dies -- never when a result moves.
"""

from __future__ import annotations

import contextlib
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ._tinyray import ClusterState, detect_cpus, detect_gpus, new_id
from .launcher import ActorProcess, Launcher

#: How often the supervisor checks for dead nodes and dead actors.
SUPERVISE_INTERVAL_SECONDS = 1.0

#: How often a node agent reports in. Must be comfortably shorter than the
#: head's heartbeat timeout, or healthy nodes get declared dead.
HEARTBEAT_INTERVAL_SECONDS = 2.0


class PlacementFailed(RuntimeError):
    """The cluster cannot host the requested actor or gang."""


@dataclass
class NodeHandle:
    """A node agent the head can ask to start processes."""

    node_id: str
    endpoint: str
    hostname: str
    agent: LocalNodeAgent  # local today; an HTTP proxy once nodes are remote


class Head:
    """Cluster bookkeeping plus the supervision loop."""

    def __init__(
        self,
        *,
        heartbeat_timeout: float = 30.0,
        supervise_interval: float = SUPERVISE_INTERVAL_SECONDS,
        heartbeat_interval: float = HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        self.supervise_interval = supervise_interval
        # The agent must report in several times per deadline. If the interval
        # ever exceeds the timeout, every healthy node is declared dead and the
        # cluster tears itself down -- so derive it rather than trust a
        # constant to stay smaller than an unrelated one.
        self.heartbeat_interval = min(heartbeat_interval, max(heartbeat_timeout / 4.0, 0.05))
        self.heartbeat_timeout = heartbeat_timeout
        self.state = ClusterState(heartbeat_timeout_seconds=heartbeat_timeout)
        self.nodes: dict[str, NodeHandle] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._supervisor: Optional[threading.Thread] = None
        self._on_actor_moved: Optional[Callable[[str, str], None]] = None
        self._on_actor_lost: Optional[Callable[[str, str], None]] = None

    # -- registration ----------------------------------------------------

    def register_node(
        self,
        agent: LocalNodeAgent,
        *,
        num_cpus: Optional[float] = None,
        num_gpus: Optional[int] = None,
    ) -> str:
        """Add a node agent to the cluster."""
        node_id = agent.node_id
        gpu_ids = agent.gpu_ids if num_gpus is None else list(range(int(num_gpus)))
        self.state.register_node(
            node_id=node_id,
            endpoint=agent.endpoint,
            hostname=agent.hostname,
            num_cpus=float(num_cpus if num_cpus is not None else agent.num_cpus),
            num_gpus=float(len(gpu_ids)),
            gpu_ids=gpu_ids,
        )
        with self._lock:
            self.nodes[node_id] = NodeHandle(
                node_id=node_id,
                endpoint=agent.endpoint,
                hostname=agent.hostname,
                agent=agent,
            )
        # Without this the node stops reporting in and the supervisor declares
        # it dead as soon as the timeout expires, tearing down every actor on
        # it. Registration alone only counts as the first heartbeat.
        agent.start_heartbeat(self)
        return node_id

    def set_callbacks(
        self,
        *,
        on_actor_moved: Optional[Callable[[str, str], None]] = None,
        on_actor_lost: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        """Register hooks the driver uses to refresh its routing table."""
        self._on_actor_moved = on_actor_moved
        self._on_actor_lost = on_actor_lost

    # -- placement -------------------------------------------------------

    def create_actor(
        self,
        *,
        name: str,
        num_cpus: float = 1.0,
        num_gpus: float = 0.0,
        memory_bytes: int = 0,
        strategy: str = "SPREAD",
        max_restarts: int = 0,
        max_pending_calls: int = 1000,
        store_max_bytes: Optional[int] = None,
        store_ttl_seconds: Optional[float] = None,
        actor_name: Optional[str] = None,
        detached: bool = False,
    ) -> dict[str, Any]:
        """Place and start one actor. Returns its registry entry."""
        try:
            node_id, _endpoint, gpu_ids = self.state.place(
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                memory_bytes=memory_bytes,
                strategy=strategy,
            )
        except Exception as exc:
            raise PlacementFailed(str(exc)) from exc

        return self._start_placed(
            node_id=node_id,
            gpu_ids=gpu_ids,
            name=name,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory_bytes=memory_bytes,
            max_restarts=max_restarts,
            max_pending_calls=max_pending_calls,
            store_max_bytes=store_max_bytes,
            store_ttl_seconds=store_ttl_seconds,
            actor_name=actor_name,
            detached=detached,
        )

    def create_actors(
        self,
        count: int,
        *,
        name: str,
        num_cpus: float = 1.0,
        num_gpus: float = 0.0,
        memory_bytes: int = 0,
        strategy: str = "SPREAD",
        max_restarts: int = 0,
        max_pending_calls: int = 1000,
    ) -> list[dict]:
        """Place and start `count` actors atomically.

        All or nothing on purpose. A group that comes up halfway cannot form a
        collective, and the run then hangs waiting for ranks that will never
        arrive -- a failure that is far harder to diagnose than a clean refusal.
        """
        try:
            placements = self.state.place_gang(
                count,
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                memory_bytes=memory_bytes,
                strategy=strategy,
            )
        except Exception as exc:
            raise PlacementFailed(str(exc)) from exc

        started: list[dict] = []
        try:
            for index, (node_id, _endpoint, gpu_ids) in enumerate(placements):
                started.append(
                    self._start_placed(
                        node_id=node_id,
                        gpu_ids=gpu_ids,
                        name=f"{name}-{index}",
                        num_cpus=num_cpus,
                        num_gpus=num_gpus,
                        memory_bytes=memory_bytes,
                        max_restarts=max_restarts,
                        max_pending_calls=max_pending_calls,
                        actor_name=None,
                        detached=False,
                    )
                )
        except Exception:
            # Reserved resources are useless without the processes; unwind so a
            # failed gang does not strand a quarter of the cluster.
            for entry in started:
                self.kill_actor(entry["actor_id"])
            for node_id, _endpoint, gpu_ids in placements[len(started) :]:
                self.state.release(node_id, num_cpus, num_gpus, gpu_ids)
            raise
        return started

    def _start_placed(
        self,
        *,
        node_id: str,
        gpu_ids: list[int],
        name: str,
        num_cpus: float,
        num_gpus: float,
        memory_bytes: int,
        max_restarts: int,
        max_pending_calls: int,
        store_max_bytes: Optional[int] = None,
        store_ttl_seconds: Optional[float] = None,
        actor_name: Optional[str],
        detached: bool,
    ) -> dict[str, Any]:
        with self._lock:
            handle = self.nodes[node_id]

        try:
            process = handle.agent.start_actor(
                actor_id=None,
                name=name,
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                gpu_ids=gpu_ids,
                max_restarts=max_restarts,
                max_pending_calls=max_pending_calls,
                store_max_bytes=store_max_bytes,
                store_ttl_seconds=store_ttl_seconds,
            )
        except Exception:
            self.state.release(node_id, num_cpus, num_gpus, gpu_ids)
            raise

        # The agent chooses the id: a pre-warmed process already has one, and
        # adopting it is what makes warm starts possible at all.
        actor_id = process.actor_id
        self.state.add_actor(
            actor_id=actor_id,
            node_id=node_id,
            endpoint=process.endpoint,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory_bytes=memory_bytes,
            gpu_ids=gpu_ids,
            name=actor_name,
            max_restarts=max_restarts,
            detached=detached,
        )
        return {
            "actor_id": actor_id,
            "name": name,
            "node_id": node_id,
            "endpoint": process.endpoint,
            "gpu_ids": gpu_ids,
            "pid": process.pid,
        }

    # -- lookup ----------------------------------------------------------

    def get_actor(self, actor_id: str) -> Optional[dict[str, Any]]:
        return self.state.actor(actor_id)

    def get_actor_by_name(self, name: str) -> Optional[dict[str, Any]]:
        actor_id = self.state.actor_by_name(name)
        return self.state.actor(actor_id) if actor_id else None

    def actors(self) -> list[dict]:
        return self.state.actors()

    def nodes_info(self) -> list[dict]:
        return self.state.nodes()

    # -- lifecycle -------------------------------------------------------

    def kill_actor(self, actor_id: str) -> None:
        entry = self.state.actor(actor_id)
        if entry is None:
            return
        with self._lock:
            handle = self.nodes.get(entry["node_id"])
        if handle is not None:
            handle.agent.kill_actor(actor_id)
        self.state.remove_actor(actor_id)

    def start_supervisor(self) -> None:
        if self._supervisor is not None:
            return
        self._supervisor = threading.Thread(
            target=self._supervise_loop, name="tinyray-supervisor", daemon=True
        )
        self._supervisor.start()

    def stop(self) -> None:
        self._stop.set()
        if self._supervisor is not None:
            self._supervisor.join(timeout=5)
            self._supervisor = None
        with self._lock:
            agents = [handle.agent for handle in self.nodes.values()]
            self.nodes.clear()
        for agent in agents:
            agent.shutdown()

    def _supervise_loop(self) -> None:
        while not self._stop.wait(self.supervise_interval):
            try:
                self._supervise_once()
            except Exception:
                import traceback

                traceback.print_exc()

    def record_heartbeat(self, agent: LocalNodeAgent) -> None:
        """Accept a node agent's periodic report of its free resources."""
        resources = agent.resources()
        self.state.heartbeat(
            resources["node_id"],
            float(resources["num_cpus"]),
            float(len(resources["gpu_ids"])),
            list(resources["gpu_ids"]),
        )

    def _supervise_once(self) -> None:
        for node_id in self.state.dead_nodes():
            for actor_id in self.state.remove_node(node_id):
                self._handle_actor_death(actor_id, reason=f"node {node_id[:8]} stopped responding")
            with self._lock:
                self.nodes.pop(node_id, None)

        with self._lock:
            handles = list(self.nodes.values())
        for handle in handles:
            for actor_id, exit_code in handle.agent.reap():
                self._handle_actor_death(actor_id, reason=f"process exited with code {exit_code}")

    def _handle_actor_death(self, actor_id: str, *, reason: str) -> None:
        entry = self.state.actor(actor_id)
        if entry is None:
            return

        should_restart = self.state.note_actor_died(actor_id)
        if not should_restart:
            self.state.remove_actor(actor_id)
            if self._on_actor_lost is not None:
                self._on_actor_lost(actor_id, reason)
            return

        with self._lock:
            handle = self.nodes.get(entry["node_id"])
        if handle is None:
            self.state.remove_actor(actor_id)
            if self._on_actor_lost is not None:
                self._on_actor_lost(actor_id, reason)
            return

        try:
            process = handle.agent.start_actor(
                actor_id=actor_id,
                name=entry.get("name") or "actor",
                num_cpus=entry["num_cpus"],
                num_gpus=entry["num_gpus"],
                gpu_ids=entry["gpu_ids"],
                max_restarts=entry["max_restarts"],
                # A restart must keep its identity, so it cannot adopt a warm
                # process (which comes with an id of its own).
                allow_prewarm=False,
            )
        except Exception as exc:
            self.state.remove_actor(actor_id)
            if self._on_actor_lost is not None:
                self._on_actor_lost(actor_id, f"{reason}; restart failed: {exc}")
            return

        # A restarted actor is a *new process* with an empty result store, so
        # every reference into the old one is now lost. Callers are told, and
        # sequence numbering restarts from zero.
        self.state.set_actor_endpoint(actor_id, process.endpoint)
        if self._on_actor_moved is not None:
            self._on_actor_moved(actor_id, process.endpoint)


class LocalNodeAgent:
    """A node agent for this machine, usable in-process.

    The multi-node agent wraps the same object behind HTTP; keeping the local
    path identical means the common case is the well-tested one.
    """

    def __init__(
        self,
        launcher: Launcher,
        *,
        num_cpus: Optional[float] = None,
        gpu_ids=None,
        prewarm: int = 0,
    ) -> None:
        import socket

        from .prewarm import PrewarmPool

        self.node_id = str(new_id())
        self.hostname = socket.gethostname()
        self.endpoint = f"{self.hostname}:local"
        self.launcher = launcher
        self.num_cpus = float(num_cpus if num_cpus is not None else detect_cpus())
        self.gpu_ids = list(gpu_ids) if gpu_ids is not None else detect_gpus()
        self._known: dict[str, ActorProcess] = {}
        # Off unless asked for: warm interpreters are a real win for sweeps but
        # a surprise for anyone who did not ask for background processes.
        self.pool = PrewarmPool(launcher, size=prewarm, enabled=prewarm > 0)
        # Warm up immediately: filling on demand would leave the pool a step
        # behind and the early actors paying full price.
        self.pool.prime()
        self._stop = threading.Event()
        self._heartbeat: Optional[threading.Thread] = None

    def start_heartbeat(self, head: Head) -> None:
        """Report in periodically so the head knows this node is alive.

        A local agent is alive whenever the driver is, but it still goes
        through the same path as a remote one: keeping a single mechanism means
        the common case is the tested case.
        """
        if self._heartbeat is not None:
            return
        self._heartbeat = threading.Thread(
            target=self._heartbeat_loop, args=(head,), name="tinyray-heartbeat", daemon=True
        )
        self._heartbeat.start()

    def _heartbeat_loop(self, head: Head) -> None:
        interval = getattr(head, "heartbeat_interval", HEARTBEAT_INTERVAL_SECONDS)
        while not self._stop.wait(interval):
            with contextlib.suppress(Exception):
                head.record_heartbeat(self)

    def start_actor(self, *, allow_prewarm: bool = True, **kwargs: Any) -> ActorProcess:
        process = None
        if allow_prewarm and self._is_default_shaped(kwargs):
            # A warm process was started with default settings, so it can only
            # serve an actor that wants them.
            process = self.pool.acquire(gpu_ids=kwargs.get("gpu_ids"))
        if process is None:
            process = self.launcher.start_actor(**kwargs)
        self._known[process.actor_id] = process
        return process

    @staticmethod
    def _is_default_shaped(kwargs: dict[str, Any]) -> bool:
        """Whether a warm process can serve this request.

        Process-level settings are fixed at startup, so an actor asking for
        anything non-default has to have its own process.
        """
        return (
            kwargs.get("actor_id") is None
            and kwargs.get("max_pending_calls", 1000) == 1000
            and kwargs.get("store_max_bytes") is None
            and kwargs.get("store_ttl_seconds") is None
        )

    def kill_actor(self, actor_id: str) -> None:
        self._known.pop(actor_id, None)
        self.launcher.kill_actor(actor_id)

    def reap(self) -> list[tuple[str, int]]:
        """Report actors whose processes have exited since the last check."""
        dead = []
        for actor_id, process in list(self._known.items()):
            code = process.exit_code()
            if code is not None:
                dead.append((actor_id, code))
                self._known.pop(actor_id, None)
        return dead

    def shutdown(self) -> None:
        self.pool.shutdown()
        self._stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(timeout=5)
            self._heartbeat = None
        self.launcher.shutdown()

    def resources(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "hostname": self.hostname,
            "num_cpus": self.num_cpus,
            "gpu_ids": list(self.gpu_ids),
        }


def wait_for(
    predicate: Callable[[], bool],
    *,
    timeout: float,
    interval: float = 0.05,
    message: str = "",
) -> None:
    """Poll until `predicate` is true, or raise TimeoutError."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise TimeoutError(message or f"condition not met within {timeout}s")
