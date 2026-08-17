"""Joining a cluster tinyray did not launch, and driving one.

The entry points for the scale path. Nothing here allocates a GPU, starts a
process or supervises anything: Slurm, Kubernetes or ``torchrun`` did all of
that before tinyray was imported.

A worker::

    # trainer.py -- launched by torchrun, srun, or a Kubernetes Job
    dist.init_process_group("nccl")  # yours
    trainer = build_trainer()  # yours
    tinyray.join(trainer, group="trainer")  # the only tinyray line

A controller, anywhere at all::

    cluster = tinyray.attach()
    trainers = cluster.group("trainer")
    trainers.wait_ready(size=1024)
    rollouts = cluster.group("rollout")

A peer, inside a worker::

    tinyray.group("rollout")[shard].generate.remote(prompt)

Rank comes from the launcher's environment -- ``RANK`` from torchrun,
``SLURM_PROCID`` from Slurm -- because inventing a second numbering scheme
alongside the one the framework already uses is how integrations break.
"""

from __future__ import annotations

import atexit
import logging
import os
import threading
import time
from typing import Any, Optional

from .registry import DEFAULT_HEARTBEAT, Registry

log = logging.getLogger("tinyray.cluster")

#: Comma-separated registry replicas. One is fine; several is how you survive
#: losing one.
REGISTRY_ENV = "TINYRAY_REGISTRY"

#: Registry replicas get their own identities, discovered on first contact.
#:
#: They were briefly given one shared constant, to save a round trip. It made a
#: single replica slightly faster and two replicas permanently broken: the
#: client routes by actor id, so the second registration overwrote the first,
#: and calls were submitted to one replica then fetched from the other. Every
#: single-replica test passed. Identity is not an optimisation site.


class RegistryUnavailable(RuntimeError):
    """No replica answered, and nothing useful was cached."""


def _env_rank() -> int:
    for name in ("RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK", "PMI_RANK"):
        value = os.environ.get(name)
        if value is not None:
            return int(value)
    return 0


def _env_world_size() -> int:
    for name in ("WORLD_SIZE", "SLURM_NTASKS", "OMPI_COMM_WORLD_SIZE", "PMI_SIZE"):
        value = os.environ.get(name)
        if value is not None:
            return int(value)
    return 0


def _env_local_rank() -> int:
    for name in ("LOCAL_RANK", "SLURM_LOCALID", "OMPI_COMM_WORLD_LOCAL_RANK"):
        value = os.environ.get(name)
        if value is not None:
            return int(value)
    return 0


def _visible_devices() -> str:
    return os.environ.get("CUDA_VISIBLE_DEVICES", "")


def _replicas(registry: Optional[str]) -> list[str]:
    raw = registry or os.environ.get(REGISTRY_ENV, "")
    found = [part.strip() for part in raw.split(",") if part.strip()]
    if not found:
        raise RegistryUnavailable(
            f"no registry address; pass registry=... or set {REGISTRY_ENV} "
            "(comma-separated for replicas)"
        )
    return found


class RegistryClient:
    """Talks to one or more registry replicas, and remembers what they said.

    Two behaviours carry the availability story:

    *Writes go everywhere.* A registration is sent to every replica, and
    succeeds if any accepted it. Since entries are re-asserted on every
    heartbeat, a replica that was down catches up on its own.

    *Reads fall back to cache.* If no replica answers, the last known answer is
    returned. A stale endpoint is worth far more than a stopped training job,
    and the endpoint is usually still correct -- what failed was the registry,
    not the peer.
    """

    def __init__(self, endpoints: Optional[str] = None, *, cache_ttl: float = 5.0) -> None:
        self.endpoints = _replicas(endpoints)
        self.cache_ttl = cache_ttl
        self._workers: dict[str, Any] = {}
        self._cache: dict[tuple, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._preferred = 0
        self.failures = 0
        self.cache_hits = 0
        self.served_from_stale = 0

    # -- transport --------------------------------------------------------

    def _worker(self, endpoint: str):
        from .attach import connect

        with self._lock:
            existing = self._workers.get(endpoint)
        if existing is None:
            existing = connect(endpoint)
            with self._lock:
                self._workers[endpoint] = existing
        return existing

    def _call_one(self, endpoint: str, method: str, *args, timeout: float) -> Any:
        from .api import get

        worker = self._worker(endpoint)
        return get(getattr(worker, method).remote(*args), timeout=timeout)

    def call_any(self, method: str, *args, timeout: float = 10.0) -> Any:
        """First replica that answers wins, starting from the last good one."""
        order = self.endpoints[self._preferred :] + self.endpoints[: self._preferred]
        last: Optional[Exception] = None
        for endpoint in order:
            try:
                result = self._call_one(endpoint, method, *args, timeout=timeout)
                self._preferred = self.endpoints.index(endpoint)
                return result
            except Exception as exc:  # any failure means try the next
                self.failures += 1
                last = exc
                log.debug("registry %s failed %s: %s", endpoint, method, exc)
        raise RegistryUnavailable(f"no registry replica answered {method}: {last}") from last

    def call_all(self, method: str, *args, timeout: float = 10.0) -> list[Any]:
        """Every replica, tolerating failures. Raises only if all of them fail."""
        results = []
        last: Optional[Exception] = None
        for endpoint in self.endpoints:
            try:
                results.append(self._call_one(endpoint, method, *args, timeout=timeout))
            except Exception as exc:
                self.failures += 1
                last = exc
        if not results:
            raise RegistryUnavailable(f"no registry replica accepted {method}: {last}") from last
        return results

    # -- operations -------------------------------------------------------

    def register(self, group: str, rank: int, endpoint: str, actor_id: str, **fields) -> dict:
        return self.call_all("register", group, rank, endpoint, actor_id, *fields.values())[0]

    def heartbeat(self, lease: str) -> list[dict]:
        return self.call_all("heartbeat", lease, timeout=5.0)

    def deregister(self, lease: str) -> None:
        try:
            self.call_all("deregister", lease, timeout=2.0)
        except RegistryUnavailable:
            # The lease will expire on its own. Shutdown must not fail because a
            # registry replica happened to be restarting.
            log.debug("deregistration of %s went nowhere; letting the lease expire", lease)

    def lookup(
        self,
        group: str,
        ranks: Optional[list[int]] = None,
        *,
        fresh: bool = False,
    ) -> list[dict]:
        key = (group, tuple(ranks) if ranks is not None else None)
        now = time.monotonic()
        if not fresh:
            with self._lock:
                cached = self._cache.get(key)
            if cached is not None and now - cached[0] < self.cache_ttl:
                self.cache_hits += 1
                return cached[1]
        try:
            answer = self.call_any("lookup", group, ranks)
            members = answer["members"]
        except RegistryUnavailable:
            with self._lock:
                cached = self._cache.get(key)
            if cached is None:
                raise
            self.served_from_stale += 1
            log.warning("registry unreachable; serving %s from cache", group)
            return cached[1]
        with self._lock:
            self._cache[key] = (now, members)
        return members

    def groups(self) -> dict[str, int]:
        return self.call_any("groups")["groups"]

    def stats(self) -> dict:
        return self.call_any("stats")


class Membership:
    """A worker's own registration, kept alive in the background.

    The heartbeat runs on a daemon thread that swallows everything. A sidecar
    losing contact with the registry must never be the reason a training job
    stops -- the worst honest outcome is that peers address a stale endpoint,
    and the worst dishonest one is a controller that thinks the job is healthy
    when it is not.
    """

    def __init__(
        self,
        client: RegistryClient,
        group: str,
        rank: int,
        endpoint: str,
        actor_id: str,
        meta: dict,
        world_size: int,
    ) -> None:
        self.client = client
        self.group = group
        self.rank = rank
        self.endpoint = endpoint
        self.actor_id = actor_id
        self.meta = meta
        self.world_size = world_size
        self.lease = f"{group}/{rank}"
        self.interval = DEFAULT_HEARTBEAT
        self.beats = 0
        self.reregistrations = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def announce(self) -> dict:
        answer = self.client.call_all(
            "register",
            self.group,
            self.rank,
            self.endpoint,
            self.actor_id,
            self.world_size,
            self.meta,
        )[0]
        self.lease = answer["lease"]
        self.interval = float(answer.get("heartbeat", DEFAULT_HEARTBEAT))
        return answer

    def start(self) -> Membership:
        self._thread = threading.Thread(
            target=self._beat, name=f"tinyray-heartbeat-{self.group}-{self.rank}", daemon=True
        )
        self._thread.start()
        atexit.register(self.stop)
        return self

    def _beat(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                answers = self.client.heartbeat(self.lease)
                self.beats += 1
                # A replica that restarted, or that evicted us during a pause,
                # has never heard of this lease. Re-assert rather than vanish.
                if any(not answer.get("known", False) for answer in answers):
                    self.announce()
                    self.reregistrations += 1
            except Exception as exc:  # see the class docstring
                log.warning("heartbeat for %s failed: %s", self.lease, exc)

    def stop(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        self.client.deregister(self.lease)


class GroupView:
    """A named group of workers, resolved lazily and cached.

    Indexing, iteration and ``run`` all work on whatever the view covers, which
    may be the whole group or a slice of it. Taking a slice is the normal thing
    to do: a rank usually needs its data-parallel peers or one inference shard,
    and materialising ten thousand endpoints to reach four of them is how a
    controller runs out of memory.
    """

    def __init__(
        self,
        client: RegistryClient,
        name: str,
        ranks: Optional[list[int]] = None,
    ) -> None:
        self._client = client
        self.name = name
        self._ranks = list(ranks) if ranks is not None else None

    def members(self, *, fresh: bool = False) -> list[dict]:
        return self._client.lookup(self.name, self._ranks, fresh=fresh)

    def ranks(self, ranks: list[int]) -> GroupView:
        """A narrower view. The registry filters; the wire carries only these."""
        return GroupView(self._client, self.name, list(ranks))

    def shard(self, index: int, count: int) -> GroupView:
        """Every `count`-th member starting at `index` -- the usual DP split."""
        everyone = sorted(entry["rank"] for entry in self._client.lookup(self.name))
        return GroupView(self._client, self.name, everyone[index::count])

    def __len__(self) -> int:
        return len(self.members())

    def __iter__(self):
        from .attach import connect

        return iter([connect(entry["endpoint"], entry["actor_id"]) for entry in self.members()])

    def __getitem__(self, index: int):
        from .attach import connect

        members = self.members()
        by_rank = {entry["rank"]: entry for entry in members}
        entry = by_rank.get(index)
        if entry is None:
            if not 0 <= index < len(members):
                raise IndexError(
                    f"group {self.name!r} has no rank {index}; "
                    f"present: {sorted(by_rank) if len(by_rank) < 20 else f'{len(by_rank)} ranks'}"
                )
            entry = members[index]
        return connect(entry["endpoint"], entry["actor_id"])

    def __repr__(self) -> str:
        scope = "" if self._ranks is None else f", ranks={len(self._ranks)}"
        return f"GroupView({self.name}{scope})"

    def wait_ready(self, size: int, *, timeout: float = 600.0, poll: float = 1.0) -> GroupView:
        """Block until `size` members have registered.

        The replacement for gang placement. tinyray cannot start ten thousand
        ranks atomically because it does not start them at all -- but it can
        refuse to proceed until the launcher has, which gives the same
        guarantee at the point where it matters.
        """
        deadline = time.monotonic() + timeout
        seen = 0
        while time.monotonic() < deadline:
            seen = len(self._client.lookup(self.name, self._ranks, fresh=True))
            if seen >= size:
                return self
            time.sleep(poll)
        raise TimeoutError(
            f"waited {timeout:.0f}s for {size} members of {self.name!r}; {seen} registered. "
            "Check the launcher: tinyray does not start these processes"
        )

    def run(self, method: str, *args: Any, timeout: float = 600.0, **kwargs: Any) -> list:
        """Call every member of the view, dispatching before awaiting.

        Dispatch-then-await is required, not merely faster: a method containing
        a collective only returns once every rank has entered it.
        """
        from .api import get

        workers = list(self)
        pending = [getattr(worker, method).remote(*args, **kwargs) for worker in workers]
        return get(pending, timeout=timeout)


class Cluster:
    """A controller's view. Holds no state that matters -- ask it again anytime."""

    def __init__(self, client: RegistryClient) -> None:
        self.client = client

    def group(self, name: str) -> GroupView:
        return GroupView(self.client, name)

    def groups(self) -> dict[str, int]:
        return self.client.groups()

    def stats(self) -> dict:
        return self.client.stats()

    def __repr__(self) -> str:
        return f"Cluster({','.join(self.client.endpoints)})"


_local: Optional[Membership] = None
_client: Optional[RegistryClient] = None
_client_lock = threading.Lock()


def client(registry: Optional[str] = None) -> RegistryClient:
    """The process-wide registry client."""
    global _client
    with _client_lock:
        if _client is None or (registry and _replicas(registry) != _client.endpoints):
            _client = RegistryClient(registry)
        return _client


def attach(registry: Optional[str] = None) -> Cluster:
    """Point a controller at a running cluster.

    Works from a login node, a notebook, a Kubernetes Job or one of the workers.
    Nothing is started and nothing is owned.
    """
    return Cluster(client(registry))


def membership() -> Membership:
    if _local is None:
        raise RegistryUnavailable("this process has not called tinyray.join()")
    return _local


def group(name: str, ranks: Optional[list[int]] = None) -> GroupView:
    """A peer group, from inside a worker or from a controller."""
    view = GroupView(client(), name)
    return view if ranks is None else view.ranks(ranks)


def join(
    target: Any,
    *,
    group: str,
    rank: Optional[int] = None,
    world_size: Optional[int] = None,
    registry: Optional[str] = None,
    bind: Optional[str] = None,
    meta: Optional[dict] = None,
    max_pending_calls: int = 1000,
    wait_for_registry: float = 300.0,
) -> Membership:
    """Serve `target` and register it, then return.

    **Does not block.** The control port runs on its own thread so the caller
    keeps ``__main__`` -- which is the whole point, because ``__main__`` belongs
    to Megatron, SGLang or whatever else started this process.

    Rank and world size come from the launcher's environment unless you override
    them. No resources are declared: the GPUs were assigned before this process
    existed, and repeating that here would only let the two disagree.
    """
    global _local
    from .serve import serve

    resolved_rank = _env_rank() if rank is None else rank
    resolved_world = _env_world_size() if world_size is None else world_size

    server = serve(target, background=True, bind=bind, max_pending_calls=max_pending_calls)

    facts = {
        "local_rank": _env_local_rank(),
        "cuda_visible_devices": _visible_devices(),
        "pid": os.getpid(),
        "host": os.environ.get("HOSTNAME", ""),
    }
    facts.update(meta or {})

    registry_client = client(registry)
    local = Membership(
        registry_client,
        group=group,
        rank=resolved_rank,
        endpoint=server.endpoint,
        actor_id=server.actor_id,
        meta=facts,
        world_size=resolved_world,
    )

    # The registry may not be up yet: Slurm starts ranks in whatever order it
    # likes, and a worker that gives up because it was early would make startup
    # a race.
    deadline = time.monotonic() + wait_for_registry
    while True:
        try:
            local.announce()
            break
        except RegistryUnavailable:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1.0)

    _local = local.start()
    return _local


def serve_registry(
    bind: str = "0.0.0.0:7777",
    *,
    ttl: float = 30.0,
    background: bool = False,
) -> Any:
    """Run a registry replica.

    Run several. They do not talk to each other and do not need to: every entry
    is re-asserted by its owner once per heartbeat, so a replica that starts
    late, restarts, or loses everything is correct again one lease later.
    """
    from .serve import serve

    return serve(Registry(ttl=ttl), bind=bind, background=background)
