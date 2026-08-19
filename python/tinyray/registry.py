"""Membership for clusters tinyray did not launch.

At a few dozen workers a driver can start every process, hand out every GPU and
push every roster. At ten thousand it can do none of those things, and it should
not be trying:

* the job was launched by Slurm, Kubernetes or ``torchrun``, which already
  allocated the GPUs and already set ``RANK``;
* pushing a roster from one process is O(N) calls and O(N^2) bytes -- measured
  on this codebase, 8,192 workers means 278 KB per push and 2.3 GB in total,
  from a single driver;
* a driver that did not start the workers cannot supervise them either.

So membership inverts. Workers **register themselves** -- one small call each,
issued from ten thousand places instead of arriving at one -- and hold the
registration open with a lease. Readers ask for the slice they need rather than
receiving the whole cluster.

Availability
------------

The registry holds **soft state**. Every entry is re-asserted by its owner once
per heartbeat interval, so a replica that loses everything is correct again one
lease later. That is what makes replication cheap here: run several, have
workers register with all of them, read from any. No consensus, no log, no
leader election -- because there is no history worth agreeing on, only a set of
facts that regenerate on their own.

The failure that matters is not "the registry lost an entry". It is "the
registry is unreachable and training stops", and the answer to that lives in the
client: :class:`RegistryClient` serves reads from cache when no replica answers,
because a stale endpoint is far better than a stalled job.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Optional

#: How long a registration survives without a heartbeat. Long enough to ride out
#: a GC pause or a network hiccup on a busy node, short enough that a dead rank
#: is not still being addressed a minute later.
DEFAULT_TTL = 30.0

#: How often a worker re-asserts itself. A third of the TTL, so two consecutive
#: heartbeats can be lost without eviction.
DEFAULT_HEARTBEAT = 10.0


class Registry:
    """Who exists, where they are, and whether they are still alive.

    An ordinary served object: ``tinyray.serve(Registry())`` gives it the same
    Rust transport every worker uses, so it inherits the framing, the connection
    pooling and the concurrency without any of it being written twice.
    """

    #: Entries scanned per sweep. Scanning 100,000 in one pass takes 43 ms with
    #: the lock held, which is enough to wreck p99. Batching bounds the pause to
    #: well under a millisecond.
    SWEEP_BATCH = 4096

    def __init__(self, ttl: float = DEFAULT_TTL, sweep_interval: float | None = None) -> None:
        self.ttl = ttl
        # Eviction granularity must scale with the TTL. Hard-coding one second
        # means a registry with ttl=0.2s evicts a second later -- not wrong, but
        # surprising, and it puts the path out of reach of a fast test.
        self.sweep_interval = sweep_interval if sweep_interval is not None else min(1.0, ttl / 4)
        self._lock = threading.Lock()
        # group -> rank -> entry
        self._members: dict[str, dict[int, dict[str, Any]]] = {}
        self._version = 0
        self._registrations = 0
        self._evictions = 0
        self._last_sweep = 0.0
        self._cursor: tuple[str, int] | None = None

    # -- worker side ------------------------------------------------------

    def register(
        self,
        group: str,
        rank: int,
        endpoint: str,
        actor_id: str,
        world_size: int = 0,
        meta: Optional[dict] = None,
        incarnation: str = "",
    ) -> dict:
        """Announce a worker. Idempotent, and replaces a stale entry.

        A restarted rank re-registers under the same ``(group, rank)`` with a
        new endpoint. Overwriting rather than rejecting is deliberate: the rank
        is the identity, the endpoint is just where it happens to be today.

        ``incarnation`` distinguishes *this* process from the one it replaced.
        During a restart both are briefly alive, and without it the old process
        would keep heartbeating a lease that now belongs to the new one --
        alternately resurrecting a dead address every few seconds.
        """
        now = time.monotonic()
        entry = {
            "group": group,
            "rank": int(rank),
            "endpoint": endpoint,
            "actor_id": actor_id,
            "world_size": int(world_size),
            "meta": dict(meta or {}),
            "incarnation": incarnation,
            "registered_at": time.time(),
            "_seen": now,
        }
        with self._lock:
            members = self._members.setdefault(group, {})
            previous = members.get(int(rank))
            members[int(rank)] = entry
            self._registrations += 1
            if previous is None or previous["endpoint"] != endpoint:
                self._version += 1
            return {
                "lease": f"{group}/{rank}",
                "ttl": self.ttl,
                "heartbeat": min(DEFAULT_HEARTBEAT, self.ttl / 3),
                "replaced": previous["endpoint"] if previous else None,
                "incarnation": incarnation,
                "version": self._version,
            }

    def heartbeat(self, lease: str, incarnation: str = "") -> dict:
        """Keep a registration alive. Cheap on purpose -- this is the hot path.

        Three answers, and the third is the interesting one:

        * ``known: False`` -- never seen, or already evicted. Re-register.
        * ``known: True`` -- alive, carry on.
        * ``superseded: True`` -- this rank now belongs to a newer process.
          Reported rather than raised, so the caller decides what to do about
          being a ghost.
        """
        group, _, rank_text = lease.partition("/")
        now = time.monotonic()
        with self._lock:
            entry = self._members.get(group, {}).get(int(rank_text))
            if entry is None:
                return {"known": False, "superseded": False, "version": self._version}
            if incarnation and entry["incarnation"] and entry["incarnation"] != incarnation:
                return {"known": True, "superseded": True, "version": self._version}
            entry["_seen"] = now
            return {"known": True, "superseded": False, "version": self._version}

    def deregister(self, lease: str, incarnation: str = "") -> dict:
        """Remove a registration, unless a newer process already took it over.

        A superseded worker shutting down must not delete its successor's entry.
        """
        group, _, rank_text = lease.partition("/")
        with self._lock:
            members = self._members.get(group, {})
            entry = members.get(int(rank_text))
            if entry is None:
                return {"removed": False, "version": self._version}
            if incarnation and entry["incarnation"] and entry["incarnation"] != incarnation:
                return {"removed": False, "superseded": True, "version": self._version}
            del members[int(rank_text)]
            self._version += 1
            return {"removed": True, "version": self._version}

    # -- reader side ------------------------------------------------------

    def lookup(
        self,
        group: str,
        ranks: Optional[list[int]] = None,
        since: int = -1,
    ) -> dict:
        """Members of `group`, optionally only the ranks asked for.

        Scoping is not an optimisation, it is the reason this scales. A trainer
        rank needs its data-parallel peers or the inference shard it is paired
        with -- a bounded number -- and asking for the whole cluster would make
        every worker's memory grow with the cluster.

        ``since`` lets a caller skip the payload when nothing has changed.
        """
        self.sweep()
        with self._lock:
            if since >= 0 and since == self._version:
                return {"version": self._version, "unchanged": True, "members": []}
            members = self._members.get(group, {})
            wanted = sorted(members) if ranks is None else [r for r in ranks if r in members]
            return {
                "version": self._version,
                "unchanged": False,
                "members": [self._public(members[rank]) for rank in wanted],
            }

    def groups(self) -> dict:
        """Group names and sizes. Bounded by the number of groups, not workers."""
        self.sweep()
        with self._lock:
            return {
                "version": self._version,
                "groups": {name: len(members) for name, members in self._members.items()},
            }

    def stats(self) -> dict:
        with self._lock:
            return {
                "version": self._version,
                "registrations": self._registrations,
                "evictions": self._evictions,
                "members": sum(len(m) for m in self._members.values()),
                "groups": len(self._members),
                "ttl": self.ttl,
            }

    # -- housekeeping -----------------------------------------------------

    def sweep(self, force: bool = False) -> int:
        """Evict whatever stopped heartbeating. Returns how many went.

        Rate limited twice, both discovered by benchmarking:

        * **By time.** This was called on every ``lookup``, and it is an O(N)
          full scan. Measured at 100,000 members: 43 ms per call, dragging
          lookup from 15,163 ops/s down to **24 ops/s**. The answer was always
          correct; only the cost was wrong, by a factor of 2,000.
        * **By batch.** One full pass over 100,000 entries holds the lock for
          43 ms, which is enough to wreck p99. A cursor bounds each pause.

        The cost is that a member can outlive its TTL by up to one sweep
        interval plus one cursor lap, which matches the documented semantics
        that eviction granularity is set by ``sweep_interval``.
        """
        now = time.monotonic()
        if not force and now - self._last_sweep < self.sweep_interval:
            return 0

        cutoff = now - self.ttl
        gone = 0
        with self._lock:
            self._last_sweep = now
            scanned = 0
            groups = sorted(self._members)
            if not groups:
                self._cursor = None
                return 0

            start_group, start_rank = self._cursor or (groups[0], -1)
            index = 0
            for offset, group in enumerate(groups):
                if group >= start_group:
                    index = offset
                    break

            cursor = None
            while scanned < self.SWEEP_BATCH and index < len(groups):
                group = groups[index]
                members = self._members.get(group)
                if not members:
                    index += 1
                    start_rank = -1
                    continue
                ranks = sorted(r for r in members if r > start_rank)
                for rank in ranks:
                    entry = members.get(rank)
                    if entry is None:
                        continue
                    if entry["_seen"] < cutoff:
                        del members[rank]
                        gone += 1
                    scanned += 1
                    if scanned >= self.SWEEP_BATCH:
                        cursor = (group, rank)
                        break
                if cursor:
                    break
                index += 1
                start_rank = -1

            self._cursor = cursor
            for group in [g for g, m in self._members.items() if not m]:
                del self._members[group]
            if gone:
                self._version += 1
                self._evictions += gone
        return gone

    @staticmethod
    def _public(entry: dict) -> dict:
        return {key: value for key, value in entry.items() if not key.startswith("_")}
