"""Peer-to-peer wiring between workers.

tinyray's first design was a star: the driver at the centre, workers as leaves,
and every piece of coordination flowing through the middle. That is the right
shape for a fan-out -- 32 rollouts and a learner, one loop in the driver -- and
the wrong shape for a *pipeline*, where a fleet of dataloader sidecars feeds a
fleet of trainer sidecars and the driver has nothing to say between steps.

A pipeline needs workers that can address each other. Three things were
missing:

* ``get_actor(name)`` inside a worker failed, because the name registry lives in
  the driver's head and a worker has no client to it;
* handles could not be pickled, so a peer reference could not be sent anywhere;
* ``connect(endpoint)`` worked, but only by falling through to ``init()`` and
  building a **second head** inside the worker -- a phantom cluster with its own
  supervision loop that believed it owned the machine.

This module is the missing piece. Endpoints are not known until every worker has
bound a port, so the roster cannot be an environment variable: the driver pushes
it once, after startup, and from then on the workers talk to each other without
asking.

What the driver keeps is what only the driver can do: placement, supervision,
restart. What it stops doing is relaying.
"""

from __future__ import annotations

import threading
from typing import Any, Optional

#: The method the driver calls to install a roster. Deliberately dunder-named so
#: it cannot collide with a user method, and intercepted before dispatch so it
#: is never looked up on the served object.
LINK_METHOD = "__tinyray_link__"

_lock = threading.Lock()
_identity: Optional[dict[str, Any]] = None
_roster: dict[str, list[dict[str, Any]]] = {}
_serving = False


class NotLinked(RuntimeError):
    """Peer lookup before the driver pushed a roster."""


def mark_serving() -> None:
    """Record that this process is a worker, not a driver.

    Authoritative, unlike sniffing environment variables: a process is a worker
    exactly when it is serving.
    """
    global _serving
    _serving = True


def in_worker() -> bool:
    return _serving


def install_roster(roster: dict[str, list[dict[str, Any]]], group: str, rank: int) -> dict:
    """Receive the topology from the driver. Idempotent, and re-linkable.

    Re-linking matters: a restarted peer comes back on a new port, and every
    worker holding the old endpoint has to be told.
    """
    global _identity
    with _lock:
        _roster.clear()
        _roster.update({name: list(members) for name, members in roster.items()})
        _identity = {"group": group, "rank": rank}
    return {"group": group, "rank": rank, "groups": {k: len(v) for k, v in _roster.items()}}


def roster() -> dict[str, list[dict[str, Any]]]:
    """The raw topology, as pushed."""
    with _lock:
        return {name: list(members) for name, members in _roster.items()}


def _require_identity() -> dict[str, Any]:
    with _lock:
        if _identity is None:
            raise NotLinked(
                "this worker has no peer roster; the driver must call "
                "tinyray.link(...) after the groups are up"
            )
        return dict(_identity)


def my_group() -> str:
    """The name of the group this worker belongs to."""
    return str(_require_identity()["group"])


def my_rank() -> int:
    """This worker's rank within its own group."""
    return int(_require_identity()["rank"])


def group_size(group: Optional[str] = None) -> int:
    name = group or my_group()
    with _lock:
        if name not in _roster:
            raise NotLinked(f"no group named {name!r}; known: {sorted(_roster)}")
        return len(_roster[name])


def peers(group: Optional[str] = None) -> list:
    """Connected handles to every member of `group`, indexed by rank.

    Returns :class:`~tinyray.RemoteWorker` objects, so calling a peer looks the
    same as calling a worker from the driver::

        trainers = tinyray.peers("trainer")
        trainers[rank].accept_batch.remote(ref)
    """
    from .attach import connect  # late: attach imports serve, which imports this

    name = group or my_group()
    with _lock:
        if name not in _roster:
            raise NotLinked(f"no group named {name!r}; known: {sorted(_roster)}")
        members = sorted(_roster[name], key=lambda entry: entry["rank"])
    return [connect(entry["endpoint"], entry["actor_id"]) for entry in members]


def peer(group: str, rank: int):
    """One member of `group` by rank."""
    found = peers(group)
    if not 0 <= rank < len(found):
        raise NotLinked(f"group {group!r} has ranks 0..{len(found) - 1}, not {rank}")
    return found[rank]


def _members(group: Any) -> list:
    """Members of a worker group, a list of handles, or a single handle.

    The discriminator is an underscore-prefixed marker, not ``hasattr``. Handles
    proxy every *public* name to a remote method, so ``hasattr(handle, "world_size")``
    is unconditionally true and returns a callable that would never be called.
    Underscore names are the only ones handles refuse.
    """
    if getattr(group, "_tinyray_group", False):
        return [group[rank] for rank in range(group.world_size)]
    if isinstance(group, (list, tuple)):
        return list(group)
    return [group]


def link(**groups: Any) -> dict[str, list[dict[str, Any]]]:
    """Introduce groups of workers to each other.

    After this, every member of every named group can reach every other member
    by name and rank, without going near the driver::

        loaders = tr.launch_workers(..., size=8, name="loader")
        trainers = tr.launch_workers(..., size=4, name="trainer")
        tr.link(loader=loaders, trainer=trainers)

    and inside a loader::

        tinyray.peers("trainer")[rank].accept_batch.remote(ref)

    This is a push rather than an environment variable because endpoints do not
    exist until every worker has bound a port. It is idempotent, and calling it
    again after a restart is how survivors learn the new address.

    Accepts worker groups, lists of handles, or single handles; actors and
    served processes can be mixed in one mesh.
    """
    from .api import get

    members = {name: _members(group) for name, group in groups.items()}
    entries = {
        name: [
            {"rank": rank, "actor_id": member.actor_id, "endpoint": member.endpoint}
            for rank, member in enumerate(group)
        ]
        for name, group in members.items()
    }

    # Every member is told before any is awaited. A member whose linking blocks
    # on a peer it has not been told about yet would otherwise deadlock the
    # whole mesh -- the same failure that gang construction has.
    pending = [
        member.link(entries, name, rank)
        for name, group in members.items()
        for rank, member in enumerate(group)
    ]
    get(pending)
    return entries
