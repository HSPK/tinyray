"""tinyray: a phone book and a roll call.

Three questions, nothing else: who is here, are they still alive, and who
should I talk to. It starts no processes, allocates no GPUs, and moves no
tensors.
"""

from __future__ import annotations

import atexit
import json
import os
import random
import time
from typing import Any

from ._tinyray import Client as _Client

__all__ = ["join", "pool", "Member", "Pool", "Handle", "NotFound", "PolicyError"]

POLICIES = ("churn", "serving", "stateful", "collective")

# Seats are declared by the launcher, never handed out by tinyray.
_RANK_VARS = ("TINYRAY_SLOT", "RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK")
_SIZE_VARS = ("TINYRAY_SIZE", "WORLD_SIZE", "SLURM_NTASKS", "OMPI_COMM_WORLD_SIZE")


class NotFound(LookupError):
    """Nobody in the pool matched. Failure is explicit; there is no None."""


class PolicyError(ValueError):
    pass


def _endpoints() -> list[str]:
    raw = os.environ.get("TINYRAY_REGISTRY", "127.0.0.1:8760")
    out = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(part if "://" in part else f"http://{part}")
    return out


def _from_env(names: tuple[str, ...]) -> int | None:
    for n in names:
        v = os.environ.get(n)
        if v is not None and v.strip().lstrip("-").isdigit():
            return int(v)
    return None


class Handle:
    """One member. Read-only; the calling layer arrives in M2."""

    __slots__ = ("pool", "id", "slot", "incarnation", "url", "state", "ready")

    def __init__(self, pool_name: str, raw: dict[str, Any]):
        self.pool = pool_name
        self.id = raw["id"]
        self.slot = raw.get("slot")
        self.incarnation = raw["incarnation"]
        self.url = raw.get("url")
        self.state = raw.get("state") or {}
        self.ready = raw["ready"]

    def __repr__(self) -> str:
        seat = self.slot if self.slot is not None else self.id
        return f"<Handle {self.pool}/{seat}#{self.incarnation} {self.url}>"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Handle)
            and (self.pool, self.id, self.incarnation)
            == (other.pool, other.id, other.incarnation)
        )

    def __hash__(self) -> int:
        return hash((self.pool, self.id, self.incarnation))


class Pool:
    """A view onto one group. Lookups read the local cache, so they do no
    network I/O and cannot time out."""

    def __init__(self, name: str, client: _Client):
        self._name = name
        self._c = client
        client.watch([name])

    def _members(self, filt: dict[str, Any], require_ready: bool) -> list[Handle]:
        raw = self._c.lookup(self._name, json.dumps(filt), require_ready)
        return [Handle(self._name, m) for m in json.loads(raw)]

    def all(self, **filt: Any) -> list[Handle]:
        return self._members(filt, require_ready=True)

    def pick(self, **filt: Any) -> Handle:
        found = self._members(filt, require_ready=True)
        if not found:
            raise NotFound(f"no ready member of {self._name!r} matching {filt}")
        return random.choice(found)

    def slot(self, k: int) -> Handle:
        for h in self._members({}, require_ready=False):
            if h.slot == k:
                return h
        # Never silently substitute another member: routing a keyed request to
        # the wrong seat corrupts data instead of raising.
        raise NotFound(f"seat {k} of {self._name!r} is empty")

    def wait(self, count: int = 1, timeout: float = 30.0, **filt: Any) -> list[Handle]:
        """Block until at least `count` members match. Waiting is bounded and
        the failure says who we were waiting for."""
        deadline = time.monotonic() + timeout
        while True:
            found = self._members(filt, require_ready=True)
            if len(found) >= count:
                return found
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"waited {timeout}s for {count} ready member(s) of "
                    f"{self._name!r} matching {filt}, saw {len(found)}"
                )
            time.sleep(0.05)

    def __len__(self) -> int:
        return len(self.all())

    def __repr__(self) -> str:
        info = self._c.pool_info(self._name)
        return f"<Pool {self._name} members={len(self.all())} version={info[0] if info else None}>"


class Member:
    """This process's own registration."""

    def __init__(self, client: _Client, pool_name: str, slot: int | None, incarnation: int):
        self._c = client
        self.pool = pool_name
        self.slot = slot
        self.incarnation = incarnation
        self._state: dict[str, Any] = {}
        self._left = False

    def ready(self, **state: Any) -> Member:
        """Hang out a sign: I can take work, and here is my state. It sends no
        message to anyone -- the next heartbeat carries it."""
        self._state.update(state)
        self._c.set_state(json.dumps(self._state), True)
        return self

    def unready(self) -> Member:
        self._c.set_state(json.dumps(self._state), False)
        return self

    @property
    def state(self) -> dict[str, Any]:
        return dict(self._state)

    @property
    def accepted(self) -> bool:
        """False once a later incarnation has taken this seat."""
        return self._c.accepted

    def stats(self) -> dict[str, int]:
        return self._c.stats()

    def leave(self) -> None:
        if not self._left:
            self._left = True
            try:
                self._c.leave()
            except Exception:  # interpreter teardown: nothing useful left to do
                pass

    def __enter__(self) -> Member:
        return self

    def __exit__(self, *exc: object) -> None:
        self.leave()

    def __repr__(self) -> str:
        seat = self.slot if self.slot is not None else "-"
        return f"<Member {self.pool}/{seat}#{self.incarnation}>"


_client: _Client | None = None


def join(
    pool: str,
    policy: str = "churn",
    *,
    slot: int | None = None,
    size: int | None = None,
    url: str | None = None,
    serves: Any = None,
) -> Member:
    """Report in. One line per process."""
    global _client
    if policy not in POLICIES:
        raise PolicyError(f"policy must be one of {POLICIES}, got {policy!r}")
    if serves is not None:
        raise NotImplementedError("serves= arrives in M2")

    slotted = policy in ("stateful", "collective")
    if slotted and slot is None:
        slot = _from_env(_RANK_VARS)
        if slot is None:
            raise PolicyError(
                f"policy={policy!r} needs a seat number; pass slot= or set one of {_RANK_VARS}"
            )
    if slotted and size is None:
        size = _from_env(_SIZE_VARS)

    # Fungible members have no seat, so their key is just a fresh identity.
    ident = slot if slot is not None else random.getrandbits(63)
    # Tenure must increase when a seat is re-taken; wall clock is enough here
    # and needs no coordination.
    incarnation = time.time_ns() // 1_000_000

    c = _Client(
        endpoints=_endpoints(),
        pool=pool,
        id=ident,
        incarnation=incarnation,
        policy=policy,
        slot=slot,
        size=size,
        url=url,
    )
    c.watch([pool])
    c.start()
    _client = c
    member = Member(c, pool, slot, incarnation)
    # A process that exits normally should say goodbye, so the seat frees up
    # immediately instead of waiting out the lease. SIGKILL still falls back
    # to lease expiry -- both paths work, they just differ in speed.
    atexit.register(member.leave)
    return member


def pool(name: str) -> Pool:
    if _client is None:
        raise RuntimeError("call tinyray.join(...) before looking anyone up")
    return Pool(name, _client)
