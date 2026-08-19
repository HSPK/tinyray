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
import socket
import time
from typing import Any

from ._rpc import DEFAULT_TIMEOUT, AsyncHandleMixin, BoundMethod
from ._errors import (
    Fenced,
    NotFound,
    PolicyError,
    RemoteError,
    Stale,
    TinyrayError,
    Unreachable,
)
from ._serve import MethodServer
from ._tinyray import Client as _Client

__all__ = [
    "join",
    "pool",
    "Member",
    "Pool",
    "Handle",
    "AsyncHandle",
    "Epoch",
    "Stale",
    "NotFound",
    "PolicyError",
    "TinyrayError",
    "Unreachable",
    "Fenced",
    "RemoteError",
    "apool",
]

POLICIES = ("churn", "serving", "stateful", "collective")

# Seats are declared by the launcher, never handed out by tinyray.
_RANK_VARS = ("TINYRAY_SLOT", "RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK")
_SIZE_VARS = ("TINYRAY_SIZE", "WORLD_SIZE", "SLURM_NTASKS", "OMPI_COMM_WORLD_SIZE")


def _endpoints() -> list[str]:
    raw = os.environ.get("TINYRAY_REGISTRY", "127.0.0.1:8760")
    out = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(part if "://" in part else f"http://{part}")
    return out


def _advertise() -> str:
    """The address peers should use to reach us.

    No loopback fallback: publishing 127.0.0.1 from a multi-node job is silent
    misrouting -- peers elsewhere reach whatever listens on that port locally.
    """
    explicit = os.environ.get("TINYRAY_ADVERTISE")
    if explicit:
        return explicit
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Asks the routing table which local address would be used; sends
        # nothing, and does not require the target to exist.
        probe.connect(("10.255.255.255", 1))
        return probe.getsockname()[0]
    except OSError as exc:
        raise RuntimeError(
            "cannot work out which address peers should use to reach this "
            "process; set TINYRAY_ADVERTISE=<host-or-ip> or pass url=..."
        ) from exc
    finally:
        probe.close()


def _from_env(names: tuple[str, ...]) -> int | None:
    for n in names:
        v = os.environ.get(n)
        if v is not None and v.strip().lstrip("-").isdigit():
            return int(v)
    return None


class Handle:
    """One member. Attribute access proxies to a method on the far side."""

    __slots__ = ("pool", "id", "slot", "incarnation", "url", "state", "ready", "_methods")

    def __init__(self, pool_name: str, raw: dict[str, Any], methods: tuple[str, ...] = ()):
        self._methods = methods
        self.pool = pool_name
        self.id = raw["id"]
        self.slot = raw.get("slot")
        self.incarnation = raw["incarnation"]
        self.url = raw.get("url")
        self.state = raw.get("state") or {}
        self.ready = raw["ready"]

    @property
    def identity(self) -> str:
        seat = self.slot if self.slot is not None else self.id
        return f"{self.pool}/{seat}#{self.incarnation}"

    def __getattr__(self, name: str) -> BoundMethod:
        # Only names the pool actually serves. An earlier design proxied
        # everything, which made hasattr() always true and turned a typo into a
        # runtime failure much later.
        if name.startswith("_") or name not in self._methods:
            raise AttributeError(
                f"{self.identity} serves {sorted(self._methods) or 'no methods'}, not {name!r}"
            )
        return BoundMethod(self, name, DEFAULT_TIMEOUT)

    def __repr__(self) -> str:
        return f"<Handle {self.identity} {self.url}>"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Handle)
            and (self.pool, self.id, self.incarnation)
            == (other.pool, other.id, other.incarnation)
        )

    def __hash__(self) -> int:
        return hash((self.pool, self.id, self.incarnation))


class AsyncHandle(AsyncHandleMixin, Handle):
    """A Handle whose methods return awaitables."""


class Epoch:
    """A frozen roster.

    `all()` is live: two ranks calling it 50ms apart can get different lists,
    build different process groups, and deadlock. A round needs everyone
    holding the *same* list, which is what freezing gives.
    """

    __slots__ = ("pool", "members", "roster", "_c")

    def __init__(self, pool_name: str, client: _Client, members: list[Handle], roster: int):
        self.pool = pool_name
        self.members = members
        self.roster = roster
        self._c = client

    @property
    def valid(self) -> bool:
        """False once the occupants change. Checking this in a training loop is
        useless -- a stuck rank never reaches the check. Use a watchdog thread;
        NCCL releases the GIL while it blocks, so one can still run."""
        info = self._c.pool_info(self.pool)
        # Losing the registry does not invalidate a group that is still
        # running; it only costs fast detection. Killing the round here would
        # contradict "the registry can die without stopping training".
        return info is None or info[1] == self.roster

    def __len__(self) -> int:
        return len(self.members)

    def __iter__(self):
        return iter(self.members)

    def slot(self, k: int) -> Handle:
        for h in self.members:
            if h.slot == k:
                return h
        raise NotFound(f"seat {k} is not in this round of {self.pool!r}")

    def __repr__(self) -> str:
        state = "valid" if self.valid else "broken"
        return f"<Epoch {self.pool} members={len(self.members)} roster={self.roster} {state}>"


class Pool:
    """One group. Lookups read the local cache: no network, so no timeouts."""

    _handle_cls = Handle

    def __init__(self, name: str, client: _Client):
        self._name = name
        self._c = client
        client.watch([name])

    def _members(self, filt: dict[str, Any], require_ready: bool) -> list[Handle]:
        raw = self._c.lookup(self._name, json.dumps(filt), require_ready)
        # The method list is stored once per pool, not once per member: members
        # of a pool run the same code.
        info = self._c.pool_info(self._name)
        methods = tuple(info[3]) if info else ()
        return [self._handle_cls(self._name, m, methods) for m in json.loads(raw)]

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
        """Block until `count` members match. Bounded, and the failure names them."""
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

    def epoch(self, min: int | None = None, timeout: float = 60.0) -> Epoch:
        """Wait for the round to be complete, then freeze it.

        Every rank that freezes the same fingerprint saw the same occupants,
        because the fingerprint is computed by the registry from seats and
        tenures alone.
        """
        deadline = time.monotonic() + timeout
        while True:
            info = self._c.pool_info(self._name)
            # A stale roster is not safe to build a collective on: ranks could
            # disagree. Refuse rather than freeze something we cannot trust.
            if self._c.silence_ms > self._lease_ms():
                raise Stale(
                    f"cannot open a round of {self._name!r}: no contact with the "
                    f"registry for {self._c.silence_ms}ms"
                )
            target = min if min is not None else (info[2] if info else None)
            if target is None:
                raise PolicyError(
                    f"{self._name!r} declares no size; pass min= or join with size="
                )
            members = self._members({}, require_ready=True)
            if len(members) >= target:
                return Epoch(self._name, self._c, members, info[1])
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"waited {timeout}s to open a round of {self._name!r}: "
                    f"{len(members)} of {target} present"
                )
            time.sleep(0.02)

    def _lease_ms(self) -> int:
        return max(int(self._c.stats().get("interval_ms", 1000)) * 4, 1000)

    def __len__(self) -> int:
        return len(self.all())

    def __repr__(self) -> str:
        info = self._c.pool_info(self._name)
        return f"<Pool {self._name} members={len(self.all())} version={info[0] if info else None}>"


class Member:
    """This process's own registration."""

    def __init__(
        self,
        client: _Client,
        pool_name: str,
        slot: int | None,
        incarnation: int,
        server: MethodServer | None = None,
    ):
        self._c = client
        self._server = server
        self.pool = pool_name
        self.slot = slot
        self.incarnation = incarnation
        self._state: dict[str, Any] = {}
        self._left = False

    def ready(self, **state: Any) -> Member:
        """Hang out a sign. Sends nothing now; the next heartbeat carries it."""
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
            global _client
            if _client is self._c:
                _client = None
            try:
                self._c.leave()
                if self._server is not None:
                    self._server.close()
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
    if _client is not None:
        raise RuntimeError(
            "this process has already joined; one process is one member. "
            "Call leave() first if you meant to re-join."
        )
    if policy not in POLICIES:
        raise PolicyError(f"policy must be one of {POLICIES}, got {policy!r}")
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
    # Tenure must increase when a seat is re-taken. Milliseconds alone collide
    # when a process restarts inside the same millisecond, which would let the
    # old one keep the seat; the random low bits break those ties. This assumes
    # clocks on nodes sharing a seat agree to within a millisecond, which any
    # cluster running collectives already needs.
    incarnation = ((time.time_ns() // 1_000_000) << 20) | random.getrandbits(20)

    server = None
    methods: list[str] = []
    if serves is not None:
        seat = slot if slot is not None else ident
        server = MethodServer(serves, f"{pool}/{seat}#{incarnation}")
        methods = server.methods
        url = url or server.url(_advertise())

    c = _Client(
        endpoints=_endpoints(),
        pool=pool,
        id=ident,
        incarnation=incarnation,
        policy=policy,
        slot=slot,
        size=size,
        url=url,
        methods=methods,
    )
    c.watch([pool])
    c.start()
    _client = c
    member = Member(c, pool, slot, incarnation, server)
    # A process that exits normally should say goodbye, so the seat frees up
    # immediately instead of waiting out the lease. SIGKILL still falls back
    # to lease expiry -- both paths work, they just differ in speed.
    atexit.register(member.leave)
    return member


class AsyncPool(Pool):
    """Same lookups -- they read the local cache and never block -- but the
    handles they return produce awaitables."""

    _handle_cls = AsyncHandle


def pool(name: str) -> Pool:
    if _client is None:
        raise RuntimeError("call tinyray.join(...) before looking anyone up")
    return Pool(name, _client)


def apool(name: str) -> AsyncPool:
    if _client is None:
        raise RuntimeError("call tinyray.join(...) before looking anyone up")
    return AsyncPool(name, _client)
