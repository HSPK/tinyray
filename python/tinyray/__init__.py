"""tinyray: a phone book and a roll call.

Three questions, nothing else: who is here, are they still alive, and who
should I talk to. It starts no processes, allocates no GPUs, and moves no
tensors.
"""

from __future__ import annotations

import asyncio
import atexit
import importlib.metadata as _metadata
import json
import os
import random
import socket
import threading
import time
import warnings
import weakref
from contextlib import ExitStack as _ExitStack
from typing import TYPE_CHECKING as _TYPE_CHECKING
from typing import Any

from msgspec.msgpack import decode as _msgpack_decode
from msgspec.msgpack import encode as _msgpack_encode

from . import _rpc
from ._errors import (
    BatchError,
    Fenced,
    NotDelivered,
    NotFound,
    OldRegistryWarning,
    OutcomeUnknown,
    OversizeWarning,
    PolicyError,
    RemoteError,
    SeatTaken,
    Stale,
    TinyrayError,
    Unreachable,
)
from ._msgpack import BlobError, BlobRef, blob
from ._msgpack import close_blobs_after_fork as _close_blobs_after_fork
from ._rpc import AsyncHandleMixin as _AsyncHandleMixin
from ._rpc import Call, abatch, batch, request_id
from ._serve import CallContext
from ._serve import MethodServer as _MethodServer
from ._tinyray import BLOB_MAX_BYTES as MAX_BLOB_BYTES
from ._tinyray import BLOB_MAX_MAPPED_BYTES_PER_MESSAGE as MAX_BLOB_MAPPED_BYTES_PER_MESSAGE
from ._tinyray import BLOB_MAX_REFS_PER_MESSAGE as MAX_BLOB_REFS_PER_MESSAGE
from ._tinyray import WAIT_CLOSED as _WAIT_CLOSED
from ._tinyray import WAIT_FENCED as _WAIT_FENCED
from ._tinyray import WAIT_MISMATCH as _WAIT_MISMATCH
from ._tinyray import WAIT_NO_SIZE as _WAIT_NO_SIZE
from ._tinyray import WAIT_PENDING as _WAIT_PENDING
from ._tinyray import WAIT_READY as _WAIT_READY
from ._tinyray import WAIT_STALE as _WAIT_STALE
from ._tinyray import WAIT_TIMEOUT as _WAIT_TIMEOUT
from ._tinyray import Client as _Client
from ._tinyray import NativeMember as _NativeMember
from ._tinyray import NativeSnapshot as _NativeSnapshot
from ._tinyray import NativeWait as _NativeWait

if _TYPE_CHECKING:
    # Only ever used in annotations, and `from __future__ import annotations`
    # keeps those as strings. Importing them for real would put `Callable` and
    # `Sequence` in `tinyray.*`, where they are not part of anything.
    from collections.abc import Callable, Sequence

try:
    __version__ = _metadata.version("tinyray")
except _metadata.PackageNotFoundError:  # running from a source tree
    __version__ = "0.0.0+unknown"

__all__ = [
    "__version__",
    "join",
    "pool",
    "Member",
    "Pool",
    "Handle",
    "AsyncHandle",
    "AsyncPool",
    "Epoch",
    "CallContext",
    "Call",
    "BlobRef",
    "BlobError",
    "blob",
    "batch",
    "abatch",
    "BatchError",
    "request_id",
    "Snapshot",
    "RegistryInfo",
    "Watch",
    "AsyncWatch",
    "Stale",
    "SeatTaken",
    "NotFound",
    "PolicyError",
    "OldRegistryWarning",
    "OversizeWarning",
    "TinyrayError",
    "Unreachable",
    "NotDelivered",
    "OutcomeUnknown",
    "Fenced",
    "RemoteError",
    "apool",
    "MAX_STATE",
    "MAX_BLOB_BYTES",
    "MAX_BLOB_MAPPED_BYTES_PER_MESSAGE",
    "MAX_BLOB_REFS_PER_MESSAGE",
    "FIRST_BEAT_S",
]

POLICIES = ("churn", "serving", "stateful", "collective")

# Seats are declared by the launcher, never handed out by tinyray.
_RANK_VARS = ("TINYRAY_SLOT", "RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK")
_SIZE_VARS = ("TINYRAY_SIZE", "WORLD_SIZE", "SLURM_NTASKS", "OMPI_COMM_WORLD_SIZE")


def _endpoint(explicit: str | None = None) -> str:
    """One registry. Losing it is survivable -- lookups keep working from cache
    and the roster regrows within one interval -- so replicas buy little and
    cost a lot: the delta cursor is per-registry, so failing over silently
    freezes the cache.

    `join(registry_url=)` beats the environment, for callers that are a library
    inside somebody else's process: setting TINYRAY_REGISTRY to configure one
    call is a process-wide side effect, and it outlives the call.

    Resolved once per join and then carried, so the address that failed is the
    address the message names. Reading the environment again in the error path
    would report whatever it says now rather than what was actually dialled.
    """
    raw = explicit if explicit is not None else os.environ.get("TINYRAY_REGISTRY", "127.0.0.1:8760")
    raw = raw.strip()
    if not raw:
        raise ValueError("the registry address is empty; give host:port")
    # There is deliberately no failover here (see above), and the docs used to
    # write the variable as `host:port,...`, so accepting a list was invited.
    if "," in raw:
        raise ValueError(
            f"the registry address {raw!r} looks like a list, and there is only "
            f"ever one registry: the delta cursor is per-registry, so failing "
            f"over between them silently freezes the cache. Give one host:port."
        )
    if "://" in raw:
        raise ValueError(
            f"the registry address {raw!r} is a URL, but the registry uses its "
            f"native length-prefixed MessagePack protocol. Give host:port."
        )
    return raw


def _advertise() -> str:
    """The address peers should use to reach us.

    No loopback fallback: publishing 127.0.0.1 from a multi-node job is silent
    misrouting -- peers elsewhere reach whatever listens on that port locally.
    """
    explicit = os.environ.get("TINYRAY_ADVERTISE")
    if explicit:
        host = explicit.strip()
        # Only a bare host composes with the native listener's chosen port.
        # Schemes, paths, and caller-supplied ports would advertise an invalid
        # endpoint while registration itself still appeared healthy.
        if not host or any(c in host for c in "/: "):
            raise ValueError(
                f"TINYRAY_ADVERTISE is {explicit!r}, which is not a bare host. "
                f"Set it to a hostname or IP and nothing else -- this process's "
                f"port is added for you. To advertise a "
                f"different address entirely, pass join(url=...)."
            )
        return host
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


def _checked_pool_name(name: str) -> str:
    """A pool name, or a refusal saying why this one cannot be used.

    The name travels in every method envelope and is part of request IDs and
    fencing tokens. Keep the established printable-ASCII contract so every
    process constructs the same identity bytes.
    """
    if not name:
        raise ValueError("a pool needs a name")
    if not name.isascii() or any(c < " " or c == "\x7f" for c in name):
        raise ValueError(
            f"pool name {name!r} has to be printable ASCII: it is sent in every "
            f"native RPC envelope. Membership would work and calling would not."
        )
    return name


def _checked_method_endpoint(endpoint: str) -> str:
    endpoint = endpoint.strip()
    if not endpoint:
        raise ValueError("the method endpoint is empty; give host:port")
    if "://" in endpoint:
        raise ValueError(
            f"the method endpoint {endpoint!r} is a URL, but method RPC uses "
            "native framed MessagePack. Give host:port."
        )
    if "," in endpoint or ":" not in endpoint:
        raise ValueError(f"the method endpoint {endpoint!r} is not one host:port")
    return endpoint


# Seats and world sizes are unsigned on the wire, and a launcher that got one
# wrong should hear about the variable rather than about the conversion.
_MAX_SEAT = (1 << 63) - 1


def _from_env(names: tuple[str, ...]) -> int | None:
    """A seat or world size from the launcher's environment, or None.

    Refuses what cannot be one. A negative or oversized value used to reach the
    Rust boundary and come back as `OverflowError: can't convert negative int
    to unsigned` -- an error about a conversion, naming neither the variable
    nor the value, from a call the application never made.
    """
    for n in names:
        v = os.environ.get(n)
        if v is None:
            continue
        raw = v.strip()
        if not raw.lstrip("-").isdigit():
            continue
        got = int(raw)
        if not 0 <= got <= _MAX_SEAT:
            raise ValueError(
                f"{n}={v!r} is not a usable seat or world size: it has to be "
                f"between 0 and {_MAX_SEAT}."
            )
        return got
    return None


_STATE_UNMATERIALIZED = object()


class _StateBatch:
    __slots__ = ("_native", "_states")

    def __init__(self, native: Any):
        self._native = native
        self._states: list[Any] | None = None

    def get(self, index: int) -> Any:
        states = self._states
        if states is None:
            states = _msgpack_decode(self._native.materialize())
            self._states = states
            self._native = None
        return states[index]


class Handle:
    """One member. Attribute access proxies to a method on the far side."""

    __slots__ = (
        "_native",
        "_methods",
        "_state",
        "_overrides",
        "state",
    )

    #: How a call goes out. `AsyncHandle` swaps this and changes nothing else.
    _send = staticmethod(_rpc.invoke)

    def __init__(self, pool_name: str, raw: dict[str, Any], methods: tuple[str, ...] = ()):
        self._native: _NativeMember | None = None
        self._methods = methods
        self.state: Any = raw.get("state") or {}
        self._state = self.state
        self._overrides: dict[str, Any] | None = {
            "pool": pool_name,
            "id": raw["id"],
            "slot": raw.get("slot"),
            "incarnation": raw["incarnation"],
            "url": raw.get("url"),
            "ready": raw["ready"],
        }

    @classmethod
    def _from_native(
        cls,
        member: _NativeMember,
        methods: tuple[str, ...],
    ) -> Handle:
        handle = cls.__new__(cls)
        handle._native = member
        handle._methods = methods
        return handle

    def _read(self, name: str) -> Any:
        overrides = getattr(self, "_overrides", None)
        if overrides is not None and name in overrides:
            return overrides[name]
        assert self._native is not None
        return getattr(self._native, name)

    def _write(self, name: str, value: Any) -> None:
        overrides = getattr(self, "_overrides", None)
        if overrides is None:
            overrides = {}
            self._overrides = overrides
        overrides[name] = value

    @property
    def pool(self) -> str:
        return self._read("pool")

    @pool.setter
    def pool(self, value: str) -> None:
        self._write("pool", value)

    @property
    def id(self) -> int:
        return self._read("id")

    @id.setter
    def id(self, value: int) -> None:
        self._write("id", value)

    @property
    def slot(self) -> int | None:
        return self._read("slot")

    @slot.setter
    def slot(self, value: int | None) -> None:
        self._write("slot", value)

    @property
    def incarnation(self) -> int:
        return self._read("incarnation")

    @incarnation.setter
    def incarnation(self, value: int) -> None:
        self._write("incarnation", value)

    @property
    def url(self) -> str | None:
        return self._read("url")

    @url.setter
    def url(self, value: str | None) -> None:
        self._write("url", value)

    @property
    def ready(self) -> bool:
        return self._read("ready")

    @ready.setter
    def ready(self, value: bool) -> None:
        self._write("ready", value)

    @property
    def identity(self) -> str:
        if self._native is not None and getattr(self, "_overrides", None) is None:
            return self._native.identity
        return _identity(self.pool, self.slot, self.id, self.incarnation)

    def __getattr__(self, name: str) -> Any:
        if name == "state":
            assert self._native is not None
            state = self._native.materialize_state() or {}
            self.state = state
            self._state = state
            return state
        # Only names the pool actually serves. An earlier design proxied
        # everything, which made hasattr() always true and turned a typo into a
        # runtime failure much later.
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._methods:
            raise AttributeError(
                f"{self.identity} serves {sorted(self._methods) or 'no methods'}, not {name!r}"
            )
        return _rpc.BoundMethod(self, name, _rpc.DEFAULT_TIMEOUT, self._send)

    @property
    def label(self) -> str:
        """Short form for humans. `identity` stays exact -- it is the fencing
        token -- but a random 63-bit id is unreadable in a log line."""
        if self._native is not None and getattr(self, "_overrides", None) is None:
            return self._native.label
        seat = self.slot if self.slot is not None else f"{self.id & 0xFFFF:04x}"
        return f"{self.pool}/{seat}#{self.incarnation & 0xFFF:03x}"

    def __repr__(self) -> str:
        return f"<Handle {self.label} {self.url}>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Handle) and (self.pool, self.id, self.incarnation) == (
            other.pool,
            other.id,
            other.incarnation,
        )

    def __hash__(self) -> int:
        return hash((self.pool, self.id, self.incarnation))


class AsyncHandle(_AsyncHandleMixin, Handle):
    """A Handle whose methods return awaitables."""


class Epoch:
    """A frozen roster.

    `all()` is live: two ranks calling it 50ms apart can get different lists,
    build different process groups, and deadlock. A round needs everyone
    holding the *same* list, which is what freezing gives.
    """

    __slots__ = ("pool", "roster", "_c", "_view", "_materialized", "_handle_cls")

    def __init__(self, pool_name: str, client: _Client, members: Sequence[Handle], roster: int):
        self.pool = pool_name
        self.roster = roster
        self._c = client
        self._view: _NativeSnapshot | None = None
        # A tuple, because "frozen" has to mean it. Handed to every rank to
        # build the same process group from, a list is one in-place sort or
        # filter away from the ranks disagreeing -- which is the deadlock this
        # type exists to prevent, arrived at through the type meant to prevent
        # it. Measured before: `epoch.members.append(...)` changed `len(epoch)`.
        self._materialized: tuple[Handle, ...] | None = tuple(members)
        self._handle_cls: type[Handle] = Handle

    @classmethod
    def _from_native(
        cls,
        pool_name: str,
        client: _Client,
        view: _NativeSnapshot,
        handle_cls: type[Handle],
    ) -> Epoch:
        epoch = cls.__new__(cls)
        epoch.pool = pool_name
        epoch.roster = view.roster
        epoch._c = client
        epoch._view = view
        epoch._materialized = None
        epoch._handle_cls = handle_cls
        return epoch

    @property
    def members(self) -> tuple[Handle, ...]:
        if self._materialized is None:
            assert self._view is not None
            self._materialized = self._view.materialize(
                self._handle_cls._from_native, _StateBatch, immutable=True
            )
        return self._materialized

    @property
    def valid(self) -> bool:
        """False once the occupants change. Checking this in a training loop is
        useless -- a stuck rank never reaches the check. Use a watchdog thread;
        NCCL releases the GIL while it blocks, so one can still run."""
        # Losing the registry does not invalidate a group that is still
        # running; it only costs fast detection. Killing the round here would
        # contradict "the registry can die without stopping training".
        return self._c.epoch_valid(self.pool, self.roster)

    def __len__(self) -> int:
        return len(self._view) if self._view is not None else len(self.members)

    def __iter__(self):
        return iter(self.members)

    def slot(self, k: int) -> Handle:
        if self._materialized is not None:
            for h in self._materialized:
                if h.slot == k:
                    return h
        elif self._view is not None:
            seat = _native_seat(k)
            if seat is not None:
                found = self._view.slot(seat, self._handle_cls._from_native)
                if found is not None:
                    return found
        raise NotFound(f"seat {k} is not in this round of {self.pool!r}")

    def __repr__(self) -> str:
        state = "valid" if self.valid else "broken"
        return f"<Epoch {self.pool} members={len(self)} roster={self.roster} {state}>"


class Snapshot:
    """One pool as it stood at a revision, unready members included.

    `all()` answers "who can I use", so it leaves out anyone who has taken a
    seat and not yet said it is ready. That is the wrong question while a round
    is being prepared: the seat is taken, so nobody else may have it, and the
    occupant will be there in a moment. Asking `all()` then reports it missing.

    Every entry carries its own `incarnation` and `ready`, which is what makes
    two snapshots comparable: a seat that went quiet, a seat that changed hands
    and a member that merely stopped being ready look nothing alike, and each
    of them wants a different reaction.
    """

    __slots__ = ("pool", "revision", "_view", "_materialized", "_handle_cls")

    def __init__(self, pool_name: str, revision: int, members: Sequence[Handle]):
        self.pool = pool_name
        self.revision = revision
        self._view: _NativeSnapshot | None = None
        # Same reason as `Epoch`: a snapshot names one moment, and a moment
        # that can be edited afterwards is not one.
        self._materialized: tuple[Handle, ...] | None = tuple(members)
        self._handle_cls: type[Handle] = Handle

    @classmethod
    def _from_native(
        cls,
        pool_name: str,
        view: _NativeSnapshot,
        handle_cls: type[Handle],
    ) -> Snapshot:
        snapshot = cls.__new__(cls)
        snapshot.pool = pool_name
        snapshot.revision = view.revision
        snapshot._view = view
        snapshot._materialized = None
        snapshot._handle_cls = handle_cls
        return snapshot

    @property
    def members(self) -> tuple[Handle, ...]:
        if self._materialized is None:
            assert self._view is not None
            self._materialized = self._view.materialize(
                self._handle_cls._from_native, _StateBatch, immutable=True
            )
        return self._materialized

    def __len__(self) -> int:
        return len(self._view) if self._view is not None else len(self.members)

    def __iter__(self):
        return iter(self.members)

    def ready(self) -> list[Handle]:
        if self._materialized is not None:
            return [h for h in self._materialized if h.ready]
        assert self._view is not None
        return self._view.materialize_ready(self._handle_cls._from_native, _StateBatch)

    def slot(self, k: int) -> Handle | None:
        """The occupant of seat k, ready or not, or None if it is empty."""
        if self._materialized is not None:
            return next((h for h in self._materialized if h.slot == k), None)
        assert self._view is not None
        seat = _native_seat(k)
        return None if seat is None else self._view.slot(seat, self._handle_cls._from_native)

    def get(self, identity: str) -> Handle | None:
        """The member with this exact identity, tenure included, or None.

        Asked of a snapshot rather than of the pool on purpose: "is that
        incarnation still there" is a question about one moment, and asking the
        live pool twice can answer about two.
        """
        if self._materialized is not None:
            return next((h for h in self._materialized if h.identity == identity), None)
        if not isinstance(identity, str):
            return None
        assert self._view is not None
        return self._view.get(identity, self._handle_cls._from_native)

    def __repr__(self) -> str:
        return f"<Snapshot {self.pool} rev={self.revision} members={len(self)}>"


class _LoopBell:
    """One pipe per event loop, written to whenever the client's bell rings.

    achanges() used to wait on `asyncio.to_thread`. Cancelling the awaitable
    does not stop the thread underneath it, so watchers that came and went left
    workers blocked in the Rust wait until the next beat: measured at 40
    cancelled watchers stalling the very next asyncio.to_thread by 3,092ms on a
    24-core box, with all 28 of the default executor's workers stuck. A pipe
    the loop can select on costs no thread at all, and cancelling is free.
    """

    __slots__ = ("_client", "_loop", "_r", "_w", "_waiters")

    def __init__(self, client: _Client, loop: asyncio.AbstractEventLoop):
        self._client = client
        self._loop = loop
        self._r, self._w = os.pipe()
        os.set_blocking(self._r, False)
        # Non-blocking on the write end too: the bell rings from the membership
        # thread, and a reader that has fallen behind must never stall it. A
        # byte already waiting says everything a second one would.
        os.set_blocking(self._w, False)
        self._waiters: list[asyncio.Future[None]] = []
        client.add_wake_fd(self._w)
        loop.add_reader(self._r, self._fire)

    def _fire(self) -> None:
        try:
            os.read(self._r, 4096)
        except BlockingIOError:
            pass
        waiters, self._waiters = self._waiters, []
        for f in waiters:
            if not f.done():
                f.set_result(None)

    @staticmethod
    def _expire(fut: asyncio.Future[None]) -> None:
        if not fut.done():
            fut.set_result(None)

    async def wait(self, timeout: float) -> None:
        """Return when the bell rings, or when `timeout` runs out.

        Running out is an ordinary answer, not an error: callers loop and
        re-check the thing they actually care about, exactly as the
        synchronous `wait_revision` lets them. Letting the TimeoutError out
        instead turned `achanges(timeout=...)` into a raise where the stream
        should simply have ended.

        Deliberately not `asyncio.wait_for`, which answers a cancellation that
        arrives in the same loop iteration as the bell with `if fut.done():
        return fut.result()` -- swallowing the CancelledError outright.
        Measured: a watcher cancelled at that instant kept iterating and
        `await task` never returned, leaving the task CANCELLING for good.
        Awaiting the future itself puts the cancellation on the very future
        the task is suspended on, where nothing can turn it into an answer,
        and the timeout arrives as the ordinary answer it already was.
        """
        fut: asyncio.Future[None] = self._loop.create_future()
        self._waiters.append(fut)
        timer = self._loop.call_later(timeout, self._expire, fut)
        try:
            await fut
        finally:
            timer.cancel()
            # Cancellation lands here too. Whatever happened, the slot in the
            # list has to go, or a caller that came and went would be woken
            # for the rest of the process's life.
            try:
                self._waiters.remove(fut)
            except ValueError:
                pass

    def close(self) -> None:
        # Deregister before closing, or the bell would write a byte into
        # whatever the descriptor number gets reused for.
        self._client.drop_wake_fd(self._w)
        if not self._loop.is_closed():
            # A rejoin can replace this bell before leave's wakeup is read.
            self._fire()
            self._loop.remove_reader(self._r)
        self._waiters.clear()
        os.close(self._r)
        os.close(self._w)


_bells: dict[int, tuple[weakref.ref[asyncio.AbstractEventLoop], _LoopBell]] = {}
# Watchers that are still running, so leave() can end them rather than leave a
# thread parked on a client that has gone.
_live_watches: weakref.WeakSet[_Watching] = weakref.WeakSet()
_live_native_waits: weakref.WeakSet[_NativeWait] = weakref.WeakSet()


def _loop_bell(client: _Client) -> _LoopBell:
    if client is not _client:
        raise RuntimeError(
            "cannot wait on a membership that has left; use the current Member or Pool"
        )
    # Waiting for the weak reference to die never fired: a bell holds its own
    # loop, so the entry kept that loop alive. What actually happens is the
    # loop being closed, which asyncio.run() does every time. Left unclaimed,
    # every run that touched a watch kept its pipe: measured at 101 bells and
    # 210 descriptors after 101 of them, with the heartbeat writing into all
    # 101 dead pipes on every beat.
    return _rpc.per_loop(
        _bells,
        lambda loop: _LoopBell(client, loop),
        lambda bell: bell.close(),
        reuse=lambda bell: bell._client is client,
    )


def _left_ms(deadline: float | None) -> int | None:
    """Milliseconds still allowed, or None once the time is up.

    Every wait in here spelled this out, and eight copies of a deadline is
    eight chances to get one of them wrong. An unbounded wait still needs a
    number to hand the Rust side: an hour, re-armed each time round. Semantic
    cache changes normally ring first; a truly quiet unbounded wait simply
    re-arms after the hour.
    """
    if deadline is None:
        return 3_600_000
    left = deadline - time.monotonic()
    return None if left <= 0 else int(left * 1000) + 1


def _native_seat(value: Any) -> int | None:
    try:
        seat = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return seat if seat == value and 0 <= seat < 1 << 64 else None


def _native_threshold(value: int) -> int:
    return max(-(1 << 127), min((1 << 127) - 1, value))


class RegistryInfo:
    """What the registry on the other end can do.

    Without this there is nothing to ask. An old registry answers a long-poll
    request immediately and correctly -- it just does not park it -- so
    "parked and nothing happened" and "does not park" are indistinguishable
    from the client. Measured against a 0.6.1 registry: 14.5 requests a second
    where a current one does 0.12, a hundredfold, with its health probe saying
    only "ok" and no attribute anywhere to probe.

    `protocol` is the number to branch on. It only goes up, and a registry too
    old to report one reads as 0.
    """

    __slots__ = ("protocol", "version")

    #: Feature name -> the protocol that first provided it. Deliberately a
    #: table rather than a per-feature flag: the registry says one number and
    #: the meaning of that number lives here, in the package that depends on
    #: it, so an old client never has to be taught about a future feature.
    FEATURES = {"long_poll": 1, "publication_ordering": 2, "native_registry": 3}

    def __init__(self, protocol: int, version: str):
        self.protocol = protocol
        self.version = version

    def supports(self, feature: str) -> bool:
        """True if the registry is new enough for `feature`."""
        want = self.FEATURES.get(feature)
        if want is None:
            raise ValueError(
                f"no such feature {feature!r}; this package knows about {sorted(self.FEATURES)}"
            )
        return self.protocol >= want

    def __repr__(self) -> str:
        who = self.version or "an unnamed version"
        return f"<RegistryInfo {who} protocol={self.protocol}>"


_NO_DIGEST = object()
"""Stands for "we have no baseline", which `None` cannot: field_digest returns
None for a pool the cache has never heard of, so None is an answer."""


class _Watching:
    """The bookkeeping behind changes() and achanges().

    Both walk the same ground -- has the pool moved, may we still wait, has
    anybody asked us to stop -- and differ only in how they wait, so only the
    waiting is written twice.
    """

    __slots__ = (
        "_pool",
        "_c",
        "_seen",
        "_deadline",
        "_closed",
        "_tick",
        "_fields",
        "_digest",
        "__weakref__",
    )

    # `int | None` from field_digest, or `_NO_DIGEST` when there is no baseline
    # to compare against; widened here so the sentinel is not a type error.
    _digest: object

    def __init__(
        self,
        pool: Pool,
        since: int | None,
        timeout: float | None,
        fields: Sequence[str] | None = None,
    ):
        self._pool = pool
        self._c = pool._c
        self._deadline = None if timeout is None else time.monotonic() + timeout
        self._closed = False
        self._tick = 0
        self._fields = None if fields is None else list(fields)
        if self._fields is None:
            self._digest = None
        elif since is None:
            # Read the digest before the revision, never after. Whichever way a
            # change lands between the two reads, this order costs a duplicate
            # snapshot instead of a lost one.
            self._digest = self._c.field_digest(pool._name, self._fields)
        else:
            # `since` names a moment we have no digest for. Taking today's
            # instead used to swallow every change in the gap -- measured: a
            # field that moved between the caller's snapshot and this call
            # yielded nothing for the full timeout, while the same watcher
            # without fields= yielded it in 0ms. Not knowing has to mean
            # yielding once, because a duplicate is recoverable and a miss is
            # not.
            self._digest = _NO_DIGEST
        self._seen = pool.snapshot().revision if since is None else since
        _live_watches.add(self)

    def close(self) -> None:
        """End the stream, including from another thread or task.

        A watcher blocked waiting for the pool to move cannot be interrupted by
        setting a flag, because it is not running. Ringing the bell is what
        gets it back to a point where it can see the flag -- without it, a
        non-daemon thread iterating changes() kept the process alive for good,
        and leave() did not release it either.
        """
        if not self._closed:
            self._closed = True
            _live_watches.discard(self)
            self._c.wake()

    def _step(self) -> tuple[Snapshot | None, int]:
        """A snapshot to hand over, or how many ms we may wait for one. Zero
        milliseconds means the stream is over.

        Raises `Fenced` if the stream is over because this process lost its
        seat, which is a different fact from the other two and needs a
        different reaction.
        """
        # Asked to stop wins over everything: a caller that closed the stream
        # is not interested in why it would have ended anyway.
        if self._closed:
            return None, 0
        if not self._c.accepted:
            # A superseded member stops beating, so the bell stops ringing and
            # nothing more is coming -- but ending quietly here made losing the
            # seat look exactly like the timeout running out, and the cache is
            # frozen from this moment on, so every later lookup is stale
            # without saying so. The only way to tell used to be asking
            # `Member.accepted` afterwards, which is the guessing this is
            # supposed to remove.
            raise Fenced(
                f"cannot watch {self._pool._name!r} any further: this process "
                f"lost its seat to a later tenure, so its view of the pool is "
                f"frozen. Nothing here can recover; the process has to stop "
                f"using whatever the seat entitled it to."
            )
        # Expiry also wins when the pool changes faster than it is consumed.
        if _left_ms(self._deadline) is None:
            return None, 0
        self._tick = self._c.cache_revision()
        info = self._c.pool_info(self._pool._name)
        # The bell is semantic rather than per-heartbeat, but another watched
        # pool or a lifecycle change can still ring it. The pool version is
        # therefore what decides whether this stream yields.
        if info is not None and info[0] != self._seen:
            self._seen = info[0]
            if self._fields is None:
                return self._pool.snapshot(), 0
            # Something moved, but maybe not anything this watcher named. Ask
            # the cache directly: building the snapshot to find out would be
            # the whole cost we are trying to avoid.
            digest = self._c.field_digest(self._pool._name, self._fields)
            if digest != self._digest:
                self._digest = digest
                return self._pool.snapshot(), 0
        ms = _left_ms(self._deadline)
        return (None, 0) if ms is None else (None, ms)


class Watch(_Watching):
    """A stream of snapshots, one every time the pool moves.

    Deliberately snapshots rather than events. The client sees the pool at
    heartbeat cadence and the registry collapses whatever happened in between:
    a member that went ready and unready again inside one interval arrives as
    one entry carrying its current state, not as two events. An event stream
    would therefore promise a completeness the wire cannot deliver. A snapshot
    promises what it can -- you never miss a state, only the transitions nobody
    could have observed -- and the events are a diff away, because every entry
    carries its incarnation.

    Ends quietly when the timeout runs out or `close()` is called, and raises
    `Fenced` if it ends because this process lost its seat. Those are three
    unrelated facts and only one of them needs the caller to do something.
    """

    __slots__ = ()

    def __iter__(self) -> Watch:
        return self

    def __next__(self) -> Snapshot:
        while True:
            snap, ms = self._step()
            if snap is not None:
                return snap
            if ms == 0:
                raise StopIteration
            self._c.wait_revision(self._tick, ms)

    def __enter__(self) -> Watch:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncWatch(_Watching):
    """`Watch` for an event loop, waiting on an fd rather than a thread."""

    __slots__ = ()

    def __aiter__(self) -> AsyncWatch:
        return self

    async def __anext__(self) -> Snapshot:
        while True:
            if self._closed:
                raise StopAsyncIteration
            # Register before checking state: fencing can be the last wakeup.
            bell = _loop_bell(self._c)
            snap, ms = self._step()
            if snap is not None:
                return snap
            if ms == 0:
                raise StopAsyncIteration
            await bell.wait(ms / 1000)

    async def __aenter__(self) -> AsyncWatch:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.close()


def _fenced_wait(pool_name: str) -> None:
    raise Fenced(
        f"cannot watch {pool_name!r} any further: this process lost its seat "
        f"to a later tenure, so its view of the pool is frozen. Nothing here "
        f"can recover; the process has to stop using whatever the seat "
        f"entitled it to."
    )


def _timed_out(result: tuple[Any, ...]) -> tuple[Any, ...]:
    return (_WAIT_TIMEOUT, None, *result[2:])


def _wait_native(
    waiter: _NativeWait, deadline: float | None
) -> tuple[int, _NativeSnapshot | None, int, int, int]:
    _live_native_waits.add(waiter)
    try:
        result = waiter.check(initial=True)
        if result[0] != _WAIT_PENDING:
            return result
        if deadline is None:
            return waiter.wait(None, initial=False)
        ms = _left_ms(deadline)
        return waiter.wait(0 if ms is None else ms, initial=False)
    finally:
        _live_native_waits.discard(waiter)


async def _await_native(
    client: _Client, waiter: _NativeWait, deadline: float | None
) -> tuple[int, _NativeSnapshot | None, int, int, int]:
    _live_native_waits.add(waiter)
    initial = True
    try:
        while True:
            # Register before checking: the cache can move, or fencing can be
            # the final wakeup, in the gap before the await.
            bell = _loop_bell(client)
            result = waiter.check(initial=initial)
            if not initial and result[0] not in (_WAIT_FENCED, _WAIT_CLOSED):
                if _left_ms(deadline) is None:
                    return _timed_out(result)
            if result[0] != _WAIT_PENDING:
                return result
            ms = _left_ms(deadline)
            if ms is None:
                return _timed_out(result)
            await bell.wait(ms / 1000)
            initial = False
    finally:
        _live_native_waits.discard(waiter)


class Pool:
    """One group. Lookups read the local cache: no network, so no timeouts."""

    _handle_cls = Handle

    def __init__(self, name: str, client: _Client):
        self._name = name
        self._c = client
        client.watch([name])

    def _settle(self) -> None:
        """Block until the first answer about this pool arrives.

        Subscribing and looking up happen in the same breath, so without this
        the first call reads a cache the registry has not answered yet and
        reports the pool empty -- measured 46-87ms of confident wrong answers
        about a pool that had been full for seconds.

        This is the one place a lookup waits on the network. On an event loop
        that shows up as a stalled tick, so AsyncPool's docstring says how to
        pay it at startup instead.
        """
        deadline = time.monotonic() + _FIRST_ANSWER_S
        while True:
            rev = self._c.cache_revision()
            if self._c.pool_info(self._name) is not None:
                return
            # A registry that is not answering will not answer this either, and
            # waiting on it would make every new pool cost the full deadline --
            # measured 10s for five pools. Losing the registry must not stall
            # lookups.
            if self._c.silence_ms > self._lease_ms() // 2:
                return
            ms = _left_ms(deadline)
            if ms is None:
                return
            self._c.wait_revision(rev, ms)

    def _members(self, filt: dict[str, Any], require_ready: bool) -> list[Handle]:
        self._settle()
        view = self._c.snapshot_view(
            self._name, None if not filt else _msgpack_encode(filt), require_ready
        )
        if view is None:
            return []
        return view.materialize(self._handle_cls._from_native, _StateBatch, immutable=False)

    def snapshot(self, include_unready: bool = True) -> Snapshot:
        """The pool as it stands, with the revision it stood at.

        Read under one lock, so the members and the revision cannot come from
        two different moments -- which is the same reason epoch() takes its
        list and its fingerprint together.
        """
        self._settle()
        view = self._c.snapshot_view(self._name, None, not include_unready)
        if view is None:
            return Snapshot(self._name, 0, [])
        return Snapshot._from_native(self._name, view, self._handle_cls)

    def changes(
        self,
        since: int | None = None,
        timeout: float | None = None,
        fields: Sequence[str] | None = None,
    ) -> Watch:
        """Snapshots of this pool, one per change. Never polls.

        The result is closeable and works as a context manager, which is the
        only way to stop a watcher that is blocked waiting for the pool to
        move:

            with pool.changes() as w:
                for snap in w:
                    ...
        """
        return Watch(self, since, timeout, fields)

    def _replacement_target(
        self, slot: int | None, identity: str | None, who: str
    ) -> tuple[int, str | None, bool]:
        """The seat to watch and the tenure that must give way."""
        if (slot is None) == (identity is None):
            raise TypeError(f"{who}() takes exactly one of slot= or identity=")
        if identity is not None:
            return _seat_of(identity), identity, False
        assert slot is not None
        seat = _native_seat(slot)
        if seat is None:
            raise ValueError(f"{slot!r} is not a usable seat")
        return seat, None, True

    def wait_replacement(
        self,
        slot: int | None = None,
        identity: str | None = None,
        timeout: float | None = None,
    ) -> Handle | None:
        """Block until a *different* tenure holds this seat, and return it.

        `Member.wait_fenced()` answers the same question from the inside, for
        a process that has to stop using a GPU it no longer owns. This is the
        outside view, for whoever was talking to that member: a seat going
        quiet, a seat changing hands and a member merely dropping out of ready
        are three different things, and only the incarnation tells them apart.

        None means the timeout ran out with the seat still held by the tenure
        it started with, or still empty.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        seat, was, capture = self._replacement_target(slot, identity, "wait_replacement")
        waiter = self._c.replacement_waiter(self._name, seat, was, capture)
        result = _wait_native(waiter, deadline)
        if result[0] == _WAIT_FENCED:
            _fenced_wait(self._name)
        if result[0] != _WAIT_READY or result[1] is None:
            return None
        return result[1].slot(seat, self._handle_cls._from_native)

    def all(self, **filt: Any) -> list[Handle]:
        return self._members(filt, require_ready=True)

    def pick(self, **filt: Any) -> Handle:
        self._settle()
        member = self._c.choose_ref(self._name, None if not filt else _msgpack_encode(filt), True)
        if member is None:
            raise NotFound(f"no ready member of {self._name!r} matching {filt}")
        info = self._c.pool_info(self._name)
        return self._handle_cls._from_native(member, tuple(info[3]) if info else ())

    def slot(self, k: int, require_ready: bool = False) -> Handle:
        self._settle()
        seat = _native_seat(k)
        member = (
            self._c.lookup_slot_ref(self._name, seat, require_ready) if seat is not None else None
        )
        if member is not None:
            info = self._c.pool_info(self._name)
            methods = tuple(info[3]) if info else ()
            return self._handle_cls._from_native(member, methods)
        # Never silently substitute another member: routing a keyed request to
        # the wrong seat corrupts data instead of raising.
        raise NotFound(f"seat {k} of {self._name!r} is empty")

    def until(
        self,
        predicate: Callable[[Snapshot], bool],
        since: int | None = None,
        timeout: float | None = None,
        describe: str = "",
    ) -> Snapshot:
        """Block until `predicate` accepts a snapshot of this pool, and return it.

        Every wait on a pool is this loop with a different condition in the
        middle, and each hand-written copy has the same four things to get
        right: test what is already true before waiting, hand the revision over
        without leaving a gap, stop when the watch is closed, and let `Fenced`
        through rather than treating a lost seat as "condition not met yet".
        Getting the second one wrong is the interesting failure -- the pool
        moves between the first look and the subscription, and the wait then
        sits out its whole timeout on a condition that came true immediately.

        `describe` is what the timeout message says was being waited for. Worth
        passing: "waited 30s" without saying for what is a bad error.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        # Already true? Then no waiting, and no chance to miss anything.
        snap = self.snapshot()
        if predicate(snap):
            return snap
        # Hand over the revision this snapshot stood at, so a change that
        # landed while the predicate was running is still delivered. The watch
        # gets what is *left* of the budget, not a fresh copy of it: settling
        # the pool and running the predicate happen inside `timeout`, and
        # handing the raw number on made `deadline` a decoration on the error
        # message rather than a deadline. Measured with a predicate that runs
        # 1s, until(timeout=0.3) took 1300ms.
        with self.changes(
            since=snap.revision if since is None else since,
            timeout=None if deadline is None else max(0.0, deadline - time.monotonic()),
        ) as w:
            for snap in w:
                if predicate(snap):
                    return snap
        raise TimeoutError(
            f"waited {timeout}s for {describe or 'a condition'} in "
            f"{self._name!r}; the pool holds {len(snap)} member(s)"
            + (f", last seen at revision {snap.revision}" if deadline else "")
        )

    def wait_departure(self, identity: str, timeout: float | None = None) -> bool:
        """Block until this exact tenure is no longer in the pool. True if it left.

        A different question from `wait_replacement()`, which only answers once
        somebody takes the seat: an owner that simply leaves and is not
        replaced makes that one sit out its whole timeout and return None.
        Whoever is waiting to take over the work usually only needs to know the
        previous owner is gone -- whether anyone succeeded it is a separate
        matter, and often nobody has yet.

        Gone covers all the ways: left, lease expired, or the seat changed
        hands. It is the tenure that is being watched, not the seat.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        result = _wait_native(
            self._c.departure_waiter(self._name, identity),
            deadline,
        )
        if result[0] == _WAIT_FENCED:
            _fenced_wait(self._name)
        return result[0] == _WAIT_READY

    def wait(self, count: int = 1, timeout: float = 30.0, **filt: Any) -> list[Handle]:
        """Block until `count` members match. Bounded, and the failure names them.

        The condition and event handoff run in Rust. Its old Python loop was
        the only wait in the library that could not say it had been fenced: a
        process whose seat had been taken sat out the whole timeout and then
        blamed the pool, because a frozen cache reports nobody. Measured on a
        fenced process asking for five members with a 4s budget:

            wait()          TimeoutError after 4000ms, "saw 0"
            await_ready()   Fenced after 1ms
            until()         Fenced after 0ms

        The pool was not empty -- a replacement was in it. Only this process
        could no longer see it, which is a different thing to be told.
        """
        deadline = time.monotonic() + timeout
        waiter = self._c.count_waiter(
            self._name,
            _native_threshold(count),
            None if not filt else _msgpack_encode(filt),
        )
        result = _wait_native(waiter, deadline)
        if result[0] == _WAIT_FENCED:
            _fenced_wait(self._name)
        if result[0] == _WAIT_READY and result[1] is not None:
            return result[1].materialize(
                self._handle_cls._from_native, _StateBatch, immutable=False
            )
        raise TimeoutError(
            f"waited {timeout}s for {count} ready member(s) matching {filt} in "
            f"{self._name!r}; the pool holds {result[3]} member(s), "
            f"last seen at revision {result[4]}"
        )

    def epoch(self, min: int | None = None, timeout: float = 60.0) -> Epoch:
        """Wait for the round to be complete, then freeze it.

        Every rank that freezes the same fingerprint holds the same list,
        because a round is only handed over carrying the fingerprint its own
        list adds up to.

        That last clause is load-bearing. The registry computes the fingerprint
        from seats and tenures, so readiness deliberately does not move it --
        but the list frozen here is filtered by readiness. While someone has
        taken a seat and not yet declared itself ready, those two disagree, and
        two ranks freezing either side of that moment used to get the *same*
        fingerprint with *different* lists, both reporting valid. Measured:
        three occupants, two ready, a round of two carrying the fingerprint of
        three. Different lists is a deadlock, and an equal fingerprint is
        exactly what stops anyone noticing.

        Reading the list and the fingerprint together also closes a narrower
        hole: they used to be two calls with the beat loop free to land between
        them, so the fingerprint could describe occupants the list never saw.
        """
        deadline = time.monotonic() + timeout
        ms = _left_ms(deadline)
        minimum = None if min is None else _native_threshold(min)
        result = self._c.wait_epoch(self._name, 0 if ms is None else ms, minimum)
        status, view, found, target, _mismatched, seen_pool, silence_ms = result
        if status == _WAIT_FENCED:
            self._check_fenced()
        if status == _WAIT_STALE:
            raise Stale(
                f"cannot open a round of {self._name!r}: no contact with the "
                f"registry for {silence_ms}ms"
            )
        if status == _WAIT_NO_SIZE:
            raise PolicyError(f"{self._name!r} declares no size; pass min= or join with size=")
        if status == _WAIT_READY and view is not None:
            return Epoch._from_native(self._name, self._c, view, self._handle_cls)
        if not seen_pool:
            raise TimeoutError(
                f"waited {timeout}s to open a round of {self._name!r}: "
                f"the registry has said nothing about it"
            )
        if status == _WAIT_MISMATCH:
            raise TimeoutError(
                f"waited {timeout}s to open a round of {self._name!r}: "
                f"{found} member(s) ready, but the pool holds a seat whose "
                f"occupant has not declared itself ready, so the fingerprint "
                f"would not describe the list -- wait for it rather than "
                f"freeze a round no other rank can be held to"
            )
        raise TimeoutError(
            f"waited {timeout}s to open a round of {self._name!r}: {found} of {target} present"
        )

    def _check_fenced(self) -> None:
        if not self._c.accepted:
            raise Fenced(
                f"cannot open a round of {self._name!r}: this process lost its "
                f"seat and its cached roster can no longer be trusted"
            )

    def _lease_ms(self) -> int:
        return max(int(self._c.stats().get("interval_ms", 1000)) * 4, 1000)

    def __len__(self) -> int:
        self._settle()
        return self._c.count(self._name, None, True)

    def __repr__(self) -> str:
        info = self._c.pool_info(self._name)
        return f"<Pool {self._name} members={len(self)} version={info[0] if info else None}>"


class Member:
    """This process's own registration."""

    def __init__(
        self,
        client: _Client,
        pool_name: str,
        slot: int | None,
        incarnation: int,
        server: _MethodServer | None = None,
        ident: int = 0,
    ):
        self._c = client
        self._server = server
        self.pool = pool_name
        self.slot = slot
        self.incarnation = incarnation
        # Seat or id, never neither: this is what the fencing token is built
        # from, and a member with no seat is keyed by its id. The parameter
        # used to default to None and fall back to `slot`, which typed as
        # `int | None` and would have spelled the token `pool/None#tenure` --
        # a token nothing can ever match. join() is the only caller and always
        # passes an id, so say so.
        self._ident = ident
        self._state: dict[str, Any] = {}
        # Merging into the published state is a read-modify-write, and two
        # publishers racing through it lose each other's keys. Not reachable
        # as things stand -- MAX_STATE bounds the encode that sits in the gap,
        # and 8 publishers over 300 trials lost nothing, while widening the gap
        # by 0.5ms lost 420 keys -- so this guards against the gap growing
        # rather than against a bug in today's code.
        self._lock = threading.Lock()
        self._left = False
        self._pid = os.getpid()

    @property
    def identity(self) -> str:
        """The same string a peer holding a Handle to this process would use,
        and the same one that rides on every call this process makes."""
        return _identity(self.pool, self.slot, self._ident, self.incarnation)

    def _mine(self) -> None:
        if os.getpid() != self._pid:
            raise RuntimeError(
                "this Member belongs to another process; fork() left its "
                "heartbeat behind. Call tinyray.join(...) again in the child."
            )

    def ready(self, **state: Any) -> Member:
        """Hang out a sign. Sends nothing now; the next heartbeat carries it.

        This declares readiness as well as publishing, so it belongs to
        whichever part of the process decides whether the member should be
        used. Anything that only reports progress wants `update()`.
        """
        self._mine()
        with self._lock:
            # Check before mutating: an over-budget call used to leave the blob
            # in place, so every later ready() failed too and one bad call
            # poisoned the member for good.
            merged = {**self._state, **state}
            raw = self._encode_state(merged)
            self._c.set_state(raw, True)
            self._state = merged
        return self

    def update(self, **state: Any) -> Member:
        """Publish state, merging into what is there, and leave readiness alone.

        `ready(**kw)` asserts both at once, and until this existed that was the
        only way to publish anything: a component reporting progress had no
        choice but to also declare the member ready, silently lifting a pause
        another component had just applied. Measured -- unready() then
        ready(step=1) put ready=True back in front of peers.
        """
        self._mine()
        with self._lock:
            merged = {**self._state, **state}
            raw = self._encode_state(merged)
            self._c.set_state_only(raw)
            self._state = merged
        return self

    def replace(self, state: dict[str, Any] | None = None) -> Member:
        """`update()` but replacing the published state outright, so keys can
        be taken back. Readiness is left alone."""
        self._mine()
        with self._lock:
            fresh = dict(state or {})
            raw = self._encode_state(fresh)
            self._c.set_state_only(raw)
            self._state = fresh
        return self

    def _encode_state(self, state: dict[str, Any]) -> bytes:
        raw = json.dumps(state, allow_nan=False)
        # The registry would refuse this, but silently and in a background
        # thread. Refusing here names the call that did it. The bound exists
        # because state is copied to every subscriber: 6 MB became 120 MB
        # across 20 of them.
        if len(raw) > MAX_STATE:
            raise ValueError(
                f"state is {len(raw)} bytes, over the {MAX_STATE} limit; the "
                f"registry copies it to every subscriber, so publish a "
                f"reference and let peers fetch the payload themselves"
            )
        # Preserve the JSON-facing state semantics (string keys, tuples as
        # arrays, finite numbers) while crossing the native boundary as bytes.
        return _msgpack_encode(json.loads(raw))

    def set_ready(self, state: dict[str, Any] | None = None) -> Member:
        """Replace the published state outright, rather than merging into it.

        `ready(**kw)` merges, which means there has been no way to take a key
        back: publish `stale=True` once and it is there for the life of the
        process. A weight switch wants the whole picture replaced at once, not
        layered over the last one.
        """
        self._mine()
        with self._lock:
            fresh = dict(state or {})
            raw = self._encode_state(fresh)
            self._c.set_state(raw, True)
            self._state = fresh
        return self

    def flush(self, timeout: float = 10.0) -> Member:
        """Block until the registry has been told what was last published.

        ready() and set_ready() only write locally and nudge the heartbeat, so
        "published" and "visible to peers" are a beat apart. Reading your own
        state back to find out is a round trip that says what this does.

        Waits for an ack for the version it published, not for a number of
        beats. Counting cannot tell the two apart: the beat in flight may have
        been composed before the change, so a count has to assume it was and
        wait for the one after -- and that one is parked for a whole interval,
        which the publish had already cut short. Measured at a 2s lease: 645ms
        counting, 1.4ms asking. Called once the state was already visible, the
        count waited 1096ms for two holds it did not owe.
        """
        self._mine()
        mine, _ = self._c.publish_versions()
        deadline = time.monotonic() + timeout
        ms = _left_ms(deadline)
        confirmed, accepted = self._c.wait_publication(mine, 0 if ms is None else ms)
        if confirmed >= mine:
            return self
        if not accepted:
            raise SeatTaken(f"{self.pool} seat {self.slot} was taken while publishing")
        raise TimeoutError(
            f"waited {timeout}s for the registry to take this state; "
            f"last error was {self._c.last_error()!r}"
        )

    def wait_fenced(self, timeout: float | None = None) -> bool:
        """Block until a later tenure has taken this seat. True if it has.

        The RPC layer already refuses calls to a superseded member, but only
        the ones that go through tinyray. A process holding a GPU, an inference
        server and a socket of its own has to be told, so it can stop those too.

        Learning it needs contact: while the registry is unreachable this stays
        blocked, because nothing here can know. That is the same reason losing
        the registry does not stop a training run -- and it means this is not
        protection against a partition, only against being replaced.
        """
        self._mine()
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            rev = self._c.cache_revision()
            if not self._c.accepted:
                return True
            ms = _left_ms(deadline)
            if ms is None:
                return False
            self._c.wait_revision(rev, ms)

    async def await_fenced(self, timeout: float | None = None) -> bool:
        """`wait_fenced()` for an event loop, on the same pipe achanges uses.

        It waited on a thread until 0.9.1, which meant cancelling it did not
        release the worker -- the executor problem achanges was moved off, left
        behind in the one other place that had it.
        """
        self._mine()
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            bell = _loop_bell(self._c)
            if not self._c.accepted:
                return True
            ms = _left_ms(deadline)
            if ms is None:
                return False
            await bell.wait(ms / 1000)

    def unready(self) -> Member:
        """Withdraw the sign, keeping the published state. Pairs with ready()
        and set_ready(), and like them belongs to the readiness owner."""
        self._mine()
        with self._lock:
            self._c.set_state(self._encode_state(self._state), False)
        return self

    @property
    def registry(self) -> RegistryInfo:
        """What the registry this member is talking to can do.

        Read it rather than probing for attributes: the package version says
        what *this* side can do, and the two are separate processes that can
        be upgraded independently.
        """
        protocol, version = self._c.registry()
        return RegistryInfo(protocol, version)

    @property
    def is_ready(self) -> bool:
        """What this member is currently telling the pool about itself."""
        return self._c.is_ready()

    @property
    def state(self) -> dict[str, Any]:
        return dict(self._state)

    @property
    def accepted(self) -> bool:
        """False once a later incarnation has taken this seat."""
        return self._c.accepted

    @property
    def silence_ms(self) -> int:
        """Milliseconds since the last beat the registry answered.

        Everything keeps working while this climbs -- lookups read the local
        cache and calls were always peer to peer -- but fast failure detection
        is gone until it drops again.
        """
        return self._c.silence_ms

    def stats(self) -> dict[str, int]:
        """Counters for this process: the heartbeat, the watches, and -- if it
        serves anything -- the calls it has been asked to run.

        The serving half exists so "do the long calls need a transport of their
        own" is a question with an answer. `max_concurrency` bounds pile-up,
        but it does not keep control traffic apart from data traffic: once every
        slot is held, a control call is refused like any other. `refused` next
        to `peak_in_flight` says whether that is happening.

        | key | meaning |
        |---|---|
        | `beats_ok` / `beats_failed` | heartbeats answered, and not |
        | `interval_ms` / `silence_ms` | current beat spacing, time since the last one |
        | `coalesce_ms` / `effective_coalesce_ms` | requested gap, bounded by the lease |
        | `watch_wakeups` | times the local cache moved and woke a waiter |
        | `state_bytes` | size of what this member is publishing |
        | `pool_revision` | version of its own pool, as last heard |
        | `calls` / `failed` / `refused` | served, raised, turned away at the limit |
        | `in_flight` / `peak_in_flight` | concurrent calls now, and the most so far |
        | `busy_ms` | total time spent inside handlers |
        """
        self._mine()
        out = self._c.stats()
        if self._server is not None:
            out.update(self._server.counters.snapshot())
            if self._server.limit is not None:
                out["concurrency_limit"] = self._server.limit
        return out

    @property
    def last_error(self) -> str:
        """The most recent beat failure, kept even after recovery.

        Read it with silence_ms, not instead of it: silence_ms says whether
        contact is healthy right now, this says what the last break looked
        like. A short silence with a message in here means it recovered.
        """
        return self._c.last_error()

    def _leave_at_exit(self) -> None:
        """What atexit gets, which is not the same thing as `leave`.

        A fork copies the exit hooks along with everything else, and a child
        must not say goodbye on the parent's behalf -- `leave()` refuses, and
        refusing is right, but it refuses by raising and atexit prints that:
        seven lines of traceback for doing nothing wrong.

        Narrower than it sounds, and worth writing down because I first
        claimed otherwise. It needs a hand-written `os.fork()` whose child
        leaves through the ordinary interpreter shutdown -- measured at seven
        lines for `sys.exit(0)` and seven for falling off the end of the
        script. `os._exit()` skips atexit and prints nothing, and that is what
        multiprocessing uses, so a Pool or a DataLoader never sees this: zero
        lines from four workers over five rounds, before the fix.
        """
        if os.getpid() == self._pid:
            self.leave()

    def leave(self) -> None:
        self._mine()
        if not self._left:
            self._left = True
            # join() handed this method to atexit, and atexit never forgets.
            # Left registered it pinned the member for the life of the
            # process, and through it the served object -- which is a model or
            # a dataset as often as not. Measured: eight join/leave rounds
            # left eight servers alive, each still holding its object.
            atexit.unregister(self._leave_at_exit)
            # A watcher blocked on a client that is about to go would wait out
            # its whole timeout, and a non-daemon thread iterating one kept the
            # process alive indefinitely.
            for w in list(_live_watches):
                w.close()
            for native_wait in list(_live_native_waits):
                native_wait.close()
            global _client, _method_server, _left
            if _client is self._c:
                _client = None
                _method_server = None
                _left = True
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
        return f"<Member {self.pool}/{seat}#{self.incarnation & 0xFFF:03x}>"


# One beat plus slack: long enough for the registry's first answer, short
# enough that a dead registry does not turn every lookup into a stall.
_FIRST_ANSWER_S = 2.0
# How long the one synchronous beat inside join() may spend before the loop
# takes over. Short on purpose: a lost packet is cheaper to re-send than to
# wait out, and the loop re-sends every interval. join(timeout=) still bounds
# the whole call -- this only decides how the budget is spent inside it.
_FIRST_BEAT_S = 5.0

# Matches MAX_STATE in the registry: a fact about where something is, not the
# something. See tests/membership/test_state.py for the amplification measurement.
MAX_STATE = 16 << 10

# Default for join(timeout=): how long to keep trying to reach the registry.
#
# Ten seconds sat exactly on the coin flip. Measured against a 40% drop rate,
# a link that works but loses packets: the first beat lands at a median of
# 5.0s, p90 9.8s, worst 12.3s -- so join() failed roughly one launch in
# fifteen on a network the member would then have run on perfectly well.
# Thirty gives 2.4x margin over the worst observed and covered 20 of 20.
#
# At 60% loss the p90 is 50.8s and this will still give up on some launches.
# That is a network where nothing else works either, and the alternative --
# waiting forever on an endpoint that may simply be wrong -- is worse.
FIRST_BEAT_S = 30.0

_client: _Client | None = None
_method_server: _MethodServer | None = None
_left = False
_owner_pid = os.getpid()


def _after_fork() -> None:
    """fork() keeps only the calling thread, so the child inherits a client
    whose heartbeat is gone: it looks registered, answers from a frozen cache,
    and the registry never hears from it again. Make that explicit instead.

    The inherited runtime also has to be let go of rather than dropped. Its
    worker threads did not survive the fork, and shutting it down waits for
    them: measured as a child that hangs forever at ordinary exit, in native
    code with no Python frame to show why, taking the parent's waitpid with it.
    """
    global _client, _method_server, _left
    _close_blobs_after_fork()
    if _client is not None:
        _client.abandon()
    if _method_server is not None:
        _method_server.abandon()
    _client = None
    _method_server = None
    _left = False
    # The inherited pipes belong to the parent's loops and its heartbeat is
    # gone, so nothing will ever write to them again. Drop them without
    # closing: the parent still owns the descriptors. reset_after_fork does
    # the same for the transports, which had been left behind.
    _bells.clear()
    _live_watches.clear()
    _live_native_waits.clear()
    _rpc.reset_after_fork()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def join(
    pool: str,
    policy: str = "churn",
    *,
    slot: int | None = None,
    size: int | None = None,
    url: str | None = None,
    serves: Any = None,
    exclusive: bool = False,
    max_concurrency: int | None = None,
    timeout: float = FIRST_BEAT_S,
    registry_url: str | None = None,
    coalesce_ms: int = 50,
) -> Member:
    """Report in. One line per process.

    `max_concurrency` bounds how many calls this process will run at once.
    Past it callers are refused rather than queued, and a refusal is bounded
    where an unbounded thread count is not -- a hundred workers all pulling at
    the same moment is otherwise a hundred threads. The refusal arrives as
    NotDelivered, because nothing ran, so retrying elsewhere is safe.

    `timeout` is how long to keep trying before giving up on the registry.
    Launchers routinely start ranks before it is listening, so waiting is the
    normal case; raise it when the registry comes up late or the link is bad,
    and lower it when a wrong address should be reported straight away.

    `registry_url` is where to report in, overriding TINYRAY_REGISTRY. Not to
    be confused with `url`, which is where *peers* should reach this process.
    The environment stays the normal channel, because a launcher sets it for
    every rank at once and nobody wants that spelled out in code. This is for
    the caller who cannot use it: a library inside somebody else's process,
    where assigning to os.environ to configure one call is a process-wide side
    effect that outlives the call.

    It picks the registry, it does not add one. A process is one member with
    one registry, so pool() and apool() follow whatever this joined.

    `coalesce_ms` bounds how long bursts of publications and roster updates
    are batched between beats. Defaults to 50ms; zero opts out. The effective
    gap never exceeds a quarter of the lease, even for a larger requested gap.
    """
    global _client, _method_server, _left, _owner_pid
    _left = False
    if _client is not None:
        raise RuntimeError(
            "this process has already joined; one process is one member. "
            "Call leave() first if you meant to re-join."
        )
    if isinstance(coalesce_ms, bool) or not isinstance(coalesce_ms, int) or coalesce_ms < 0:
        raise ValueError("coalesce_ms must be a nonnegative integer")
    pool = _checked_pool_name(pool)
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
    if slot is not None and not 0 <= slot <= _MAX_SEAT:
        raise PolicyError(f"seat number has to be between 0 and {_MAX_SEAT}, got {slot}")
    if size is not None and not 1 <= size <= _MAX_SEAT:
        # Zero is the dangerous one, and it does not announce itself: a pool
        # that declares no seats makes `epoch()` freeze on whoever happens to
        # be there. Measured -- a lone member froze a round in 82ms against a
        # world that was supposed to have several, so "wait for everyone"
        # quietly became "carry on alone".
        raise PolicyError(f"a world has at least one seat, got size={size}")
    if slot is not None and size is not None and slot >= size:
        raise PolicyError(f"seat {slot} is outside a world of {size}")

    # Fungible members have no seat, so their key is just a fresh identity.
    ident = slot if slot is not None else random.getrandbits(63)
    # Tenure must increase when a seat is re-taken. Milliseconds alone collide
    # when a process restarts inside the same millisecond, which would let the
    # old one keep the seat; the random low bits break those ties. This assumes
    # clocks on nodes sharing a seat agree to within a millisecond, which any
    # cluster running collectives already needs.
    incarnation = ((time.time_ns() // 1_000_000) << 20) | random.getrandbits(20)

    # Resolved once. Every later mention -- the client, the unreachable
    # message, the old-registry warning -- reads this and not the environment,
    # so there is one spelling of where we actually dialled.
    endpoint = _endpoint(registry_url)
    # Transfer ownership only after every initialization step succeeds.
    # This also cleans up validation errors and warnings promoted to errors.
    with _ExitStack() as cleanup:
        server = None
        methods: list[str] = []
        if serves is not None:
            server = _MethodServer(
                serves, _identity(pool, slot, ident, incarnation), max_concurrency=max_concurrency
            )
            cleanup.callback(server.close)
            methods = server.methods
            url = _checked_method_endpoint(url) if url is not None else server.url(_advertise())
            server.track_endpoint(url)
        elif url is not None:
            url = _checked_method_endpoint(url)

        c = _Client(
            endpoint=endpoint,
            pool=pool,
            id=ident,
            incarnation=incarnation,
            policy=policy,
            slot=slot,
            size=size,
            url=url,
            methods=methods,
            exclusive=exclusive,
            coalesce_ms=min(coalesce_ms, (1 << 64) - 1),
        )
        cleanup.callback(c.leave)
        if server is not None:
            # A superseded process can keep listening; its heartbeat is what
            # tells the server it must stop answering as the old occupant.
            server.still_ours = lambda: c.accepted
        c.watch([pool])
        deadline = time.monotonic() + timeout
        # Include the first exchange in the budget, but leave time to retry
        # a dropped request rather than spending the whole budget on it.
        if not c.start(int(min(timeout, _FIRST_BEAT_S) * 1000)):
            ms = _left_ms(deadline)
            if ms is not None:
                c.wait_registered(ms)
            if not c.stats()["beats_ok"]:
                raise Unreachable(
                    f"no answer from the registry at {endpoint} after "
                    f"{timeout:g}s and {c.stats()['beats_failed']} attempts: "
                    f"{c.last_error()}. Pass join(timeout=) to wait longer."
                )
        if not c.accepted:
            # A refusal stops the beat loop: never hand back a member whose
            # apparently live cache will remain frozen forever.
            if c.refused():
                raise PolicyError(c.refused())
            if exclusive:
                raise SeatTaken(f"seat {slot} of {pool!r} is already held")
            raise SeatTaken(
                f"the registry refused tenure {incarnation} for seat {slot} of "
                f"{pool!r}: a later one holds it. A restarting process normally "
                f"carries the newer tenure, so the usual cause is a clock that "
                f"went backwards on this node."
            )
        seen = RegistryInfo(*c.registry())
        missing = [feature for feature in RegistryInfo.FEATURES if not seen.supports(feature)]
        if missing:
            required = max(RegistryInfo.FEATURES[feature] for feature in missing)
            effects = []
            if "long_poll" in missing:
                effects.append(
                    "changes take up to a heartbeat interval and requests are more frequent"
                )
            if "publication_ordering" in missing:
                effects.append("delayed requests can roll back already-confirmed state")
            warnings.warn(
                f"the registry at {endpoint} reports protocol {seen.protocol} "
                f"({seen.version or 'version not reported'}) but tinyray "
                f"{__version__} expects {required} for {', '.join(missing)}: "
                f"{'; '.join(effects)}. Upgrade the registry, or silence this with "
                f"warnings.filterwarnings('ignore', "
                f"category=tinyray.OldRegistryWarning).",
                OldRegistryWarning,
                stacklevel=2,
            )
        member = Member(c, pool, slot, incarnation, server, ident)
        # A normal exit releases the seat; SIGKILL still falls back to expiry.
        atexit.register(member._leave_at_exit)
        _rpc.set_identity(member.identity)
        _client = c
        _method_server = server
        _owner_pid = os.getpid()
        cleanup.pop_all()
        return member


class AsyncPool(Pool):
    """Same lookups, but the handles they return produce awaitables.

    Lookups read the local cache, with one exception worth knowing about on an
    event loop: the very first one for a pool has to wait for the registry's
    first answer, or it would report a full pool empty. Measured at 42ms of
    stalled loop per unfamiliar pool, 169ms for four of them.

    Constructing the pool is what subscribes, so building the ones you need at
    startup removes it entirely -- the same four then cost 0ms, with the loop
    never stalled longer than one of its own ticks:

        POOLS = [tinyray.pool(n) for n in ("trainers", "rollout")]
    """

    _handle_cls = AsyncHandle

    def achanges(
        self,
        since: int | None = None,
        timeout: float | None = None,
        fields: Sequence[str] | None = None,
    ) -> AsyncWatch:
        """`changes()` for an event loop.

        Waits on a pipe the heartbeat writes to, so no executor thread is held
        and cancelling the iteration is immediate. Closeable and usable as an
        async context manager, same as the synchronous one.
        """
        return AsyncWatch(self, since, timeout, fields)

    async def auntil(
        self,
        predicate: Callable[[Snapshot], bool],
        since: int | None = None,
        timeout: float | None = None,
        describe: str = "",
    ) -> Snapshot:
        """`until()` for an event loop."""
        deadline = None if timeout is None else time.monotonic() + timeout
        snap = self.snapshot()
        if predicate(snap):
            return snap
        watch = self.achanges(
            since=snap.revision if since is None else since,
            timeout=None if deadline is None else max(0.0, deadline - time.monotonic()),
        )
        async with watch as w:
            async for snap in w:
                if predicate(snap):
                    return snap
        raise TimeoutError(
            f"waited {timeout}s for {describe or 'a condition'} in "
            f"{self._name!r}; the pool holds {len(snap)} member(s)"
            + (f", last seen at revision {snap.revision}" if deadline else "")
        )

    async def await_ready(self, count: int = 1, timeout: float = 30.0, **filt: Any) -> list[Handle]:
        """`Pool.wait()` for an event loop.

        `AsyncPool` used to inherit the blocking one, which does not merely
        feel wrong on a loop -- it stops the loop. Measured: one second of
        `apool.wait()` let five 10ms ticks through where a hundred were due.
        Wrapping it in `asyncio.to_thread` is the caller doing the library's
        job, and it strands a worker for as long as the wait lasts.
        """
        deadline = time.monotonic() + timeout
        waiter = self._c.count_waiter(
            self._name,
            _native_threshold(count),
            None if not filt else _msgpack_encode(filt),
        )
        result = await _await_native(self._c, waiter, deadline)
        if result[0] == _WAIT_FENCED:
            _fenced_wait(self._name)
        if result[0] == _WAIT_READY and result[1] is not None:
            return result[1].materialize(
                self._handle_cls._from_native, _StateBatch, immutable=False
            )
        raise TimeoutError(
            f"waited {timeout}s for {count} ready member(s) matching {filt} in "
            f"{self._name!r}; the pool holds {result[3]} member(s), "
            f"last seen at revision {result[4]}"
        )

    async def await_departure(self, identity: str, timeout: float | None = None) -> bool:
        """`wait_departure()` for an event loop."""
        deadline = None if timeout is None else time.monotonic() + timeout
        result = await _await_native(
            self._c,
            self._c.departure_waiter(self._name, identity),
            deadline,
        )
        if result[0] == _WAIT_FENCED:
            _fenced_wait(self._name)
        return result[0] == _WAIT_READY

    async def await_replacement(
        self,
        slot: int | None = None,
        identity: str | None = None,
        timeout: float | None = None,
    ) -> Handle | None:
        """`wait_replacement()` for an event loop."""
        deadline = None if timeout is None else time.monotonic() + timeout
        seat, was, capture = self._replacement_target(slot, identity, "await_replacement")
        result = await _await_native(
            self._c,
            self._c.replacement_waiter(self._name, seat, was, capture),
            deadline,
        )
        if result[0] == _WAIT_FENCED:
            _fenced_wait(self._name)
        if result[0] != _WAIT_READY or result[1] is None:
            return None
        return result[1].slot(seat, self._handle_cls._from_native)


def _identity(pool: str, slot: int | None, ident: int, incarnation: int) -> str:
    """The fencing token: which pool, which seat, which tenure.

    Written once. It used to be spelled out in three places -- the handle a
    peer holds, the member's view of itself, and the header that rides on every
    call -- and those three have to agree letter for letter or a superseded
    member passes a check it should fail. The same argument `frozen()` makes
    about the roster hash: a second implementation drifts silently, and this
    one drifts into the security check.

    A member with no seat is keyed by its own id instead, and `slot` is tested
    against None rather than for truth because seat 0 is a seat.
    """
    return f"{pool}/{slot if slot is not None else ident}#{incarnation}"


def _seat_of(identity: str) -> int:
    """The seat number out of `pool/slot#tenure`."""
    seat = identity.partition("/")[2].partition("#")[0]
    if not seat.isdigit():
        raise ValueError(f"{identity!r} does not name a numbered seat")
    return int(seat)


def _require_client() -> _Client:
    if _client is not None:
        if os.getpid() != _owner_pid:
            raise RuntimeError(
                "this client belongs to another process; fork() left its "
                "heartbeat behind. Call tinyray.join(...) again in the child."
            )
        return _client
    # Never joined and already left look the same from here, and they need
    # opposite reactions, so say which one it is.
    if _left:
        raise RuntimeError(
            "this process has left; a lookup after leave() cannot work. "
            "Background threads outliving leave() are the usual cause."
        )
    raise RuntimeError("call tinyray.join(...) before looking anyone up")


def pool(name: str) -> Pool:
    """Look up a group. Subscribing is implicit and takes effect immediately."""
    return Pool(_checked_pool_name(name), _require_client())


def apool(name: str) -> AsyncPool:
    """Same as `pool`, but its handles hand back awaitables."""
    return AsyncPool(_checked_pool_name(name), _require_client())
