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
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING as _TYPE_CHECKING
from typing import Any

from . import _rpc
from ._errors import (
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
from ._rpc import AsyncHandleMixin as _AsyncHandleMixin
from ._rpc import request_id
from ._serve import CallContext
from ._serve import MethodServer as _MethodServer
from ._tinyray import Client as _Client

if _TYPE_CHECKING:
    from ._rpc import BoundMethod

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
    "FIRST_BEAT_S",
]

POLICIES = ("churn", "serving", "stateful", "collective")

# Seats are declared by the launcher, never handed out by tinyray.
_RANK_VARS = ("TINYRAY_SLOT", "RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK")
_SIZE_VARS = ("TINYRAY_SIZE", "WORLD_SIZE", "SLURM_NTASKS", "OMPI_COMM_WORLD_SIZE")


def _endpoint() -> str:
    """One registry. Losing it is survivable -- lookups keep working from cache
    and the roster regrows within one interval -- so replicas buy little and
    cost a lot: the delta cursor is per-registry, so failing over silently
    freezes the cache."""
    raw = os.environ.get("TINYRAY_REGISTRY", "127.0.0.1:8760").strip()
    return raw if "://" in raw else f"http://{raw}"


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
        return _rpc.BoundMethod(self, name, _rpc.DEFAULT_TIMEOUT)

    @property
    def label(self) -> str:
        """Short form for humans. `identity` stays exact -- it is the fencing
        token -- but a random 63-bit id is unreadable in a log line."""
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

    __slots__ = ("pool", "revision", "members")

    def __init__(self, pool_name: str, revision: int, members: list[Handle]):
        self.pool = pool_name
        self.revision = revision
        self.members = members

    def __len__(self) -> int:
        return len(self.members)

    def __iter__(self):
        return iter(self.members)

    def ready(self) -> list[Handle]:
        return [h for h in self.members if h.ready]

    def slot(self, k: int) -> Handle | None:
        """The occupant of seat k, ready or not, or None if it is empty."""
        for h in self.members:
            if h.slot == k:
                return h
        return None

    def get(self, identity: str) -> Handle | None:
        """The member with this exact identity, tenure included, or None.

        Asked of a snapshot rather than of the pool on purpose: "is that
        incarnation still there" is a question about one moment, and asking the
        live pool twice can answer about two.
        """
        for h in self.members:
            if h.identity == identity:
                return h
        return None

    def __repr__(self) -> str:
        return f"<Snapshot {self.pool} rev={self.revision} members={len(self.members)}>"


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
        # Non-blocking on the write end too: the bell rings from the heartbeat
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

    async def wait(self, timeout: float) -> None:
        """Return when the bell rings, or when `timeout` runs out.

        Running out is an ordinary answer, not an error: callers loop and
        re-check the thing they actually care about, exactly as the
        synchronous `wait_revision` lets them. Letting the TimeoutError out
        instead turned `achanges(timeout=...)` into a raise where the stream
        should simply have ended.
        """
        fut: asyncio.Future[None] = self._loop.create_future()
        self._waiters.append(fut)
        try:
            await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            pass
        finally:
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
        try:
            self._loop.remove_reader(self._r)
        except Exception:
            pass
        os.close(self._r)
        os.close(self._w)


_bells: dict[int, tuple[weakref.ref[asyncio.AbstractEventLoop], _LoopBell]] = {}
# Watchers that are still running, so leave() can end them rather than leave a
# thread parked on a client that has gone.
_live_watches: weakref.WeakSet[_Watching] = weakref.WeakSet()


def _loop_bell(client: _Client) -> _LoopBell:
    loop = asyncio.get_running_loop()
    # id() is reused once a loop is collected, which is how the RPC client
    # cache used to hand one loop's transport to another. Prune by the weak
    # reference rather than trusting the number.
    for key, (ref, bell) in list(_bells.items()):
        if ref() is None:
            bell.close()
            del _bells[key]
    key = id(loop)
    got = _bells.get(key)
    if got is not None and got[0]() is loop:
        return got[1]
    bell = _LoopBell(client, loop)
    _bells[key] = (weakref.ref(loop), bell)
    return bell


def _left_ms(deadline: float | None) -> int | None:
    """Milliseconds still allowed, or None once the time is up.

    Every wait in here spelled this out, and eight copies of a deadline is
    eight chances to get one of them wrong. An unbounded wait still needs a
    number to hand the Rust side: an hour, re-armed each time round, since the
    bell rings far more often than that.
    """
    if deadline is None:
        return 3_600_000
    left = deadline - time.monotonic()
    return None if left <= 0 else int(left * 1000) + 1


class RegistryInfo:
    """What the registry on the other end can do.

    Without this there is nothing to ask. An old registry answers a long-poll
    request immediately and correctly -- it just does not park it -- so
    "parked and nothing happened" and "does not park" are indistinguishable
    from the client. Measured against a 0.6.1 registry: 14.5 requests a second
    where a current one does 0.12, a hundredfold, with `/health` saying only
    `{"status": "ok"}` and no attribute anywhere to probe.

    `protocol` is the number to branch on. It only goes up, and a registry too
    old to report one reads as 0.
    """

    __slots__ = ("protocol", "version")

    #: Feature name -> the protocol that first provided it. Deliberately a
    #: table rather than a per-feature flag: the registry says one number and
    #: the meaning of that number lives here, in the package that depends on
    #: it, so an old client never has to be taught about a future feature.
    FEATURES = {"long_poll": 1}

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

    def __init__(
        self,
        pool: Pool,
        since: int | None,
        timeout: float | None,
        fields: Sequence[str] | None = None,
    ):
        self._pool = pool
        self._c = pool._c
        self._seen = pool.snapshot().revision if since is None else since
        self._deadline = None if timeout is None else time.monotonic() + timeout
        self._closed = False
        self._tick = 0
        self._fields = None if fields is None else list(fields)
        self._digest = (
            None if self._fields is None else self._c.field_digest(pool._name, self._fields, False)
        )
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
        self._tick = self._c.cache_revision()
        info = self._c.pool_info(self._pool._name)
        # The bell rings once a beat whether or not anything happened, so what
        # decides a yield is the pool's own version. Waking on the bell but
        # yielding on the beat was measured at 25 snapshots for 4 real changes
        # -- and at 5,000 members a snapshot is 10.6ms spent rebuilding what
        # did not move.
        if info is not None and info[0] != self._seen:
            self._seen = info[0]
            if self._fields is None:
                return self._pool.snapshot(), 0
            # Something moved, but maybe not anything this watcher named. Ask
            # the cache directly: building the snapshot to find out would be
            # the whole cost we are trying to avoid.
            digest = self._c.field_digest(self._pool._name, self._fields, False)
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
        bell = _loop_bell(self._c)
        while True:
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
        raw = self._c.lookup(self._name, json.dumps(filt), require_ready)
        # The method list is stored once per pool, not once per member: members
        # of a pool run the same code.
        info = self._c.pool_info(self._name)
        methods = tuple(info[3]) if info else ()
        return [self._handle_cls(self._name, m, methods) for m in json.loads(raw)]

    def snapshot(self, include_unready: bool = True) -> Snapshot:
        """The pool as it stands, with the revision it stood at.

        Read under one lock, so the members and the revision cannot come from
        two different moments -- which is the same reason epoch() takes its
        list and its fingerprint together.
        """
        self._settle()
        info = self._c.pool_info(self._name)
        got = self._c.frozen(self._name, not include_unready)
        if got is None:
            return Snapshot(self._name, 0, [])
        raw, _, _, version = got
        methods = tuple(info[3]) if info else ()
        members = [self._handle_cls(self._name, m, methods) for m in json.loads(raw)]
        return Snapshot(self._name, version, members)

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
    ) -> tuple[int, str | None]:
        """The seat to watch and the tenure that must give way."""
        if (slot is None) == (identity is None):
            raise TypeError(f"{who}() takes exactly one of slot= or identity=")
        if identity is not None:
            return _seat_of(identity), identity
        assert slot is not None
        here = self.snapshot().slot(slot)
        return slot, here.identity if here else None

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
        seat, was = self._replacement_target(slot, identity, "wait_replacement")
        with self.changes(timeout=timeout) as w:
            for snap in w:
                now = snap.slot(seat)
                if now is not None and now.identity != was:
                    return now
        return None

    def all(self, **filt: Any) -> list[Handle]:
        return self._members(filt, require_ready=True)

    def pick(self, **filt: Any) -> Handle:
        found = self._members(filt, require_ready=True)
        if not found:
            raise NotFound(f"no ready member of {self._name!r} matching {filt}")
        return random.choice(found)

    def slot(self, k: int, require_ready: bool = False) -> Handle:
        for h in self._members({}, require_ready=require_ready):
            if h.slot == k:
                return h
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
        # landed while the predicate was running is still delivered.
        with self.changes(since=snap.revision if since is None else since, timeout=timeout) as w:
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

        def departed(snap: Snapshot) -> bool:
            return snap.get(identity) is None

        try:
            self.until(departed, timeout=timeout, describe=f"{identity} to leave")
        except TimeoutError:
            return False
        return True

    def wait(self, count: int = 1, timeout: float = 30.0, **filt: Any) -> list[Handle]:
        """Block until `count` members match. Bounded, and the failure names them."""
        deadline = time.monotonic() + timeout
        while True:
            rev = self._c.cache_revision()
            found = self._members(filt, require_ready=True)
            if len(found) >= count:
                return found
            ms = _left_ms(deadline)
            if ms is None:
                raise TimeoutError(
                    f"waited {timeout}s for {count} ready member(s) of "
                    f"{self._name!r} matching {filt}, saw {len(found)}"
                )
            self._c.wait_revision(rev, ms)

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
        self._settle()
        found, mismatched = 0, False
        while True:
            # A stale roster is not safe to build a collective on: ranks could
            # disagree. Refuse rather than freeze something we cannot trust.
            if self._c.silence_ms > self._lease_ms():
                raise Stale(
                    f"cannot open a round of {self._name!r}: no contact with the "
                    f"registry for {self._c.silence_ms}ms"
                )
            rev = self._c.cache_revision()
            info = self._c.pool_info(self._name)
            if info is None:
                # No answer about this pool yet, so there is no fingerprint to
                # freeze. min=0 used to reach the line below and crash on it.
                ms = _left_ms(deadline)
                if ms is None:
                    raise TimeoutError(
                        f"waited {timeout}s to open a round of {self._name!r}: "
                        f"the registry has said nothing about it"
                    )
                self._c.wait_revision(rev, ms)
                continue
            target = min if min is not None else info[2]
            if target is None:
                raise PolicyError(f"{self._name!r} declares no size; pass min= or join with size=")
            got = self._c.frozen(self._name, True)
            if got is not None:
                raw, ours, whole, _ = got
                members = [self._handle_cls(self._name, m, tuple(info[3])) for m in json.loads(raw)]
                found, mismatched = len(members), ours != whole
                if found >= target and not mismatched:
                    return Epoch(self._name, self._c, members, whole)
            ms = _left_ms(deadline)
            if ms is None:
                if mismatched and found >= target:
                    raise TimeoutError(
                        f"waited {timeout}s to open a round of {self._name!r}: "
                        f"{found} member(s) ready, but the pool holds a seat whose "
                        f"occupant has not declared itself ready, so the fingerprint "
                        f"would not describe the list -- wait for it rather than "
                        f"freeze a round no other rank can be held to"
                    )
                raise TimeoutError(
                    f"waited {timeout}s to open a round of {self._name!r}: "
                    f"{found} of {target} present"
                )
            self._c.wait_revision(rev, ms)

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
        server: _MethodServer | None = None,
        ident: int | None = None,
    ):
        self._c = client
        self._server = server
        self.pool = pool_name
        self.slot = slot
        self.incarnation = incarnation
        self._ident = slot if ident is None else ident
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
        seat = self.slot if self.slot is not None else self._ident
        return f"{self.pool}/{seat}#{self.incarnation}"

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
            self._state = merged
            self._c.set_state(raw, True)
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
            self._state = merged
            self._c.set_state_only(raw)
        return self

    def replace(self, state: dict[str, Any] | None = None) -> Member:
        """`update()` but replacing the published state outright, so keys can
        be taken back. Readiness is left alone."""
        self._mine()
        with self._lock:
            fresh = dict(state or {})
            raw = self._encode_state(fresh)
            self._state = fresh
            self._c.set_state_only(raw)
        return self

    def _encode_state(self, state: dict[str, Any]) -> str:
        raw = json.dumps(state)
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
        return raw

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
            self._state = fresh
            self._c.set_state(raw, True)
        return self

    def flush(self, timeout: float = 10.0) -> Member:
        """Block until the registry has been told what was last published.

        ready() and set_ready() only write locally and nudge the heartbeat, so
        "published" and "visible to peers" are a beat apart. Reading your own
        state back to find out is a round trip that says what this does.

        Costs at most one extra beat: a beat already in flight was composed
        before the change, so confirmation waits for the one after it.
        """
        self._mine()
        target = self._c.stats()["beats_ok"] + 2
        deadline = time.monotonic() + timeout
        while True:
            rev = self._c.cache_revision()
            if self._c.stats()["beats_ok"] >= target:
                return self
            if not self._c.accepted:
                raise SeatTaken(f"{self.pool} seat {self.slot} was taken while publishing")
            ms = _left_ms(deadline)
            if ms is None:
                raise TimeoutError(
                    f"waited {timeout}s for the registry to take this state; "
                    f"last error was {self._c.last_error()!r}"
                )
            self._c.wait_revision(rev, ms)

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
        bell = _loop_bell(self._c)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
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

    def leave(self) -> None:
        self._mine()
        if not self._left:
            self._left = True
            # A watcher blocked on a client that is about to go would wait out
            # its whole timeout, and a non-daemon thread iterating one kept the
            # process alive indefinitely.
            for w in list(_live_watches):
                w.close()
            global _client, _left
            if _client is self._c:
                _client = None
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

# Matches MAX_STATE in the registry: a fact about where something is, not the
# something. See tests/test_state_budget.py for the amplification measurement.
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
    global _client, _left
    if _client is not None:
        _client.abandon()
    _client = None
    _left = False
    # The inherited pipes belong to the parent's loops and its heartbeat is
    # gone, so nothing will ever write to them again. Drop them without
    # closing: the parent still owns the descriptors.
    _bells.clear()
    _live_watches.clear()


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
    """
    global _client, _left, _owner_pid
    _left = False
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
        server = _MethodServer(
            serves, f"{pool}/{seat}#{incarnation}", max_concurrency=max_concurrency
        )
        methods = server.methods
        url = url or server.url(_advertise())

    c = _Client(
        endpoint=_endpoint(),
        pool=pool,
        id=ident,
        incarnation=incarnation,
        policy=policy,
        slot=slot,
        size=size,
        url=url,
        methods=methods,
        exclusive=exclusive,
    )
    if server is not None:
        # A superseded process keeps running and keeps its port open, so the
        # only thing that knows it is a ghost is the registry's answer to its
        # own heartbeat. Wire that in, or a caller holding a stale handle gets
        # a cheerful reply from the wrong process.
        server.still_ours = lambda: c.accepted
    c.watch([pool])
    if not c.start():
        # Losing the registry later is survivable -- the cache carries the
        # process. Never reaching it is not: there is nothing to carry, and
        # the process would publish state nobody sees and wait for peers who
        # cannot appear. Measured against a wrong port, an unroutable address
        # and a name that does not resolve: join() returned in 0.0s to 5.0s
        # with accepted=True, zero beats through and its own pool empty.
        deadline = time.monotonic() + timeout
        while not c.stats()["beats_ok"]:
            rev = c.cache_revision()
            if c.stats()["beats_ok"]:
                break
            ms = _left_ms(deadline)
            if ms is None:
                break
            c.wait_revision(rev, ms)
        if not c.stats()["beats_ok"]:
            c.leave()
            if server is not None:
                server.close()
            raise Unreachable(
                f"no answer from the registry at {_endpoint()} after "
                f"{timeout:g}s and {c.stats()['beats_failed']} attempts: "
                f"{c.last_error()}. Pass join(timeout=) to wait longer."
            )
    if not c.accepted:
        # Seats are last-writer-wins by default, because a restarting rank has
        # to reclaim its seat while the dead one's lease is still running.
        # exclusive= asks for the opposite, which is what an election wants.
        #
        # Either way a refusal has to be raised. The beat loop stops on one,
        # so returning would hand back a member that never beats again while
        # accepted, silence_ms and an empty pool are the only clues -- measured
        # at beats_ok frozen at 2, last_error empty and its own pool showing
        # zero members.
        c.leave()
        if server is not None:
            server.close()
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
    _client = c
    _owner_pid = os.getpid()
    seen = RegistryInfo(*c.registry())
    if not seen.supports("long_poll"):
        warnings.warn(
            f"the registry at {_endpoint()} reports protocol {seen.protocol} "
            f"({seen.version or 'version not reported'}) but tinyray "
            f"{__version__} expects {RegistryInfo.FEATURES['long_poll']}. "
            f"Everything works; changes will take up to a heartbeat interval "
            f"to show up instead of a round trip, and this process will beat "
            f"far more often. Upgrade the registry, or silence this with "
            f"warnings.filterwarnings('ignore', "
            f"category=tinyray.OldRegistryWarning).",
            OldRegistryWarning,
            stacklevel=2,
        )
    _rpc.set_identity(f"{pool}/{slot if slot is not None else ident}#{incarnation}")
    member = Member(c, pool, slot, incarnation, server, ident)
    # A process that exits normally should say goodbye, so the seat frees up
    # immediately instead of waiting out the lease. SIGKILL still falls back
    # to lease expiry -- both paths work, they just differ in speed.
    atexit.register(member.leave)
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
        watch = self.achanges(since=snap.revision if since is None else since, timeout=timeout)
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
        found: list[Handle] = []

        def enough(_: Snapshot) -> bool:
            # Matching stays in Rust, where `wait()` does it too: the rules are
            # not obvious (numbers compare by value, booleans strictly) and a
            # second implementation here would drift from the first.
            nonlocal found
            found = self._members(filt, require_ready=True)
            return len(found) >= count

        await self.auntil(
            enough, timeout=timeout, describe=f"{count} ready member(s) matching {filt}"
        )
        return found

    async def await_departure(self, identity: str, timeout: float | None = None) -> bool:
        """`wait_departure()` for an event loop."""

        def departed(snap: Snapshot) -> bool:
            return snap.get(identity) is None

        try:
            await self.auntil(departed, timeout=timeout, describe=f"{identity} to leave")
        except TimeoutError:
            return False
        return True

    async def await_replacement(
        self,
        slot: int | None = None,
        identity: str | None = None,
        timeout: float | None = None,
    ) -> Handle | None:
        """`wait_replacement()` for an event loop."""
        seat, was = self._replacement_target(slot, identity, "await_replacement")
        async with self.achanges(timeout=timeout) as w:
            async for snap in w:
                now = snap.slot(seat)
                if now is not None and now.identity != was:
                    return now
        return None


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
    return Pool(name, _require_client())


def apool(name: str) -> AsyncPool:
    """Same as `pool`, but its handles hand back awaitables."""
    return AsyncPool(name, _require_client())
