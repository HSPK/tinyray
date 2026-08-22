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
import time
from typing import TYPE_CHECKING as _TYPE_CHECKING
from typing import Any

from . import _rpc
from ._errors import (
    Fenced,
    NotDelivered,
    NotFound,
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
    "Snapshot",
    "Stale",
    "SeatTaken",
    "NotFound",
    "PolicyError",
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
            left = deadline - time.monotonic()
            if left <= 0:
                return
            self._c.wait_revision(rev, int(left * 1000) + 1)

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

    def changes(self, since: int | None = None, timeout: float | None = None):
        """Yield a fresh snapshot every time this pool moves. Never polls.

        Deliberately a stream of snapshots rather than of events. The client
        sees the pool at heartbeat cadence, and the registry collapses whatever
        happened in between: a member that went ready and unready again inside
        one interval arrives as one entry carrying its current state, not as
        two events. An event stream would therefore promise a completeness the
        wire cannot deliver. A snapshot promises what it can -- you never miss
        a state, only the transitions nobody could have observed -- and the
        events are a diff away, because every entry carries its incarnation.
        """
        seen = self.snapshot().revision if since is None else since
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            # A superseded member stops beating, so the bell stops ringing and
            # a consumer would wait here for as long as it was willing to. Ends
            # the stream instead: nothing more is coming.
            if not self._c.accepted:
                return
            # The bell rings once a beat whether or not anything happened, so
            # what decides a yield is the pool's own version. Waking on the
            # bell but yielding on the beat was measured at 25 snapshots for 4
            # real changes -- and at 5,000 members a snapshot is 10.6ms spent
            # rebuilding what did not move.
            tick = self._c.cache_revision()
            info = self._c.pool_info(self._name)
            if info is not None and info[0] != seen:
                seen = info[0]
                yield self.snapshot()
                continue
            left = 3600.0 if deadline is None else deadline - time.monotonic()
            if left <= 0:
                return
            self._c.wait_revision(tick, int(left * 1000) + 1)

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

    def wait(self, count: int = 1, timeout: float = 30.0, **filt: Any) -> list[Handle]:
        """Block until `count` members match. Bounded, and the failure names them."""
        deadline = time.monotonic() + timeout
        while True:
            rev = self._c.cache_revision()
            found = self._members(filt, require_ready=True)
            if len(found) >= count:
                return found
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError(
                    f"waited {timeout}s for {count} ready member(s) of "
                    f"{self._name!r} matching {filt}, saw {len(found)}"
                )
            self._c.wait_revision(rev, int(left * 1000) + 1)

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
                left = deadline - time.monotonic()
                if left <= 0:
                    raise TimeoutError(
                        f"waited {timeout}s to open a round of {self._name!r}: "
                        f"the registry has said nothing about it"
                    )
                self._c.wait_revision(rev, int(left * 1000) + 1)
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
            left = deadline - time.monotonic()
            if left <= 0:
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
            self._c.wait_revision(rev, int(left * 1000) + 1)

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
        """Hang out a sign. Sends nothing now; the next heartbeat carries it."""
        self._mine()
        # Check before mutating: an over-budget call used to leave the blob in
        # place, so every later ready() failed too and one bad call poisoned
        # the member for good.
        merged = {**self._state, **state}
        raw = self._encode_state(merged)
        self._state = merged
        self._c.set_state(raw, True)
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
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError(
                    f"waited {timeout}s for the registry to take this state; "
                    f"last error was {self._c.last_error()!r}"
                )
            self._c.wait_revision(rev, int(left * 1000) + 1)

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
            left = 3600.0 if deadline is None else deadline - time.monotonic()
            if left <= 0:
                return False
            self._c.wait_revision(rev, int(left * 1000) + 1)

    async def await_fenced(self, timeout: float | None = None) -> bool:
        """wait_fenced() for an event loop, waiting on a thread."""
        return await asyncio.to_thread(self.wait_fenced, timeout)

    def unready(self) -> Member:
        self._mine()
        self._c.set_state(self._encode_state(self._state), False)
        return self

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
        self._mine()
        return self._c.stats()

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
            left = deadline - time.monotonic()
            if left <= 0:
                break
            c.wait_revision(rev, int(left * 1000) + 1)
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

    async def achanges(self, since: int | None = None, timeout: float | None = None):
        """`changes()` for an event loop. Waits on the client's bell, on a
        thread, so the loop keeps turning while nothing is happening."""
        seen = self.snapshot().revision if since is None else since
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            # A superseded member stops beating, so the bell stops ringing and
            # a consumer would wait here for as long as it was willing to. Ends
            # the stream instead: nothing more is coming.
            if not self._c.accepted:
                return
            # The bell rings once a beat whether or not anything happened, so
            # what decides a yield is the pool's own version. Waking on the
            # bell but yielding on the beat was measured at 25 snapshots for 4
            # real changes -- and at 5,000 members a snapshot is 10.6ms spent
            # rebuilding what did not move.
            tick = self._c.cache_revision()
            info = self._c.pool_info(self._name)
            if info is not None and info[0] != seen:
                seen = info[0]
                yield self.snapshot()
                continue
            left = 3600.0 if deadline is None else deadline - time.monotonic()
            if left <= 0:
                return
            await asyncio.to_thread(self._c.wait_revision, tick, int(left * 1000) + 1)


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
