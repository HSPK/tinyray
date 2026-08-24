"""M3 acceptance: freeze a round, and notice when it breaks."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time

import pytest
import tinyray

RANK = textwrap.dedent(
    """
    import sys, tinyray
    rank, world = int(sys.argv[1]), int(sys.argv[2])
    with tinyray.join("trainer", "collective", slot=rank, size=world) as me:
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


class Ranks:
    def __init__(self, n: int, world: int, first: int = 1):
        self.procs = []
        for r in range(first, first + n):
            p = subprocess.Popen(
                [sys.executable, "-c", RANK, str(r), str(world)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            assert p.stdout.readline().strip() == "READY"
            self.procs.append(p)

    def kill(self, i: int) -> None:
        self.procs[i].kill()
        self.procs[i].wait(timeout=5)

    def stop(self) -> None:
        for p in self.procs:
            try:
                p.stdin.write("\n")
                p.stdin.flush()
                p.wait(timeout=5)
            except Exception:
                p.kill()


def test_a_round_waits_for_everyone_then_freezes(registry):
    world = 4
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(world - 1, world)
    try:
        ep = tinyray.pool("trainer").epoch(timeout=20)
        assert len(ep) == world
        assert sorted(h.slot for h in ep) == [0, 1, 2, 3]
        assert ep.valid
        # Frozen: the roster does not move under us the way all() does.
        before = tuple(ep.members)
        time.sleep(0.3)
        assert ep.members == before
        assert ep.slot(2).slot == 2
    finally:
        peers.stop()
        me.leave()


def test_an_incomplete_round_does_not_open(registry):
    me = tinyray.join("trainer", "collective", slot=0, size=4)
    me.ready()
    peers = Ranks(2, 4)  # only 3 of 4 seats filled
    try:
        with pytest.raises(TimeoutError, match="3 of 4"):
            tinyray.pool("trainer").epoch(timeout=2)
        # ...unless the caller says a smaller round is acceptable.
        assert len(tinyray.pool("trainer").epoch(min=3, timeout=10)) == 3
    finally:
        peers.stop()
        me.leave()


def test_losing_a_rank_breaks_the_round_within_the_lease(registry):
    """The acceptance number: detection in seconds, not the ten minutes a
    collective timeout has to allow for a legitimately slow operation."""
    world = 3
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(world - 1, world)
    try:
        ep = tinyray.pool("trainer").epoch(timeout=20)
        assert ep.valid

        broke = threading.Event()
        t0 = time.monotonic()

        def watchdog() -> None:
            # Checking inside a training loop is useless: a stuck rank never
            # reaches the check. A separate thread can, because NCCL releases
            # the GIL while it blocks.
            while ep.valid and time.monotonic() - t0 < 30:
                time.sleep(0.05)
            broke.set()

        w = threading.Thread(target=watchdog, daemon=True)
        w.start()
        peers.kill(0)  # SIGKILL: no farewell, only the lease can notice

        assert broke.wait(timeout=20), "the round never noticed a missing rank"
        elapsed = time.monotonic() - t0
        lease = registry.ttl_ms / 1000
        assert elapsed < lease * 2 + 1, f"took {elapsed:.1f}s against a {lease}s lease"
        assert not ep.valid
    finally:
        peers.stop()
        me.leave()


def test_publishing_state_does_not_break_a_round(registry):
    """Two numbers exist for exactly this: the same event has to count as
    changed for caches and unchanged for a frozen round."""
    world = 2
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready(step=0)
    peers = Ranks(1, world)
    try:
        ep = tinyray.pool("trainer").epoch(timeout=20)
        for step in range(1, 6):
            me.ready(step=step)
            time.sleep(0.05)
            assert ep.valid, f"publishing step={step} voided the round"
    finally:
        peers.stop()
        me.leave()


def test_a_replacement_breaks_the_round_even_in_the_same_seat(registry):
    world = 2
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(1, world)
    ep = tinyray.pool("trainer").epoch(timeout=20)
    assert ep.valid

    peers.kill(0)
    deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 2
    while ep.valid and time.monotonic() < deadline:
        time.sleep(0.05)
    replacement = Ranks(1, world)  # same seat, new tenure
    try:
        tinyray.pool("trainer").wait(count=2, timeout=10)
        assert not ep.valid, "a new occupant of seat 1 is not the old round"
        fresh = tinyray.pool("trainer").epoch(timeout=10)
        assert fresh.valid and fresh.roster != ep.roster
    finally:
        replacement.stop()
        peers.stop()
        me.leave()


def test_opening_a_round_is_refused_while_out_of_touch(registry):
    """Two rules that look contradictory but are not.

    A round already running keeps going when the registry dies -- the ranks are
    all alive and the group is intact, so killing it would contradict the
    promise that the registry can die without stopping training. Opening a
    *new* round is refused, because ranks could freeze different rosters.
    """
    world = 2
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(1, world)
    try:
        ep = tinyray.pool("trainer").epoch(timeout=20)
        assert ep.valid

        registry.stop()
        time.sleep(registry.ttl_ms / 1000 * 2)

        assert ep.valid, "an established group must survive losing the phone book"
        with pytest.raises(tinyray.Stale, match="no contact"):
            tinyray.pool("trainer").epoch(timeout=5)
    finally:
        registry.start()
        peers.stop()
        me.leave()


def test_min_can_open_divergent_rounds_while_members_are_arriving(registry):
    """A sharp edge worth pinning down rather than hiding.

    epoch(min=) returns as soon as enough seats are filled, so callers racing
    a still-arriving group can freeze different lists. Two different lists is
    a deadlock, not a smaller group -- which is why the first round has to be
    strict and min= belongs to rebuilds.
    """
    world = 3
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    early = tinyray.pool("trainer").epoch(min=1, timeout=10)
    assert len(early) == 1, "min=1 opened before anyone else arrived, as documented"

    peers = Ranks(world - 1, world)
    try:
        full = tinyray.pool("trainer").epoch(timeout=20)
        assert len(full) == world
        # The two rounds disagree, which is exactly the hazard.
        assert full.roster != early.roster
        assert not early.valid
    finally:
        peers.stop()
        me.leave()


def test_a_rebuild_after_a_loss_is_what_min_is_for(registry):
    world = 3
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(world - 1, world)
    try:
        first = tinyray.pool("trainer").epoch(timeout=20)  # strict: everyone
        assert len(first) == world

        peers.kill(0)
        deadline = time.monotonic() + registry.ttl_ms / 1000 * 3 + 3
        while first.valid and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not first.valid

        rebuilt = tinyray.pool("trainer").epoch(min=world - 1, timeout=20)
        assert len(rebuilt) == world - 1
        assert rebuilt.valid
    finally:
        peers.stop()
        me.leave()


LAZY_RANK = textwrap.dedent(
    """
    import sys, tinyray
    rank, world = int(sys.argv[1]), int(sys.argv[2])
    with tinyray.join("trainer", "collective", slot=rank, size=world) as me:
        print("JOINED", flush=True)
        sys.stdin.readline()
        me.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


def _fnv(seat: int, tenure: int) -> int:
    """独立重算，跟 test_roster_fingerprint 一样故意抄一份。"""
    h = 1469598103934665603
    for b in seat.to_bytes(8, "little") + tenure.to_bytes(8, "little"):
        h = ((h ^ b) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return h


def _fingerprint_of(members) -> int:
    acc = 0
    for h in members:
        acc ^= _fnv(h.id, h.incarnation)
    return acc


def test_a_frozen_round_is_described_by_the_fingerprint_it_carries(registry):
    """指纹是各 rank 之间唯一的共识凭据，所以它必须正好描述被冻结的那份名单。

    指纹由注册中心按「座位+任期」算，占位者在场就算数，ready 与否不影响它 ——
    这是有意的（ready 是"能不能用"，不是"是不是同一批人"）。但 epoch() 冻结的
    名单是按 ready 过滤的。于是出现一个注册中心看不见、客户端也不报警的缺口：
    有人已入座但还没 ready 时，两个 rank 可以冻出**同一个指纹、不同的名单**，
    而且两边的 valid 都是 True。

    上面 test_min_can_open_divergent_rounds 记录的分歧风险，靠的正是
    `full.roster != early.roster` 这张网 —— 而占位者不变、只有 ready 变化时，
    这张网是漏的。

    修复前实测：3 个占位者、2 个 ready，epoch(min=2) 返回 2 人的名单，却带着
    描述 3 个人的指纹。

    这条不要求 epoch() 一定能开出一轮 —— 等不到就超时是诚实的答案。它要求的是
    ：凡是开出来的一轮，它带的指纹必须就是它那份名单算出来的。
    """
    world = 3
    me = tinyray.join("trainer", "collective", slot=0, size=world)
    me.ready()
    peers = Ranks(1, world, first=1)  # slot 1: 入座并 ready
    lazy = subprocess.Popen(
        [sys.executable, "-c", LAZY_RANK, "2", str(world)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert lazy.stdout.readline().strip() == "JOINED"

        # 等到三个人都在册（指纹已经把第三个人算进去），但只有两个 ready。
        pool = tinyray.pool("trainer")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if len(pool._members({}, require_ready=False)) == world:
                break
            time.sleep(0.05)
        assert len(pool._members({}, require_ready=False)) == world, "第三个 rank 没有入座"
        assert len(pool.all()) == world - 1, "第三个 rank 不该是 ready 的"

        try:
            early = pool.epoch(min=2, timeout=3)
        except TimeoutError:
            early = None  # 开不出一轮是可以接受的答案
        assert early is None or early.roster == _fingerprint_of(early.members), (
            f"冻结的一轮带着 {early.roster}，但它自己的 {len(early.members)} 人算出来是 "
            f"{_fingerprint_of(early.members)}；同一个指纹配得上不同的名单，"
            f"各 rank 就会在都认为一致的情况下建出不同的通信组"
        )

        # 第三个人 ready 之后，必须能开出一轮，而且同样自洽。
        lazy.stdin.write("\n")
        lazy.stdin.flush()
        assert lazy.stdout.readline().strip() == "READY"
        pool.wait(count=world, timeout=20)

        full = pool.epoch(timeout=20)
        assert len(full) == world
        assert full.roster == _fingerprint_of(full.members), "齐员之后的一轮也对不上"
    finally:
        try:
            lazy.stdin.write("\n")
            lazy.stdin.flush()
            lazy.wait(timeout=5)
        except Exception:
            lazy.kill()
        peers.stop()
        me.leave()


def test_a_frozen_round_cannot_be_edited(registry):
    """ "冻结"必须真的是冻结。

    这份名单会发给每个 rank 去建同一个进程组。是列表的话，任何一次就地
    `sort()` 或 `filter` 都能让各 rank 拿到不同的名单 —— 那正是这个类型存在
    要防的死锁，却是从这个类型本身走过去的。实测过：`epoch.members.append(...)`
    会让 `len(epoch)` 跟着变。
    """
    with tinyray.join("r", "collective", slot=0, size=1) as me:
        me.ready()
        ep = tinyray.pool("r").epoch(timeout=15)
        assert len(ep) == 1
        with pytest.raises(AttributeError):
            ep.members.append("不是 Handle")  # type: ignore[attr-defined]
        with pytest.raises(TypeError):
            ep.members[0] = "换一个"  # type: ignore[index]
        assert len(ep) == 1


def test_a_snapshot_cannot_be_edited_either(registry):
    """快照命名的是一个时刻，事后能改的就不是一个时刻。"""
    with tinyray.join("p", "churn") as me:
        me.ready()
        snap = tinyray.pool("p").snapshot()
        with pytest.raises(AttributeError):
            snap.members.append("不是 Handle")  # type: ignore[attr-defined]
        with pytest.raises(TypeError):
            snap.members[0] = "换一个"  # type: ignore[index]


def test_readiness_does_not_break_a_round_but_leaving_does(registry):
    """指纹认的是"谁占着座位"，不是"谁准备好了"。

    这个区分是刻意的：一轮开始之后有 rank 短暂 unready，不该让整轮作废；而有
    人真的走了，就必须作废。三种变化的反应都实测过。
    """
    peer = textwrap.dedent(
        f"""
        import os, sys, tinyray
        os.environ["TINYRAY_REGISTRY"] = "{registry.endpoint}"
        m = tinyray.join("r", "collective", slot=1, size=2)
        m.ready()
        print("READY", flush=True)
        for line in sys.stdin:
            cmd = line.strip()
            if cmd == "unready":
                m.unready()
            elif cmd == "ready":
                m.ready()
            elif cmd == "quit":
                break
            print("ok", flush=True)
        m.leave()
        """
    )
    p = subprocess.Popen(
        [sys.executable, "-c", peer], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        assert p.stdout.readline().strip() == "READY"
        with tinyray.join("r", "collective", slot=0, size=2) as me:
            me.ready()
            ep = tinyray.pool("r").epoch(timeout=20)
            assert len(ep) == 2 and ep.valid

            def settle() -> None:
                time.sleep(registry.ttl_ms / 1000 * 1.5)

            p.stdin.write("unready\n")
            p.stdin.flush()
            p.stdout.readline()
            settle()
            assert ep.valid, "有人 unready 不该让整轮作废"

            p.stdin.write("ready\n")
            p.stdin.flush()
            p.stdout.readline()
            settle()
            assert ep.valid

            p.stdin.write("quit\n")
            p.stdin.flush()
            p.wait(timeout=10)
            settle()
            assert not ep.valid, "有人离开了，这一轮必须作废"
    finally:
        if p.poll() is None:
            p.kill()
