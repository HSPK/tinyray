"""等待必须由事件唤醒，不能靠轮询。

以前每个等待都是 sleep 循环：_settle 每秒 500 圈、wait 20 圈、epoch 50 圈、
join 50 圈。它们花 CPU 去确认"什么都没发生"，又在真的发生时白等半个 tick。

这里钉两件事：
  - 唤醒够快（不是等下一次 sleep 到期）
  - 空转时不烧 CPU（这条才能证明它真的没在轮询）
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time

import tinyray

LATE = textwrap.dedent(
    """
    import sys, time, tinyray
    time.sleep(float(sys.argv[1]))
    with tinyray.join("late", "churn") as m:
        m.ready(who="late")
        print("READY", flush=True)
        sys.stdin.readline()
    """
)

SEAT = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join("seats", "collective", slot=int(sys.argv[1]), size=2) as m:
        print("JOINED", flush=True)
        sys.stdin.readline()
        m.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


def _stop(p: subprocess.Popen) -> None:
    try:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=10)
    except Exception:
        p.kill()


def test_a_wait_wakes_on_the_event_not_on_the_next_tick(registry):
    """唤醒延迟应当由心跳决定，而不是由 sleep 的粒度决定。"""
    me = tinyray.join("obs", "churn")
    me.ready()
    pool = tinyray.pool("late")
    pool.snapshot()  # 先订阅，免得把首次同步的代价算进唤醒延迟

    late = subprocess.Popen(
        [sys.executable, "-c", LATE, "0.5"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        t0 = time.monotonic()
        found = pool.wait(count=1, timeout=30)
        woke = time.monotonic() - t0
        assert len(found) == 1
        # 一拍是 500ms，两拍是发现所需的上限；给足余量但不给到"轮询也能过"。
        assert woke < 3.0, f"等了 {woke:.2f}s 才醒"
    finally:
        _stop(late)
        me.leave()


def test_changes_yields_a_snapshot_when_the_pool_moves(registry):
    """变化流：池子一动就给一份新快照，不动就一直阻塞。"""
    me = tinyray.join("obs", "churn")
    me.ready()
    pool = tinyray.pool("late")
    start = pool.snapshot()

    seen: list[int] = []
    done = threading.Event()

    def watcher() -> None:
        for snap in pool.changes(since=start.revision, timeout=25):
            seen.append(len(snap.ready()))
            if snap.ready():
                done.set()
                return

    t = threading.Thread(target=watcher, daemon=True)
    t.start()
    late = subprocess.Popen(
        [sys.executable, "-c", LATE, "0.3"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert done.wait(timeout=25), f"变化流没有醒过来，只看到 {seen}"
        assert seen[-1] == 1
    finally:
        _stop(late)
        t.join(timeout=5)
        me.leave()


def test_a_snapshot_holds_a_seat_that_has_not_said_it_is_ready(registry):
    """这正是 all() 答不了的那个问题。

    prepare 阶段成员已经入座但还没 ready：座位被占着，别人拿不走，可是 all()
    会说它不在。快照要能同时回答"谁在座"和"谁能用"。
    """
    me = tinyray.join("seats", "collective", slot=0, size=2)
    me.ready()
    peer = subprocess.Popen(
        [sys.executable, "-c", SEAT, "1"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert peer.stdout.readline().strip() == "JOINED"
        pool = tinyray.pool("seats")

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and len(pool.snapshot()) < 2:
            time.sleep(0.05)  # 测试自己等外部进程，不是库在轮询

        snap = pool.snapshot()
        assert len(snap) == 2, "入座的成员必须出现在快照里"
        assert len(snap.ready()) == 1, "但它还没 ready"
        assert len(pool.all()) == 1, "all() 只答『谁能用』，所以看不到它"

        occupant = snap.slot(1)
        assert occupant is not None and not occupant.ready
        assert occupant.incarnation > 0, "每条记录都要带任期号，快照才能互相比较"

        # ready 之后，座位没换人 —— 任期号相同，正是"仍是同一个人"的判据。
        peer.stdin.write("\n")
        peer.stdin.flush()
        assert peer.stdout.readline().strip() == "READY"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and len(pool.snapshot().ready()) < 2:
            time.sleep(0.05)

        after = pool.snapshot()
        assert len(after.ready()) == 2
        assert after.slot(1).incarnation == occupant.incarnation, "同一个任期，不是换人"
        assert after.revision > snap.revision, "revision 必须单调前进"
    finally:
        _stop(peer)
        me.leave()


def test_the_library_never_sleeps_its_way_through_a_wait():
    """把这条钉死：产品代码里不允许出现 sleep 轮询。

    这条读源码，因为运行时量不出来。实测空转 5 秒的等待：事件驱动 0.0054s
    CPU，0.05 秒一圈的轮询 0.0109s —— 只差两倍，而且绝对值小到被机器噪声盖住，
    撑不起一条阈值断言。硬要拿它当断言，只会得到一条会乱叫的测试，而教人忽略
    一条测试比没有它更糟。

    唤醒延迟同样不区分：两种实现都被心跳间隔兜住，看起来一样快。所以真正把
    "不许轮询"钉住的，只有这一条。
    """
    import pathlib

    root = pathlib.Path(tinyray.__file__).parent
    offenders = []
    for path in root.glob("*.py"):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if "time.sleep" in stripped:
                offenders.append(f"{path.name}:{n}: {stripped}")
    assert not offenders, "等待必须靠事件唤醒，不能轮询:\n" + "\n".join(offenders)


SUPERSEDE = textwrap.dedent(
    """
    import sys, tinyray
    with tinyray.join("seats", "collective", slot=0, size=2) as m:
        m.ready()
        print("READY", flush=True)
        sys.stdin.readline()
    """
)


def test_a_superseded_member_does_not_leave_its_watcher_hanging(registry):
    """被顶替之后心跳循环就退出了，铃不会再响。

    如果变化流只是"等下一次响铃"，消费者会在这里永久阻塞 —— 它等的是一件
    再也不会发生的事，而且没有任何东西说明为什么。失败必须有界，所以流要结束。
    """
    me = tinyray.join("seats", "collective", slot=0, size=2)
    me.ready()
    pool = tinyray.pool("seats")
    pool.snapshot()

    # 另一个进程用更晚的任期抢走同一个座位，本进程随即被判出局。
    thief = subprocess.Popen(
        [sys.executable, "-c", SUPERSEDE],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert thief.stdout.readline().strip() == "READY"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and me.accepted:
            time.sleep(0.05)  # 测试自己等外部进程
        assert not me.accepted, "没有被顶替，这条测试就没测到东西"

        done = threading.Event()

        def watcher() -> None:
            for _ in pool.changes():  # 故意不给 timeout
                pass
            done.set()

        t = threading.Thread(target=watcher, daemon=True)
        t.start()
        assert done.wait(timeout=15), "变化流在成员被顶替后挂住了"
    finally:
        _stop(thief)
        try:
            me.leave()
        except Exception:
            pass
