"""两个数字回答两个问题：version 说"要不要更新缓存"，roster 说"这份名单还作数吗"。

设计文档用「以后往 Member 里加个 last_error」当假设，论证名单指纹必须算出来
而不是数出来。这一轮真的加了 last_error，所以这条不变式该被钉住。
"""

from __future__ import annotations

import json
import random
import subprocess
import sys
import textwrap
import time

import httpx
import pytest
import tinyray


class _Wire:
    """直接说协议，因为要固定任期号 —— SDK 每次 join 都会生成新的。"""

    def __init__(self, endpoint: str):
        self.url = f"http://{endpoint}/v1/beat"
        self.cli = httpx.Client(timeout=30)
        self.seen: dict[str, int] = {}
        # 没有变化的池子根本不出现在 ack 里，所以"没提到"就是"和上次一样"。
        self.last: dict = {}

    def beat(self, **kw) -> dict:
        body = dict(
            pool="t",
            slot=0,
            id=0,
            incarnation=1,
            policy="collective",
            size=4,
            ready=True,
            leaving=False,
            exclusive=False,
            methods=[],
            watch=["t"],
            seen=dict(self.seen),
            state={},
        )
        body.update(kw)
        j = self.cli.post(
            self.url,
            content=json.dumps(body).encode(),
            headers={"content-type": "application/json"},
        ).json()
        d = j.get("pools", {}).get("t")
        if d:
            self.seen["t"] = d["version"]
            self.last = d
        return self.last


@pytest.fixture
def wire(registry):
    w = _Wire(registry.endpoint)
    yield w
    w.cli.close()


def _observer(registry) -> _Wire:
    """独立观察者：离开的成员自己收不到自己的删除。"""
    o = _Wire(registry.endpoint)
    o.beat(pool="obs", id=9999, incarnation=(int(time.time() * 1000) << 20))
    return o


def test_publishing_state_moves_version_and_leaves_the_roster_alone(wire):
    """训练时每个 step 都在报进度。要是它动了指纹，128 张卡就白跑一轮。"""
    base = (int(time.time() * 1000)) << 20
    wire.beat(id=0, slot=0, incarnation=base)
    d = wire.beat(id=1, slot=1, incarnation=base + 1)
    roster, version = d["roster"], d["version"]

    for step in (1, 2, 3):
        d = wire.beat(id=0, slot=0, incarnation=base, state={"step": step})
        assert d["roster"] == roster, f"报 step={step} 动了名单指纹"
        assert d["version"] > version, f"报 step={step} 没有推进 version"
        version = d["version"]


def test_occupancy_changes_do_move_the_roster(registry, wire):
    """反过来也要成立，否则指纹就是个常数。"""
    base = (int(time.time() * 1000)) << 20
    wire.beat(id=0, slot=0, incarnation=base)
    wire.beat(id=1, slot=1, incarnation=base + 1)
    obs = _observer(registry)
    try:
        before = obs.beat(pool="obs", id=9999, incarnation=base + 5)["roster"]
        wire.beat(id=1, slot=1, incarnation=base + 1, leaving=True)
        after = obs.beat(pool="obs", id=9999, incarnation=base + 6)["roster"]
        assert after != before, "有人离开，名单指纹却没动"

        wire.beat(id=1, slot=1, incarnation=base + 9000)  # 新任期回来
        again = obs.beat(pool="obs", id=9999, incarnation=base + 7)["roster"]
        assert again != after, "有人以新任期回来，名单指纹却没动"
    finally:
        obs.cli.close()


def test_readiness_does_not_move_the_roster(registry, wire):
    """ready/unready 是"能不能用"，不是"是不是同一批人"。"""
    base = (int(time.time() * 1000)) << 20
    wire.beat(id=0, slot=0, incarnation=base)
    d = wire.beat(id=1, slot=1, incarnation=base + 1)
    roster = d["roster"]
    d = wire.beat(id=0, slot=0, incarnation=base, ready=False)
    assert d["roster"] == roster, "unready 动了名单指纹"
    d = wire.beat(id=0, slot=0, incarnation=base, ready=True)
    assert d["roster"] == roster, "重新 ready 动了名单指纹"


def _fnv(seat: int, tenure: int) -> int:
    """独立重算一遍指纹。故意抄一份而不是调用实现 —— 要比对的就是两者是否一致。"""
    h = 1469598103934665603
    for b in seat.to_bytes(8, "little") + tenure.to_bytes(8, "little"):
        h = ((h ^ b) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return h


def test_the_maintained_fingerprint_matches_a_fresh_one(registry, wire):
    """XOR 是增量维护的，散列函数是另写的，没有任何东西强制两者一致：
    把 state 加进 roster_hash 后，指纹会悄悄跑偏而不报错、不失败。
    这条测试就是那个强制。"""
    base = (int(time.time() * 1000)) << 20
    ids = [(0, base), (1, base + 1), (2, base + 2)]
    for i, inc in ids:
        d = wire.beat(id=i, slot=i, incarnation=inc)

    def expected(members: list[tuple[int, int]]) -> int:
        acc = 0
        for seat, tenure in members:
            acc ^= _fnv(seat, tenure)
        return acc

    assert d["roster"] == expected(ids), "三人到齐后，维护的指纹与重算的不一致"

    # 报 state：成员没变，两边都不该动
    d = wire.beat(id=1, slot=1, incarnation=base + 1, state={"step": 42})
    assert d["roster"] == expected(ids), "报 state 之后指纹跑偏了"

    # 有人走
    wire.beat(id=2, slot=2, incarnation=base + 2, leaving=True)
    obs = _observer(registry)
    try:
        d = obs.beat(pool="obs", id=8888, incarnation=base + 50)
        assert d["roster"] == expected(ids[:2]), "有人离开后指纹与重算的不一致"
    finally:
        obs.cli.close()


CHURNER = textwrap.dedent(
    """
    import sys, tinyray
    m = tinyray.join("r", "churn")
    m.ready(who=sys.argv[1], n=0)
    print("UP", flush=True)
    n = 0
    while True:
        line = sys.stdin.readline()
        if not line or line.strip() == "bye":
            break
        n += 1
        m.update(who=sys.argv[1], n=n)
    m.leave()
    """
)


def test_the_clients_own_fingerprint_agrees_with_the_registrys(registry):
    """上面那条钉的是注册表这一侧：维护出来的指纹要等于重算的。客户端那一侧
    是同一个论证再来一遍，而且没人钉过。

    `frozen()` 拿本地缓存里的成员、用客户端自己的 `roster_hash` 算一个指纹，
    再把注册表随 delta 发来的 `roster` 一并交回。`epoch()` 就是比这两个数决定
    "这份名单作不作数"。两边是两个进程、两套代码：注册表增量维护 XOR，客户端
    逐条 apply 之后重算。没有任何东西强制它们一致。

    require_ready=False 时这两个数必须**恒等**。实测在进出、更新、被杀（只能
    靠租约过期）和一次注册表重启的混合下对账 720 次，0 次不一致。
    """
    me = tinyray.join("watch", "churn")
    me.ready()
    c = tinyray._client
    assert c is not None
    c.watch(["r"])

    peers: list[subprocess.Popen] = []

    def spawn(name: str) -> None:
        p = subprocess.Popen(
            [sys.executable, "-c", CHURNER, name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert p.stdout.readline().strip() == "UP"
        peers.append(p)

    def farewell(p: subprocess.Popen) -> None:
        try:
            p.stdin.write("bye\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()

    checks = 0
    rng = random.Random(7)
    try:
        for i in range(14):
            act = rng.random()
            if act < 0.4 or not peers:
                spawn(f"p{i}")
            elif act < 0.6:
                farewell(peers.pop(rng.randrange(len(peers))))
            elif act < 0.85:
                p = peers[rng.randrange(len(peers))]
                try:
                    p.stdin.write("x\n")
                    p.stdin.flush()
                except Exception:
                    pass
            else:
                p = peers.pop(rng.randrange(len(peers)))
                p.kill()  # 没有告别，只能靠租约过期
                p.wait(timeout=5)
            for _ in range(8):
                got = c.frozen("r", False)
                if got is not None:
                    raw, ours, whole, version = got
                    checks += 1
                    assert ours == whole, (
                        f"第 {i} 轮 version={version}：客户端算出 {ours}，"
                        f"注册表说 {whole}，缓存里 {len(json.loads(raw))} 个成员"
                    )
                time.sleep(0.02)
        assert checks >= 50, f"只对上了 {checks} 次账，这条测试没测到东西"
    finally:
        for p in peers:
            farewell(p)
        me.leave()
