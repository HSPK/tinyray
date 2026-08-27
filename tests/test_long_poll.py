"""变化要在发生时到达，而不是在下一拍到达。

以前注册中心是纯应答式的：有没有变化都立刻回答，然后闭嘴。信息在变化那一刻
就已经在它手里了，但没有通道送出去，所以订阅方只能等自己下一拍再来问 ——
发现延迟因此结构性地等于一个心跳间隔，也就是 ttl/4。

现在没话说的时候它把应答挂起，池子一动就立刻回。实测（ttl=20s）：
发现延迟从 5,000ms 上界变成 21ms 均值，而心跳数量一次都没多。

这里钉三条，缺一条这个改动就是负收益：
  - 变化真的能穿过挂起送达
  - 挂起没有让请求变多
  - 挂起没有拖慢自己发布（那条路以前是即时的）
"""

from __future__ import annotations

import concurrent.futures
import json
import subprocess
import sys
import textwrap
import threading
import time
import urllib.request

import tinyray

PUBLISHER = textwrap.dedent(
    """
    import json, sys, time, tinyray
    with tinyray.join("lp", "churn") as me:
        me.ready(v=0)
        print("READY", flush=True)
        sys.stdin.readline()
        for i in range(1, 4):
            me.ready(v=i, at=time.time())
            print(f"SENT {i}", flush=True)
            time.sleep(1.0)
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


def _server_version(endpoint: str, pool: str) -> int:
    with urllib.request.urlopen(f"http://{endpoint}/v1/pools", timeout=5) as r:
        got = json.loads(r.read()).get(pool)
    return 0 if got is None else got["version"]


def test_a_change_arrives_long_before_the_next_beat_would_have(registry):
    """这条是整个改动的理由。

    registry fixture 的 ttl 是 2000ms，所以心跳间隔 500ms —— 改动之前，发现
    延迟的上界就是这 500ms，均值约 250ms。挂起之后实测均值 21ms。

    断言放在 200ms：明显低于旧的上界（否则证明不了什么），又给足了 CI 上的抖动
    余量。ttl=20s 的默认配置下差距会更大 —— 5,000ms 对 21ms。
    """
    me = tinyray.join("obs", "churn")
    me.ready()
    pool = tinyray.pool("lp")
    start = pool.snapshot()

    seen: list[float] = []
    got_three = threading.Event()

    def watch() -> None:
        for snap in pool.changes(since=start.revision, timeout=25):
            now = time.time()
            for h in snap:
                sent = h.state.get("at")
                if sent is not None:
                    seen.append((now - sent) * 1000)
                if h.state.get("v") == 3:
                    got_three.set()
                    return

    t = threading.Thread(target=watch, daemon=True)
    t.start()
    pub = subprocess.Popen(
        [sys.executable, "-c", PUBLISHER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert pub.stdout.readline().strip() == "READY"
        pub.stdin.write("\n")
        pub.stdin.flush()
        assert got_three.wait(timeout=25), f"没收齐，只看到 {len(seen)} 次"
        worst = max(seen)
        assert worst < 200, (
            f"最慢一次 {worst:.0f}ms —— 心跳间隔是 500ms，这个数字说明变化还是在"
            f"等下一拍，而不是在发生时就送达"
        )
    finally:
        _stop(pub)
        t.join(timeout=5)
        me.leave()


def test_holding_does_not_cost_extra_beats(registry):
    """挂起不该把请求变多 —— 它的全部意义就是同样的请求量换来即时性。

    客户端请求挂起的时长，正好是它本来要睡的那个间隔，所以速率应当不变：
    ttl=2000 -> 间隔 500ms -> 约 2 次/秒。
    """
    me = tinyray.join("obs", "churn")
    me.ready()
    tinyray.pool("quiet-pool")  # 订阅一个永不变化的池子，让它去挂

    before = me.stats()["beats_ok"]
    t0 = time.monotonic()
    time.sleep(6.0)
    rate = (me.stats()["beats_ok"] - before) / (time.monotonic() - t0)

    # 间隔 500ms -> 2/s。给足余量，但要能抓住"挂起失效退化成自旋"。
    assert rate < 6, f"每秒 {rate:.1f} 拍 —— 挂起没生效，退化成轮询了"
    assert rate > 0.5, f"每秒 {rate:.1f} 拍 —— 心跳停了，租约会过期"
    me.leave()


def test_publishing_still_leaves_at_once_while_a_request_is_parked(registry):
    """这条最容易被牺牲掉，而且不测就发现不了。

    心跳循环现在的休息姿势是挂在一个请求里。如果发布只是设个本地变量等下一拍，
    那么 ready() 的延迟就会从即时退化到最多一个 hold —— 用一条即时通道换来了
    另一条，净收益为零。

    所以发布必须能取消在途的挂起请求。心跳是幂等的，丢掉一个在途请求无害。
    实测（ttl=20s、hold=5s）：0.6ms；不取消的话这里会是 5,000ms 量级。
    """
    me = tinyray.join("pub", "churn")
    me.ready(v=0)
    try:
        worst = 0.0
        for i in range(1, 4):
            # 先等一会，确保心跳循环确实已经挂在请求里而不是正好在收发之间
            time.sleep(1.2)
            base = _server_version(registry.endpoint, "pub")
            t0 = time.monotonic()
            me.ready(v=i)
            while time.monotonic() - t0 < 10:
                if _server_version(registry.endpoint, "pub") > base:
                    break
            worst = max(worst, (time.monotonic() - t0) * 1000)

        # 心跳间隔 500ms。发布若在等挂起结束，这里会是几百毫秒。
        assert worst < 150, (
            f"最慢一次发布 {worst:.0f}ms —— 发布在等挂起的请求结束，即时发布这条路被挂起吃掉了"
        )
    finally:
        me.leave()


def test_a_client_that_asks_for_no_hold_is_answered_at_once(registry):
    """join() 和 leave() 是一次性的、有人在等，绝不能被挂起。

    也是旧客户端的兼容路径：不带 hold_ms 的请求会被当成 0，行为和以前完全一样。
    """
    body = {
        "pool": "probe",
        "id": 1,
        "incarnation": 1,
        "policy": "churn",
        "ready": True,
        "leaving": False,
        "exclusive": False,
        "methods": [],
        "watch": ["never-changes"],
        "seen": {},
        "state": {},
    }
    req = urllib.request.Request(
        f"http://{registry.endpoint}/v1/beat",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
    )
    t0 = time.monotonic()
    with urllib.request.urlopen(req, timeout=10) as r:
        assert r.status == 200
    took = (time.monotonic() - t0) * 1000
    assert took < 300, f"不要求挂起的请求被挂了 {took:.0f}ms"


def test_publishing_flat_out_does_not_starve_the_heartbeat(registry):
    """发布会取消挂起的请求，但取消不能没有下界。

    挂起的请求是心跳循环的休息位置，发布把它取消掉才能立刻送出去。取消之后
    过去是直接 continue —— 绕过了每完成一拍才走的那段间隔。于是下一次发布又
    在回答到达之前把它取消掉，如此往复，一拍都完不成。

    实测：每秒 379,000 次发布之下，注册表在第 4 秒把这个还在不停说话的成员
    从它自己的池子里删掉了 —— beats_ok 全程停在 0。
    """

    def registry_says() -> int:
        url = f"http://{registry.endpoint}/v1/pools"
        with urllib.request.urlopen(url, timeout=2) as r:
            return json.loads(r.read()).get("p", {}).get("members", 0)

    with tinyray.join("p", slot=0, size=1) as m:
        m.ready()
        tinyray.pool("p")
        time.sleep(registry.ttl_ms / 1000)
        before = m.stats()["beats_ok"]
        stop = threading.Event()

        def spin() -> None:
            i = 0
            while not stop.is_set():
                m.update(i=i)  # 不加任何节流
                i += 1

        t = threading.Thread(target=spin, daemon=True)
        t.start()
        try:
            worst = 99
            for _ in range(8):
                time.sleep(0.5)
                worst = min(worst, registry_says())
        finally:
            stop.set()
            t.join(timeout=10)

        assert worst >= 1, "成员一边发布一边被注册表清出了自己的池子"
        assert m.stats()["beats_ok"] > before, "全程一拍都没完成"


def test_a_superseded_member_finds_out_within_a_round_trip(long_lease):
    """被顶替的成员必须**立刻**知道，而不是等下一拍。

    它是围栏的两道关之一：调用方带的 identity 只挡得住"地址被后一任复用"，
    两代跑在不同端口时就只剩这一道 —— 旧进程自己知道自己是幽灵。

    这条测试是补的。心跳循环里 `hold == 0` 一度身兼两义：既表示"注册中心老得
    不会挂起"，也表示"取消之后顶上来的那个不挂起"。完成一拍后的分支按前者理解，
    于是去睡满一个 interval，而且不再挂起 —— 注册中心叫不醒它。被顶替的成员因此
    要 940ms 才知道，而挂起时只要 1-2ms。是 examples 里的 06_fencing 在双核上
    偶然撞出来的，这里把它钉死。

    那段错误的睡眠只在**刚发生过一次取消**之后才开始，所以这条测试必须自己把
    那个窗口打开：让对方明确发布一次，说一声，然后立刻抢座。指望 join 时的
    `ready()` 碰巧制造出取消是不行的 —— 三次里会漏一次。
    """
    peer = textwrap.dedent(
        f"""
        import os, sys, time, tinyray
        os.environ["TINYRAY_REGISTRY"] = "{long_lease.endpoint}"
        m = tinyray.join("seat", "stateful", slot=0)
        m.ready()
        m.flush()
        print("UP", flush=True)
        sys.stdin.readline()
        # 一次发布取消掉挂起的请求，取消之后那一个是不挂起的 —— 这正是那段
        # 错误睡眠的入口。发完就闭嘴，否则后续发布会把它从睡眠里叫醒。
        before = m.stats()["beats_ok"]
        m.update(nudge=1)
        # 等那个不挂起的替补请求真的回来，窗口才算打开。原先这里是对面 sleep
        # 固定 0.3s，机器一忙就不够 —— 顶替落在窗口打开之前，幽灵当场就知道，
        # 于是把 bug 放回去测试照样通过（在变异脚本里量到过 0.70s 的"通过"）。
        while m.stats()["beats_ok"] <= before:
            time.sleep(0.002)
        print("PUBLISHED", flush=True)
        while m.accepted:
            time.sleep(0.002)
        print("FENCED %.6f" % time.time(), flush=True)
        """
    )
    ghost = subprocess.Popen(
        [sys.executable, "-c", peer],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert ghost.stdout.readline().strip() == "UP", ghost.stderr.read()
        ghost.stdin.write("\n")
        ghost.stdin.flush()
        # 对面自己确认了替补请求已经回来，所以窗口一定是开着的，不必猜时间。
        assert ghost.stdout.readline().strip() == "PUBLISHED", ghost.stderr.read()

        took = time.time()
        with tinyray.join("seat", "stateful", slot=0) as taker:
            taker.ready()
            line = ghost.stdout.readline().strip()
            assert line.startswith("FENCED"), f"旧任期一直没发现自己被顶替: {line!r}"
            noticed = (float(line.split()[1]) - took) * 1000
            ghost.wait(timeout=10)

    finally:
        if ghost.poll() is None:
            ghost.kill()

    # 一个往返是毫秒级；一拍是 ttl/4 = 5s。中间差三个数量级，取十分之一拍做界。
    budget = long_lease.ttl_ms / 4 / 10
    assert noticed < budget, (
        f"被顶替之后过了 {noticed:.0f}ms 才发现，超过 {budget:.0f}ms —— "
        f"像是在等下一拍而不是等注册中心叫醒"
    )


def test_publishing_never_makes_the_loop_fall_back_to_a_timer(long_lease):
    """注册中心答得上话时，心跳循环永远等在它身上，不等定时器。

    这条是上面那条围栏延迟测试的确定版。那条量的是后果 —— 被顶替之后多久发现 ——
    而后果要撞上一个内部瞬态才看得见，五次里只能抓到两次。这条量的是机制本身：
    循环有没有走到"睡满一个 interval"那条分支。

    那条分支只有在**一次 ack 都还没拿到**时才该走：没人应答就没有东西可挂，
    而猛敲一个死掉的注册中心比等着更糟。一旦有过一次成功心跳，就永远该挂起。

    实测二十次发布：正常 0 次，把判断从"循环的意图"改回"上一个请求要了什么"
    是 12 次。
    """
    with tinyray.join("p", "churn") as me:
        me.ready(a=1)
        tinyray.pool("p")
        me.flush()
        assert me.stats()["short_polls"] == 0, "还没发布就已经在轮询了"
        for i in range(20):
            me.update(step=i)
            time.sleep(0.1)
        time.sleep(1.0)
        got = me.stats()["short_polls"]
        assert got == 0, f"发布了二十次之后，循环有 {got} 次是等在定时器上而不是等在注册中心上"


def _held_beat_as(registry, who: int, hold_ms: int, seen: dict, timeout: float):
    """同 `_held_beat`，但由调用方指定身份 —— 抖动是按 id 取模的。"""
    return _held_beat(registry, hold_ms, seen, timeout, who=who, watch="quiet")


def _held_beat(
    registry, hold_ms: int, seen: dict, timeout: float, who: int = 5150, watch: str = "cap"
) -> tuple[float, dict]:
    body = json.dumps(
        {
            "pool": "cap",
            "id": who,
            "incarnation": 9,
            "policy": "churn",
            "ttl_ms": registry.ttl_ms,
            "ready": True,
            "state": {},
            "methods": [],
            "watch": [watch],
            "seen": seen,
            "hold_ms": hold_ms,
            "leaving": False,
            "exclusive": False,
        }
    ).encode()
    req = urllib.request.Request(
        f"http://{registry.endpoint}/v1/beat",
        data=body,
        headers={"content-type": "application/json"},
    )
    started = time.monotonic()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return time.monotonic() - started, json.load(r)


def test_a_beat_is_never_parked_longer_than_half_a_lease(registry):
    """一个成员的租约是被它自己的心跳**送达**续上的。

    所以挂起它的时间不能超过半个租约 —— 挂久了就是拿它的座位去换一次长轮询。
    我们自己的客户端只会要 ttl/4，永远够不到这个上限，所以这条只有手写心跳能试，
    而它防的正是别的客户端：协议是公开的，`hold_ms` 是调用方填的。

    实测租约 2000ms 时：要求挂 500ms 得到 557ms，要求挂 60000ms 得到 1057ms ——
    封在 1000ms 加上不超过八分之一预算的抖动。没有这个上限，那次请求会挂满一分
    钟，而它的租约两秒就过期了。
    """
    _, first = _held_beat(registry, 0, {}, timeout=10)
    at = first["pools"]["cap"]["version"]
    half = registry.ttl_ms / 2

    took, _ = _held_beat(registry, 60_000, {"cap": at}, timeout=half / 1000 + 4)
    assert took < half / 1000 * 1.3, (
        f"要求挂 60s、租约只有 {registry.ttl_ms}ms，却被挂了 {took:.2f}s —— "
        f"上限应该是半个租约（{half:.0f}ms）加一点抖动"
    )

    # 上限不该把一个短请求撑长。
    short, _ = _held_beat(registry, 300, {"cap": at}, timeout=10)
    assert short < half / 1000, f"只要求挂 300ms，却被挂了 {short:.2f}s"


def test_parked_watchers_do_not_all_come_back_at_once(registry):
    """一池子观察者不该在同一毫秒醒来。

    有变更的时候，铃一响大家一起走，那没问题 —— 那一下是真有事要说。要打散的是
    **没等到东西**的那一批：预算到点了，所有人同时超时、同时重新订阅，就是一个
    自己造出来的尖峰。注释里量过：4 万个挂在同一个池子上时，一次变更花掉 1.75
    核·秒，而挤在一起正是它从"肩"变成"峰"的原因。

    抖动按调用方的 id 取模，所以对它自己是稳定的（重试不会越推越晚），对整体是
    散开的，上限是预算的八分之一。

    实测租约 4s（预算 2000ms、抖动上限 250ms）：40 个观察者在 2003ms 到 2255ms
    之间回来，跨度 251ms。抖动去掉就全挤在 2000ms 上。
    """
    budget = registry.ttl_ms / 2
    spread_cap = budget / 8

    def parked(i: int) -> float:
        # id 之间隔开，好让取模的结果铺开而不是撞在一起。
        who = 7000 + i * 6
        first = _held_beat_as(registry, who, 0, {}, timeout=10)[1]
        at = first["pools"].get("quiet", {}).get("version", 0)
        return _held_beat_as(registry, who, registry.ttl_ms, {"quiet": at}, timeout=30)[0]

    with concurrent.futures.ThreadPoolExecutor(20) as pool:
        took = sorted(pool.map(parked, range(20)))

    spread = (took[-1] - took[0]) * 1000
    assert spread > spread_cap / 3, (
        f"20 个观察者在 {spread:.0f}ms 内全回来了 —— 抖动上限是 {spread_cap:.0f}ms，"
        f"挤成这样说明根本没打散"
    )
    assert took[-1] * 1000 < budget + spread_cap * 1.2, (
        f"最晚的一个等了 {took[-1] * 1000:.0f}ms，超过了预算 {budget:.0f}ms 加抖动上限"
    )
