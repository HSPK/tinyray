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
