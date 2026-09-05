"""任期号来自挂钟，所以时钟走错就是这套机制的失效模式。"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import time

import httpx
import pytest
import tinyray


# 直接构造任期号：毫秒左移 20 位，低位是打平用的随机数
def INC(ms: int) -> int:
    return (ms << 20) | 1


def _beat(registry, **kw):
    body = dict(
        pool="p",
        slot=0,
        id=0,
        incarnation=0,
        policy="stateful",
        size=1,
        ready=True,
        leaving=False,
        exclusive=False,
        methods=[],
        watch=["p"],
        seen={},
        state={},
    )
    body.update(kw)
    r = httpx.post(
        f"http://{registry.endpoint}/v1/beat",
        content=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        timeout=30,
    )
    return r.json()["accepted"]


def test_a_clock_that_goes_backwards_cannot_take_a_seat_back(registry):
    """高水位就是为这个存在的：倒退的任期不能顶掉在任的。"""
    now = int(time.time() * 1000)
    assert _beat(registry, incarnation=INC(now)) is True
    assert _beat(registry, incarnation=INC(now + 1000)) is True
    for back in (1000, 3600_000, 365 * 86400_000):
        assert _beat(registry, incarnation=INC(now - back)) is False, f"倒退 {back}ms 被接受了"
    # 时钟修好就能继续
    assert _beat(registry, incarnation=INC(now + 2000)) is True


FUTURE_HOLDER = textwrap.dedent(
    """
    import sys, tinyray
    real = tinyray.time.time_ns
    tinyray.time.time_ns = lambda: real() + 3600 * 10**9
    m = tinyray.join("p", "stateful", slot=0, size=1)
    tinyray.time.time_ns = real
    m.ready()
    print("READY", flush=True)
    sys.stdin.readline()
    """
)


def test_a_refused_seat_is_raised_not_returned(registry):
    """修前 join() 返回成功而 accepted=False：心跳停在 2 拍不再前进，
    last_error 是空的（它不是失败，是被拒绝），自己的池子 0 个成员。"""
    p = subprocess.Popen(
        [sys.executable, "-c", FUTURE_HOLDER],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert p.stdout.readline().strip() == "READY"
        with pytest.raises(tinyray.SeatTaken, match="clock that went backwards"):
            tinyray.join("p", "stateful", slot=0, size=1)
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()


RANK = textwrap.dedent(
    """
    import sys, tinyray
    m = tinyray.join("t", "collective", slot=0, size=1)
    m.ready()
    print("READY", flush=True)
    sys.stdin.readline()
    """
)


def test_a_restarting_rank_still_reclaims_its_seat(registry):
    """默认 last-writer-wins 就是为这条路存在的，别为了报错把它堵死。"""
    p1 = subprocess.Popen(
        [sys.executable, "-c", RANK], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert p1.stdout.readline().strip() == "READY"
    p1.kill()  # 不说再见，租约还挂着
    p1.wait()
    t0 = time.monotonic()
    p2 = subprocess.Popen(
        [sys.executable, "-c", RANK], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        assert p2.stdout.readline().strip() == "READY"
        assert time.monotonic() - t0 < 5, "抢回座位不该等到租约过期"
    finally:
        try:
            p2.stdin.write("\n")
            p2.stdin.flush()
            p2.wait(timeout=5)
        except Exception:
            p2.kill()


def test_exclusive_still_means_first_come(registry):
    p = subprocess.Popen(
        [sys.executable, "-c", RANK], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    try:
        assert p.stdout.readline().strip() == "READY"
        with pytest.raises(tinyray.SeatTaken, match="already held"):
            tinyray.join("t", "collective", slot=0, size=1, exclusive=True)
    finally:
        try:
            p.stdin.write("\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()
