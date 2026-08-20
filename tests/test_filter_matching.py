"""过滤是标签匹配。pick/all/wait 都接 **filt，规则得说得清。"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap

import pytest
import tinyray

PEER = textwrap.dedent(
    """
    import json, sys, tinyray
    with tinyray.join("w", "churn") as m:
        m.ready(**json.loads(sys.argv[1]))
        print("READY", flush=True)
        sys.stdin.readline()
    """
)

STATES = [
    {"gpu": "a100", "step": 1, "free": True, "tags": ["fast"], "nested": {"zone": "a"}},
    {"gpu": "A100", "step": 2, "free": False, "tags": ["slow"], "nested": {"zone": "b"}},
    {"gpu": "h100", "step": 1, "free": True, "tags": ["fast"], "nested": {"zone": "a"}},
    {"shard": 3, "rank": 2.0},
]


@pytest.fixture
def fleet(registry):
    procs = []
    for st in STATES:
        p = subprocess.Popen(
            [sys.executable, "-c", PEER, json.dumps(st)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert p.stdout.readline().strip() == "READY"
        procs.append(p)
    me = tinyray.join("c", "churn")
    me.ready()
    tinyray.pool("w").wait(count=len(STATES), timeout=20)
    try:
        yield tinyray.pool("w")
    finally:
        me.leave()
        for p in procs:
            try:
                p.stdin.write("\n")
                p.stdin.flush()
                p.wait(timeout=5)
            except Exception:
                p.kill()


@pytest.mark.parametrize(
    "filt,expected",
    [
        ({"shard": 3}, 1),
        ({"shard": 3.0}, 1),
        ({"shard": 6 / 2}, 1),
        ({"rank": 2.0}, 1),
        ({"rank": 2}, 1),
        ({"step": 1}, 2),
        ({"step": 1.0}, 2),
    ],
)
def test_numbers_match_by_value_across_the_wire(fleet, filt, expected):
    """JSON 把 3 和 3.0 当两回事，Python 不。shard=6/2 是算分片下标最自然的
    写法，修前它匹配 0 个人，而 shard=3 匹配 1 个 —— 静默的空结果。"""
    assert len(fleet.all(**filt)) == expected, filt


@pytest.mark.parametrize(
    "filt,expected",
    [
        ({"free": True}, 2),
        ({"free": 1}, 0),
        ({"step": "1"}, 0),
        ({"gpu": "a100"}, 1),
        ({"gpu": "A100"}, 1),
        ({"gpu": None}, 0),
        ({"nosuchkey": "x"}, 0),
        ({"gpu": "a100", "step": 1}, 1),
        ({"gpu": "a100", "step": 2}, 0),
        ({"tags": ["fast"]}, 2),
        ({"nested": {"zone": "a"}}, 2),
        ({}, 4),
    ],
)
def test_everything_else_stays_exact(fleet, filt, expected):
    """数字放宽了，别的一概没有。布尔尤其：True == 1 是 Python 自己的怪癖，
    让 free=1 匹配上 free=true 带来的意外比帮助多。"""
    assert len(fleet.all(**filt)) == expected, filt
