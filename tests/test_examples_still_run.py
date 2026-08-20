"""示例是文档表面的一部分，但它们不在默认测试里，只有人想起来才会跑。

今晚改了六处行为（join 遇拒绝抛错、池子形状要一致、state 有预算、TTL 有下限、
首次查询等回音、方法服务端答复超预算），全都是能让示例静默失效的改动。

用 `-m examples` 跑。默认排除，因为它要几分钟。
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXAMPLES = sorted(
    p
    for p in list(ROOT.glob("examples/*.py")) + list(ROOT.glob("examples/*/*.py"))
    if not p.name.startswith("_")
)


def test_the_probe_found_some_examples():
    assert len(EXAMPLES) >= 20, f"只找到 {len(EXAMPLES)} 个示例，收集方式坏了"


@pytest.mark.examples
@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
def test_example_runs_clean(path: pathlib.Path):
    out = subprocess.run(
        [sys.executable, str(path)], cwd=ROOT, capture_output=True, text=True, timeout=300
    )
    assert out.returncode == 0, f"{path.name} 退出码 {out.returncode}\n{out.stdout[-800:]}\n{out.stderr[-800:]}"
