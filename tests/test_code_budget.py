"""预算是刹车，但只有会响的刹车才算刹车。

上一版预算是 1,280 行，实际长到 1,641 都没人发现 —— 因为没有任何东西在量它。
"""

from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
BUDGET = 2000


def _pure_lines(path: pathlib.Path) -> int:
    """去掉注释和空行 —— 注释是资产，不该算进刹车里。"""
    n = 0
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith(("//", "#")):
            n += 1
    return n


def _product_files() -> list[pathlib.Path]:
    files = [p for p in ROOT.glob("crates/*/src/**/*.rs") if "loadgen" not in p.name]
    files += [p for p in ROOT.glob("python/tinyray/*.py")]
    return sorted(files)


def test_the_budget_in_the_plan_matches_the_one_enforced_here():
    """两个数字对不上，等于没有预算。"""
    plan = (ROOT / "docs" / "03-plan.md").read_text()
    stated = re.search(r"\*\*合计\*\* \| \| \*\*([\d,]+) 行\*\*", plan)
    assert stated, "计划里的预算表格式变了，这条测试跟不上了"
    assert int(stated.group(1).replace(",", "")) == BUDGET


def test_product_code_stays_inside_its_budget():
    """超了不一定是写多了，但一定是该停下来问为什么的信号。"""
    files = _product_files()
    assert len(files) >= 8, f"只找到 {len(files)} 个产品文件，收集方式坏了"
    total = sum(_pure_lines(p) for p in files)
    biggest = sorted(files, key=_pure_lines, reverse=True)[:3]
    detail = ", ".join(f"{p.name}={_pure_lines(p)}" for p in biggest)
    assert total <= BUDGET, f"产品代码 {total} 行，超出 {BUDGET}。最大的三个：{detail}"
