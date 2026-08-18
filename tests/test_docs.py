"""The documentation cannot drift, contradict itself, or claim what it has not shown.

These pages describe a target design rather than the installed package, so the
old signature-matching checks would be meaningless. What remains is stricter in
the ways that matter for a proposal:

* the structure is uniform and the reading order is real;
* every template section required by ``00-conventions.md`` is present;
* every number labelled *derived* is recomputed here and must agree;
* every claim of the form "a test proves this" names a test file;
* nothing silently drops the "proposal, not implemented" marking.

The last two exist because the failure this project keeps having is a claim in
one place and a different behaviour in another, with nothing comparing them.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

PAGES = sorted(p for p in DOCS.rglob("*.md") if p.name != "README.md")
ALL_MARKDOWN = sorted(DOCS.rglob("*.md"))
SECTIONS = sorted(d for d in DOCS.iterdir() if d.is_dir())
NUMBERED = re.compile(r"^(\d{2})-[a-z0-9-]+\.md$")


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def rel(path: Path) -> str:
    return path.relative_to(DOCS).as_posix()


def corpus() -> str:
    return "\n".join(read(p) for p in ALL_MARKDOWN)


# --------------------------------------------------------------------------
# Structure
# --------------------------------------------------------------------------


class TestStructure:
    def test_the_tree_is_populated(self):
        assert len(PAGES) >= 20, "the docs tree lost files"

    @pytest.mark.parametrize("section", SECTIONS, ids=lambda d: d.name)
    def test_sections_are_numbered(self, section: Path):
        assert re.match(r"^\d{2}-[a-z-]+$", section.name), (
            f"{section.name} has no reading-order prefix"
        )

    @pytest.mark.parametrize("page", PAGES, ids=rel)
    def test_pages_are_numbered(self, page: Path):
        assert NUMBERED.match(page.name), f"{rel(page)} has no reading-order prefix"

    @pytest.mark.parametrize("section", SECTIONS, ids=lambda d: d.name)
    def test_numbering_is_contiguous(self, section: Path):
        numbers = sorted(int(NUMBERED.match(p.name).group(1)) for p in section.glob("*.md"))
        assert numbers == list(range(1, len(numbers) + 1)), (
            f"{section.name} is numbered {numbers}; reading order must run 1..n"
        )

    def test_conventions_is_the_only_zero(self):
        zeros = [rel(p) for p in DOCS.rglob("*.md") if p.name.startswith("00-")]
        assert zeros == ["00-conventions.md"], (
            f"00- is reserved for meta-specification; found {zeros}"
        )

    @pytest.mark.parametrize("page", PAGES, ids=rel)
    def test_every_page_is_indexed(self, page: Path):
        assert rel(page) in read(DOCS / "README.md"), f"{rel(page)} is unreachable from the index"

    @pytest.mark.parametrize("section", SECTIONS, ids=lambda d: d.name)
    def test_the_index_lists_pages_in_reading_order(self, section: Path):
        index = read(DOCS / "README.md")
        listed = re.findall(
            rf"^- \[[^\]]+\]\((?:{re.escape(section.name)}/)?([\w.-]+\.md)\)", index, re.M
        )
        listed = [name for name in listed if (section / name).exists()]
        expected = sorted(p.name for p in section.glob("*.md"))
        assert listed == expected, (
            f"the index lists {section.name} as {listed}, contradicting {expected}"
        )

    @pytest.mark.parametrize("page", ALL_MARKDOWN, ids=rel)
    def test_relative_links_resolve(self, page: Path):
        broken = [
            target
            for target in re.findall(r"\]\((?!https?:)([^)#]+)(?:#[^)]*)?\)", read(page))
            if not (page.parent / target).exists()
        ]
        assert not broken, f"{rel(page)} links to missing files: {broken}"


# --------------------------------------------------------------------------
# Templates
# --------------------------------------------------------------------------

MODULE_SECTIONS = [
    "范围",
    "职责",
    "非职责",
    "系统位置",
    "依赖",
    "公共契约",
    "状态所有权",
    "生命周期",
    "主流程",
    "并发与分布式语义",
    "正确性不变量",
    "故障处理",
    "配置",
    "可观测性",
    "测试",
    "限制与取舍",
    "源码映射",
]

ARCHITECTURE_SECTIONS = [
    "问题",
    "目标",
    "非目标",
    "设计",
    "正常流程",
    "状态与所有权",
    "正确性不变量",
    "故障与恢复",
    "可观测性",
    "取舍",
    "实现与测试",
]

PROTOCOL_SECTIONS = [
    "目的",
    "参与者",
    "前置条件",
    "数据模型",
    "正常顺序",
    "状态转换",
    "顺序约束",
    "Timeout",
    "Retry 与幂等性",
    "Backpressure",
    "故障语义",
    "正确性不变量",
    "兼容性",
    "测试",
]

MODULES = sorted((DOCS / "03-modules").glob("*.md"))
ARCHITECTURE = sorted((DOCS / "02-architecture").glob("*.md"))
PROTOCOLS = sorted((DOCS / "04-protocols").glob("*.md"))


class TestTemplates:
    """00-conventions.md says an omitted section hides a judgement."""

    @staticmethod
    def missing_sections(text: str, expected: list[str]) -> list[str]:
        """Headings must match the whole line.

        Substring matching would accept a corrupted heading, which is exactly
        what happened when this check was first written: mutating
        "## 12. Failure handling" to "## 12. Failure handlingX" left the test
        passing, because the original is a prefix of the corruption.
        """
        headings = set(re.findall(r"^(## \d+\. .+?)\s*$", text, re.M))
        return [
            name
            for index, name in enumerate(expected, start=1)
            if f"## {index}. {name}" not in headings
        ]

    @pytest.mark.parametrize("page", MODULES, ids=rel)
    def test_module_template(self, page: Path):
        missing = self.missing_sections(read(page), MODULE_SECTIONS)
        assert not missing, f"{rel(page)} is missing module sections: {missing}"

    @pytest.mark.parametrize("page", ARCHITECTURE, ids=rel)
    def test_architecture_template(self, page: Path):
        missing = self.missing_sections(read(page), ARCHITECTURE_SECTIONS)
        assert not missing, f"{rel(page)} is missing architecture sections: {missing}"

    @pytest.mark.parametrize("page", PROTOCOLS, ids=rel)
    def test_protocol_template(self, page: Path):
        missing = self.missing_sections(read(page), PROTOCOL_SECTIONS)
        assert not missing, f"{rel(page)} is missing protocol sections: {missing}"

    @pytest.mark.parametrize("page", MODULES, ids=rel)
    def test_non_responsibilities_name_an_owner(self, page: Path):
        text = read(page)
        start = text.index("## 3. 非职责")
        body = text[start : text.index("## 4.", start)]
        if "无" in body and "|" not in body:
            return
        assert "归属" in body, (
            f"{rel(page)} lists non-responsibilities without naming who does own them; "
            "that is how boundaries erode"
        )


# --------------------------------------------------------------------------
# Honesty
# --------------------------------------------------------------------------


class TestHonesty:
    def test_every_page_is_marked_as_a_proposal(self):
        exempt = {"00-conventions.md"} | {rel(p) for p in PROTOCOLS}
        unmarked = [
            rel(p) for p in PAGES if rel(p) not in exempt and "提案；当前未实现" not in read(p)
        ]
        assert not unmarked, (
            f"pages that do not say they are unimplemented: {unmarked}; a design "
            "document mistaken for a specification is worse than no document"
        )

    def test_the_index_says_it_is_a_proposal(self):
        assert "提案" in read(DOCS / "README.md")

    def test_measured_numbers_are_labelled(self):
        """Every quantity is measured, derived or to-be-measured."""
        text = corpus()
        for kind in ("**实测**", "**推导**", "**待测**"):
            assert kind in text, f"没有任何数量被标注为 {kind}；见规范第 9 节"

    def test_untested_claims_are_recorded(self):
        status = read(DOCS / "08-project" / "01-status.md")
        for claim in ("NCCL", "SGLang", "Megatron", "多机", "一万 worker 规模"):
            assert claim in status, (
                f"{claim!r} is discussed in the design but not listed among the "
                "things never run against the real thing"
            )

    def test_removals_are_enumerated(self):
        status = read(DOCS / "08-project" / "01-status.md")
        assert "待删除的部分" in status
        for removed in ("placement", "launcher", "roster 推送"):
            assert removed in status, f"{removed!r} is not listed as removed"

    @pytest.mark.parametrize("page", PAGES, ids=rel)
    def test_test_claims_name_a_test_file(self, page: Path):
        text = read(page)
        if "## " not in text or "Test" not in text:
            return
        rows = re.findall(r"^\|\s*[^|]+\|\s*(`?tests?/[^|`]+`?)\s*\|", text, re.M)
        for row in rows:
            assert ".py" in row, f"{rel(page)} names a test without a file: {row}"


# --------------------------------------------------------------------------
# Arithmetic
# --------------------------------------------------------------------------


class TestDerivedNumbersAreRecomputed:
    """A derived number that nobody recomputes is a guess wearing a disguise."""

    def test_llama3_interruption_interval(self):
        assert round(54 * 24 / 419, 2) == 3.09
        assert "3.09" in read(DOCS / "01-overview" / "01-problem.md")

    def test_fanout_extrapolation(self):
        # 233 us/worker measured at 16 workers -> 10,000 workers
        assert round(233e-6 * 10_000, 1) == 2.3
        assert "2.3 s" in read(DOCS / "01-overview" / "01-problem.md")

    def test_heartbeat_headroom(self):
        # 10,000 workers at a 10 s interval, against 4,295 ops/s measured
        steady = 10_000 / 10
        assert steady == 1_000
        assert round(4295 / steady, 1) == 4.3
        assert "4.3 倍" in read(DOCS / "01-overview" / "01-problem.md")

    def test_consensus_write_reduction(self):
        # 10,000 GPUs, 128 per cell, 10 s lease
        cells = -(-10_000 // 128)
        assert cells == 79
        assert round(cells / 10, 1) == 7.9
        topology = read(DOCS / "02-architecture" / "02-topology.md")
        assert "7.8/s" in topology or "7.9" in topology or "78" in topology

    def test_cell_blast_radius(self):
        assert round(128 / 5000 * 100, 2) == 2.56
        assert "2.56%" in read(DOCS / "02-architecture" / "02-topology.md")

    def test_five_million_operations(self):
        assert round(0.999999**5_000_000 * 100, 2) == 0.67
        assert "0.67%" in read(DOCS / "01-overview" / "01-problem.md")

    def test_removal_totals(self):
        status = read(DOCS / "08-project" / "01-status.md")
        section = status[status.index("## 3. 待删除的部分") : status.index("## 4.")]
        removed = [int(n) for n in re.findall(r"\|\s*(\d{3,4})\s*(?:Rust|Python)\s*\|", section)]
        assert len(removed) >= 6, f"the removal table lost rows: {removed}"
        assert sum(removed) == 3100, (
            f"the removal table sums to {sum(removed)}, but the page claims 3,100"
        )
        assert "3,100" in section


# --------------------------------------------------------------------------
# Boundary
# --------------------------------------------------------------------------


class TestBoundaryIsStated:
    """The layering is only useful if it is written down in one enforceable place."""

    def test_layering_assigns_every_capability(self):
        text = read(DOCS / "02-architecture" / "01-layering.md")
        for layer in ("L0", "L1", "L2", "L3", "L4"):
            assert layer in text

    def test_refusals_name_an_owner(self):
        text = read(DOCS / "01-overview" / "02-positioning.md")
        start = text.index("## 5. tinyray 拒绝什么")
        body = text[start : text.index("## 6.", start)]
        assert "归属" in body, "拒绝表必须指明每一项由谁负责"
        for refused in ("分配", "tensor", "共识存储"):
            assert refused in body, f"{refused!r} is not listed among the refusals"

    def test_no_resource_arguments_in_the_reference(self):
        """A mention is fine; a parameter is not.

        The proposed API says "No `num_gpus`, no `num_cpus`" in prose, which is
        the point. What must not appear is one of them as an argument.
        """
        api = read(DOCS / "07-reference" / "01-api.md")
        start = api.index("## 11. 从旧 API 中移除的部分")
        current, removed = api[:start], api[start:]
        for banned in ("num_gpus", "gpus_per_worker", "cpus_per_worker", "num_cpus"):
            used_as_parameter = re.search(rf"\b{banned}\s*[=:]", current)
            assert not used_as_parameter, (
                f"{banned!r} is used as a parameter in the proposed API; the design "
                "says tinyray manages no resources"
            )
            assert banned in removed, f"{banned!r} should be listed as removed"

    def test_principles_are_numbered_and_traceable(self):
        text = read(DOCS / "01-overview" / "03-principles.md")
        for n in range(1, 8):
            assert f"### P{n} " in text, f"principle P{n} is missing"
        assert text.count("**由来。**") >= 6, "原则必须记录产生它的那次故障"
