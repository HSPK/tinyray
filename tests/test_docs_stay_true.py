"""文档也会腐坏，而且没人会发现。

docs/README.md 一路写着「提案阶段，当前未实现。代码已清空」，那时 0.5.0 已经
发在 PyPI 上了。示例里那条「超过 1 MB 直接报错」也在语义改成警告之后活了一阵子。
两次都是同一个原因：没有任何东西在量它。

这条测试量的是文档里那些**可以机械核对**的部分。论证和取舍核对不了，只能靠人读。
"""

from __future__ import annotations

import pathlib

import tinyray

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = sorted((ROOT / "docs").glob("*.md"))


def test_the_docs_are_all_in_the_nav():
    """写了但没挂上导航，等于没写。"""
    nav = (ROOT / "mkdocs.yml").read_text()
    missing = [p.name for p in DOCS if p.name not in nav]
    assert not missing, f"这些文档没有出现在 mkdocs.yml 的 nav 里: {missing}"


def test_no_document_still_calls_the_project_unimplemented():
    """0.5.0 已经发布，任何「未实现」的说法都是错的。"""
    stale = ("当前未实现", "代码已清空", "提案阶段")
    found = [
        f"{p.name}: {phrase}"
        for p in DOCS + [ROOT / "README.md"]
        for phrase in stale
        if phrase in p.read_text()
    ]
    assert not found, f"文档还在说项目没实现: {found}"


def test_every_public_name_is_documented():
    """API 参考漏掉一个名字，使用者就只能去读源码。"""
    reference = (ROOT / "docs" / "api.md").read_text()
    undocumented = [name for name in tinyray.__all__ if name not in reference]
    assert not undocumented, f"docs/api.md 里没有提到这些公开名字: {undocumented}"


def test_the_documented_exception_tree_matches_the_real_one():
    """异常分类是调用方决定「能不能重试」的依据，写错比不写更糟。

    这里核对的是关系而不是文字：NotDelivered 和 OutcomeUnknown 必须仍是
    Unreachable 的子类，文档才敢说 `except Unreachable` 照旧能接住两者。
    """
    reference = (ROOT / "docs" / "api.md").read_text()
    for name in ("NotDelivered", "OutcomeUnknown"):
        assert name in reference
        assert issubclass(getattr(tinyray, name), tinyray.Unreachable)
    assert "except Unreachable" in reference or "`Unreachable` 的子类" in reference


def test_no_document_promises_a_hard_limit_the_code_stopped_enforcing():
    """体积预算从「拒发」改成了「警告」。示例里那句话曾经活得比实现久。"""
    for p in DOCS:
        text = p.read_text()
        for line in text.splitlines():
            if "1 MB" in line and ("直接报错" in line or "当场失败" in line):
                raise AssertionError(f"{p.name} 仍把 1 MB 说成硬限: {line.strip()}")


def test_code_fences_in_the_guides_name_their_language():
    """无标注的代码块在站点上不会高亮，读起来是一堵墙。"""
    bare = []
    for p in (ROOT / "docs" / "getting-started.md", ROOT / "docs" / "api.md"):
        inside = False
        for n, line in enumerate(p.read_text().splitlines(), 1):
            if not line.strip().startswith("```"):
                continue
            if not inside and line.strip() == "```":
                bare.append(f"{p.name}:{n}")
            inside = not inside
    assert not bare, f"这些代码块没标语言: {bare}"
