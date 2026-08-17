"""Documentation cannot drift.

The worst bugs in this project shared a shape: something was *claimed* in one
place and *implemented* differently in another, and nothing compared the two.
Documentation is the largest surface for that failure, because prose has no
compiler.

So the docs are checked against the code the same way the code is checked
against itself: signatures, defaults, symbol names and links are all extracted
from ``docs/`` and asserted against the installed package.

These tests never execute a documented snippet. Most of them launch processes
or want a GPU. What they assert instead is that every name, argument and
default a reader would copy is real.
"""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path
from typing import Any, ClassVar

import pytest

import tinyray as tr

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

MARKDOWN = sorted(DOCS.rglob("*.md"))


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def fenced(text: str, language: str) -> list[str]:
    """Every fenced block tagged with ``language``."""
    pattern = re.compile(rf"^```{language}\n(.*?)^```", re.DOTALL | re.MULTILINE)
    return [m.group(1) for m in pattern.finditer(text)]


# --------------------------------------------------------------------------
# The docs tree itself
# --------------------------------------------------------------------------


class TestTheTreeIsWhole:
    def test_there_are_docs(self):
        assert len(MARKDOWN) >= 20, "the docs tree lost files"

    def test_every_page_is_reachable_from_the_index(self):
        index = read(DOCS / "README.md")
        orphans = [
            page.relative_to(DOCS).as_posix()
            for page in MARKDOWN
            if page.name != "README.md" and page.relative_to(DOCS).as_posix() not in index
        ]
        assert not orphans, (
            f"pages nobody can navigate to: {orphans}; link them from docs/README.md or delete them"
        )

    @pytest.mark.parametrize("page", MARKDOWN, ids=lambda p: p.relative_to(DOCS).as_posix())
    def test_relative_links_resolve(self, page: Path):
        broken = []
        for target in re.findall(r"\]\((?!https?:)([^)#]+)(?:#[^)]*)?\)", read(page)):
            if (page.parent / target).exists():
                continue
            broken.append(target)
        assert not broken, f"{page.name} links to files that do not exist: {broken}"

    @pytest.mark.parametrize("page", MARKDOWN, ids=lambda p: p.relative_to(DOCS).as_posix())
    def test_every_page_has_a_purpose(self, page: Path):
        if page.name == "README.md":
            return
        assert "## Purpose" in read(page), (
            f"{page.name} has no Purpose section; the structure is uniform on purpose"
        )


# --------------------------------------------------------------------------
# Reading order
# --------------------------------------------------------------------------

NUMBERED = re.compile(r"^(\d{2})-[a-z0-9-]+\.md$")

SECTIONS = sorted(d for d in DOCS.iterdir() if d.is_dir())


class TestReadingOrderIsReal:
    """The numbers in the filenames are a promise about reading order.

    A promise nobody checks is how this project's worst bugs started, so the
    numbering is asserted rather than assumed: contiguous within a section,
    and listed in the index in the same order.
    """

    def test_sections_are_numbered(self):
        unnumbered = [d.name for d in SECTIONS if not re.match(r"^\d{2}-[a-z-]+$", d.name)]
        assert not unnumbered, f"sections without a reading-order prefix: {unnumbered}"

    @pytest.mark.parametrize("section", SECTIONS, ids=lambda d: d.name)
    def test_pages_are_numbered(self, section: Path):
        unnumbered = [p.name for p in section.glob("*.md") if not NUMBERED.match(p.name)]
        assert not unnumbered, (
            f"{section.name} contains pages with no reading-order prefix: {unnumbered}"
        )

    @pytest.mark.parametrize("section", SECTIONS, ids=lambda d: d.name)
    def test_numbering_is_contiguous_from_one(self, section: Path):
        numbers = sorted(int(NUMBERED.match(p.name).group(1)) for p in section.glob("*.md"))
        assert numbers == list(range(1, len(numbers) + 1)), (
            f"{section.name} is numbered {numbers}; reading order must run 1..n "
            "with no gaps and no duplicates"
        )

    @pytest.mark.parametrize("section", SECTIONS, ids=lambda d: d.name)
    def test_the_index_lists_pages_in_reading_order(self, section: Path):
        # Only the bullet list matters. Prose above it links pages by relevance,
        # which is a different question from what order to read them in.
        index = read(DOCS / "README.md")
        listed = re.findall(rf"^- \[[^\]]+\]\({re.escape(section.name)}/([^)]+)\)", index, re.M)
        pages = sorted(p.name for p in section.glob("*.md"))
        assert listed == pages, (
            f"docs/README.md lists {section.name} as {listed}, which contradicts "
            f"the numbering {pages}; the index and the filenames disagree about "
            "reading order"
        )

    def test_the_index_explains_the_numbering(self):
        assert "## Reading order" in read(DOCS / "README.md"), (
            "the numbers only mean something if the index says what they mean"
        )


# --------------------------------------------------------------------------
# Names a reader would type
# --------------------------------------------------------------------------


class TestDocumentedNamesExist:
    """Anything written as ``tr.foo`` must be importable as ``tinyray.foo``."""

    # Attributes of *objects*, not of the module.
    NOT_MODULE_LEVEL: ClassVar[set[str]] = set()

    def documented_names(self) -> set[str]:
        names: set[str] = set()
        for page in MARKDOWN:
            names.update(re.findall(r"\btr\.([A-Za-z_][A-Za-z0-9_]*)", read(page)))
        return names - self.NOT_MODULE_LEVEL

    def test_every_tr_name_is_exported(self):
        missing = sorted(n for n in self.documented_names() if not hasattr(tr, n))
        assert not missing, f"docs reference tinyray.{{{','.join(missing)}}}, which do not exist"

    def test_the_docs_actually_reference_something(self):
        # A positive control: if the extraction regex breaks, the test above
        # passes vacuously.
        assert len(self.documented_names()) >= 15, (
            "extracted almost no names from the docs; the regex is broken, and "
            "test_every_tr_name_is_exported is passing for the wrong reason"
        )

    def test_every_documented_exception_exists(self):
        text = "\n".join(read(p) for p in MARKDOWN)
        # ``(?<![.\w])`` skips qualified names such as ``torch.OutOfMemoryError``,
        # which belong to another project and are quoted in example output.
        documented = set(
            re.findall(
                r"(?<![.\w])([A-Z][A-Za-z]*(?:Error|Lost|Died|Failed|Pressure|Rebuilding))\b",
                text,
            )
        )
        known = {name for name in dir(tr) if isinstance(getattr(tr, name), type)}
        # Names that belong to Python or to other projects.
        external = {"TypeError", "ValueError", "RuntimeError", "OSError", "KeyError"}
        missing = sorted(documented - known - external)
        assert not missing, f"docs name exceptions that tinyray does not export: {missing}"


# --------------------------------------------------------------------------
# Signatures
# --------------------------------------------------------------------------

SIGNATURE = re.compile(r"^([a-z_][a-z0-9_]*)\((.*)\)(?:\s*->.*)?$", re.DOTALL)

# Every main-line callable the reference documents with a signature block.
MAIN_LINE = [
    "init",
    "shutdown",
    "remote",
    "create_actors",
    "get_actor",
    "kill",
    "get",
    "wait",
    "release",
    "launch_process",
    "stop_process",
    "processes",
    "serve",
    "launch_workers",
    "create_worker_group",
    "connect",
    "torchrun_env",
    "nodes",
    "actors",
    "transport_stats",
]


def split_signatures(block: str) -> list[str]:
    """One string per signature in a block that may hold several.

    A signature can wrap across lines, so lines are accumulated until the
    parentheses balance. Trailing ``#`` comments are dropped first.
    """
    signatures: list[str] = []
    buffer = ""
    depth = 0
    for raw in block.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line and depth == 0:
            continue
        buffer = f"{buffer} {line}".strip()
        depth += line.count("(") - line.count(")")
        if depth <= 0 and buffer:
            signatures.append(buffer)
            buffer, depth = "", 0
    if buffer:
        signatures.append(buffer)
    return signatures


def documented_signatures() -> dict[str, str]:
    """``{name: argument text}`` for every signature block in the reference."""
    found: dict[str, str] = {}
    for page in (DOCS / "03-reference").glob("*.md"):
        for block in fenced(read(page), "python"):
            for signature in split_signatures(block):
                match = SIGNATURE.match(signature)
                if match and match.group(1) in MAIN_LINE:
                    found[match.group(1)] = match.group(2)
    return found


def parse_documented(arguments: str) -> tuple[list[str], dict[str, str]]:
    """Parameter names in order, plus the defaults that were written down."""
    if not arguments.strip():
        return [], {}
    tree = ast.parse(f"def _({arguments}): pass").body[0]
    assert isinstance(tree, ast.FunctionDef)
    args = tree.args

    order: list[str] = [a.arg for a in args.posonlyargs + args.args]
    if args.vararg:
        order.append(f"*{args.vararg.arg}")
    order += [a.arg for a in args.kwonlyargs]
    if args.kwarg:
        order.append(f"**{args.kwarg.arg}")

    defaults: dict[str, str] = {}
    positional = args.posonlyargs + args.args
    for name, node in zip(positional[len(positional) - len(args.defaults) :], args.defaults):
        defaults[name.arg] = ast.unparse(node)
    for name, node in zip(args.kwonlyargs, args.kw_defaults):
        if node is not None:
            defaults[name.arg] = ast.unparse(node)
    return order, defaults


def actual(name: str) -> tuple[list[str], dict[str, str]]:
    signature = inspect.signature(getattr(tr, name))
    order: list[str] = []
    defaults: dict[str, str] = {}
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            order.append(f"*{parameter.name}")
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            order.append(f"**{parameter.name}")
        else:
            order.append(parameter.name)
        if parameter.default is not inspect.Parameter.empty:
            defaults[parameter.name] = repr(parameter.default)
    return order, defaults


def equivalent(documented: str, real: str) -> bool:
    """Whether two rendered defaults mean the same value."""
    if documented == real:
        return True
    for text in (documented, real):
        # ``None`` in the docs may stand for a value computed at runtime.
        if text in {"detected", "required", "..."}:
            return True
    try:
        return ast.literal_eval(documented) == ast.literal_eval(real)
    except (ValueError, SyntaxError):
        return False


class TestSignaturesMatch:
    def test_every_main_line_symbol_has_a_documented_signature(self):
        missing = sorted(set(MAIN_LINE) - set(documented_signatures()))
        assert not missing, (
            f"the reference has no signature block for {missing}; a main-line "
            "symbol nobody documented is a symbol nobody can use"
        )

    @pytest.mark.parametrize("name", MAIN_LINE)
    def test_parameters_match(self, name: str):
        blocks = documented_signatures()
        if name not in blocks:
            pytest.skip("covered by test_every_main_line_symbol_has_a_documented_signature")
        documented_order, _ = parse_documented(blocks[name])
        real_order, _ = actual(name)
        assert documented_order == real_order, (
            f"docs describe {name}{tuple(documented_order)} but the code has {tuple(real_order)}"
        )

    @pytest.mark.parametrize("name", MAIN_LINE)
    def test_defaults_match(self, name: str):
        blocks = documented_signatures()
        if name not in blocks:
            pytest.skip("covered by test_every_main_line_symbol_has_a_documented_signature")
        _, documented_defaults = parse_documented(blocks[name])
        _, real_defaults = actual(name)
        wrong = {
            parameter: (written, real_defaults.get(parameter))
            for parameter, written in documented_defaults.items()
            if not equivalent(written, real_defaults.get(parameter, "<no default>"))
        }
        assert not wrong, f"{name} has documented defaults the code disagrees with: {wrong}"


# --------------------------------------------------------------------------
# The configuration table
# --------------------------------------------------------------------------


class TestConfigurationTableIsTrue:
    """``04-configuration.md`` is the page a reader tunes from. It must be right."""

    # (page phrase, callable, parameter, expected default)
    CLAIMS: ClassVar[list[tuple[str, str, Any]]] = [
        ("init", "heartbeat_timeout", 30.0),
        ("init", "supervise_interval", 1.0),
        ("init", "prewarm", 0),
        ("launch_process", "startup_timeout", 600.0),
        ("launch_process", "num_cpus", 1.0),
        ("launch_process", "num_gpus", 0.0),
        ("launch_process", "strategy", "PACK"),
        ("launch_workers", "startup_timeout", 900.0),
        ("launch_workers", "gpus_per_worker", 1.0),
        ("launch_workers", "cpus_per_worker", 1.0),
        ("launch_workers", "strategy", "PACK"),
        ("serve", "max_pending_calls", 1000),
        ("get", "timeout", 300.0),
        ("wait", "timeout", 300.0),
        ("create_actors", "strategy", "SPREAD"),
    ]

    @pytest.mark.parametrize(("function", "parameter", "expected"), CLAIMS, ids=lambda v: str(v))
    def test_default_is_what_the_docs_say(self, function: str, parameter: str, expected: Any):
        signature = inspect.signature(getattr(tr, function))
        assert parameter in signature.parameters, (
            f"docs document {function}({parameter}=...), which no longer exists"
        )
        assert signature.parameters[parameter].default == expected, (
            f"docs say {function}({parameter}={expected!r}) but the code says "
            f"{signature.parameters[parameter].default!r}"
        )

    def test_the_page_states_every_claimed_default(self):
        page = read(DOCS / "03-reference" / "04-configuration.md")
        missing = [
            f"{function}.{parameter}"
            for function, parameter, _ in self.CLAIMS
            if parameter not in page
        ]
        assert not missing, f"asserted defaults that the page never mentions: {missing}"


# --------------------------------------------------------------------------
# Environment variables
# --------------------------------------------------------------------------


class TestEnvironmentVariablesAreDocumented:
    def used_by_the_code(self) -> set[str]:
        names: set[str] = set()
        for source in (ROOT / "python" / "tinyray").glob("*.py"):
            names.update(re.findall(r"TINYRAY_[A-Z_]+", read(source)))
        return names

    def test_every_variable_the_code_reads_is_documented(self):
        page = read(DOCS / "03-reference" / "03-cli.md")
        missing = sorted(name for name in self.used_by_the_code() if name not in page)
        assert not missing, (
            f"the code reads {missing} but the reference never mentions them; an "
            "undocumented environment variable is a setting nobody can find"
        )

    def test_the_docs_do_not_invent_variables(self):
        page = read(DOCS / "03-reference" / "03-cli.md")
        documented = set(re.findall(r"TINYRAY_[A-Z_]+", page))
        invented = sorted(documented - self.used_by_the_code())
        assert not invented, f"the reference documents variables nothing reads: {invented}"


# --------------------------------------------------------------------------
# Honesty
# --------------------------------------------------------------------------


class TestGapsStayDeclared:
    """The status page exists so gaps are stated, not discovered.

    Each of these is a thing tinyray does *not* do. If one is ever implemented,
    the corresponding assertion fails and forces the page to be updated -- which
    is the point.
    """

    def status(self) -> str:
        return read(DOCS / "05-project" / "01-status.md")

    def test_detached_lifetime_is_still_refused(self):
        assert "lifetime=" in self.status()

        @tr.remote(lifetime="detached")
        class Detached:
            pass

        with pytest.raises(Exception, match="detached"):
            Detached.remote()

    def test_multi_node_is_still_declared_missing(self):
        assert "Multi-node deployment" in self.status()
        assert not (ROOT / "python" / "tinyray" / "node_agent_main.py").exists(), (
            "a node agent entry point appeared; 01-status.md still says multi-node is unpackaged"
        )

    def test_the_deleted_fast_path_stays_deleted(self):
        assert not (ROOT / "crates" / "tinyray-runtime" / "src" / "shm.rs").exists(), (
            "shm.rs is back; 02-decisions.md records it as deleted dead code"
        )

    def test_untested_hardware_claims_are_declared(self):
        status = self.status()
        for claim in ("NCCL", "SGLang", "vLLM", "Megatron"):
            assert claim in status, (
                f"{claim} is exercised in the codebase but 01-status.md no longer "
                "states that it is unverified on real hardware"
            )
