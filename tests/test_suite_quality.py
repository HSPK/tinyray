"""Checks on the test suite itself.

Every bug that got through review in this project fell into one of a handful of
patterns. These tests encode the patterns, so the next instance is caught by CI
rather than by a user whose job died after thirty seconds.

They are deliberately structural: they read the source rather than exercise it.
That makes them cheap, and it lets them assert things no functional test can,
such as "this module is reachable from the public API at all".
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import ClassVar

import pytest

ROOT = Path(__file__).resolve().parent.parent
PY_PACKAGE = ROOT / "python" / "tinyray"
RUST_SRC = [
    ROOT / "crates" / name / "src" for name in ("tinyray-core", "tinyray-runtime", "tinyray-py")
]
TEST_DIR = ROOT / "tests"


def python_sources() -> list[Path]:
    return sorted(p for p in PY_PACKAGE.glob("*.py") if p.name != "__init__.py")


def rust_sources() -> list[Path]:
    return sorted(p for root in RUST_SRC for p in root.rglob("*.rs"))


def test_text() -> str:
    """All Python test source, as one string."""
    return "\n".join(p.read_text() for p in TEST_DIR.glob("test_*.py"))


test_text.__test__ = False  # not a test itself, despite the name


class TestNoDeadModules:
    """A module nobody calls is a feature that does not exist.

    `shm.rs` was written, unit tested and never wired in. Its tests all passed;
    the fast path simply never ran. Unit tests cannot detect this, because from
    inside a module everything looks fine.
    """

    # Modules known to be unwired, with the reason. Anything not listed here
    # must be reachable from production code.
    #
    # This list is the record: a gap that is written down is a decision, and a
    # gap that is not is an oversight. Adding an entry should be uncomfortable.
    KNOWN_UNWIRED: ClassVar[dict[str, str]] = {
        "shm": (
            "same-node fast path: written and unit tested, but the transport "
            "never calls it, so same-node results still go through a socket"
        ),
    }

    def test_every_rust_module_is_used_somewhere(self):
        runtime_src = ROOT / "crates" / "tinyray-runtime" / "src"
        modules = {p.stem for p in runtime_src.glob("*.rs")} - {"lib"}

        unwired = []
        for module in sorted(modules):
            definition = runtime_src / f"{module}.rs"
            callers = [
                path
                for path in rust_sources()
                if path != definition and f"{module}::" in path.read_text()
            ]
            # lib.rs only re-exports; that is not a use.
            real_callers = [p for p in callers if p.name != "lib.rs"]
            if not real_callers:
                unwired.append(module)

        unexpected = set(unwired) - set(self.KNOWN_UNWIRED)
        assert not unexpected, (
            f"modules with no callers: {sorted(unexpected)}. Either wire them into the "
            "production path or record them in KNOWN_UNWIRED so the gap is visible."
        )

    def test_known_unwired_modules_carry_a_reason(self):
        """An entry without an explanation is an oversight wearing a disguise."""
        for module, reason in self.KNOWN_UNWIRED.items():
            assert len(reason) > 40, (
                f"{module} is exempted from the wiring check with only {reason!r}; "
                "say what it does and why nothing calls it, or wire it up"
            )


class TestTimingConstantsAreInjectable:
    """A constant that only ever runs at its production value is untested.

    The heartbeat deadline was thirty seconds and every test finished in under
    five, so nothing ever reached it. The bug shipped: sessions lost all their
    actors after half a minute.
    """

    def test_intervals_are_parameters_not_just_constants(self):
        """Each timing constant needs a documented way to shorten it.

        Either it is read from the environment, or it is the default of a
        keyword argument. A bare module constant cannot be reached by a test
        that finishes in less time than the constant itself.
        """
        parameter_defaults: set[str] = set()
        for path in python_sources():
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    defaults = list(node.args.defaults) + [
                        d for d in node.args.kw_defaults if d is not None
                    ]
                    for default in defaults:
                        if isinstance(default, ast.Name):
                            parameter_defaults.add(default.id)

        offenders = []
        for path in python_sources():
            source = path.read_text()
            for match in re.finditer(
                r"^([A-Z_]+(?:SECONDS|INTERVAL|TIMEOUT))\s*=\s*(.*)$", source, re.M
            ):
                name, value = match.group(1), match.group(2)
                overridable = (
                    "os.environ" in value or name in parameter_defaults or name in test_text()
                )
                if not overridable:
                    offenders.append(f"{path.name}:{name}")
        assert not offenders, (
            f"timing constants with no way to shorten them in a test: {offenders}. "
            "Read them from the environment or make them keyword defaults, or no "
            "test will ever reach the deadline."
        )

    def test_the_heartbeat_deadline_is_reachable_from_init(self):
        # The specific one that bit us.
        source = (PY_PACKAGE / "api.py").read_text()
        assert "heartbeat_timeout" in source
        tree = ast.parse(source)
        init = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "init"
        )
        names = {arg.arg for arg in init.args.kwonlyargs}
        assert "heartbeat_timeout" in names, (
            "init() must expose the heartbeat deadline so tests can run it in "
            "seconds rather than assume the production value"
        )


class TestOptionsDoSomething:
    """An accepted option that changes nothing is worse than a rejected one.

    `lifetime="detached"` was stored in the registry and then ignored; the actor
    was killed on shutdown like any other. Users cannot tell the difference
    between "supported" and "silently dropped".
    """

    def test_every_actor_option_is_exercised_by_a_test(self):
        source = (PY_PACKAGE / "api.py").read_text()
        options = set(re.findall(r'options\.get\("([a-z_]+)"', source))
        tests = test_text()

        untested = sorted(option for option in options if option not in tests)
        assert not untested, (
            f"actor options no test mentions: {untested}. Each needs a test showing it "
            "changes behaviour, or it may be silently ignored."
        )


class TestErrorTaxonomyIsAsserted:
    """Collapsing distinct failures into one exception hides real bugs.

    `release()` failed to leave a tombstone, so a released result reported
    NotFound -- indistinguishable from a genuine bug -- instead of ObjectLost.
    """

    KINDS: ClassVar[list[str]] = [
        "UserException",
        "ObjectLost",
        "ActorDied",
        "NotFound",
        "Backpressure",
    ]

    def test_each_error_kind_has_a_test_that_asserts_it(self):
        # Read as text so the check does not depend on import side effects.
        tests = "\n".join(p.read_text() for p in TEST_DIR.glob("test_*.py"))
        rust_tests = "\n".join(p.read_text() for p in (ROOT / "crates").rglob("*.rs"))
        combined = tests + rust_tests

        missing = [kind for kind in self.KINDS if kind not in combined]
        assert not missing, (
            f"error kinds never asserted in any test: {missing}. Asserting only "
            "'it raises' lets two very different failures look identical."
        )

    def test_python_tests_do_not_only_catch_the_base_class(self):
        """At least some tests must pin the specific exception type."""
        tests = test_text()
        specific = sum(
            tests.count(f"tinyray.{name}")
            for name in ("UserCodeError", "ObjectLost", "ActorDied", "NotFound")
        )
        assert specific >= 8, (
            f"only {specific} assertions on specific exception types; catching "
            "TinyrayError everywhere would not have caught the NotFound/ObjectLost mix-up"
        )


class TestEfficiencyClaimsAreAsserted:
    """Features that exist for speed need tests that measure, not just check.

    Inverting `buffer_callback` doubled every payload and produced completely
    correct results. Only a size assertion could see it.
    """

    def test_serialisation_overhead_is_pinned(self):
        source = (TEST_DIR / "test_serde.py").read_text()
        assert "len(body) <" in source or "payload_size" in source, (
            "no test bounds the pickle body size; a payload that is silently "
            "copied twice still passes every equality check"
        )

    def test_zero_copy_is_asserted_not_assumed(self):
        source = (TEST_DIR / "test_buffers.py").read_text()
        assert "owndata" in source, "no test proves results are views rather than copies"
        assert "buffer_address" in source, "no test compares addresses to prove sharing"

    def test_prewarm_has_a_latency_assertion(self):
        source = (TEST_DIR / "test_lifecycle.py").read_text()
        assert re.search(r"warm\s*<\s*0\.\d+", source), (
            "the prewarm pool exists purely to make actor creation fast; without a "
            "latency bound the pool can silently never be used"
        )


class TestConcurrencyPathsAreCovered:
    """Lock-taking code needs both branches exercised.

    `PrewarmPool.acquire` deadlocked on itself, but only on a cache hit. The
    miss path took a different route and every early test hit it.
    """

    def test_the_suite_has_a_global_timeout(self):
        # A deadlock must fail the run, not hang it.
        config = (ROOT / "pyproject.toml").read_text()
        assert "--timeout" in config, (
            "pytest needs a timeout, or a deadlock hangs CI instead of failing it"
        )

    def test_prewarm_pool_hit_path_is_tested(self):
        source = (TEST_DIR / "test_lifecycle.py").read_text()
        assert '"hits"' in source, (
            "only the miss path is covered; the hit path took the lock twice and deadlocked"
        )


class TestDesignClaimsHaveTests:
    """The design document makes promises. Promises need tests.

    The deepest cause of the worst bugs was that the suite tested what was
    built, not what was promised.
    """

    CLAIMS: ClassVar[dict[str, list[str]]] = {
        "ordering": ["submission order", "in_order", "out_of_order"],
        "backpressure": ["backpressure", "rejected_backpressure"],
        "gang placement": ["all or nothing", "gang"],
        "ref passing between actors": ["owner_endpoint", "RefsBetweenActors"],
        "eviction reports ObjectLost": ["ObjectLost"],
        "collective admission": ["whole GPU", "deadlock"],
        "epoch state machine": ["epoch", "stale"],
        "restart replays __init__": ["restarted", "reconstruct"],
    }

    @pytest.mark.parametrize("claim", sorted(CLAIMS))
    def test_claim_is_covered(self, claim):
        haystack = test_text() + "\n".join(p.read_text() for p in (ROOT / "crates").rglob("*.rs"))
        needles = self.CLAIMS[claim]
        assert any(needle in haystack for needle in needles), (
            f"no test mentions {claim!r} (looked for {needles}); a design promise "
            "with no test is a promise nobody is keeping"
        )


class TestTypeStubsMatchTheExtension:
    """The stub file is hand written, so it drifts unless something checks it.

    PyO3 cannot generate stubs, and a stale `.pyi` is worse than none: editors
    and type checkers confidently report an API that no longer exists.
    """

    @staticmethod
    def _stub_names() -> set[str]:
        tree = ast.parse((PY_PACKAGE / "_tinyray.pyi").read_text())
        names = set()
        for node in tree.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
                names.add(node.name)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names.add(node.target.id)
        return names

    @staticmethod
    def _runtime_names() -> set[str]:
        import tinyray._tinyray as extension

        return {name for name in dir(extension) if not name.startswith("_")}

    def test_every_export_is_declared(self):
        missing = sorted(self._runtime_names() - self._stub_names())
        assert not missing, (
            f"exported by the extension but absent from _tinyray.pyi: {missing}. "
            "A type checker will report these as errors on correct code."
        )

    def test_no_stubs_for_things_that_do_not_exist(self):
        extra = sorted(self._stub_names() - self._runtime_names() - {"__all__"})
        assert not extra, (
            f"declared in _tinyray.pyi but not exported: {extra}. "
            "A stub that promises a missing symbol is worse than no stub."
        )

    def test_all_lists_every_public_symbol(self):
        tree = ast.parse((PY_PACKAGE / "_tinyray.pyi").read_text())
        declared = next(
            node
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(getattr(t, "id", None) == "__all__" for t in node.targets)
        )
        listed = {element.value for element in declared.value.elts}
        assert listed == self._runtime_names()

    def test_class_methods_are_declared(self):
        """Spot-check the classes people actually call into."""
        tree = ast.parse((PY_PACKAGE / "_tinyray.pyi").read_text())
        classes = {
            node.name: {item.name for item in node.body if isinstance(item, ast.FunctionDef)}
            for node in tree.body
            if isinstance(node, ast.ClassDef)
        }

        import tinyray._tinyray as extension

        for name in ("ClientRuntime", "ActorRuntime", "ClusterState", "CollectiveRegistry"):
            runtime_methods = {
                attribute
                for attribute in dir(getattr(extension, name))
                if not attribute.startswith("_")
            }
            missing = sorted(runtime_methods - classes.get(name, set()))
            assert not missing, f"{name} methods missing from the stub: {missing}"

    def test_package_is_marked_as_typed(self):
        assert (PY_PACKAGE / "py.typed").exists(), (
            "without py.typed, type checkers ignore the annotations entirely"
        )
