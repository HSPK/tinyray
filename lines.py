#!/usr/bin/env python3
"""How big is it, and how much of that is actually code?

Comments are counted apart on purpose. They carry the measurements that
justify the decisions -- "940ms against 1-2ms", "8.78ms against 0.40ms" --
and deleting them to look smaller would only mean the next person has to
measure it all again.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent
SOURCE = [
    "crates/tinyray-proto/src/lib.rs",
    "crates/tinyray-registry/src/lib.rs",
    "crates/tinyray-registry/src/main.rs",
    "crates/tinyray-registry/src/server.rs",
    "crates/tinyray-registry/src/state.rs",
    "crates/tinyray-client/src/lib.rs",
    "crates/tinyray-client/src/beat.rs",
    "python/tinyray/__init__.py",
    "python/tinyray/_errors.py",
    "python/tinyray/_rpc.py",
    "python/tinyray/_serve.py",
    "python/tinyray/registry.py",
]


def split(path: pathlib.Path) -> tuple[int, int, int]:
    """(code, comment, blank) for one file."""
    rust = path.suffix == ".rs"
    code = comment = blank = 0
    in_doc = False
    for line in path.read_text().splitlines():
        t = line.strip()
        if not t:
            blank += 1
            continue
        if rust:
            if t.startswith("//"):
                comment += 1
                continue
        else:
            if t.startswith("#"):
                comment += 1
                continue
            quotes = t.count('"""')
            if in_doc:
                comment += 1
                in_doc = quotes % 2 == 0
                continue
            if t.startswith(('"""', 'r"""')) or quotes:
                comment += 1
                in_doc = quotes % 2 == 1
                continue
        code += 1
    return code, comment, blank


def tally(paths: list[str]) -> tuple[int, int, int]:
    totals = [0, 0, 0]
    for rel in paths:
        for i, n in enumerate(split(ROOT / rel)):
            totals[i] += n
    return totals[0], totals[1], totals[2]


def main() -> int:
    rust = [p for p in SOURCE if p.endswith(".rs")]
    python = [p for p in SOURCE if p.endswith(".py")]
    print(f"{'':10}{'代码':>8}{'注释':>8}{'空行':>8}{'总行':>8}")
    for label, group in (("Rust", rust), ("Python", python), ("合计", SOURCE)):
        c, m, b = tally(group)
        print(f"  {label:<8}{c:>8}{m:>8}{b:>8}{c + m + b:>8}")

    tests = sorted(str(p.relative_to(ROOT)) for p in (ROOT / "tests").glob("*.py"))
    c, m, b = tally(tests)
    print(f"  {'测试':<8}{c:>8}{m:>8}{b:>8}{c + m + b:>8}   ({len(tests)} 个文件)")

    n = subprocess.run(
        [sys.executable, str(ROOT / "mutation_check.py"), "--count"],
        capture_output=True,
        text=True,
    )
    if n.returncode == 0 and n.stdout.strip():
        print(f"\n  变异条目 {n.stdout.strip()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
