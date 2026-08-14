#!/usr/bin/env bash
# Run every layer of the tinyray test suite.
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"

echo "=== Rust unit tests (core protocol) ==="
cargo test --workspace

echo
echo "=== Building the extension module ==="
.venv/bin/maturin develop --release -q

echo
echo "=== Python tests (bindings, buffers, serde) ==="
.venv/bin/python -m pytest tests/ -q

echo
echo "=== Suite quality (structural checks on the tests themselves) ==="
.venv/bin/python -m pytest tests/test_suite_quality.py -q

echo
echo "=== Benchmarks ==="
.venv/bin/python -m pytest benchmarks/ -q -s -m bench

echo
echo "=== Mutation testing (are the tests worth anything?) ==="
echo "Skipped by default; it rewrites source files. Run explicitly:"
echo "  .venv/bin/python scripts/mutate.py"
