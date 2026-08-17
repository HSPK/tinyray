# Testing

## Purpose

How this project is tested, and — more usefully — the specific ways its tests
failed to catch real bugs, encoded so they cannot fail the same way again.

## What exists

| Layer | Count | Where |
|---|---|---|
| Rust unit tests | 117 | inline `#[cfg(test)]` |
| Python tests | 469 | `tests/` |
| Mutants | 21 | `scripts/mutate.py` |
| Benchmarks | — | `benchmarks/` |

```bash
export PATH="$HOME/.cargo/bin:$PATH"
scripts/test.sh            # exactly what CI runs
cargo test --workspace --release
.venv/bin/python -m pytest tests/ -q
.venv/bin/python scripts/mutate.py
```

`scripts/test.sh` runs the CI commands verbatim, so a local pass means a CI
pass. Any divergence between the two is a bug in the script.

## The six blind spots

Every bug that reached a release fell into one of six categories. They are
listed here because the categories generalise; the individual bugs do not.

### 1. Asserting results but not costs

`wait()` fetched entire payloads to answer a yes/no question — 237 ms for a
200 MB result. Every test passed, because the *answer* was correct.

The tests verified the value and never asked what it cost.

**Encoded as:** `tests/test_driver_byte_budget.py` asserts a byte budget for
every driver operation, plus a meta-test requiring that every wire-touching
operation *has* one. Adding the meta-test immediately found three more
operations with no budget.

### 2. Dead code with passing unit tests

`shm.rs` had thorough unit tests and no caller. It was deleted.

Coverage of a module says nothing about whether anything uses it.

**Encoded as:** a meta-test that every Rust module is reachable from a public
entry point.

### 3. Timing constants only ever run at production values

Heartbeats were never sent. Any session over 30 s lost every actor. No test ran
for 30 s, so every test passed.

**Encoded as:** `TINYRAY_STARTUP_TIMEOUT` and `TINYRAY_SWEEP_INTERVAL` are
environment-overridable, and tests drive the timeout path at 1-2 s. A meta-test
requires that every timing constant be overridable.

### 4. Options accepted but ignored

`lifetime="detached"` was accepted and silently did nothing. `prewarm` was
accepted and never primed the pool.

An option that parses is not an option that works.

**Encoded as:** `test_every_actor_option_is_exercised_by_a_test`, which requires
each actor option to appear in a test that asserts its effect.

### 5. Collapsed error taxonomies

`ObjectLost` and `NotFound` were indistinguishable, so a fetch after eviction
looked like a typo.

**Encoded as:** a test per `ErrorKind` asserting it is reachable and distinct.

### 6. Only one branch of a lock path tested

The double-release bug: `release` had no upper bound, so releasing twice
*invented* a GPU. The single-release path was tested; the double was not.

**Encoded as:** release paths are tested for idempotence, and `release` clamps.

### The meta-cause

**Tests verified what was built, not what was promised.** Every one of these
passed against the implementation as written. None of them checked the
implementation against the claim.

`tests/test_suite_quality.py` (29 tests) is the structural response: it asserts
properties of the test suite itself, not of the runtime.

## Documentation is a claim surface

Prose has no compiler, so `docs/` is the largest place where something can be
claimed and never checked. Same failure shape, larger surface.

`tests/test_docs.py` (147 tests) closes it. Extracted from the pages and
asserted against the installed package:

- every signature block matches `inspect.signature` — parameter names, order
  and defaults
- every `tinyray` symbol a reader could copy exists
- every documented exception is exported
- every documented default equals the real default
- every environment variable the code reads is documented, and every one
  documented is read
- every relative link resolves and every page is reachable from the index
- the declared gaps stay gaps: `lifetime="detached"` still raises, `shm.rs`
  stays deleted, the unverified-hardware list still names NCCL, SGLang, vLLM
  and Megatron

Snippets are never executed — most of them launch processes or want a GPU. What
is asserted is that every name, argument and default is real.

The tests were checked against deliberate corruption of the docs before being
trusted: a wrong default, an invented symbol and a dead link each produced a
failure. Writing the check is not the same as knowing the check works.

## Mutation testing

`scripts/mutate.py` applies 21 targeted source mutations, each a plausible
mistake — an inverted comparison, an off-by-one, a dropped clamp — and requires
that the suite fail for every one.

All 21 are currently caught.

### The first run found a false positive

The only survivor was the heartbeat test. Mutating the heartbeat sender changed
nothing, because the assertion was:

```python
assert dead_nodes() == []
```

which is true when the node is healthy **and** true when it has already been
reaped and removed. The test could not fail.

This is what mutation testing is for. Coverage said the line was exercised. The
mutant said the assertion was vacuous.

### The mutation script had its own bug

`shutil.copy2` preserves mtime, so restoring an original file left cargo
believing the mutated binary was current. Mutants appeared to be caught when
the test had actually run against stale code.

Worth stating plainly: **the tool that verifies the tests needed verifying
too.**

## What tests look like here

Three rules, learned rather than designed.

**Assert the cost, not only the result.** Bytes moved, calls made, time taken.

**Drive the real path.** A test that constructs a `LocalStore` directly proves
less than one that starts an actor and fetches from it. The suite is
deliberately more integration than unit.

**Make the failure mode reachable.** If a code path only runs after 30 s, give
it a knob and turn it down.

## Benchmarks

`benchmarks/` measures the claims this design rests on:

| Measurement | Result |
|---|---|
| Decode under GIL contention, native thread | 1.04x |
| Decode under GIL contention, Python-initiated | 49x |
| `wait()` on a settled 200 MB result | 0.14 ms (was 237 ms) |
| Actor creation, cold | 49.5 ms |
| Actor creation, `prewarm=2` | 2.6 ms |

These are regression guards. The first pair is the justification for the entire
Rust core; if it stopped holding, the design would need revisiting.

## Examples are claims too

`examples/` was linted and never executed — the same shape as blind spot 2, one
directory over. Three complete programs, checked for style, never once run.

`tests/test_examples.py` runs all three and parses their own output. The
assertions are loose on magnitude and strict on direction, because the exact
ratio depends on the machine but "the driver moved three orders of magnitude
less than the workers did" must not silently become false:

- the driver's share of the data stays negligible
- prefetching overlaps, and does not exceed its theoretical ceiling
- `wait(num_returns=6)` returns exactly six
- the workers that straggle are the slow ones, so `wait` really is ordering by
  completion
- the policy version advances every iteration, and reward follows it
- no generated scratch file survives

It cost 15 seconds of suite time and immediately earned it. The RL example was
written with a `run_on` call on a method containing `dist.barrier()` — rank 0
entered the barrier and waited forever for a rank nobody had asked. It hung with
no error, no traceback and no failing unit test, because every component was
individually correct.

That is design principle 5, violated by the person who had just written design
principle 5. Encoding a rule does not make you immune to it; running the thing
does.

## Gate

Twelve pre-commit hooks. `cargo fmt --check`, `ruff check`, `ruff format
--check` over `python/ tests/ benchmarks/ scripts/ examples/`, `mypy`, then both
test suites.

mypy is not decoration. It found six real type inconsistencies, including
`serde.deserialize` declaring a `bytes` parameter while every caller passed a
`Frame`.

## What is not tested

Stated honestly, because an untested claim is a claim.

- **NCCL has never run on a real GPU.** Admission rules and the epoch state
  machine are tested; the collective itself is not.
- **Real SGLang, vLLM and Megatron have never been exercised.** Stand-in scripts
  with the same launch shape were used instead.
- **Multi-node has never run end to end.** Placement across nodes is unit
  tested; a real second machine has not been involved.

See [status](../05-project/01-status.md).

## Pitfalls

**Do not run pytest with `-p no:randomly`** unless reproducing a specific
failure; order independence is part of what is being tested.

**Debug builds change benchmark numbers by 10-30x.** Always `--release`.

**Editing a Python file may not do what you expect** — ruff format reflows
signatures, so string replacements can silently miss. Verify the anchor exists.

## See also

- [01-status.md](../05-project/01-status.md) — what is unverified
- [02-decisions.md](../05-project/02-decisions.md) — choices and reversals
