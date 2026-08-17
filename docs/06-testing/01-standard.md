# Testing standard

> Proposal; not the current implementation.

> Tests here verify what was promised, not what was built. The difference is
> where every serious bug in this project came from.

## 1. Problem

Every bug that reached a release in the previous implementation passed its own
tests. Not because the tests were absent, but because each verified the
implementation as written rather than the claim it was meant to satisfy.

## 2. Goals

- Encode each past failure as a check that cannot pass vacuously.
- Assert cost, not only result.
- Make design invariants executable.

## 3. Non-goals

- Coverage targets. Coverage said the heartbeat line was exercised while the
  assertion was vacuous.

## 4. The seven blind spots

Each is a category, each produced a real bug, and each is now a structural test.

### 4.1 Asserting results but not costs

A readiness check fetched the payload to answer a boolean — **measured** 237 ms
for a 200 MB result against 0.14 ms. Every test passed; the answer was right.

**Encoded as** a byte budget per operation, plus a meta-test requiring every
wire-touching operation to have one. Adding the meta-test found three operations
with none.

### 4.2 Dead code with passing unit tests

A same-host fast path had thorough unit tests and no caller. Coverage of a module
says nothing about whether anything uses it.

**Encoded as** a meta-test that every module is reachable from a public entry
point.

### 4.3 Timing constants only ever run at production values

Heartbeats were never sent. Any session over 30 s lost every worker. No test ran
for 30 s, so every test passed.

**Encoded as** every timing constant being environment-overridable, with tests
driving the timeout path in seconds, and a meta-test requiring the overridability.

### 4.4 Options accepted but ignored

Options that parsed and did nothing.

**Encoded as** a meta-test requiring every public option to appear in a test that
asserts its effect.

### 4.5 Collapsed error taxonomies

`ObjectLost` and `NotFound` were indistinguishable, so a fetch after eviction
looked like a typo.

**Encoded as** a test per error kind asserting it is reachable and distinct.

### 4.6 Only one branch of a lock path tested

A double release had no upper bound, so releasing twice *invented* a resource.
The single-release path was tested; the double was not.

**Encoded as** idempotence tests on every release path.

### 4.7 High-availability tested with one replica

A registry was given a fixed identity to save a round trip. Clients route by
identity, so two replicas shared one and calls were submitted to one and fetched
from the other. **Every single-replica test passed.**

**Encoded as** every availability test running with at least two replicas and
killing one. A single-replica test proves nothing about availability.

### 4.8 The meta-cause

> Tests verified what was built, not what was promised.

`tests/test_suite_quality.py` is the structural response: it asserts properties
of the test suite itself.

## 5. What a test looks like here

**Assert the cost.** Bytes moved, calls made, consensus writes. A functional
assertion alone would have caught none of §4.1.

**Assert across the range.** A budget checked at one payload size will be
violated at another. Scale-sensitive assertions are parameterised over cluster
size up to 8,192.

**Drive the real path.** A test constructing a registry object directly proves
less than one starting processes and killing them.

**Make the failure mode reachable.** A path that only runs after 30 s gets a knob
turned down.

**Kill things.** Availability claims are tested by killing, not by describing.

## 6. Levels

| Level | Scope | Runtime |
|---|---|---|
| Unit | One module, no processes | Milliseconds |
| Integration | Real processes, one machine | Seconds |
| Structural | Properties of the codebase and its tests | Milliseconds |
| Chaos | Real processes, injected faults | Seconds to minutes |
| Scale | Simulated workers, 10k to 100k | Minutes |

Scale and chaos are the two that would have caught the failures in §4.7 and
§4.3.

## 7. Mutation testing

Targeted source mutations, each a plausible mistake — an inverted comparison, a
dropped clamp, an off-by-one — with the suite required to fail for every one.

Its first run found the only vacuous assertion in the suite: a heartbeat test
asserting `dead_nodes() == []`, which is true when the node is healthy **and**
when it has already been reaped. Coverage said the line was exercised; the mutant
said the assertion could not fail.

The mutation harness itself had a bug — copying files preserved modification
times, so the build reused stale binaries and mutants appeared caught when the
test had run against unmutated code. **The tool that verifies the tests needed
verifying too.**

## 8. Invariants as tests

Each principle from
[01-overview/03-principles.md](../01-overview/03-principles.md) maps to a check:

| Principle | Test |
|---|---|
| P1 control plane carries no bulk data | `tests/test_driver_byte_budget.py` |
| P2 no resource is claimed | `tests/test_suite_quality.py::test_no_resource_arguments` |
| P3 launcher's interface is used | `tests/test_membership.py::test_rank_from_launcher` |
| P4 every write is fenced | `tests/test_suite_quality.py::test_fencing_is_in_the_transport` |
| P5 no operation needs all members | `tests/test_reconcile.py::test_epoch_membership_is_frozen` |
| P6 failures are bounded, never hangs | `tests/test_suite_quality.py::test_every_wait_has_a_deadline` |
| P7 soft state where possible | `tests/test_fake_cluster.py::test_consensus_writes_are_flat` |

A principle with no test is a preference.

## 9. What must not be claimed

Untested claims are recorded as untested, in
[08-project/01-status.md](../08-project/01-status.md). The previous
implementation shipped with NCCL support that had never run on a GPU; it was
listed as such, and that honesty is a requirement rather than a courtesy.

## 10. Trade-offs

- **The scale suite is slow** and gates a merge only nightly.
- **Chaos tests are timing-sensitive** and will occasionally be flaky. A flaky
  chaos test is worth more than no chaos test, but it must be quarantined rather
  than deleted.
- **Structural tests constrain refactoring.** That is their purpose, and they
  will occasionally be wrong.

## 11. Implementation

`tests/test_suite_quality.py` for structural checks,
`tests/test_driver_byte_budget.py` for cost, `tests/test_chaos.py` for injection,
`tests/test_fake_cluster.py` for scale, `scripts/mutate.py` for mutation.
