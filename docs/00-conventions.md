# Document conventions

This file defines the structure, writing style and completeness standard for
`docs/`. It follows the rl-bridge conventions, so a reader moving between the
two projects finds the same shapes.

## 1. Document boundary

Design documents record observable behaviour, state ownership, cross-module
contracts and correctness constraints. They do not paraphrase implementations
line by line.

The following must be recorded:

- Logic that changes input, output, state, resources or failure outcome.
- Protocols between processes, over HTTP, and against external stores.
- Concurrency, ordering, idempotence, retry, timeout and backpressure semantics.
- Configuration, metrics, logs and recovery behaviour.
- Constraints proven by a test.

A private helper needs its own description only when it affects one of these.

## 2. Authoritative location

| Information | Location |
|---|---|
| Why the system is designed this way | `02-architecture/` |
| A module's responsibility and internal state | `03-modules/` |
| Cross-process message order and schema | `04-protocols/` |
| Running and troubleshooting | `05-operations/` |
| Test evidence | `06-testing/` |
| Exhaustive lists: config, endpoints, metrics | `07-reference/` |
| Status, decisions, plan | `08-project/` |

One fact is defined completely in one place. Others link to it with a sentence
of context.

## 3. Files and language

- `docs/README.md` is the index GitHub renders. No number.
- Other documents are `<NN>-<topic>.md`, where `NN` is the reading order within
  the directory.
- `00-` is reserved for meta-specification after the index and before content.
  Ordinary content starts at `01-`.
- Numbers increase independently within each directory and match the order in
  `docs/README.md`.
- File names use lowercase English and hyphens only.
- Body text is **English**. Identifiers — classes, functions, configuration
  keys, metrics, endpoints, environment variables, protocol names — keep their
  source spelling in backticks.
- Fixed terms: Slot, Incarnation, Lease, Cell, Node Agent, Registry, Reconciler,
  Readiness, Admission.
- Specification documents use declarative sentences. Because the whole of this
  tree is currently a proposal, the index and every top-level summary states
  that explicitly.
- No source line numbers. Link to file paths, class names or test names.

## 4. Module template

Module documents follow this order. Where a section has no content, write
"None" rather than dropping it — an omitted section hides a judgement.

```markdown
# <Module>

> One sentence on the problem this module solves.

## 1. Scope
## 2. Responsibilities
## 3. Non-responsibilities
## 4. Position in the system
## 5. Dependencies
## 6. Public contract
## 7. State ownership
## 8. Lifecycle
## 9. Main flow
## 10. Concurrency and distributed semantics
## 11. Correctness invariants
## 12. Failure handling
## 13. Configuration
## 14. Observability
## 15. Testing
## 16. Limitations and trade-offs
## 17. Source mapping
```

Non-responsibilities must name the module that does own the thing. That is what
stops boundaries eroding.

## 5. Architecture template

```markdown
# <Design>

> One sentence giving the conclusion.

## 1. Problem
## 2. Goals
## 3. Non-goals
## 4. Design
## 5. Normal flow
## 6. State and ownership
## 7. Correctness invariants
## 8. Failure and recovery
## 9. Observability
## 10. Trade-offs
## 11. Implementation and testing
```

## 6. Protocol template

```markdown
# <Protocol>

## 1. Purpose
## 2. Participants
## 3. Preconditions
## 4. Data model
## 5. Normal sequence
## 6. State transitions
## 7. Ordering constraints
## 8. Timeouts
## 9. Retry and idempotence
## 10. Backpressure
## 11. Failure semantics
## 12. Correctness invariants
## 13. Compatibility
## 14. Testing
```

A protocol document must state the interaction order. "Sends data" and "waits
until ready" are not specifications.

## 7. Fixed tables

Use only the rows relevant to the document. Exhaustive lists go in
`07-reference/`.

### 7.1 State

| State | Owner | Created | Updated by | Read by | Lifetime | Persisted |
|---|---|---|---|---|---|---|

### 7.2 Public interface

| Interface | Input | Output | Side effect | Blocking | Failure |
|---|---|---|---|---|---|

### 7.3 RPC

| Caller | Callee | Method | Payload | Timeout | Retry | Idempotent |
|---|---|---|---|---|---|---|

### 7.4 Queue

| Queue | Producer | Consumer | Ordering | Capacity | Backpressure | Drop rule |
|---|---|---|---|---|---|---|

### 7.5 Configuration

| Field | Type | Default | Validation | Reader | Effect |
|---|---|---|---|---|---|

### 7.6 Metrics

| Metric | Producer | Meaning | Unit | Reduction |
|---|---|---|---|---|

### 7.7 Tests

| Behaviour | Test file | Test case | Level |
|---|---|---|---|

## 8. Diagrams

- System relationships: Mermaid `flowchart`.
- Request and data order: `sequenceDiagram`.
- Lifecycle and control state: `stateDiagram-v2`.
- Node names are stable role names, not local variables.
- After every diagram, list the ordering, failure and invariants the diagram
  cannot express.

## 9. Numbers

Every quantity is one of three kinds, and the kind is stated:

| Kind | Meaning |
|---|---|
| **Measured** | Produced by a benchmark or test in this repository, with the command named |
| **Derived** | Computed from stated assumptions; the arithmetic must be reproducible |
| **To be measured** | Required before the design is frozen; listed with where it comes from |

A number with no kind is a guess wearing a disguise. `tests/test_docs.py`
re-computes the derived numbers in these pages and fails if a page disagrees
with its own arithmetic.

## 10. Completeness standard

A document is complete when all of the following hold:

- Every template section is present.
- Non-responsibilities name the owning module.
- Every state has an owner and a lifetime.
- Every protocol states order, timeout, retry and idempotence.
- Every failure mode states what is detected, by whom, and within what bound.
- Every number carries its kind.
- Every claim that a test proves names the test.
- Every relative link resolves and every page is reachable from the index.

The last two are enforced by `tests/test_docs.py`.
