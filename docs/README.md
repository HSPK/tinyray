# tinyray documentation

tinyray is an HTTP control plane for ML experiments. It places processes,
assigns ranks, watches them and restarts them. It does not move your tensors.

## Reading order

Pages are numbered. The numbers are the order to read them in, both across
sections and within one.

Sections build on each other: concepts explain the shape, guides use it,
reference pins it down, internals open it up, project says what is missing. A
page never depends on a later one.

**Straight through** — sections 01 and 02, in order, is the intended first pass.
About an hour, and it covers everything most people need.

**If you are in a hurry**, the shortest useful path is three pages:
[01-positioning](01-concepts/01-positioning.md) →
[01-getting-started](02-guides/01-getting-started.md) →
[02-native-frameworks](02-guides/02-native-frameworks.md).

Sections 03 to 05 are lookup, not narrative. Read them when you need them.

## Where to start

**You want to run Megatron, SGLang, vLLM or torchrun under a controller.**
Read [positioning](01-concepts/01-positioning.md) for the stance, then
[native frameworks](02-guides/02-native-frameworks.md) for the code. That is the
main line and everything else is optional.

**You are writing new code and want actors.** Start with
[getting started](02-guides/01-getting-started.md), then [actors](02-guides/03-actors.md).

**Something is stuck and you need to know why.** Go to
[observability](02-guides/06-observability.md).

**You are changing tinyray itself.** Read [architecture](01-concepts/02-architecture.md)
and then the relevant page under [internals](04-internals/).

## Layout

| Section | Contents |
|---|---|
| [01-concepts](01-concepts/) | Why tinyray is shaped this way. Read once. |
| [02-guides](02-guides/) | How to do a particular thing. |
| [03-reference](03-reference/) | Exact signatures, protocol and configuration. |
| [04-internals](04-internals/) | How it works inside, for people changing it. |
| [05-project](05-project/) | What is built, what is not, and what was decided. |

### Concepts

- [01-positioning.md](01-concepts/01-positioning.md) — control plane versus framework, and the six design principles
- [02-architecture.md](01-concepts/02-architecture.md) — components, process model, control and data planes
- [03-tradeoffs.md](01-concepts/03-tradeoffs.md) — every major choice and what it costs

### Guides

- [01-getting-started.md](02-guides/01-getting-started.md) — install and run something
- [02-native-frameworks.md](02-guides/02-native-frameworks.md) — Megatron, SGLang, vLLM, torchrun
- [03-actors.md](02-guides/03-actors.md) — the actor API, for code written for tinyray
- [04-placement.md](02-guides/04-placement.md) — resources, gangs, placement strategies
- [05-fault-tolerance.md](02-guides/05-fault-tolerance.md) — restarts, readiness, failure semantics
- [06-observability.md](02-guides/06-observability.md) — finding out what is stuck

### Reference

- [01-api-python.md](03-reference/01-api-python.md) — the main-line API
- [02-protocol.md](03-reference/02-protocol.md) — wire format, endpoints, error taxonomy
- [03-cli.md](03-reference/03-cli.md) — the `tinyray` command
- [04-configuration.md](03-reference/04-configuration.md) — every knob and its default

### Internals

- [01-rust-core.md](04-internals/01-rust-core.md) — crates, the GIL discipline, the language boundary
- [02-store-and-queue.md](04-internals/02-store-and-queue.md) — results, ordering, backpressure
- [03-transport.md](04-internals/03-transport.md) — framing, pooling, zero copy
- [04-scheduler.md](04-internals/04-scheduler.md) — the resource table and placement
- [05-testing.md](04-internals/05-testing.md) — the testing standard and why it exists

### Project

- [01-status.md](05-project/01-status.md) — an honest inventory of what works
- [02-decisions.md](05-project/02-decisions.md) — decisions, including the reversed ones
- [03-roadmap.md](05-project/03-roadmap.md) — known gaps, in priority order

## Conventions

**Numbering.** `NN-name.md` inside `NN-section/`. The number is reading order,
and it is stable — a page inserted later takes the next number rather than
renumbering its neighbours.

**Page shape.** Every page opens with **Purpose** and closes with **Pitfalls**
and **See also**. Pitfalls is not filler: it records mistakes actually made
here, and is usually the most useful part of the page.

**Checked, not trusted.** `tests/test_docs.py` extracts the signatures,
defaults, symbol names, exception names, environment variables and links from
these pages and asserts them against the installed package. A page that drifts
from the code fails the build.

It does **not** execute the examples — most of them launch processes or want a
GPU. What is guaranteed is that every name, argument and default you could copy
is real; not that a snippet runs unmodified in your environment.

**Numbers are measured**, not estimated, and the measurement is named where it
matters.
