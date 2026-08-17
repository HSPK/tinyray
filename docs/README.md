# tinyray documentation

tinyray is an HTTP control plane for ML experiments. It places processes,
assigns ranks, watches them and restarts them. It does not move your tensors.

## Where to start

**You want to run Megatron, SGLang, vLLM or torchrun under a controller.**
Read [positioning](01-concepts/positioning.md) for the stance, then
[native frameworks](02-guides/native-frameworks.md) for the code. That is the
main line and everything else is optional.

**You are writing new code and want actors.** Start with
[getting started](02-guides/getting-started.md), then [actors](02-guides/actors.md).

**Something is stuck and you need to know why.** Go to
[observability](02-guides/observability.md).

**You are changing tinyray itself.** Read [architecture](01-concepts/architecture.md)
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

- [positioning.md](01-concepts/positioning.md) — control plane versus framework, and the six design principles
- [architecture.md](01-concepts/architecture.md) — components, process model, control and data planes
- [tradeoffs.md](01-concepts/tradeoffs.md) — every major choice and what it costs

### Guides

- [getting-started.md](02-guides/getting-started.md) — install and run something
- [native-frameworks.md](02-guides/native-frameworks.md) — Megatron, SGLang, vLLM, torchrun
- [actors.md](02-guides/actors.md) — the actor API, for code written for tinyray
- [placement.md](02-guides/placement.md) — resources, gangs, placement strategies
- [fault-tolerance.md](02-guides/fault-tolerance.md) — restarts, readiness, failure semantics
- [observability.md](02-guides/observability.md) — finding out what is stuck

### Reference

- [api-python.md](03-reference/api-python.md) — the main-line API
- [protocol.md](03-reference/protocol.md) — wire format, endpoints, error taxonomy
- [cli.md](03-reference/cli.md) — the `tinyray` command
- [configuration.md](03-reference/configuration.md) — every knob and its default

### Internals

- [rust-core.md](04-internals/rust-core.md) — crates, the GIL discipline, the language boundary
- [store-and-queue.md](04-internals/store-and-queue.md) — results, ordering, backpressure
- [transport.md](04-internals/transport.md) — framing, pooling, zero copy
- [scheduler.md](04-internals/scheduler.md) — the resource table and placement
- [testing.md](04-internals/testing.md) — the testing standard and why it exists

### Project

- [status.md](05-project/status.md) — an honest inventory of what works
- [decisions.md](05-project/decisions.md) — decisions, including the reversed ones
- [roadmap.md](05-project/roadmap.md) — known gaps, in priority order

## Conventions

Every page follows the same shape: **purpose**, **concepts**, **usage**,
**contract**, **pitfalls**, **see also**. The pitfalls section is not filler —
it records mistakes that were actually made here, and it is usually the most
useful part of the page.

Python examples in the guides are extracted and executed by
`tests/test_docs.py`. A documented example that stops working fails the build.

Numbers quoted in these pages are measured, not estimated, and the measurement
is named where it matters.
