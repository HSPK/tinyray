# tinyray

*[中文](../index.md)*

**A phone book and a roll call.** A membership layer for asynchronous ML jobs.

It starts no processes, allocates no GPUs and moves no tensors. It answers
three questions: **who is here, are they still alive, and who should I talk
to?**

```python
import tinyray

me = tinyray.join("engine", "serving")
me.ready(model_version=17)

engine = tinyray.pool("engine").pick(model_version=17)
print(engine.url)
```

```bash
pip install tinyray
tinyray --listen 127.0.0.1:8760
```

[Getting started](getting-started.md){ .md-button .md-button--primary }
[API reference](api.md){ .md-button }

---

## What it solves for you

- **Who is here** -- reporting in, leases, a locally cached roster. A lookup
  never touches the network, so looking a thousand times costs the registry
  nothing.
- **Are they still alive** -- tenures and fencing. When a seat changes hands,
  a call made with the old address is refused explicitly rather than landing
  on the wrong process.
- **Who should I talk to** -- find by name, by seat or by filtering on state,
  then call the method directly.
- **Roll call together** -- freeze one round's roster so every rank is handed
  the same list.

Underneath it is ordinary HTTP, so nothing is lost for debugging with `curl`.

## What it deliberately does not do

Task queues, schedulers, result stores, the data plane. **Registering per task
would drown it**, and tensors and weights should never travel through a
control plane. Both of those are backed by measurements in the design notes.

If you want a work queue with leases, write one on top of tinyray: it hands
you membership, tenure fencing and change notification, while the queue owns
job identity, payloads, results and the retry policy -- because only the
application knows what a job is, how large a result may be, and whether it is
safe to run twice.

## Status

**0.13.3 is out** ([PyPI](https://pypi.org/project/tinyray/)), around 2,900
lines. Wheels for py3.10-3.13 on Linux x86_64 / aarch64 and macOS universal2.

Multi-host and scale are both measured: three containers across network
namespaces discovered and called each other with matching fingerprints;
100,000 members registered with subscribers seeing all of them, the version
landing on exactly 100,000 (an idle heartbeat is not a change); a peak of
216,335 requests a second with zero errors.

## Documentation

| Document | What is in it |
|---|---|
| [Getting started](getting-started.md) | Ten minutes, from install to two processes calling each other |
| [API reference](api.md) | The whole surface, written against the implementation |
| [Benchmarks](bench.md) | What it costs, measured, and the traps in measuring it |

The design notes -- the problem, why existing tools do not fit, the pool
policies and the reasoning behind the API -- are in Chinese only:
[为什么](../01-why.md) and [是什么](../02-design.md).

## The rules these docs are written by

- **Every number says where it came from**: *measured* (out of a benchmark
  run), *derived* (the arithmetic is reproducible), or *not yet measured*
  (required before the design is frozen). **An unlabelled number is a guess in
  disguise.**
- **The user-facing documents exist in both English and Chinese** (getting
  started, API, benchmarks); the design notes are Chinese only. Code,
  identifiers and code comments are English throughout -- one repository
  mixing two languages inside the source is worse than either.
- Plain words beat jargon. "The whole choir stops when one singer is missing"
  is easier to follow than "single-member failure blocks the group under
  collective semantics".
