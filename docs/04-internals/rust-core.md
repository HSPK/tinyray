# Rust core

## Purpose

Why there is Rust here at all, how the three crates divide, and the GIL
discipline that makes the split worthwhile.

## Why Rust

One measurement decided it.

Decode a 10 MB message while four GIL-bound Python threads run:

| Who initiates the decode | Slowdown vs idle |
|---|---|
| A native tokio thread | **1.04x** |
| A Python thread calling into the same Rust code | **49x** |

The same code. The difference is who owns the GIL when the work starts. A
Python-initiated call holds the GIL through argument marshalling and re-acquires
it to build the result, so it queues behind every other Python thread. A tokio
thread never touches the GIL until it has a finished object to hand over.

This is the load-bearing fact of the whole design, and it has a direct
consequence: **the serving path must be tokio-driven, never a Python loop.**
An actor's HTTP server accepts, frames, decodes and queues without Python
involvement; Python is entered only to run the user's method.

Rust was chosen over C++ for the borrow checker in exactly the place it pays —
handing raw buffers between two runtimes with different ownership models.

## Crate split

```
tinyray-core      no Python, no I/O      pure data
  framing.rs      wire format, streaming decoder
  ids.rs          128-bit identifiers
  proto.rs        message envelopes
  limits.rs       decode bounds
  error.rs

tinyray-runtime   tokio, hyper           behaviour
  transport/      HTTP client and server
  store.rs        result store
  queue.rs        ordered queue
  actor.rs        the actor loop
  cluster.rs      resource table and placement
  collective.rs   NCCL rendezvous
  client.rs       driver-side calls

tinyray-py        PyO3                   the boundary
  buffers.rs      the only unsafe code
  worker.rs       the serving side
  driver.rs       the calling side
  cluster.rs      placement bindings
  ...
```

The split is enforced by dependency direction: `core` depends on neither of the
others, `runtime` depends on `core`, `py` depends on both.

The point of `core` having no Python and no I/O is that the wire format can be
tested exhaustively at memory speed. `framing.rs` is 647 lines, roughly half of
them tests, and it never needs a socket to be exercised.

## The boundary

Three rules govern every crossing.

**1. Never hold the GIL across I/O.** Any `await`, socket read or lock
acquisition happens inside `py.allow_threads`. A blocking call that holds the
GIL stops the entire process, including the actor's own Python.

**2. Copy Python → Rust, borrow Rust → Python.** Asymmetric, and deliberately
so. This reverses the original design, which wanted zero-copy in both
directions.

`.remote()` returns immediately. The caller can — and in a training loop
will — mutate the array on the next line. Borrowing would mean the bytes on the
wire depend on when the transport happened to read them. So the argument is
copied once, at submit time.

The other direction has no such hazard. A fetched result is a fresh buffer
nobody else holds a reference to, so Python borrows it through the buffer
protocol with no copy. The cost is that **results are read-only**: one buffer
may serve many consumers, so none of them may write.

**3. All unsafe lives in `buffers.rs`.** 188 lines, seven `unsafe` blocks, one
job: implement `__getbuffer__` and `__releasebuffer__` for a Rust-owned byte
range, and read an incoming Python buffer. Everything else is safe Rust.

`BufferGuard` exists so a `Py_buffer` is released on every path, including
panics. The buffer protocol is refcounted by hand; a missed release is a leak
that only shows up under load.

## Why not abi3

abi3 would give one wheel per platform instead of one per Python version. It
was tried and abandoned: the limited API does not expose `Py_buffer`, and
`Py_buffer` **is** the zero-copy mechanism.

The cost is a 15-job wheel matrix in CI. The alternative was to give up the
thing Rust was introduced for.

## Quirks

**`buffer_callback` return semantics are inverted.** In pickle protocol 5, a
callback returning a *truthy* value keeps the buffer **inline**; returning
false or `None` makes it out-of-band. Getting this backwards produced a payload
that was silently correct and twice the size — 400,153 bytes for a 10 MB array
instead of 135. Tests that only checked round-trip equality passed.

Now asserted directly: `test_serde.py` checks the body is small, not just that
the value survives.

**`PyBuffer<u8>` rejects non-uint8 dtypes.** A `float32` array does not satisfy
`PyBuffer<u8>`, even though its bytes are perfectly readable. Hence
`PyBUF_SIMPLE` through the raw FFI rather than the typed helper.

**`from_str_radix` accepts a leading `+`.** `"+1f2e..."` parsed as a valid
identifier, giving one id two spellings. Parsing now requires exactly 32 hex
characters.

**Python only runs signal handlers while the main thread executes bytecode.**
An indefinite block in Rust makes SIGTERM unreachable — the handler is queued
and never runs. `next_task` originally blocked forever, so shutdown took 10.00 s
(the supervisor's SIGKILL). It now takes a deadline and returns to Python
periodically: 0.24 s.

## Building

```bash
export PATH="$HOME/.cargo/bin:$PATH"   # the system rustc is too old
.venv/bin/maturin develop --release
```

`rmp-serde` needs edition 2024, which means rustc 1.85 or newer.

Debug builds are 10-30x slower on the serialisation path. Benchmark numbers in
this documentation are all `--release`.

## See also

- [transport.md](transport.md) — the tokio side
- [store-and-queue.md](store-and-queue.md) — what the runtime holds
- [protocol.md](../03-reference/protocol.md) — the format `core` implements
