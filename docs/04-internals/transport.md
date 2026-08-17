# Transport

## Purpose

How bytes get from one process to another: the three-thread actor model, the
HTTP client, and the choices that keep a busy actor responsive.

## The three-thread model

Every serving process — a tinyray actor or a native worker calling `serve()` —
runs three kinds of thread.

| Thread | Language | Job |
|---|---|---|
| tokio pool | Rust | accept calls, serve fetches, frame and decode |
| executor | Python | run user methods |
| collective | Python | NCCL calls only |

The tokio pool **never needs the GIL**. An actor grinding through a 200 ms
`train_step` still answers `/task/fetch` immediately, still answers `/health`,
and still accepts new calls into its queue.

This is not an optimisation. It is the reason the design works: an actor that
only responded between method calls would be unobservable exactly when you need
to observe it, and stragglers would be undetectable.

### The executor thread

Pulls work with `next_task`, which blocks **without the GIL held** — otherwise
an idle actor would freeze every other Python thread in the process, including
whatever the native framework is doing.

`next_task` takes a deadline and returns to Python periodically even when idle.
It originally blocked indefinitely, which made SIGTERM unreachable: Python runs
signal handlers only while the main thread executes bytecode. Shutdown took
10.00 s — the supervisor's SIGKILL. With the deadline: 0.24 s.

### The collective thread

Separate because NCCL calls block for as long as the slowest rank. Running them
on the executor would make a collective barrier stop the actor answering
anything, which turns a slow peer into an unreachable actor.

## Client

### Connections per peer: 4

HTTP/1.1 has head-of-line blocking. On a single connection, a 10 MB response
stalls every small control message queued behind it — a `/health` probe waits
behind a tensor.

Four is enough to keep control traffic moving without a connection explosion at
32 actors (128 sockets total, which is unremarkable).

### Keep-alive is mandatory

A TCP handshake per call would cost more than a small call itself. Idle
connections are held for 90 s.

### `TCP_NODELAY`

Set on both client and server. Nagle's algorithm buffers small writes waiting
for more data, adding up to 40 ms — which for a control message is the entire
latency budget.

### Retry policy

Only `Backpressure` is retried: linear backoff, `25 ms × min(attempt, 8)`, up to
16 attempts, so roughly 1.8 s of patience.

Backoff is linear rather than exponential because the peer is draining a queue,
not collapsing. Exponential backoff would overshoot a queue that clears in
milliseconds.

Nothing else is retried. A user exception, a lost result and a dead actor are
all facts about state, and resending will not change them. Retrying a stateful
call because it raised would apply it twice.

## Server

`hyper` HTTP/1.1, one task per connection. Handlers are `Handler` implementors
returning `Reply`.

Request bodies use a known `Content-Length` rather than chunked encoding: the
size is known before the write, and a length lets the reader allocate once.

`/health` and `/introspect` return plain JSON so `curl` and the CLI work without
a tinyray client. Everything else carries framed messages.

## Inline threshold

Results at or below `inline_threshold` (256 KiB) could ride back inside the call
acknowledgement, saving a round trip.

**Currently the field exists and the acknowledgement always reports
`inline: false`.** The acknowledgement is sent when the call is *queued*, not
when it completes, so the result does not exist yet. Honouring the threshold
means holding the acknowledgement until completion — which would make
`.remote()` blocking, defeating the point.

Listed here because it is a real gap, not a hidden feature. See
[status](../05-project/status.md).

## Driver-side accounting

Every driver operation records bytes in and out, per peer:

```python
tr.transport_stats()
# {"127.0.0.1:40755": {"requests": 12, "retries": 0, "failures": 0,
#                      "bytes_sent": 4096, "bytes_received": 2048}}
```

This exists because of a specific failure. `wait()` was fetching entire payloads
to answer a yes/no question — 237 ms for a 200 MB result — and every functional
test passed, because the *answer* was right. The cost was invisible.

Counters made cost assertable, and `tests/test_driver_byte_budget.py` asserts a
budget for every driver operation that touches the wire. A meta-test requires
that every such operation *has* a budget, which immediately found three more
that did not.

## Pitfalls

**Retries are invisible without the counters.** A rising `retries` means a peer
is slower than its callers.

**No TLS, no authentication.** tinyray assumes a trusted network. Do not expose
a port outside a cluster.

**The transport is not versioned.** Client and server must be the same release.

**Long-poll fetches hold a connection.** Four in flight to one peer will
saturate its pool; a fifth waits.

## See also

- [protocol.md](../03-reference/protocol.md) — the wire format
- [store-and-queue.md](store-and-queue.md) — where a call lands
- [rust-core.md](rust-core.md) — the GIL measurement behind the thread model
