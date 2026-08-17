# Protocol

## Purpose

What crosses the wire: the framing, the HTTP endpoints, the message envelopes
and the error taxonomy. Read this to debug with `tcpdump`, or to write a client
in another language.

## Framing

Every message is a small header plus N out-of-band frames.

```
offset 0        magic         b"TRY1"          4 bytes
offset 4        header_len    u32 big endian   4 bytes
offset 8        n_frames      u32 big endian   4 bytes
offset 12       frame_sizes   u32 big endian × n_frames
offset 12+4n    header        msgpack, header_len bytes
then            frames        concatenated, sizes as declared
```

Content type: `application/x-tinyray`.

Two decisions worth explaining.

**Frame sizes live in the fixed prefix, not in the msgpack header.** The framing
layer therefore never parses msgpack, which keeps it purely mechanical and means
a corrupt header cannot desynchronise the byte stream.

**Frames are out-of-band because of pickle protocol 5.** Large tensor buffers
are handed to us separately from the small pickle body and travel to the socket
without being concatenated or copied. Inlining them would memcpy every tensor
through the pickle stream — measured at the time: a 10 MB array produced a
400,153-byte body instead of 135 bytes.

### Limits

Decoding is bounded, because the decoder allocates based on values read off the
wire.

| Limit | Default |
|---|---|
| `max_header_len` | 1 MiB |
| `max_frames` | 4096 |
| `max_frame_len` | 4 GiB |
| `max_message_len` | 8 GiB |

Violations are fatal: a binary framing has no resynchronisation point, so the
decoder poisons itself and the connection must be closed.

## Endpoints

### Actor and served process

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/actor/call` | Submit a call. Returns an acknowledgement |
| `POST` | `/task/fetch` | Fetch a result. Long-polls until ready |
| `POST` | `/task/release` | Drop a result |
| `GET` | `/health` | `{"status":"ok","actor":"...","shutting_down":false}` |
| `GET` | `/introspect` | Queues, store, inflight method, stuck callers |
| `POST` | `/shutdown` | Stop accepting and fail what is queued |

`/health` and `/introspect` are plain JSON, so `curl` works. The rest carry
framed messages.

## Envelopes

The msgpack header is one of:

### `Call`

```
task_id      TaskId      identifies the result
actor_id     ActorId     rejected if it does not match
caller_id    CallerId    for per-caller ordering
seq          u64         monotonic per (caller, actor)
method       string
want_inline  bool        currently always answered false
```

Frames: `[pickle_body, *out_of_band_buffers]`.

### `CallAck`

```
task_id      TaskId
inline       bool
```

Sent when the call is *queued*, not when it completes. That is why `inline` is
always false: the result does not exist yet.

Also used to answer a fetch that is not ready, meaning "ask again".

### `Fetch`

```
task_id      TaskId
timeout_ms   u64      how long the owner may hold the request open
status_only  bool     answer without sending the payload
```

`status_only` is what `wait` uses. Without it, a readiness question would drag
every payload to the driver and discard it — 32 rollouts of 10 MB to answer
yes or no. Measured before the fix: 237 ms for a settled 200 MB result. After:
0.14 ms.

### `Result`

```
task_id      TaskId
```

Frames: `[pickle_body, *buffers]`, or empty for a `status_only` probe.

### `Error`

```
task_id      TaskId
kind         ErrorKind
message      string
traceback    string?      the remote Python traceback
```

The traceback travels on the wire because in a distributed run it is usually the
only useful artefact.

## Ordering

HTTP gives no ordering guarantee, and tinyray keeps four connections per peer,
so concurrent calls arrive in arbitrary order. Each call carries a monotonic
`seq` per `(caller, actor)` pair; the actor buffers arrivals that overtake their
predecessors and dispatches in order.

Different callers are independent, which matches Ray and stops one slow caller
blocking the rest.

A repeated `seq` is refused with `DuplicateSeq` and acknowledged rather than
executed — replaying a stateful call would corrupt state.

## Error taxonomy

| `ErrorKind` | Python exception | Retryable |
|---|---|---|
| `UserException` | `UserCodeError` | no |
| `ObjectLost` | `ObjectLost` | no |
| `ActorDied` | `ActorDied` | no |
| `NotFound` | `NotFound` | no |
| `Backpressure` | `Backpressure` | **yes** |
| `Internal` | `RemoteCallError` | no |

Backpressure is the only one retried automatically. It is the only failure where
resending the identical request is safe: a stateful call must not be replayed
just because it raised.

HTTP status `429` also signals backpressure, carrying the queue depth and limit.

## Identifiers

128-bit, rendered as 32 lowercase hex characters. The high half is a
per-process seed derived from pid and wall clock; the low half is a monotonic
counter.

Parsing is strict: exactly 32 hex digits. A leading `+` is rejected, because
`from_str_radix` accepts it and would give one identifier two spellings.

## Transport behaviour

- **Keep-alive is mandatory.** A TCP handshake per call would cost more than the
  call.
- **Four connections per peer** by default. HTTP/1.1 has head-of-line blocking,
  so a 10 MB response on a single connection stalls every small control message
  behind it.
- `TCP_NODELAY` is set: Nagle would add up to 40 ms to a small message.
- Bodies use a known `Content-Length`, not chunked encoding.
- Only backpressure is retried, with linear backoff capped at eight steps.

## Pitfalls

**A framing error is not recoverable.** The decoder poisons itself deliberately.
Close the connection.

**`CallAck` means queued, not done.** Do not treat it as completion.

**`status_only` fetches return a `Result` with no frames.** That is not an empty
result; check the flag you sent.

**The wire format is not stable.** There is no version negotiation beyond the
magic bytes. Client and server must be the same release.

## See also

- [03-transport.md](../04-internals/03-transport.md) — the implementation
- [02-store-and-queue.md](../04-internals/02-store-and-queue.md) — what happens after a call arrives
- [01-api-python.md](01-api-python.md) — the Python side
