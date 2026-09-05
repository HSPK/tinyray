"""Receiving: a plain object becomes callable methods.

No decorators, no IDL, no code generation -- public methods are the interface
and their type hints are the schema.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import socket
import threading
import time
import traceback
import typing
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import msgspec

from ._json import dumps, loads
from ._rpc import MAX_BATCH, _batch_request_id

# A caller that announces a body and never sends it pins a thread for as long
# as it cares to: measured 200 such connections holding 203 threads, released
# only when the attacker closed them. Real bodies are under a megabyte.
#
# What this bound is worth, measured since, so nobody has to wonder twice:
# threads stop climbing rather than tracking the connection count, because
# each one leaves after the timeout -- 500 stalled connections settled at 125
# threads, and an ordinary call went on being answered in 1.7ms throughout.
# The declared length is not allocated either: content-length of a terabyte,
# with no body behind it, moved the serving process's RSS not at all.
BODY_TIMEOUT = 15.0

# A caller that has been served once holds its connection open for the next
# call, and that wait has to outlast the client's own idle expiry (httpx is
# told 60s) or the two would race to close the same socket and an ordinary
# call would sometimes land on a socket the server had just dropped.
IDLE_TIMEOUT = 90.0


class CallContext:
    """Who is calling, as they described themselves.

    Declared as a parameter annotation and it is filled in for you:

        def pull_job(self, ctx: tinyray.CallContext) -> dict: ...

    Self-declared, like every identity here -- a member picks its own tenure
    too -- so this is not authentication and must not be used as one. What it
    does buy is that the caller cannot forget to send it, or send the wrong
    one, which is what happens when the same fact travels as an argument.
    """

    __slots__ = ("identity", "pool", "slot", "incarnation", "request_id")

    def __init__(self, identity: str, request_id: str = ""):
        self.identity = identity
        # What the caller called this attempt. Useful in logs on both sides,
        # and the key an application would build idempotency on if it needs
        # one -- tinyray does not keep results, because how long to keep them
        # and what counts as the same call are application questions.
        self.request_id = request_id
        self.pool, _, seat = identity.partition("/")
        seat, _, tenure = seat.partition("#")
        self.slot = int(seat) if seat.isdigit() else None
        self.incarnation = int(tenure) if tenure.isdigit() else 0

    def __repr__(self) -> str:
        return f"<CallContext {self.identity or 'anonymous'}>"


_ABSENT = object()


def scan(obj: Any) -> dict[str, Callable[..., Any]]:
    """Public methods, in declaration order. Leading underscore means private.

    Asks the class before the instance. `getattr` runs descriptors, so simply
    reading every public name evaluates every property -- and serving objects
    in this domain are full of them. Measured on a class with one property:
    it fired once during discovery; one that raises took `join(serves=...)`
    down with it, reporting `RuntimeError: no GPU on this box` from a call the
    application never made; and one returning a callable was published as a
    remote method.

    A property is not a method. Neither is a `cached_property`, and neither is
    data. What they have in common is that the object found on the class is
    not itself callable -- which is nearly the whole test, with two exceptions
    either way: `classmethod` is a method that is not a callable object until
    it is bound, and a *class* is a callable object that is not a method.

    A class bound on the served class -- a nested one, an imported enum, a
    dataclass kept as `Request = SomeDataclass` -- used to be published like
    any other method. Measured on a class holding three of them: all three
    appeared in the list. That list rides on every heartbeat, is stored per
    pool, is what every peer is shown, and has a size limit, and calling one
    of them builds an object on the far side that then fails to encode. None
    of that is what the caller meant by `serves=`.
    """
    out: dict[str, Callable[..., Any]] = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        static = inspect.getattr_static(obj, name, _ABSENT)
        if static is _ABSENT:
            # Nothing on the class or in the instance dict, so `__dir__` and
            # `__getattr__` are answering together: a proxy. Only the instance
            # can say what this is, and there is no descriptor to trip.
            attr = getattr(obj, name, None)
        elif callable(static) or isinstance(static, classmethod):
            attr = getattr(obj, name)
        else:
            continue
        if not callable(attr) or isinstance(attr, type):
            continue
        # 一道就够，而且是这一道：真正被调用的是 `attr`。挡在 `static` 上
        # 那一道写过又删了 —— 实测单独去掉它，两条测试照样绿，因为类属性取出来
        # 还是同一个类。挡在结果上还能顺带管住代理那条路。
        # The name goes in the URL path of every call, so it has to survive
        # being put there. `def 处理(self)` is legal Python and registered
        # fine, and then the call arrived asking for
        # `%E5%A4%84%E7%90%86` and was answered "no such method". A space
        # does the same. A slash or a question mark happen to work today,
        # for the wrong reason -- the path is read back verbatim -- and would
        # stop the moment anything normalised the URL between the two ends.
        if not (name.isascii() and name.isidentifier()):
            raise ValueError(
                f"{type(obj).__name__}.{name} cannot be served: a method name "
                f"is put in the URL of every call, so it has to be an ASCII "
                f"identifier. Rename it, or make it private with a leading "
                f"underscore."
            )
        out[name] = attr
    return out


_SHAPES: dict[Any, tuple[dict[str, Any], inspect.Signature | None]] = {}


def _shape(fn: Callable[..., Any]) -> tuple[dict[str, Any], inspect.Signature | None]:
    """A method's annotations and signature, worked out once.

    Both used to be worked out again on every call: get_type_hints at 3.63 µs
    and signature at 11.26 µs. Kept, they cost nothing per call, which is what
    pays for binding the arguments below -- 2.29 µs against a signature we
    already have.

    Either can be unavailable: an unresolvable forward reference, or a builtin
    with no signature to read. Neither is a reason to refuse the call, so the
    checks that need them are skipped instead.
    """
    # Keyed by the underlying function, which belongs to the class, not by the
    # bound method, which would hold the instance -- and the instance is the
    # object being served. Every instance of a class answers to the same
    # annotations and the same signature anyway, so one entry does for all of
    # them. Measured with the bound method as the key: 20 members left and
    # their 20 objects still held.
    key = getattr(fn, "__func__", fn)
    got = _SHAPES.get(key)
    if got is None:
        try:
            hints = typing.get_type_hints(fn)
        except Exception:
            hints = {}
        try:
            sig: inspect.Signature | None = inspect.signature(fn)
        except (TypeError, ValueError):
            sig = None
        got = _SHAPES[key] = (hints, sig)
    return got


def _coerce(
    fn: Callable[..., Any], payload: Any, caller: str = "", request_id: str = ""
) -> tuple[list, dict]:
    """Unpack {"args": [...], "kwargs": {...}} and check it against annotations."""
    if isinstance(payload, dict) and set(payload) <= {"args", "kwargs"}:
        # Checked rather than coerced, because `list(5)` raises a plain
        # TypeError, which is neither of the two this is wrapped in -- it came
        # back as HTTP 500 and reached the caller as OutcomeUnknown, "it may
        # have run in full", for a request whose arguments were never
        # unpacked. Measured: {"args": 5} answered 500 where {"args": "ab"}
        # answered 422. A malformed envelope is the caller's mistake and
        # nothing has run, exactly like an argument that does not fit.
        given_args, given_kwargs = payload.get("args"), payload.get("kwargs")
        if given_args is not None and not isinstance(given_args, list):
            raise msgspec.ValidationError(
                f"'args' has to be an array, got {type(given_args).__name__}"
            )
        if given_kwargs is not None and not isinstance(given_kwargs, dict):
            raise msgspec.ValidationError(
                f"'kwargs' has to be an object, got {type(given_kwargs).__name__}"
            )
        args = list(given_args or [])
        kwargs = dict(given_kwargs or {})
    elif isinstance(payload, dict):
        args, kwargs = [], dict(payload)  # curl shorthand: a bare object is kwargs
    elif isinstance(payload, list):
        args, kwargs = list(payload), {}  # curl shorthand: a bare array is args
    else:
        args, kwargs = [payload], {}

    hints, sig = _shape(fn)
    if sig is None:
        return args, kwargs
    kinds = inspect.Parameter
    injected = {name for name in sig.parameters if hints.get(name) is CallContext}
    for name in injected:
        if sig.parameters[name].kind in (kinds.VAR_POSITIONAL, kinds.VAR_KEYWORD):
            raise msgspec.ValidationError("CallContext needs a named, non-variadic parameter")
    # Bind the caller's signature before injecting anything, preserving Python's
    # positional-only, variadic and duplicate-argument rules.
    public = (
        sig.replace(parameters=[p for p in sig.parameters.values() if p.name not in injected])
        if injected
        else sig
    )
    try:
        bound = public.bind(*args, **{k: v for k, v in kwargs.items() if k not in injected})
    except TypeError as e:
        # The arguments do not fit the signature, so nothing has run. That is
        # the caller's mistake and goes back the same way a type mismatch
        # does. Only the type mismatch used to: too many arguments, a missing
        # one, a keyword the method has no parameter for and one value given
        # twice all came back as RemoteError -- which says the method ran and
        # raised, and which `except TypeError` does not catch.
        raise msgspec.ValidationError(str(e)) from None

    for name, value in bound.arguments.items():
        want = hints.get(name)
        if want is None:
            continue
        kind = sig.parameters[name].kind
        if kind is kinds.VAR_POSITIONAL:
            bound.arguments[name] = tuple(msgspec.convert(v, want, strict=False) for v in value)
        elif kind is kinds.VAR_KEYWORD:
            bound.arguments[name] = {
                k: msgspec.convert(v, want, strict=False) for k, v in value.items()
            }
        else:
            bound.arguments[name] = msgspec.convert(value, want, strict=False)

    if injected:
        # Defaults fill positional gaps before an injected parameter, but are
        # Python values already: only caller-provided JSON is coerced above.
        bound.apply_defaults()
        context = CallContext(caller or "", request_id)
        bound = inspect.BoundArguments(
            sig,
            {
                name: context if name in injected else bound.arguments[name]
                for name in sig.parameters
            },
        )
    return list(bound.args), bound.kwargs


class Counters:
    """What the serving side has been asked to do, and how much of it it refused.

    Exists so the question "do these long calls need a transport of their own"
    can be settled with numbers. `max_concurrency` bounds pile-up, but it does
    not separate control traffic from data traffic: once every slot is held,
    a control call is refused exactly like any other. Whether that is happening
    is not something to have an opinion about.

    Plain ints under a lock rather than a metrics library: this is a membership
    layer, and whoever wants histograms can build them from `calls` and
    `busy_ns`. Batches count as one request, including one failure if any item
    fails, and hold one concurrency slot until their complete reply is written.
    Invalid admitted batch bodies count as failed requests, like single calls.
    """

    __slots__ = ("calls", "refused", "failed", "in_flight", "peak_in_flight", "busy_ns", "_lock")

    def __init__(self) -> None:
        self.calls = 0
        self.refused = 0
        self.failed = 0
        self.in_flight = 0
        self.peak_in_flight = 0
        self.busy_ns = 0
        self._lock = threading.Lock()

    def entered(self) -> None:
        with self._lock:
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)

    def answered(self, failed: bool) -> None:
        """Recorded before the answer is written, not after.

        A caller with the answer in its hands then reads stats() and must see
        that call. Counted afterwards it sometimes did not: measured at 0.2% of
        ordinary calls and 0.8% of raising ones, which is a coin toss for
        anything asserting on it and a wrong number for anything reading it.
        """
        with self._lock:
            self.calls += 1
            self.failed += failed

    def left(self, spent_ns: int) -> None:
        """The rest, once the answer really is out.

        `in_flight` counts handlers still running and `busy_ns` how long they
        ran, and writing the answer is part of both -- a 16 MiB reply keeps a
        thread busy. So these two stay on this side of the write even though
        the count does not.
        """
        with self._lock:
            self.in_flight -= 1
            self.busy_ns += spent_ns

    def refuse(self) -> None:
        with self._lock:
            self.refused += 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "calls": self.calls,
                "refused": self.refused,
                "failed": self.failed,
                "in_flight": self.in_flight,
                "peak_in_flight": self.peak_in_flight,
                "busy_ms": self.busy_ns // 1_000_000,
            }


class _Server(ThreadingHTTPServer):
    """Carries what the handler needs. Declared rather than attached on the
    fly, so a rename is a type error instead of an AttributeError at request
    time."""

    daemon_threads = True
    # A worker fleet starts all at once, so the first RPC connections arrive as
    # one burst, and socketserver's default backlog of 5 is nowhere near it.
    # An overflowing accept queue is dropped, not refused, so the caller waits
    # out a SYN retransmit: measured with 64 simultaneous connects, 55 of them
    # over 100ms, p50 1013.70ms and a worst case of 2038ms -- against 5.47ms
    # with room in the queue. The knee sits exactly at the size of the burst,
    # because accepting means spawning a thread and the loop cannot drain that
    # fast, so 400 peers overflow a backlog of 128 just the same (p50
    # 1089.84ms). The burst is however many peers there are, which is not
    # something this process can know, so ask for as much as the kernel is
    # willing to give -- it caps this at net.core.somaxconn regardless.
    request_queue_size = socket.SOMAXCONN
    dispatch: dict[str, Callable[..., Any]]
    identity: str
    still_ours: Callable[[], bool]
    loop: asyncio.AbstractEventLoop | None
    slots: threading.Semaphore | None
    counters: Counters

    def __init__(self, *a: Any, **k: Any):
        # Keep-alive means a handler thread sits in a read waiting for the
        # caller's next request, and closing the listening socket does not
        # disturb it. When it ends is then the caller's business, and a worker
        # taking one job after another pays for that: measured at 30 rounds of
        # join, one call, leave, and 30 threads, 30 servers and 60 descriptors
        # left behind, against a default limit of 1024. They have to be reached
        # to be ended, so they are written down.
        self.live: set[socket.socket] = set()
        super().__init__(*a, **k)

    def process_request(self, request: Any, client_address: Any) -> None:
        self.live.add(request)
        super().process_request(request, client_address)

    def shutdown_request(self, request: Any) -> None:
        self.live.discard(request)
        super().shutdown_request(request)


class _Handler(BaseHTTPRequestHandler):
    server: _Server
    protocol_version = "HTTP/1.1"  # keep-alive; without it every call burns a socket
    server_version = "tinyray/0.1"
    # The status line, the headers and the body are three separate writes.
    # With Nagle on, the last one waits for the peer's delayed ACK and every
    # call costs an extra 40ms.
    disable_nagle_algorithm = True
    _headers_buffer: list[bytes]

    def setup(self) -> None:
        super().setup()
        # The first read is the one a silent connection never returns from, and
        # only the *body* was bounded before, so the same attack one stage
        # earlier was free: measured 100 connections that said nothing at all
        # and 100 that sent half a header, holding 200 threads that were still
        # there after 16s -- against a body-stalling attack that self-limits at
        # 125 threads for 500 connections. Read here rather than declared as
        # the class's `timeout`, so there is one knob and it is the same one.
        self.connection.settimeout(BODY_TIMEOUT)

    def log_message(self, *args: Any) -> None:  # keep the test output readable
        pass

    def _send(self, code: int, body: dict) -> None:
        code, raw, _ = self._encode_reply(code, body)
        self._send_raw(code, raw)

    @staticmethod
    def _encode_reply(code: int, body: dict) -> tuple[int, bytes, bool]:
        try:
            return code, dumps(body), False
        except (TypeError, ValueError, RecursionError) as e:
            # A method returning something JSON cannot carry used to kill the
            # handler thread, and the caller saw a reset connection -- which
            # reads as "the peer died" rather than "your return value".
            raw = dumps(
                {
                    "error": {
                        "type": "TypeError",
                        "message": f"the return value cannot be sent as JSON: {e}",
                        "traceback": "",
                    }
                }
            )
            return 200, raw, True

    def _send_raw(self, code: int, raw: bytes) -> None:
        if self.close_connection:
            # Keep refusal replies small despite the required close header.
            # The optional server banner adds no useful information here.
            self.send_response_only(code)
            self.send_header("date", self.date_time_string())
        else:
            self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        if self.close_connection:
            self.send_header("connection", "close")
        # Buffer the head and send it together with the body: one write, one
        # segment, no interaction with delayed ACK.
        self._headers_buffer.append(b"\r\n")
        self.wfile.write(b"".join(self._headers_buffer) + raw)
        self._headers_buffer = []

    def do_GET(self) -> None:
        if self.path == "/_methods":
            self._send(
                200, {"methods": sorted(self.server.dispatch), "identity": self.server.identity}
            )
        else:
            self._send(404, {})

    def handle_one_request(self) -> None:
        # A handler that raises answers nothing and the caller sees a reset,
        # which is indistinguishable from the process dying.
        try:
            super().handle_one_request()
        except Exception as e:
            self.close_connection = True
            try:
                self._send(500, {"error": f"{type(e).__name__}: {e}"})
            except Exception:
                pass
        # Past this point the connection has been used, so waiting on it is
        # keep-alive rather than an opening that never came, and it gets the
        # longer budget.
        try:
            self.connection.settimeout(IDLE_TIMEOUT)
        except OSError:
            pass

    def do_POST(self) -> None:
        # Read the body before anything else can return. An early reply that
        # leaves it in the socket desynchronises keep-alive, and the next
        # request parses from the leftovers: measured as a fenced call and a
        # good one alternating, every second call coming back with an empty
        # body. Fencing and routing used to answer above this point.
        #
        # A chunked body reaches that same failure by another road: it carries
        # no content-length, so the read below sized itself to zero and left
        # the whole body in the socket. Measured: echo(x=7) answered as echo()
        # -- a wrong result with a 200 on it -- and the chunk framing then
        # parsed as the next request line, ending the connection with an HTML
        # 400 that the next request never got past. Decoding chunked properly
        # means chunk extensions and trailers, which is more framing code than
        # a control plane capped at a megabyte should carry, so it is refused
        # in the one way HTTP has for exactly this. The connection goes with
        # it: the framing bytes are still unread, so it can never be reused.
        if "chunked" in (self.headers.get("transfer-encoding") or "").lower():
            self.close_connection = True
            return self._send(
                411,
                {"error": "chunked bodies are not read; send a content-length"},
            )
        try:
            length = int(self.headers.get("content-length") or 0)
        except ValueError:
            self.close_connection = True
            return self._send(400, {"error": "content-length is not a number"})
        if length < 0:
            self.close_connection = True
            return self._send(400, {"error": "content-length is negative"})
        raw = b"{}"
        if length:
            self.connection.settimeout(BODY_TIMEOUT)
            try:
                raw = self.rfile.read(length)
            except (TimeoutError, OSError):
                # The stream cannot be resynchronised after a partial body.
                self.close_connection = True
                return self._send(408, {"error": "body never arrived"})
            finally:
                self.connection.settimeout(IDLE_TIMEOUT)

        if length and len(raw) != length:
            self.close_connection = True
            return self._send(400, {"error": "body is shorter than content-length"})

        batching = self.path == "/_batch"
        if not batching and not self.path.startswith("/call/"):
            return self._send(404, {})
        name = self.path[len("/call/") :]

        # Fencing lives here, not at the call site: a check fifteen call sites
        # must remember is one fourteen of them forget.
        #
        # Two ways to be stale. The caller may hold an address a later tenure
        # now answers on -- that is the identity check. Or the seat may have
        # moved on while this process kept running and listening, in which case
        # nothing about the request looks wrong and only we know we are a ghost.
        fenced = self._fenced()
        if fenced is not None:
            return self._send(409, fenced)

        fn = None if batching else self.server.dispatch.get(name)
        if not batching and fn is None:
            return self._send(404, {"error": f"no method {name!r}"})

        # A thread per connection with nothing bounding it: a hundred workers
        # all pulling at once is a hundred threads, and the ones that lose the
        # race still hold their slot while they wait. Saying no is bounded, and
        # saying no *here* -- after the body, before the method -- is what
        # makes it safe to retry: nothing ran.
        slots = self.server.slots
        counters = self.server.counters
        if slots is not None and not slots.acquire(blocking=False):
            counters.refuse()
            return self._send(503, {"error": "at the concurrency limit"})
        counters.entered()
        started = time.perf_counter_ns()
        counted = False
        try:
            if batching:
                code, encoded, failed = self._dispatch_batch(raw)
            else:
                assert fn is not None
                code, body, failed = self._dispatch(fn, name, raw)
                code, encoded, encoding_failed = self._encode_reply(code, body)
                failed = failed or encoding_failed
            counters.answered(failed)
            counted = True
            self._send_raw(code, encoded)
        finally:
            # _dispatch answers for anything the method itself can do. Getting
            # here without having counted means something above the method came
            # apart, and that is still a call that failed.
            if not counted:
                counters.answered(True)
            counters.left(time.perf_counter_ns() - started)
            if slots is not None:
                slots.release()

    def _fenced(self) -> dict | None:
        if not self.server.still_ours():
            return {"error": "superseded", "identity": self.server.identity}
        target = self.headers.get("x-tinyray-target")
        if target and target != self.server.identity:
            return {"error": "fenced", "identity": self.server.identity}
        return None

    def _dispatch_batch(self, raw: bytes) -> tuple[int, bytes, bool]:
        try:
            payload = loads(raw)
        except (ValueError, RecursionError) as exc:
            return 400, dumps({"error": str(exc)}), True
        if (
            not isinstance(payload, dict)
            or set(payload) != {"calls"}
            or not isinstance(payload["calls"], list)
        ):
            return 400, dumps({"error": "a batch needs a 'calls' array"}), True
        calls = payload["calls"]
        if len(calls) > MAX_BATCH:
            return 413, dumps({"error": f"a batch can contain at most {MAX_BATCH} calls"}), True
        # Validate every envelope before any method runs. Signature and method
        # lookup failures, unlike malformed envelopes, belong to individual items.
        for item in calls:
            if not isinstance(item, dict) or set(item) != {"method", "args", "kwargs"}:
                return 400, dumps({"error": "each batch item needs method, args and kwargs"}), True
            name = item["method"]
            if (
                not isinstance(name, str)
                or not name.isascii()
                or not name.isidentifier()
                or name.startswith("_")
                or not isinstance(item["args"], list)
                or not isinstance(item["kwargs"], dict)
            ):
                return 400, dumps({"error": "invalid batch method or argument envelope"}), True
        replies: list[bytes] = []
        batch_id = self.headers.get("x-tinyray-request") or ""
        failed = False
        for index, item in enumerate(calls):
            fenced = self._fenced()
            name = item["method"]
            fn = self.server.dispatch.get(name)
            if fenced is not None:
                code, body, failed = 409, fenced, True
            elif fn is None:
                code, body, failed = 404, {"error": f"no method {name!r}"}, True
            else:
                code, body, failed = self._dispatch_payload(
                    fn,
                    name,
                    {"args": item["args"], "kwargs": item["kwargs"]},
                    _batch_request_id(batch_id, index),
                )
            # Freeze each result before the next method can mutate it, and
            # stop on serialization failure before any later item is executed.
            code, encoded, encoding_failed = self._encode_reply(code, body)
            replies.append(b'{"status":' + str(code).encode() + b',"body":' + encoded + b"}")
            if failed or encoding_failed:
                failed = True
                break
        return 200, b'{"items":[' + b",".join(replies) + b"]}", failed

    def _dispatch(self, fn: Callable[..., Any], name: str, raw: bytes) -> tuple[int, dict, bool]:
        """The answer to give, and whether the call failed however it failed.

        Worked out rather than written: the caller records the call and only
        then puts the answer on the wire, so anyone holding an answer can see
        it counted. One place writes instead of four.
        """
        try:
            payload = loads(raw or b"{}")
        except json.JSONDecodeError as e:
            return 400, {"error": str(e)}, True
        return self._dispatch_payload(
            fn, name, payload, self.headers.get("x-tinyray-request") or ""
        )

    def _dispatch_payload(
        self, fn: Callable[..., Any], name: str, payload: Any, request_id: str
    ) -> tuple[int, dict, bool]:
        try:
            args, kwargs = _coerce(
                fn,
                payload,
                self.headers.get("x-tinyray-caller") or "",
                request_id,
            )
        except msgspec.ValidationError as e:
            # A type mismatch is the caller's fault, so it is reported as one
            # rather than dressed up as a business failure.
            return 422, {"error": f"{name}(): {e}"}, True

        try:
            result = fn(*args, **kwargs)
            if inspect.iscoroutine(result):
                loop = self.server.loop
                if loop is None or not loop.is_running():
                    result = asyncio.run(result)
                else:
                    # Run on the loop that existed at join() time, while it is
                    # still turning. Never make a new one to run alongside it:
                    # the user's clients are bound to theirs.
                    #
                    # What matters is that it is running *now*, not that it
                    # existed at join(). A member that joined inside
                    # asyncio.run() keeps serving after that block ends, and
                    # handing work to a loop nobody turns any more never comes
                    # back -- result() has no timeout. Measured with
                    # max_concurrency=2: two calls to an async method and the
                    # member answered nothing again, ever, sync methods
                    # included, while still registered and still beating. A
                    # loop that has been closed outright was no better, telling
                    # the caller its method raised "Event loop is closed" when
                    # the method had not run at all.
                    #
                    # A loop that stops between this check and the handover
                    # still strands that one call, but only that one: the next
                    # takes the branch above.
                    result = asyncio.run_coroutine_threadsafe(result, loop).result()
        except Exception as exc:
            return (
                200,
                {
                    "error": {
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "traceback": traceback.format_exc(),
                    }
                },
                True,
            )
        return 200, {"result": result}, False


class MethodServer:
    """One small server per process, started only when serves= is given."""

    def __init__(
        self,
        obj: Any,
        identity: str,
        host: str = "0.0.0.0",
        max_concurrency: int | None = None,
    ):
        self.still_ours: Callable[[], bool] = lambda: True
        self.dispatch = scan(obj)
        try:
            loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        slots = None if max_concurrency is None else threading.Semaphore(max_concurrency)
        # Port 0: the kernel picks, and the actual one goes into the record.
        # One less configuration knob, and no port collisions.
        self._srv = _Server((host, 0), _Handler)
        self._srv.dispatch = self.dispatch
        self._srv.identity = identity
        self._srv.still_ours = lambda: self.still_ours()
        self._srv.loop = loop
        self._srv.slots = slots
        self.counters = Counters()
        self._srv.counters = self.counters
        self.limit = max_concurrency
        self.port = self._srv.server_address[1]
        self._thread = threading.Thread(target=self._srv.serve_forever, daemon=True)
        self._thread.start()

    @property
    def methods(self) -> list[str]:
        return sorted(self.dispatch)

    def url(self, advertise: str) -> str:
        return f"http://{advertise}:{self.port}"

    def close(self) -> None:
        self._srv.shutdown()
        self._srv.server_close()
        for conn in list(self._srv.live):
            # Shut the read side down and the handler's blocked read returns
            # empty, which ends it the same way a caller hanging up would.
            with contextlib.suppress(OSError):
                conn.shutdown(socket.SHUT_RDWR)
        # Closing the listening socket does not end a handler already parked
        # on a keep-alive connection, and that handler holds the server, the
        # dispatch table and through it the served object -- a model or a
        # dataset. It only lets go when the caller's client drops the
        # connection, which is its own business: measured at 20 members left
        # and 20 objects still held, still held a minute later.
        #
        # The lookup is what has to go, not the thread. Anything arriving now
        # is answered 409 by the fencing check above it, so an empty table
        # costs nothing and the object is free the moment we leave.
        self.dispatch.clear()
        self._srv.dispatch = {}
