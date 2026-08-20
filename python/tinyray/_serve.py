"""Receiving: a plain object becomes callable methods.

No decorators, no IDL, no code generation -- public methods are the interface
and their type hints are the schema.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import threading
import traceback
import typing
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import msgspec

# A caller that announces a body and never sends it pins a thread for as long
# as it cares to: measured 200 such connections holding 203 threads, released
# only when the attacker closed them. Real bodies are under a megabyte.
BODY_TIMEOUT = 15.0


def scan(obj: Any) -> dict[str, Callable[..., Any]]:
    """Public methods, in declaration order. Leading underscore means private."""
    out: dict[str, Callable[..., Any]] = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        attr = getattr(obj, name)
        if callable(attr):
            out[name] = attr
    return out


def _hints(fn: Callable[..., Any]) -> dict[str, Any]:
    try:
        return typing.get_type_hints(fn)
    except Exception:  # unresolvable forward ref: skip checking rather than fail
        return {}


def _coerce(fn: Callable[..., Any], payload: Any) -> tuple[list, dict]:
    """Unpack {"args": [...], "kwargs": {...}} and check it against annotations."""
    if isinstance(payload, dict) and set(payload) <= {"args", "kwargs"}:
        args = list(payload.get("args") or [])
        kwargs = dict(payload.get("kwargs") or {})
    elif isinstance(payload, dict):
        args, kwargs = [], dict(payload)  # curl shorthand: a bare object is kwargs
    elif isinstance(payload, list):
        args, kwargs = list(payload), {}  # curl shorthand: a bare array is args
    else:
        args, kwargs = [payload], {}

    hints = _hints(fn)
    if not hints:
        return args, kwargs
    try:
        names = [p for p in inspect.signature(fn).parameters]
    except (TypeError, ValueError):
        return args, kwargs

    for i, value in enumerate(args):
        if i < len(names) and names[i] in hints:
            args[i] = msgspec.convert(value, hints[names[i]], strict=False)
    for key, value in kwargs.items():
        if key in hints:
            kwargs[key] = msgspec.convert(value, hints[key], strict=False)
    return args, kwargs


class _Server(ThreadingHTTPServer):
    """Carries what the handler needs. Declared rather than attached on the
    fly, so a rename is a type error instead of an AttributeError at request
    time."""

    daemon_threads = True
    dispatch: dict[str, Callable[..., Any]]
    identity: str
    still_ours: Callable[[], bool]
    loop: asyncio.AbstractEventLoop | None


class _Handler(BaseHTTPRequestHandler):
    server: _Server
    protocol_version = "HTTP/1.1"  # keep-alive; without it every call burns a socket
    server_version = "tinyray/0.1"
    # The status line, the headers and the body are three separate writes.
    # With Nagle on, the last one waits for the peer's delayed ACK and every
    # call costs an extra 40ms.
    disable_nagle_algorithm = True
    _headers_buffer: list[bytes]

    def log_message(self, *args: Any) -> None:  # keep the test output readable
        pass

    def _send(self, code: int, body: dict) -> None:
        try:
            raw = json.dumps(body).encode()
        except (TypeError, ValueError) as e:
            # A method returning something JSON cannot carry used to kill the
            # handler thread, and the caller saw a reset connection -- which
            # reads as "the peer died" rather than "your return value".
            code = 200
            raw = json.dumps(
                {
                    "error": {
                        "type": "TypeError",
                        "message": f"the return value cannot be sent as JSON: {e}",
                        "traceback": "",
                    }
                }
            ).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
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
                self.connection.settimeout(None)

        if not self.path.startswith("/call/"):
            return self._send(404, {})
        name = self.path[len("/call/") :]

        # Fencing lives here, not at the call site: a check fifteen call sites
        # must remember is one fourteen of them forget.
        #
        # Two ways to be stale. The caller may hold an address a later tenure
        # now answers on -- that is the identity check. Or the seat may have
        # moved on while this process kept running and listening, in which case
        # nothing about the request looks wrong and only we know we are a ghost.
        if not self.server.still_ours():
            return self._send(409, {"error": "superseded", "identity": self.server.identity})
        target = self.headers.get("x-tinyray-target")
        if target and target != self.server.identity:
            return self._send(409, {"error": "fenced", "identity": self.server.identity})

        fn = self.server.dispatch.get(name)
        if fn is None:
            return self._send(404, {"error": f"no method {name!r}"})
        try:
            args, kwargs = _coerce(fn, json.loads(raw or b"{}"))
        except json.JSONDecodeError as e:
            return self._send(400, {"error": str(e)})
        except msgspec.ValidationError as e:
            # A type mismatch is the caller's fault, so it is reported as one
            # rather than dressed up as a business failure.
            return self._send(422, {"error": f"{name}(): {e}"})

        try:
            result = fn(*args, **kwargs)
            if inspect.iscoroutine(result):
                loop = self.server.loop
                if loop is None:
                    result = asyncio.run(result)
                else:
                    # Run on the loop that existed at join() time. Never make a
                    # new one: the user's clients are bound to theirs.
                    result = asyncio.run_coroutine_threadsafe(result, loop).result()
        except Exception as exc:
            return self._send(
                200,
                {
                    "error": {
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "traceback": traceback.format_exc(),
                    }
                },
            )
        self._send(200, {"result": result})


class MethodServer:
    """One small server per process, started only when serves= is given."""

    def __init__(self, obj: Any, identity: str, host: str = "0.0.0.0"):
        self.still_ours: Callable[[], bool] = lambda: True
        self.dispatch = scan(obj)
        try:
            loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        # Port 0: the kernel picks, and the actual one goes into the record.
        # One less configuration knob, and no port collisions.
        self._srv = _Server((host, 0), _Handler)
        self._srv.dispatch = self.dispatch
        self._srv.identity = identity
        self._srv.still_ours = lambda: self.still_ours()
        self._srv.loop = loop
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
