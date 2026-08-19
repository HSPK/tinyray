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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

import msgspec

MAX_BODY = 1 << 20  # Control plane only. Bigger payloads use .url instead.


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


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive; without it every call burns a socket
    server_version = "tinyray/0.1"
    # The status line, the headers and the body are three separate writes.
    # With Nagle on, the last one waits for the peer's delayed ACK and every
    # call costs an extra 40ms.
    disable_nagle_algorithm = True

    def log_message(self, *args: Any) -> None:  # keep the test output readable
        pass

    def _send(self, code: int, body: dict) -> None:
        raw = json.dumps(body).encode()
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
            self._send(200, {"methods": sorted(self.server.dispatch), "identity": self.server.identity})
        else:
            self._send(404, {})

    def do_POST(self) -> None:
        if not self.path.startswith("/call/"):
            return self._send(404, {})
        name = self.path[len("/call/") :]

        # Fencing lives here, not at the call site: a check fifteen call sites
        # must remember is one fourteen of them forget.
        target = self.headers.get("x-tinyray-target")
        if target and target != self.server.identity:
            return self._send(409, {"error": "fenced", "identity": self.server.identity})

        length = int(self.headers.get("content-length") or 0)
        if length > MAX_BODY:
            return self._send(413, {"error": "payload too large"})
        raw = self.rfile.read(length) if length else b"{}"

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
            if inspect.isawaitable(result):
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
        self.dispatch = scan(obj)
        try:
            loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        # Port 0: the kernel picks, and the actual one goes into the record.
        # One less configuration knob, and no port collisions.
        self._srv = ThreadingHTTPServer((host, 0), _Handler)
        self._srv.dispatch = self.dispatch
        self._srv.identity = identity
        self._srv.loop = loop
        self._srv.daemon_threads = True
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
