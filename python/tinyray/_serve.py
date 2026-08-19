"""The receiving half: turn a plain object into callable methods.

No decorators, no IDL, no code generation. Public methods are the interface and
their type hints are the schema.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

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


def _coerce(fn: Callable[..., Any], payload: Any) -> tuple[list, dict]:
    """Positional args come as a list, keyword args as an object."""
    if isinstance(payload, dict):
        return [], payload
    if isinstance(payload, list):
        return payload, {}
    return [payload], {}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "tinyray/0.1"

    def log_message(self, *args: Any) -> None:  # keep the test output readable
        pass

    def _send(self, code: int, body: dict) -> None:
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

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
        # must remember is a check fourteen of them forget.
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
    """One small HTTP server per process, started only when serves= is given."""

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
