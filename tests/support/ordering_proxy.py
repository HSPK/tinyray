"""Delay native registry frames without changing their contents."""

from __future__ import annotations

import contextlib
import select
import socket
import threading
import time

from tests.support.registry_wire import decode_envelope, read_exact, read_frame


def _framed(payload: bytes) -> bytes:
    return len(payload).to_bytes(4, "big") + payload


class OrderingProxy:
    def __init__(
        self,
        target: str,
        *,
        hold_startup: bool = False,
        header_delay: float = 0,
        body_delay: float = 0,
        reply_gate: threading.Event | None = None,
    ):
        host, port = target.rsplit(":", 1)
        self.target = host, int(port)
        self.hold_startup = hold_startup
        self.header_delay = header_delay
        self.body_delay = body_delay
        self.reply_gate = reply_gate
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.release = threading.Event()
        self.canceled = threading.Event()
        self.forwarded = threading.Event()
        self.reset_forwarded = threading.Event()
        self.arm_reply = threading.Event()
        self.reply_held = threading.Event()
        self.selected: int | None = None
        self.startup: dict | None = None
        self.response: int | None = None
        self.opened = 0
        self.requests: list[tuple[float, dict]] = []
        self.sockets: list[socket.socket] = []
        self.threads: list[threading.Thread] = []
        self.server = socket.socket()
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(32)
        self.server.settimeout(0.1)
        self.sockets.append(self.server)
        self.endpoint = f"127.0.0.1:{self.server.getsockname()[1]}"
        self._launch(self._accept)

    def _launch(self, fn, *args) -> None:
        thread = threading.Thread(target=fn, args=args, daemon=True)
        self.threads.append(thread)
        thread.start()

    def _accept(self) -> None:
        connection = 0
        while not self.stop.is_set():
            try:
                downstream, _ = self.server.accept()
            except (TimeoutError, OSError):
                continue
            with self.lock:
                self.opened += 1
            self._launch(self._serve, connection, downstream)
            connection += 1

    def _serve(self, connection: int, downstream: socket.socket) -> None:
        upstream = None
        try:
            upstream = socket.create_connection(self.target, timeout=2)
            upstream.settimeout(None)
            downstream.settimeout(None)
            with self.lock:
                self.sockets.extend([downstream, upstream])
            for sock in (downstream, upstream):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            while not self.stop.is_set():
                request_payload = read_frame(downstream)
                envelope = decode_envelope(request_payload)
                beat = envelope.get("payload") if envelope.get("operation") == "beat" else None
                hold = False
                with self.lock:
                    if self.hold_startup and self.selected is None and isinstance(beat, dict):
                        self.selected = connection
                        self.startup = beat
                        hold = True

                if hold:
                    while not self.release.wait(0.01):
                        readable, _, _ = select.select([downstream], [], [], 0)
                        if readable and downstream.recv(1) == b"":
                            self.canceled.set()
                            break
                    self.release.wait(5)
                    upstream.sendall(_framed(request_payload))
                    with self.lock:
                        self.requests.append((time.monotonic(), beat))
                    self.forwarded.set()
                    if self.canceled.is_set():
                        self.reset_forwarded.set()
                        return

                if not hold:
                    upstream.sendall(_framed(request_payload))
                    if isinstance(beat, dict):
                        with self.lock:
                            self.requests.append((time.monotonic(), beat))

                while not self.stop.is_set():
                    readable, _, _ = select.select([upstream, downstream], [], [], 0.1)
                    if downstream in readable:
                        # Serial protocol: before the reply, readability means
                        # cancellation/close or illegal pipelining.
                        if downstream.recv(1) == b"":
                            upstream.close()
                        return
                    if upstream not in readable:
                        continue
                    prefix = read_exact(upstream, 4)
                    length = int.from_bytes(prefix, "big")
                    selected = False
                    if self.arm_reply.is_set():
                        with self.lock:
                            if self.response is None:
                                self.response = connection
                                selected = True
                        if selected:
                            self.reply_held.set()
                            if self.reply_gate is None:
                                time.sleep(self.header_delay)
                            else:
                                self.reply_gate.wait(10)
                    downstream.sendall(prefix)
                    body = read_exact(upstream, length)
                    if selected:
                        time.sleep(self.body_delay)
                    downstream.sendall(body)
                    break
        except (OSError, EOFError):
            pass
        finally:
            for sock in (downstream, upstream):
                if sock is not None:
                    with contextlib.suppress(OSError):
                        sock.close()

    def close(self) -> None:
        self.stop.set()
        self.release.set()
        if self.reply_gate is not None:
            self.reply_gate.set()
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                sock.close()
        for thread in self.threads:
            thread.join(timeout=0.2)
