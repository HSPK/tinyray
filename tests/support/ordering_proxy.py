"""Delay real h2 frames without changing their contents or per-connection order."""

from __future__ import annotations

import contextlib
import json
import queue
import socket
import threading
import time


def _read(sock: socket.socket, count: int) -> bytes:
    out = bytearray()
    while len(out) < count:
        part = sock.recv(count - len(out))
        if not part:
            raise EOFError
        out.extend(part)
    return bytes(out)


def _frame(sock: socket.socket) -> tuple[bytes, bytes]:
    header = _read(sock, 9)
    return header, _read(sock, int.from_bytes(header[:3], "big"))


class OrderingProxy:
    def __init__(
        self,
        target: str,
        *,
        hold_startup: bool = False,
        header_delay: float = 0,
        body_delay: float = 0,
    ):
        host, port = target.rsplit(":", 1)
        self.target = host, int(port)
        self.hold_startup = hold_startup
        self.header_delay = header_delay
        self.body_delay = body_delay
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.release = threading.Event()
        self.canceled = threading.Event()
        self.forwarded = threading.Event()
        self.reset_forwarded = threading.Event()
        self.arm_reply = threading.Event()
        self.reply_held = threading.Event()
        self.selected: tuple[int, int] | None = None
        self.startup: dict | None = None
        self.response: tuple[int, int] | None = None
        self.requests: list[tuple[float, dict]] = []
        self.sockets: list[socket.socket] = []
        self.threads: list[threading.Thread] = []
        self.server = socket.socket()
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(32)
        self.server.settimeout(0.1)
        self.sockets.append(self.server)
        self.endpoint = f"http://127.0.0.1:{self.server.getsockname()[1]}"
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
            try:
                upstream = socket.create_connection(self.target, timeout=2)
            except OSError:
                downstream.close()
                continue
            upstream.settimeout(None)
            with self.lock:
                self.sockets.extend([downstream, upstream])
            for sock in (downstream, upstream):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            pending: queue.Queue = queue.Queue()
            self._launch(self._read_up, connection, downstream, pending)
            self._launch(self._write_up, connection, upstream, pending)
            self._launch(self._down, connection, upstream, downstream)
            connection += 1

    def _read_up(self, connection, sock, pending) -> None:
        try:
            pending.put((None, _read(sock, 24), False, None))
            while not self.stop.is_set():
                header, data = _frame(sock)
                stream = int.from_bytes(header[5:9], "big") & 0x7FFFFFFF
                hold = False
                beat = None
                if header[3] == 0 and data:
                    # These tests send small beats, each in a single DATA frame.
                    beat = json.loads(data)
                    with self.lock:
                        if self.hold_startup and self.selected is None:
                            self.selected = connection, stream
                            self.startup = beat
                            hold = True
                if header[3] == 3 and (connection, stream) == self.selected:
                    self.canceled.set()
                pending.put((header, data, hold, beat))
        except (OSError, EOFError):
            pass
        finally:
            pending.put(None)

    def _write_up(self, connection, sock, pending) -> None:
        try:
            while not self.stop.is_set():
                item = pending.get()
                if item is None:
                    return
                header, data, hold, beat = item
                if hold:
                    self.release.wait(5)
                sock.sendall((header or b"") + data)
                if beat is not None:
                    with self.lock:
                        self.requests.append((time.monotonic(), beat))
                if hold:
                    self.forwarded.set()
                    # The registry receives the delayed request before its
                    # already-queued cancellation; the two are not equivalent.
                    time.sleep(0.08)
                if header and header[3] == 3:
                    stream = int.from_bytes(header[5:9], "big") & 0x7FFFFFFF
                    if (connection, stream) == self.selected:
                        self.reset_forwarded.set()
        except OSError:
            pass

    def _down(self, connection, upstream, downstream) -> None:
        try:
            while not self.stop.is_set():
                header, data = _frame(upstream)
                stream = int.from_bytes(header[5:9], "big") & 0x7FFFFFFF
                selected = False
                if header[3] == 1 and self.arm_reply.is_set():
                    with self.lock:
                        if self.response is None:
                            self.response = connection, stream
                            selected = True
                    if selected:
                        self.reply_held.set()
                        time.sleep(self.header_delay)
                if header[3] == 0 and (connection, stream) == self.response:
                    time.sleep(self.body_delay)
                downstream.sendall(header + data)
        except (OSError, EOFError):
            pass

    def close(self) -> None:
        self.stop.set()
        self.release.set()
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        for thread in self.threads:
            thread.join(timeout=0.2)
