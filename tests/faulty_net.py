"""A TCP proxy that misbehaves on purpose.

Every test so far ran on loopback, where nothing is ever slow, reordered or
cut off. This sits between a client and a server and does the things a real
network does: delay, drop, reset, close half of a connection, and split writes
so a reader sees a message in pieces.
"""

from __future__ import annotations

import random
import socket
import threading
import time


class FaultyProxy:
    """Forwards 127.0.0.1:<listen> to <target>, breaking things as told."""

    def __init__(
        self,
        target: str,
        *,
        delay_ms: float = 0.0,
        drop_rate: float = 0.0,
        reset_rate: float = 0.0,
        chunk_bytes: int = 0,
        seed: int = 0,
    ):
        host, _, port = target.rpartition(":")
        self.target = (host or "127.0.0.1", int(port))
        self.delay_ms = delay_ms
        self.drop_rate = drop_rate
        self.reset_rate = reset_rate
        self.chunk_bytes = chunk_bytes
        self._rng = random.Random(seed)
        self._lock = threading.Lock()

        self.opened = 0
        self.reset = 0
        self.dropped = 0

        self._srv = socket.socket()
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(256)
        self.port = self._srv.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    @property
    def endpoint(self) -> str:
        return f"127.0.0.1:{self.port}"

    def _roll(self, p: float) -> bool:
        if p <= 0:
            return False
        with self._lock:
            return self._rng.random() < p

    def _accept_loop(self) -> None:
        self._srv.settimeout(0.2)
        while not self._stop.is_set():
            try:
                client, _ = self._srv.accept()
            except (socket.timeout, OSError):
                continue
            with self._lock:
                self.opened += 1
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client: socket.socket) -> None:
        try:
            upstream = socket.create_connection(self.target, timeout=5)
        except OSError:
            client.close()
            return
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        for src, dst in ((client, upstream), (upstream, client)):
            threading.Thread(target=self._pump, args=(src, dst), daemon=True).start()

    def _hard_reset(self, sock: socket.socket) -> None:
        """RST rather than FIN, which is what a killed peer or a firewall does."""
        try:
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00"
            )
            sock.close()
        except OSError:
            pass

    def _pump(self, src: socket.socket, dst: socket.socket) -> None:
        try:
            while not self._stop.is_set():
                data = src.recv(65536)
                if not data:
                    break
                if self._roll(self.reset_rate):
                    with self._lock:
                        self.reset += 1
                    self._hard_reset(src)
                    self._hard_reset(dst)
                    return
                if self._roll(self.drop_rate):
                    with self._lock:
                        self.dropped += 1
                    continue  # swallowed: the sender never learns
                if self.delay_ms:
                    time.sleep(self.delay_ms / 1000.0)
                if self.chunk_bytes:
                    # Deliver in pieces, so a reader that assumes one recv is
                    # one message finds out otherwise.
                    for i in range(0, len(data), self.chunk_bytes):
                        dst.sendall(data[i : i + self.chunk_bytes])
                        time.sleep(0.001)
                else:
                    dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.close()
                except OSError:
                    pass

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"opened": self.opened, "reset": self.reset, "dropped": self.dropped}

    def close(self) -> None:
        self._stop.set()
        try:
            self._srv.close()
        except OSError:
            pass
