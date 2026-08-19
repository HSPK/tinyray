from __future__ import annotations

import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target" / "release" / "tinyray-registry"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RegistryProc:
    """A registry we can kill and restart, because soft state is the point."""

    def __init__(self, ttl_ms: int):
        self.ttl_ms = ttl_ms
        self.port = free_port()
        self.proc: subprocess.Popen | None = None

    @property
    def endpoint(self) -> str:
        return f"127.0.0.1:{self.port}"

    def start(self) -> None:
        assert BIN.exists(), f"{BIN} missing; run: cargo build --release"
        self.proc = subprocess.Popen(
            [str(BIN), "--listen", self.endpoint, "--ttl-ms", str(self.ttl_ms)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"http://{self.endpoint}/health", timeout=0.5) as r:
                    if r.status == 200:
                        return
            except (urllib.error.URLError, ConnectionError, OSError):
                time.sleep(0.02)
        raise RuntimeError("registry did not become healthy")

    def stop(self) -> None:
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
            self.proc = None


@pytest.fixture
def registry():
    r = RegistryProc(ttl_ms=2000)
    r.start()
    os.environ["TINYRAY_REGISTRY"] = r.endpoint
    try:
        yield r
    finally:
        r.stop()
