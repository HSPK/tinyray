"""Shared plumbing for the examples: start a registry, run some roles.

Every example is a single file that can be run directly. This module exists so
none of them has to repeat forty lines of process bookkeeping.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path

TINYRAY = Path(sys.executable).parent / "tinyray"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Fleet:
    """A registry plus a set of child processes, all cleaned up on exit."""

    def __init__(self, ttl_ms: int = 2000):
        if not TINYRAY.exists():
            raise SystemExit(f"install first: maturin develop --release ({TINYRAY} missing)")
        self.port = free_port()
        self.ttl_ms = ttl_ms
        self.registry: subprocess.Popen | None = None
        self.procs: list[tuple[str, subprocess.Popen]] = []

    @property
    def endpoint(self) -> str:
        return f"127.0.0.1:{self.port}"

    @property
    def env(self) -> dict[str, str]:
        return dict(os.environ, TINYRAY_REGISTRY=self.endpoint)

    def start_registry(self) -> None:
        self.registry = subprocess.Popen(
            [str(TINYRAY), "--listen", self.endpoint, "--ttl-ms", str(self.ttl_ms)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(0.6)

    def stop_registry(self) -> None:
        if self.registry is not None:
            self.registry.terminate()
            self.registry.wait(timeout=10)
            self.registry = None

    def spawn(self, script: str, *args: object, label: str | None = None) -> subprocess.Popen:
        cmd = [sys.executable, script, *[str(a) for a in args]]
        p = subprocess.Popen(cmd, env=self.env)
        self.procs.append((label or " ".join(str(a) for a in args), p))
        return p

    def wait_all(self, timeout: float = 120, expect_nonzero: Sequence[str] = ()) -> int:
        rc = 0
        for label, p in self.procs:
            try:
                code = p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                p.kill()
                print(f"!! {label} timed out")
                rc = 1
                continue
            if code != 0 and label not in expect_nonzero:
                print(f"!! {label} exited {code}")
                rc = 1
        return rc

    def __enter__(self) -> Fleet:
        self.start_registry()
        return self

    def __exit__(self, *exc: object) -> None:
        for _, p in self.procs:
            if p.poll() is None:
                p.kill()
        self.stop_registry()


def role_main(roles: dict[str, Callable[[list[str]], None]], driver: Callable[[], int]) -> int:
    """Dispatch to a role when given one, otherwise run the driver."""
    if len(sys.argv) > 1 and sys.argv[1] in roles:
        roles[sys.argv[1]](sys.argv[2:])
        return 0
    return driver()
