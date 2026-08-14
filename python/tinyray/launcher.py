"""Starting and supervising actor processes on this machine.

This is the node agent's job, and it is deliberately available as a library so
a single-machine driver can embed it instead of running a separate daemon. The
same code path serves both, which means the local mode everyone actually
develops against is the one that gets tested.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import IO, Optional

from ._tinyray import new_id

#: How long to wait for a freshly started actor to report its endpoint.
#: Overridable through TINYRAY_STARTUP_TIMEOUT so the failure path is testable
#: without a minute of waiting.
STARTUP_TIMEOUT_SECONDS = float(os.environ.get("TINYRAY_STARTUP_TIMEOUT", "60.0"))


class ActorStartupError(RuntimeError):
    """The actor process died or never reported an endpoint."""


@dataclass
class ActorProcess:
    """A running actor process."""

    actor_id: str
    name: str
    endpoint: str
    process: subprocess.Popen
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    gpu_ids: list[int] = field(default_factory=list)
    restarts: int = 0
    max_restarts: int = 0

    @property
    def pid(self) -> int:
        return self.process.pid

    def is_alive(self) -> bool:
        return self.process.poll() is None

    def exit_code(self) -> Optional[int]:
        return self.process.poll()


class Launcher:
    """Starts actor processes and keeps track of them."""

    def __init__(self, python_executable: Optional[str] = None) -> None:
        self.python = python_executable or sys.executable
        self._actors: dict[str, ActorProcess] = {}
        self._lock = threading.Lock()

    def start_actor(
        self,
        *,
        name: str = "actor",
        actor_id: Optional[str] = None,
        num_cpus: float = 1.0,
        num_gpus: float = 0.0,
        gpu_ids: Optional[list[int]] = None,
        max_restarts: int = 0,
        max_pending_calls: int = 1000,
        store_max_bytes: Optional[int] = None,
        store_ttl_seconds: Optional[float] = None,
        bind: str = "127.0.0.1:0",
        env: Optional[dict[str, str]] = None,
    ) -> ActorProcess:
        # A fresh id unless the caller is restarting an actor and must keep its
        # identity so existing handles still resolve.
        actor_id = actor_id or str(new_id())
        gpu_ids = list(gpu_ids or [])

        child_env = os.environ.copy()
        child_env.update(env or {})
        # Physical GPU selection happens here, once, so user code can simply
        # assume it owns device 0.
        child_env["CUDA_VISIBLE_DEVICES"] = ",".join(str(g) for g in gpu_ids)
        child_env.setdefault("TINYRAY_ACTOR_NAME", name)

        read_fd, write_fd = os.pipe()
        command = [
            self.python,
            "-m",
            "tinyray.worker_main",
            "--actor-id",
            actor_id,
            "--bind",
            bind,
            "--name",
            name,
            "--max-pending-calls",
            str(max_pending_calls),
            "--ready-fd",
            str(write_fd),
        ]
        if store_max_bytes is not None:
            command += ["--store-max-bytes", str(store_max_bytes)]
        if store_ttl_seconds is not None:
            command += ["--store-ttl-seconds", str(store_ttl_seconds)]

        process = subprocess.Popen(
            command,
            env=child_env,
            pass_fds=(write_fd,),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        os.close(write_fd)

        try:
            endpoint = self._await_endpoint(process, read_fd, name)
        except Exception:
            process.kill()
            raise

        actor = ActorProcess(
            actor_id=actor_id,
            name=name,
            endpoint=endpoint,
            process=process,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            gpu_ids=gpu_ids,
            max_restarts=max_restarts,
        )
        with self._lock:
            self._actors[actor_id] = actor
        _start_log_forwarder(process, name)
        return actor

    def _await_endpoint(self, process: subprocess.Popen[str], read_fd: int, name: str) -> str:
        """Read the endpoint the actor writes once its server is bound."""
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        with os.fdopen(read_fd, "r", closefd=True) as handle:
            while True:
                line = handle.readline()
                if line:
                    try:
                        return json.loads(line)["endpoint"]
                    except (ValueError, KeyError) as exc:
                        raise ActorStartupError(
                            f"actor {name} sent an unreadable announcement: {line!r}"
                        ) from exc
                # An empty read means EOF: the child exited before announcing.
                code = process.poll()
                if code is not None:
                    stderr = ""
                    if process.stderr is not None:
                        stderr = process.stderr.read() or ""
                    raise ActorStartupError(
                        f"actor {name} exited with code {code} before it was ready.\n"
                        f"--- child stderr ---\n{stderr.strip()}"
                    )
                if time.monotonic() > deadline:
                    raise ActorStartupError(
                        f"actor {name} did not report an endpoint within "
                        f"{STARTUP_TIMEOUT_SECONDS:.0f}s"
                    )

    def get(self, actor_id: str) -> Optional[ActorProcess]:
        with self._lock:
            return self._actors.get(actor_id)

    def all_actors(self) -> list[ActorProcess]:
        with self._lock:
            return list(self._actors.values())

    def stop_actor(self, actor_id: str, *, timeout: float = 10.0) -> None:
        with self._lock:
            actor = self._actors.pop(actor_id, None)
        if actor is None:
            return
        _terminate(actor.process, timeout=timeout)

    def kill_actor(self, actor_id: str) -> None:
        """Terminate immediately, for early stopping of a bad trial."""
        with self._lock:
            actor = self._actors.pop(actor_id, None)
        if actor is not None and actor.is_alive():
            actor.process.kill()
            actor.process.wait(timeout=10)

    def shutdown(self, *, timeout: float = 10.0) -> None:
        for actor in self.all_actors():
            self.stop_actor(actor.actor_id, timeout=timeout)


def _terminate(process: subprocess.Popen[str], *, timeout: float) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout)


def _start_log_forwarder(process: subprocess.Popen[str], name: str) -> None:
    """Prefix and forward the child's output.

    Without this, 32 actors write to the same terminal and nobody can tell
    which one produced a message.
    """

    def pump(stream: Optional[IO[str]], sink: IO[str]) -> None:
        if stream is None:
            return
        try:
            for line in stream:
                sink.write(f"[{name}:{process.pid}] {line}")
                sink.flush()
        except (ValueError, OSError):
            pass

    for stream, sink in ((process.stdout, sys.stdout), (process.stderr, sys.stderr)):
        thread = threading.Thread(target=pump, args=(stream, sink), daemon=True)
        thread.start()
