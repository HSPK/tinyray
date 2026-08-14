"""Supervising processes tinyray did not write.

An SGLang server, a vLLM server, a `torchrun` job: all ordinary processes that
need GPUs assigned, an environment injected, readiness detected, logs labelled
and a restart when they die. A control plane that can only supervise its own
actors is not a control plane.

The subtle part is readiness. "The process is running" is not the same as "the
server is serving": a model takes minutes to load, and a driver that starts
sending requests during that window sees connection refused and concludes the
cluster is broken. So a managed process is not considered ready until something
observable says so -- a port accepting connections, an HTTP endpoint returning
success, or a line appearing in its log.
"""

from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from re import Pattern
from typing import IO, Callable, Optional, Union

#: Substituted into the command and environment with the port tinyray picked.
PORT_PLACEHOLDER = "{port}"


class ProcessStartupError(RuntimeError):
    """The process exited, or never became ready, before its deadline."""


def free_port(host: str = "127.0.0.1") -> int:
    """Reserve a port by binding and releasing it.

    Inherently racy -- something else can take it in the gap -- but it is what
    every launcher does, and the alternative is asking the framework to report
    a port it only knows after binding.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


class Readiness:
    """How to tell that a process has finished starting."""

    def check(self, process: ManagedProcess) -> bool:
        raise NotImplementedError

    def describe(self) -> str:
        raise NotImplementedError


@dataclass
class PortOpen(Readiness):
    """Ready once something accepts connections on the port."""

    host: str = "127.0.0.1"
    port: Optional[int] = None

    def check(self, process: ManagedProcess) -> bool:
        port = self.port or process.port
        if port is None:
            return False
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            return sock.connect_ex((self.host, port)) == 0

    def describe(self) -> str:
        return f"a listener on {self.host}:{self.port or PORT_PLACEHOLDER}"


@dataclass
class HttpOk(Readiness):
    """Ready once an HTTP endpoint answers with an acceptable status.

    Stricter than :class:`PortOpen`, and worth it for inference servers: they
    bind the port early and then spend minutes loading weights, so an open port
    says nothing about whether a request would succeed.
    """

    path: str = "/health"
    host: str = "127.0.0.1"
    port: Optional[int] = None
    accept: tuple[int, ...] = (200, 204)

    def check(self, process: ManagedProcess) -> bool:
        import urllib.error
        import urllib.request

        port = self.port or process.port
        if port is None:
            return False
        url = f"http://{self.host}:{port}{self.path}"
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                return response.status in self.accept
        except (urllib.error.URLError, OSError, ValueError):
            return False

    def describe(self) -> str:
        return f"HTTP {self.accept} from {self.path}"


@dataclass
class LogMatch(Readiness):
    """Ready once a line matching `pattern` appears on stdout or stderr.

    The fallback for processes that announce themselves in the log and offer
    nothing else to probe.
    """

    pattern: Union[str, Pattern[str]] = ""

    def __post_init__(self) -> None:
        self._regex = re.compile(self.pattern) if isinstance(self.pattern, str) else self.pattern

    def check(self, process: ManagedProcess) -> bool:
        return any(self._regex.search(line) for line in process.recent_log())

    def describe(self) -> str:
        return f"a log line matching {self._regex.pattern!r}"


@dataclass
class ProcessAlive(Readiness):
    """Ready as soon as the process is running.

    Honest about what it checks. Fine for a batch job, misleading for a server.
    """

    def check(self, process: ManagedProcess) -> bool:
        return process.is_alive()

    def describe(self) -> str:
        return "the process still being alive"


@dataclass
class ManagedProcess:
    """A supervised process that tinyray started but does not implement."""

    name: str
    command: list[str]
    process: subprocess.Popen
    gpu_ids: list[int] = field(default_factory=list)
    port: Optional[int] = None
    host: str = "127.0.0.1"
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    restarts: int = 0
    max_restarts: int = 0
    #: Remembered so a deferred wait knows what it is waiting for.
    readiness: Optional[Readiness] = None
    #: Ring buffer of recent output, for readiness matching and diagnostics.
    _log: list[str] = field(default_factory=list)
    _log_lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def pid(self) -> int:
        return self.process.pid

    @property
    def endpoint(self) -> Optional[str]:
        return f"{self.host}:{self.port}" if self.port else None

    def is_alive(self) -> bool:
        return self.process.poll() is None

    def exit_code(self) -> Optional[int]:
        return self.process.poll()

    def record_log(self, line: str, *, limit: int = 200) -> None:
        with self._log_lock:
            self._log.append(line)
            if len(self._log) > limit:
                del self._log[: len(self._log) - limit]

    def recent_log(self) -> list[str]:
        with self._log_lock:
            return list(self._log)

    def tail(self, lines: int = 20) -> str:
        return "".join(self.recent_log()[-lines:])

    def terminate(self, *, timeout: float = 15.0) -> None:
        """Ask the process to stop, then insist.

        Inference servers hold GPU memory until they exit, so leaving one
        half-dead strands a card until someone notices.
        """
        if not self.is_alive():
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=timeout)

    def kill(self) -> None:
        if self.is_alive():
            self.process.kill()
            self.process.wait(timeout=15)


def _substitute(values: Sequence[str], port: Optional[int]) -> list[str]:
    if port is None:
        return list(values)
    return [item.replace(PORT_PLACEHOLDER, str(port)) for item in values]


class ProcessSupervisor:
    """Starts and tracks processes that are not tinyray actors."""

    def __init__(self) -> None:
        self._processes: dict[str, ManagedProcess] = {}
        self._lock = threading.Lock()

    def start(
        self,
        command: Sequence[str],
        *,
        name: str,
        gpu_ids: Optional[Sequence[int]] = None,
        env: Optional[dict[str, str]] = None,
        cwd: Optional[str] = None,
        port: Optional[int] = None,
        allocate_port: bool = False,
        host: str = "127.0.0.1",
        ready_when: Optional[Readiness] = None,
        startup_timeout: float = 600.0,
        num_cpus: float = 1.0,
        num_gpus: float = 0.0,
        max_restarts: int = 0,
        wait_ready: bool = True,
    ) -> ManagedProcess:
        """Start a process and, by default, wait until it is ready.

        `{port}` in the command or in any environment value is replaced with
        the port tinyray allocated, so a server can be told where to listen
        without the caller having to find a free one.

        The default readiness check is merely that the process is alive, which
        is honest but weak; pass :class:`HttpOk` or :class:`PortOpen` for
        anything that serves requests.

        `wait_ready=False` returns as soon as the process is spawned, leaving
        the wait to :meth:`await_ready`. That split is mandatory when starting a
        group whose members rendezvous with each other: rank 0 blocks inside
        ``init_process_group`` until the last rank arrives, so waiting for it
        before starting rank 1 deadlocks the launch.
        """
        if port is None and allocate_port:
            port = free_port(host)

        gpu_ids = list(gpu_ids or [])
        child_env = os.environ.copy()
        # Device selection happens once, here, so the process can assume it
        # owns everything it can see.
        child_env["CUDA_VISIBLE_DEVICES"] = ",".join(str(g) for g in gpu_ids)
        for key, value in (env or {}).items():
            child_env[key] = value.replace(PORT_PLACEHOLDER, str(port)) if port else value
        child_env.setdefault("TINYRAY_PROCESS_NAME", name)

        argv = _substitute(list(command), port)
        try:
            popen = subprocess.Popen(
                argv,
                env=child_env,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError as exc:
            raise ProcessStartupError(
                f"cannot start {name}: {argv[0]!r} was not found on PATH"
            ) from exc

        managed = ManagedProcess(
            name=name,
            command=argv,
            process=popen,
            gpu_ids=gpu_ids,
            port=port,
            host=host,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            max_restarts=max_restarts,
        )
        _forward_output(managed)

        with self._lock:
            self._processes[name] = managed

        managed.readiness = ready_when or ProcessAlive()
        if wait_ready:
            self.await_ready(managed, startup_timeout)
        return managed

    def await_ready(
        self,
        managed: ManagedProcess,
        timeout: float,
        readiness: Optional[Readiness] = None,
    ) -> None:
        """Block until a process started with `wait_ready=False` is up."""
        self._await_ready(managed, readiness or managed.readiness or ProcessAlive(), timeout)

    @staticmethod
    def _await_ready(managed: ManagedProcess, readiness: Readiness, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code = managed.exit_code()
            if code is not None:
                # Died during startup. The log is the only useful artefact, so
                # it travels with the error rather than scrolling past.
                raise ProcessStartupError(
                    f"{managed.name} exited with code {code} before it was ready.\n"
                    f"command: {' '.join(managed.command)}\n"
                    f"--- last output ---\n{managed.tail()}"
                )
            if readiness.check(managed):
                return
            time.sleep(0.1)

        managed.terminate()
        raise ProcessStartupError(
            f"{managed.name} did not become ready within {timeout:.0f}s "
            f"(waiting for {readiness.describe()}).\n"
            f"command: {' '.join(managed.command)}\n"
            f"--- last output ---\n{managed.tail()}"
        )

    def get(self, name: str) -> Optional[ManagedProcess]:
        with self._lock:
            return self._processes.get(name)

    def all_processes(self) -> list[ManagedProcess]:
        with self._lock:
            return list(self._processes.values())

    def reap(self) -> list[tuple[str, int]]:
        """Report processes that have exited since the last check."""
        dead = []
        with self._lock:
            for name, managed in list(self._processes.items()):
                code = managed.exit_code()
                if code is not None:
                    dead.append((name, code))
                    del self._processes[name]
        return dead

    def stop(self, name: str, *, timeout: float = 15.0) -> None:
        with self._lock:
            managed = self._processes.pop(name, None)
        if managed is not None:
            managed.terminate(timeout=timeout)

    def shutdown(self, *, timeout: float = 15.0) -> None:
        for managed in self.all_processes():
            self.stop(managed.name, timeout=timeout)


def _forward_output(managed: ManagedProcess) -> None:
    """Label and forward the child's output, keeping a tail for diagnostics.

    Without the prefix, eight inference servers write to one terminal and
    nobody can tell which one is complaining.
    """

    def pump(stream: Optional[IO[str]]) -> None:
        if stream is None:
            return
        try:
            for line in stream:
                managed.record_log(line)
                sys.stdout.write(f"[{managed.name}:{managed.pid}] {line}")
                sys.stdout.flush()
        except (ValueError, OSError):
            pass

    threading.Thread(target=pump, args=(managed.process.stdout,), daemon=True).start()


def ready_when(
    spec: Union[str, Readiness, Callable[[ManagedProcess], bool], None],
) -> Optional[Readiness]:
    """Build a readiness check from a shorthand.

    Accepts ``"port"``, ``"http"``, ``"http:/custom/path"``, ``"alive"``, a
    :class:`Readiness`, or any callable taking the process.
    """
    if spec is None or isinstance(spec, Readiness):
        return spec
    if not isinstance(spec, str):
        # Anything else callable is treated as the predicate itself.
        predicate: Callable[[ManagedProcess], bool] = spec

        class _Callable(Readiness):
            def check(self, process: ManagedProcess) -> bool:
                return bool(predicate(process))

            def describe(self) -> str:
                return getattr(predicate, "__name__", "a custom check")

        return _Callable()
    if spec == "port":
        return PortOpen()
    if spec == "alive":
        return ProcessAlive()
    if spec == "http":
        return HttpOk()
    if spec.startswith("http:"):
        return HttpOk(path=spec[len("http:") :])
    if spec.startswith("log:"):
        return LogMatch(pattern=spec[len("log:") :])
    raise ValueError(
        f"unknown readiness spec {spec!r}; expected 'port', 'http', "
        "'http:/path', 'log:regex', 'alive', a Readiness or a callable"
    )
