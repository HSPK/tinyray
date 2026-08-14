"""A pool of pre-started interpreters.

``import torch`` costs three to eight seconds. A hyperparameter sweep starts and
stops hundreds of short-lived actors, so that import is frequently the single
largest consumer of wall-clock time in the whole run -- far larger than the
trials themselves when they are cheap.

The fix is to keep a few interpreters warm: they have already imported the heavy
modules and are parked waiting for a class to instantiate. Actor creation then
costs a round trip instead of an import.

**A warm process must never touch CUDA.** Once a process initialises a CUDA
context, its ``CUDA_VISIBLE_DEVICES`` is fixed, and the process can only ever
serve actors wanting exactly those devices. So warm processes import torch and
stop there; the first real CUDA call happens inside the user's ``__init__``,
after the device assignment is known. Pools are keyed by device assignment for
the same reason.
"""

from __future__ import annotations

import contextlib
import os
import threading
from collections import defaultdict, deque
from typing import Any, Optional

from .launcher import ActorProcess, Launcher

#: Modules worth importing ahead of time. Anything slow and commonly used.
DEFAULT_PREIMPORTS: tuple[str, ...] = ("numpy", "torch")

#: How many warm interpreters to keep per device assignment.
DEFAULT_POOL_SIZE = 2


class PrewarmPool:
    """Keeps warm interpreters ready, keyed by device assignment.

    Keying by device matters: a process that has already selected its GPUs
    cannot be repurposed for a different assignment, so one pool per assignment
    is the only arrangement that is both correct and useful.
    """

    def __init__(
        self,
        launcher: Launcher,
        *,
        size: int = DEFAULT_POOL_SIZE,
        preimports: tuple[str, ...] = DEFAULT_PREIMPORTS,
        enabled: bool = True,
    ) -> None:
        self.launcher = launcher
        self.size = size
        self.preimports = tuple(preimports)
        self.enabled = enabled
        self._idle: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()
        self._refilling: dict[str, threading.Thread] = {}
        self._stats = {"hits": 0, "misses": 0, "spawned": 0, "discarded": 0}

    def prime(self, *, gpu_ids: Optional[list[int]] = None) -> None:
        """Start filling the pool now, rather than after the first miss.

        Refills are otherwise triggered by `acquire`, which means the pool is
        always one step behind and the first few actors -- often all of them in
        a short sweep -- pay full price.
        """
        if not self.enabled:
            return
        self._schedule_refill(self.key_for(gpu_ids), gpu_ids)

    @staticmethod
    def key_for(gpu_ids: Optional[list[int]]) -> str:
        """Pool key for a device assignment."""
        return ",".join(str(g) for g in sorted(gpu_ids or []))

    def acquire(self, *, gpu_ids: Optional[list[int]] = None) -> Optional[ActorProcess]:
        """Take a warm process for this device assignment, if one is ready."""
        if not self.enabled:
            return None
        key = self.key_for(gpu_ids)

        process = None
        with self._lock:
            queue = self._idle[key]
            while queue:
                candidate = queue.popleft()
                if candidate.is_alive():
                    process = candidate
                    self._stats["hits"] += 1
                    break
                # A warm process that died while idle is no use; drop it.
                self._stats["discarded"] += 1
            else:
                self._stats["misses"] += 1

        # Outside the lock: `_schedule_refill` takes it as well, and
        # `threading.Lock` is not reentrant.
        self._schedule_refill(key, gpu_ids)
        return process

    def _schedule_refill(self, key: str, gpu_ids: Optional[list[int]]) -> None:
        """Top the pool up in the background, never on the caller's path."""
        if not self.enabled:
            return
        with self._lock:
            existing = self._refilling.get(key)
            if existing is not None and existing.is_alive():
                return
            thread = threading.Thread(
                target=self._refill,
                args=(key, list(gpu_ids or [])),
                name=f"tinyray-prewarm-{key or 'cpu'}",
                daemon=True,
            )
            self._refilling[key] = thread
        thread.start()

    def _refill(self, key: str, gpu_ids: list[int]) -> None:
        while True:
            with self._lock:
                if not self.enabled or len(self._idle[key]) >= self.size:
                    return
            try:
                process = self._spawn(gpu_ids)
            except Exception:
                return
            with self._lock:
                self._idle[key].append(process)
                self._stats["spawned"] += 1

    def _spawn(self, gpu_ids: list[int]) -> ActorProcess:
        return self.launcher.start_actor(
            name="prewarm",
            num_gpus=float(len(gpu_ids)),
            gpu_ids=gpu_ids,
            env={
                "TINYRAY_PREIMPORT": ",".join(self.preimports),
                # Belt and braces: the worker also refuses to touch CUDA while
                # warm, but making the intent explicit helps when debugging.
                "TINYRAY_PREWARM": "1",
            },
        )

    def stats(self) -> dict[str, Any]:
        with self._lock:
            idle = {key: len(queue) for key, queue in self._idle.items() if queue}
        return {**self._stats, "idle": idle}

    def shutdown(self) -> None:
        self.enabled = False
        with self._lock:
            pools = list(self._idle.values())
            self._idle.clear()
        for queue in pools:
            for process in queue:
                with contextlib.suppress(Exception):
                    self.launcher.kill_actor(process.actor_id)


def preimport_from_env() -> list[str]:
    """Import the modules named in ``TINYRAY_PREIMPORT``.

    Called by the actor process at startup. Failures are ignored: warming is an
    optimisation, and a missing optional dependency must not stop the actor.
    """
    names = os.environ.get("TINYRAY_PREIMPORT", "")
    imported = []
    for name in filter(None, (n.strip() for n in names.split(","))):
        try:
            __import__(name)
            imported.append(name)
        except ImportError:
            continue
    return imported


def cuda_is_initialised() -> bool:
    """Whether this process has already created a CUDA context.

    A warm interpreter that has must not be reused for a different device
    assignment, because ``CUDA_VISIBLE_DEVICES`` can no longer be changed.
    """
    torch = __import__("sys").modules.get("torch")
    if torch is None:
        return False
    try:
        return bool(torch.cuda.is_initialized())
    except Exception:
        return False
