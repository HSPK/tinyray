"""Entry point for an actor process.

Started by the node agent as::

    python -m tinyray.worker_main --actor-id <id> --bind 127.0.0.1:0

The process then does one thing forever: pull a call, run it, publish the
result. Everything else -- accepting connections, serving fetches, ordering,
backpressure, eviction -- happens on tokio threads that never take the GIL.

The class to instantiate arrives over the wire as the first call, named
``__init__``, so the node agent never needs to know anything about user code.
"""

from __future__ import annotations

import argparse
import contextlib
import faulthandler
import json
import os
import signal
import sys
import threading
import traceback
from typing import Any, Optional

from . import mesh, serde
from ._tinyray import ActorRuntime, Task

#: How often to drop results past their TTL. Overridable through
#: TINYRAY_SWEEP_INTERVAL so a test can reach the deadline in seconds rather
#: than assume the production value works.
SWEEP_INTERVAL_SECONDS = float(os.environ.get("TINYRAY_SWEEP_INTERVAL", "30.0"))


class ActorHost:
    """Owns the user's actor instance and runs calls against it."""

    def __init__(self, runtime: ActorRuntime, log_prefix: str) -> None:
        self.runtime = runtime
        self.instance: Optional[Any] = None
        self.log_prefix = log_prefix

    def run_forever(self) -> None:
        sweeper = threading.Thread(target=self._sweep_loop, daemon=True)
        sweeper.start()

        while True:
            # The short timeout returns control to the interpreter several
            # times a second, which is what allows SIGTERM to be handled at all.
            task = self.runtime.next_task(timeout_seconds=0.2)
            if task is None:
                if self.runtime.shutting_down:
                    break
                continue
            self._run_one(task)

    def _sweep_loop(self) -> None:
        stop = threading.Event()
        while not stop.wait(SWEEP_INTERVAL_SECONDS):
            if self.runtime.shutting_down:
                return
            self.runtime.sweep_expired()

    def _run_one(self, task: Task) -> None:
        try:
            args, kwargs = serde.deserialize(task.body, task.frames)
        except Exception:
            self.runtime.fail(
                task.task_id.hex,
                "Internal",
                "failed to deserialise call arguments",
                traceback.format_exc(),
            )
            return

        try:
            from .api import resolve_arguments

            args, kwargs = resolve_arguments(args, kwargs)
        except Exception:
            self.runtime.fail(
                task.task_id.hex,
                "Internal",
                "failed to resolve an ObjectRef argument",
                traceback.format_exc(),
            )
            return

        try:
            result = self._invoke(task.method, args, kwargs)
        except Exception as exc:
            # The remote traceback is the only useful debugging artefact the
            # caller will ever see, so it travels with the error.
            self.runtime.fail(
                task.task_id.hex,
                "UserException",
                f"{type(exc).__name__}: {exc}",
                traceback.format_exc(),
            )
            return

        try:
            body, frames = serde.serialize(result)
        except Exception:
            self.runtime.fail(
                task.task_id.hex,
                "UserException",
                f"method {task.method!r} returned a value that cannot be serialised",
                traceback.format_exc(),
            )
            return

        self.runtime.complete(task.task_id.hex, body, frames)

    def _invoke(self, method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        # Collective control methods are handled by tinyray itself. They run on
        # the executor thread but hand the actual NCCL call to the collective
        # thread, which is what keeps the ordering NCCL requires.
        if method == "__tinyray_join_collective__":
            from .collective import actor_state

            return actor_state().join(args[0])
        if method == "__tinyray_abort_collective__":
            from .collective import actor_state

            actor_state().abort(args[0])
            return None
        if method == mesh.LINK_METHOD:
            # An actor can be a mesh member too. Handled here rather than on the
            # user object, for the same reason as the collective methods above.
            return mesh.install_roster(*args, **kwargs)

        if method == "__init__":
            cls, init_args, init_kwargs = args
            self.instance = cls(*init_args, **init_kwargs)
            return None

        if self.instance is None:
            raise RuntimeError(f"actor received {method!r} before its constructor completed")
        if method.startswith("__tinyray_"):
            raise AttributeError(f"unknown tinyray control method {method!r}")
        if method.startswith("_"):
            # Not a security boundary, just a guard against a caller poking at
            # internals by accident.
            raise AttributeError(f"{method!r} is private and not remotely callable")

        target = getattr(self.instance, method, None)
        if target is None:
            available = sorted(
                name
                for name in dir(self.instance)
                if not name.startswith("_") and callable(getattr(self.instance, name, None))
            )
            raise AttributeError(
                f"{type(self.instance).__name__} has no method {method!r}; "
                f"available: {', '.join(available)}"
            )
        if not callable(target):
            raise TypeError(f"{method!r} is not callable")
        return target(*args, **kwargs)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="tinyray-actor")
    parser.add_argument("--actor-id", required=True)
    parser.add_argument("--bind", default="127.0.0.1:0")
    parser.add_argument("--name", default="actor")
    parser.add_argument("--max-pending-calls", type=int, default=1000)
    parser.add_argument("--store-max-bytes", type=int, default=None)
    parser.add_argument("--store-ttl-seconds", type=float, default=None)
    parser.add_argument(
        "--ready-fd",
        type=int,
        default=None,
        help="write the endpoint as JSON to this fd once bound",
    )
    args = parser.parse_args(argv)

    # Warm the heavy modules before binding, so the cost lands here rather than
    # inside the user's first call. Deliberately does not touch CUDA: doing so
    # would freeze this process's device assignment.
    from .prewarm import preimport_from_env

    preimport_from_env()

    # A hung actor is the single most common failure in a distributed ML run.
    # SIGUSR1 dumps every thread's stack, so `py-spy` is not the only option.
    faulthandler.enable()
    with contextlib.suppress(AttributeError, ValueError):
        faulthandler.register(signal.SIGUSR1)

    mesh.mark_serving()
    runtime = ActorRuntime(
        actor_id=args.actor_id,
        bind=args.bind,
        max_pending_calls=args.max_pending_calls,
        store_max_bytes=args.store_max_bytes,
        store_ttl_seconds=args.store_ttl_seconds,
    )

    announcement = json.dumps(
        {"actor_id": args.actor_id, "endpoint": runtime.endpoint, "pid": os.getpid()}
    )
    if args.ready_fd is not None:
        with os.fdopen(args.ready_fd, "w", closefd=True) as handle:
            handle.write(announcement + "\n")
    else:
        print(announcement, flush=True)

    def request_shutdown(_signum: int, _frame: Any) -> None:
        runtime.begin_shutdown()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    host = ActorHost(runtime, log_prefix=f"[{args.name}:{os.getpid()}]")
    try:
        host.run_forever()
    finally:
        runtime.begin_shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
