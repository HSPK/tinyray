"""Attaching to native scripts, with minimal intrusion.

The property under test is what the script is *allowed to keep*: its own
entrypoint, its own imports, its own ``init_process_group``, its own model. Not
decorated, not subclassed, not pickled to the worker. tinyray adds a control
port and nothing else.

That is the difference between a control plane and a framework. Ray takes over
the process; this takes a port.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

import tinyray

torch = pytest.importorskip("torch")
dist = pytest.importorskip("torch.distributed")

pytestmark = pytest.mark.skipif(
    not dist.is_available() or not dist.is_gloo_available(),
    reason="torch.distributed with gloo is required",
)

#: An ordinary training script. Note what is absent: no tinyray decorator, no
#: base class, no tinyray import until the very last line.
NATIVE_TRAINER = textwrap.dedent(
    """
    import os

    import torch
    import torch.distributed as dist


    class Trainer:
        def __init__(self):
            dist.init_process_group(backend="gloo")
            self.rank = dist.get_rank()
            self.world_size = dist.get_world_size()
            self.weights = torch.zeros(8)
            self.steps = 0

        def train_step(self, lr):
            self.steps += 1
            grad = torch.full((8,), float(self.rank + 1) * lr)
            dist.all_reduce(grad, op=dist.ReduceOp.SUM)
            self.weights += grad
            return {"rank": self.rank, "steps": self.steps, "w0": float(self.weights[0])}

        def info(self):
            return {
                "rank": self.rank,
                "world_size": self.world_size,
                "env_rank": int(os.environ["RANK"]),
                "pid": os.getpid(),
                "owns_default_group": dist.is_initialized(),
            }

        def boom(self):
            raise ValueError("diverged")


    if __name__ == "__main__":
        import tinyray

        tinyray.serve(Trainer())
    """
)

#: A script with no distributed anything, to show serve() is independent of it.
PLAIN_SERVICE = textwrap.dedent(
    """
    class Service:
        def __init__(self):
            self.calls = 0

        def echo(self, value):
            self.calls += 1
            return value

        def count(self):
            return self.calls


    if __name__ == "__main__":
        import tinyray

        tinyray.serve(Service())
    """
)


@pytest.fixture
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


@pytest.fixture
def trainer_script(tmp_path: Path) -> str:
    path = tmp_path / "train.py"
    path.write_text(NATIVE_TRAINER)
    return str(path)


@pytest.fixture
def service_script(tmp_path: Path) -> str:
    path = tmp_path / "service.py"
    path.write_text(PLAIN_SERVICE)
    return str(path)


class TestTheScriptKeepsItsOwnProcess:
    def test_a_native_script_runs_unmodified_apart_from_serve(self, ray, trainer_script):
        workers = tinyray.launch_workers(
            [sys.executable, trainer_script],
            size=4,
            name="trainer",
            gpus_per_worker=0.0,
            cpus_per_worker=0.1,
            startup_timeout=120,
        )
        try:
            infos = workers.run("info")
            assert [item["rank"] for item in infos] == [0, 1, 2, 3]
            assert all(item["world_size"] == 4 for item in infos)
            # tinyray's rank and the script's rank agree.
            assert all(item["rank"] == item["env_rank"] for item in infos)
            # Four real processes, not four handles onto one.
            assert len({item["pid"] for item in infos}) == 4
        finally:
            for handle in workers:
                tinyray.stop_process(handle._process.name)

    def test_the_default_process_group_belongs_to_the_script(self, ray, trainer_script):
        """The property that makes Megatron and SGLang possible.

        The script called `init_process_group` itself. tinyray never touched it,
        which is why a framework that needs the default group can still have it.
        """
        workers = tinyray.launch_workers(
            [sys.executable, trainer_script],
            size=2,
            name="own-group",
            gpus_per_worker=0.0,
            cpus_per_worker=0.1,
            startup_timeout=120,
        )
        try:
            assert all(item["owns_default_group"] for item in workers.run("info"))
        finally:
            for handle in workers:
                tinyray.stop_process(handle._process.name)

    def test_the_frameworks_collective_works(self, ray, trainer_script):
        workers = tinyray.launch_workers(
            [sys.executable, trainer_script],
            size=4,
            name="collective",
            gpus_per_worker=0.0,
            cpus_per_worker=0.1,
            startup_timeout=120,
        )
        try:
            # all_reduce of (1+2+3+4)*0.1 = 1.0, seen identically everywhere.
            results = workers.run("train_step", 0.1)
            assert [round(item["w0"], 5) for item in results] == [1.0] * 4
        finally:
            for handle in workers:
                tinyray.stop_process(handle._process.name)


class TestLaunchIsCollectiveSafe:
    def test_every_rank_is_spawned_before_any_is_awaited(self, ray, trainer_script):
        """Otherwise the launch itself deadlocks.

        The script rendezvous during setup, so rank 0 blocks inside
        `init_process_group` until the last rank exists. Waiting for rank 0 to
        become ready before starting rank 1 hangs until the timeout, and the
        error blames readiness rather than the launch order.
        """
        started = time.perf_counter()
        workers = tinyray.launch_workers(
            [sys.executable, trainer_script],
            size=4,
            name="concurrent-launch",
            gpus_per_worker=0.0,
            cpus_per_worker=0.1,
            startup_timeout=60,
        )
        try:
            assert workers.world_size == 4
            # Comfortably under the timeout; a serial launch would sit there
            # until it expired.
            assert time.perf_counter() - started < 45
        finally:
            for handle in workers:
                tinyray.stop_process(handle._process.name)


class TestServeWithoutDistributed:
    def test_a_plain_script_needs_no_torch(self, ray, service_script):
        workers = tinyray.launch_workers(
            [sys.executable, service_script],
            size=2,
            name="plain",
            gpus_per_worker=0.0,
            cpus_per_worker=0.1,
            startup_timeout=60,
        )
        try:
            assert workers.run("echo", "hello") == ["hello", "hello"]
            # Separate processes with separate state: each saw one call, not two.
            assert workers.run("count") == [1, 1]
        finally:
            for handle in workers:
                tinyray.stop_process(handle._process.name)

    def test_errors_carry_the_remote_traceback(self, ray, trainer_script):
        workers = tinyray.launch_workers(
            [sys.executable, trainer_script],
            size=2,
            name="failing",
            gpus_per_worker=0.0,
            cpus_per_worker=0.1,
            startup_timeout=120,
        )
        try:
            with pytest.raises(tinyray.UserCodeError) as excinfo:
                workers.run("boom")
            assert "diverged" in str(excinfo.value)
            assert "raise ValueError" in excinfo.value.remote_traceback
        finally:
            for handle in workers:
                tinyray.stop_process(handle._process.name)

    def test_a_missing_method_lists_the_alternatives(self, ray, service_script):
        workers = tinyray.launch_workers(
            [sys.executable, service_script],
            size=1,
            name="missing-method",
            gpus_per_worker=0.0,
            cpus_per_worker=0.1,
            startup_timeout=60,
        )
        try:
            with pytest.raises(tinyray.UserCodeError) as excinfo:
                workers.run("no_such_method")
            message = str(excinfo.value)
            assert "no method 'no_such_method'" in message
            assert "echo" in message
        finally:
            for handle in workers:
                tinyray.stop_process(handle._process.name)


class TestConnectToSomethingAlreadyRunning:
    def test_a_process_started_by_hand_can_be_driven(self, ray, tmp_path):
        """tinyray takes no responsibility for the lifecycle, only calls in.

        For a worker launched by a scheduler, by an existing shell script, or
        by a person.
        """
        script = tmp_path / "standalone.py"
        script.write_text(
            textwrap.dedent(
                """
                class Service:
                    def double(self, x):
                        return x * 2

                if __name__ == "__main__":
                    import tinyray
                    tinyray.serve(Service(), bind="127.0.0.1:0")
                """
            )
        )
        process = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            endpoint = None
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                line = process.stdout.readline() if process.stdout else ""
                if "serving control on" in line:
                    endpoint = line.strip().split()[-1]
                    break
                if process.poll() is not None:
                    pytest.fail(f"the standalone process exited: {line}")
            assert endpoint, "the process never announced its endpoint"

            worker = tinyray.connect(endpoint)
            assert tinyray.get(worker.double.remote(21)) == 42
        finally:
            process.terminate()
            process.wait(timeout=15)


class TestControlPlaneStaysThin:
    def test_collectives_do_not_reach_the_driver(self, ray, trainer_script):
        workers = tinyray.launch_workers(
            [sys.executable, trainer_script],
            size=4,
            name="thin",
            gpus_per_worker=0.0,
            cpus_per_worker=0.1,
            startup_timeout=120,
        )
        try:

            def driver_bytes():
                return sum(peer["bytes_received"] for peer in tinyray.transport_stats().values())

            before = driver_bytes()
            for _ in range(5):
                workers.run("train_step", 0.1)
            moved = driver_bytes() - before

            assert moved < 32 * 1024, (
                f"the driver received {moved:,} bytes from framework collectives; "
                "it should be moving commands, not gradients"
            )
        finally:
            for handle in workers:
                tinyray.stop_process(handle._process.name)
