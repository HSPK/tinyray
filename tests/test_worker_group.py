"""tinyray as a control plane, with the framework owning its own collectives.

The arrangement Megatron, SGLang, vLLM and DeepSpeed all expect: something
places the processes, tells each one its rank, and then stays out of the way
while the framework builds its own process groups.

These tests use gloo so they run without a GPU, but nothing about the mechanism
is backend-specific: the worker calls ``init_process_group`` itself, exactly as
it would under ``torchrun``, and tinyray never touches it.
"""

from __future__ import annotations

import os

import pytest

import tinyray

torch = pytest.importorskip("torch")
dist = pytest.importorskip("torch.distributed")

pytestmark = pytest.mark.skipif(
    not dist.is_available() or not dist.is_gloo_available(),
    reason="torch.distributed with gloo is required",
)


@pytest.fixture
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


@tinyray.remote(num_cpus=0.1)
class Worker:
    """A worker that owns its process group, as a framework would."""

    def __init__(self, init_now=True):
        import torch.distributed as dist

        self.initialised = False
        if init_now:
            # `env://` is the default: it reads RANK, WORLD_SIZE, MASTER_ADDR
            # and MASTER_PORT, all of which tinyray injected.
            dist.init_process_group(backend="gloo")
            self.initialised = True

    def identity(self):
        import torch.distributed as dist

        return {
            "rank": dist.get_rank(),
            "world_size": dist.get_world_size(),
            "local_rank": int(os.environ["LOCAL_RANK"]),
            "env_rank": int(os.environ["RANK"]),
        }

    def all_reduce(self, value):
        import torch
        import torch.distributed as dist

        tensor = torch.tensor([float(value) * (dist.get_rank() + 1)])
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        return float(tensor[0])

    def broadcast(self, payload):
        import torch
        import torch.distributed as dist

        rank = dist.get_rank()
        tensor = torch.full((4,), float(payload)) if rank == 0 else torch.zeros(4)
        dist.broadcast(tensor, src=0)
        return float(tensor[0])

    def owns_the_default_group(self):
        import torch.distributed as dist

        # The property that makes this arrangement work at all: the default
        # process group belongs to the framework, not to tinyray.
        return dist.is_initialized() and dist.group.WORLD is not None


class TestEnvironmentInjection:
    def test_every_rank_gets_a_torchrun_environment(self, ray):
        group = tinyray.create_worker_group(Worker, size=4, gpus_per_worker=0.0)
        try:
            identities = group.run("identity")
            assert [item["rank"] for item in identities] == [0, 1, 2, 3]
            assert all(item["world_size"] == 4 for item in identities)
            # Rank and RANK must agree, or the framework and tinyray disagree
            # about who is who.
            assert all(item["rank"] == item["env_rank"] for item in identities)
        finally:
            group.shutdown()

    def test_local_rank_reflects_actual_placement(self, ray):
        # One node here, so every rank is local and LOCAL_RANK == RANK. The
        # value has to come from placement rather than a guess, which is why it
        # is computed after the gang lands.
        group = tinyray.create_worker_group(Worker, size=3, gpus_per_worker=0.0)
        try:
            identities = group.run("identity")
            assert [item["local_rank"] for item in identities] == [0, 1, 2]
        finally:
            group.shutdown()

    def test_torchrun_env_names_are_the_standard_ones(self):
        env = tinyray.torchrun_env(
            rank=2,
            world_size=8,
            local_rank=2,
            local_world_size=4,
            master_addr="10.0.0.1",
            master_port=29500,
        )
        # Frameworks read these exact names; a typo here is a silent hang.
        assert env["RANK"] == "2"
        assert env["WORLD_SIZE"] == "8"
        assert env["LOCAL_RANK"] == "2"
        assert env["LOCAL_WORLD_SIZE"] == "4"
        assert env["MASTER_ADDR"] == "10.0.0.1"
        assert env["MASTER_PORT"] == "29500"


class TestFrameworkOwnsTheProcessGroup:
    def test_the_default_group_belongs_to_the_worker(self, ray):
        """The blocker this design exists to remove.

        `tinyray.collective` calls `init_process_group` on the caller's behalf,
        and a process only has one default group -- so Megatron, which needs it
        for its own topology, could not coexist. A worker group leaves it alone.
        """
        group = tinyray.create_worker_group(Worker, size=2, gpus_per_worker=0.0)
        try:
            assert group.run("owns_the_default_group") == [True, True]
        finally:
            group.shutdown()

    def test_all_reduce_runs_in_the_frameworks_group(self, ray):
        group = tinyray.create_worker_group(Worker, size=4, gpus_per_worker=0.0)
        try:
            # 10*(1+2+3+4) = 100, and every rank must see the same sum.
            assert group.run("all_reduce", 10) == [100.0] * 4
        finally:
            group.shutdown()

    def test_broadcast_reaches_every_rank(self, ray):
        group = tinyray.create_worker_group(Worker, size=4, gpus_per_worker=0.0)
        try:
            assert group.run("broadcast", 42.0) == [42.0] * 4
        finally:
            group.shutdown()


class TestConstructionIsConcurrent:
    def test_a_group_whose_constructor_rendezvous_comes_up(self, ray):
        """Serial construction deadlocks, so this is a correctness test.

        Rank 0 blocks inside `init_process_group` until the last rank arrives.
        Constructing the group one actor at a time hangs on the first one, and
        the failure looks like a timeout rather than a design mistake.
        """
        group = tinyray.create_worker_group(Worker, size=4, gpus_per_worker=0.0)
        try:
            assert group.world_size == 4
            assert group.run("all_reduce", 1) == [10.0] * 4
        finally:
            group.shutdown()


class TestControlPlaneStaysOutOfTheDataPath:
    def test_collectives_do_not_touch_the_driver(self, ray):
        group = tinyray.create_worker_group(Worker, size=4, gpus_per_worker=0.0)
        try:

            def driver_bytes():
                return sum(peer["bytes_received"] for peer in tinyray.transport_stats().values())

            before = driver_bytes()
            for _ in range(5):
                group.run("all_reduce", 7)
            moved = driver_bytes() - before

            # Control traffic only: the tensors never leave the worker group.
            assert moved < 32 * 1024, (
                f"the driver received {moved:,} bytes from framework collectives; "
                "it should only be exchanging call headers and small results"
            )
        finally:
            group.shutdown()


class TestGroupLifecycle:
    def test_placement_is_atomic(self, ray):
        # A group that comes up halfway cannot complete a rendezvous, and the
        # framework blocks forever on ranks that will never arrive.
        with pytest.raises(tinyray.PlacementFailed):
            tinyray.create_worker_group(
                Worker, size=100_000, gpus_per_worker=0.0, cpus_per_worker=1.0
            )

    def test_group_indexing_and_iteration(self, ray):
        group = tinyray.create_worker_group(Worker, size=2, gpus_per_worker=0.0)
        try:
            assert len(group) == 2
            assert group[0] is not group[1]
            assert len(list(group)) == 2
            assert "world_size=2" in repr(group)
        finally:
            group.shutdown()

    def test_run_on_targets_one_rank(self, ray):
        group = tinyray.create_worker_group(Worker, size=3, gpus_per_worker=0.0)
        try:
            assert group.run_on(2, "identity")["rank"] == 2
        finally:
            group.shutdown()
