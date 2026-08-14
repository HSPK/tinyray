"""Tests for collective group management.

There is no GPU here, so these test the part tinyray actually owns: admission
rules, rank assignment, the epoch state machine, and the interaction between a
dying actor and the groups it belonged to.

That split is the point. Every admission rule below corresponds to a NCCL
failure that manifests as a *hang* rather than an error, so catching it in the
registry is the difference between a clear message and a stuck job.
"""

from __future__ import annotations

import os
import time

import pytest

import tinyray
from tinyray._tinyray import CollectiveRegistry, new_id


def member(index: int, *, gpus=1.0, node="node0", gpu_ids=None, alive=True):
    """A candidate tuple: (actor_id, num_gpus, node_id, gpu_ids, alive)."""
    if gpu_ids is None:
        gpu_ids = [index] if gpus >= 1.0 else []
    return (str(new_id()), gpus, node, gpu_ids, alive)


def registry_with(members, group_id="g1"):
    registry = CollectiveRegistry()
    registry.create(group_id, members, backend="nccl", store_host="10.0.0.1", store_port=29500)
    return registry


class TestAdmission:
    def test_ranks_are_assigned_in_order(self):
        members = [member(i) for i in range(4)]
        registry = registry_with(members)
        info = registry.info("g1")
        assert info["world_size"] == 4
        assert info["state"] == "FORMING"
        for rank, (actor_id, *_rest) in enumerate(members):
            assert registry.rendezvous_for("g1", actor_id)["rank"] == rank

    def test_rendezvous_has_everything_init_process_group_needs(self):
        members = [member(i) for i in range(2)]
        registry = registry_with(members)
        rendezvous = registry.rendezvous_for("g1", members[1][0])
        assert rendezvous == {
            "group_id": "g1",
            "epoch": 0,
            "rank": 1,
            "world_size": 2,
            "store_host": "10.0.0.1",
            "store_port": 29500,
            "backend": "nccl",
        }

    def test_a_group_of_one_is_refused(self):
        with pytest.raises(tinyray.TinyrayError, match="at least two members"):
            registry_with([member(0)])

    def test_fractional_gpu_members_are_refused_with_the_reason(self):
        # The rule that decides whether a deployment can use NCCL at all: a
        # CPU-only or shared-GPU rollout actor simply cannot join.
        with pytest.raises(tinyray.TinyrayError) as excinfo:
            registry_with([member(0), member(1, gpus=0.25)])
        message = str(excinfo.value)
        assert "at least one whole GPU" in message
        assert "deadlock" in message

    def test_two_ranks_on_one_device_are_refused(self):
        # NCCL hangs rather than erroring here, so the check has to be ours.
        with pytest.raises(tinyray.TinyrayError, match="deadlock"):
            registry_with([member(0, gpu_ids=[0]), member(1, gpu_ids=[0])])

    def test_the_same_gpu_index_on_different_nodes_is_fine(self):
        registry = registry_with(
            [member(0, node="a", gpu_ids=[0]), member(1, node="b", gpu_ids=[0])]
        )
        assert registry.info("g1")["world_size"] == 2

    def test_dead_members_are_refused(self):
        with pytest.raises(tinyray.TinyrayError, match="not alive"):
            registry_with([member(0), member(1, alive=False)])

    def test_duplicate_members_are_refused(self):
        first = member(0)
        duplicate = (first[0], 1.0, "node1", [1], True)
        with pytest.raises(tinyray.TinyrayError, match="appears twice"):
            registry_with([first, duplicate])


class TestEpochStateMachine:
    def test_group_is_ready_only_when_everyone_joins(self):
        members = [member(i) for i in range(3)]
        registry = registry_with(members)
        for actor_id, *_ in members[:2]:
            assert registry.acknowledge("g1", actor_id, 0) == "FORMING"
        assert registry.acknowledge("g1", members[2][0], 0) == "READY"

    def test_repeated_acknowledgements_do_not_fake_readiness(self):
        members = [member(i) for i in range(3)]
        registry = registry_with(members)
        for _ in range(10):
            registry.acknowledge("g1", members[0][0], 0)
        assert registry.info("g1")["state"] == "FORMING"
        assert registry.info("g1")["acknowledged"] == 1

    def test_breaking_a_group_names_every_rank(self):
        # All ranks must abort, not just the dead one: a communicator is only
        # as alive as its least alive member.
        members = [member(i) for i in range(4)]
        registry = registry_with(members)
        to_abort = registry.break_group("g1", "rank 2 died")
        assert len(to_abort) == 4
        assert registry.info("g1")["state"] == "BROKEN"

    def test_rebuild_bumps_the_epoch(self):
        members = [member(i) for i in range(2)]
        registry = registry_with(members)
        for actor_id, *_ in members:
            registry.acknowledge("g1", actor_id, 0)
        registry.break_group("g1", "restart")

        assert registry.begin_rebuild("g1") == 1
        info = registry.info("g1")
        assert info["state"] == "FORMING"
        assert info["acknowledged"] == 0
        assert registry.rendezvous_for("g1", members[0][0])["epoch"] == 1

    def test_stale_acknowledgements_are_ignored(self):
        # A slow member acking the old epoch must not make a rebuilding group
        # look ready before everyone has genuinely rejoined.
        members = [member(i) for i in range(2)]
        registry = registry_with(members)
        for actor_id, *_ in members:
            registry.acknowledge("g1", actor_id, 0)
        registry.break_group("g1", "died")
        registry.begin_rebuild("g1")

        registry.acknowledge("g1", members[0][0], 0)  # stale epoch
        assert registry.info("g1")["acknowledged"] == 0
        assert registry.info("g1")["state"] == "FORMING"

    def test_replacing_a_member_keeps_other_ranks_stable(self):
        members = [member(i) for i in range(4)]
        registry = registry_with(members)
        replacement = str(new_id())
        assert registry.replace_member("g1", members[2][0], replacement, "node9", [3])

        assert registry.rendezvous_for("g1", replacement)["rank"] == 2
        assert registry.rendezvous_for("g1", members[3][0])["rank"] == 3
        assert registry.rendezvous_for("g1", members[2][0]) is None

    def test_destroyed_groups_are_inert(self):
        members = [member(i) for i in range(2)]
        registry = registry_with(members)
        registry.destroy("g1")
        assert registry.info("g1")["state"] == "DESTROYED"
        assert registry.begin_rebuild("g1") is None
        assert registry.break_group("g1", "too late") == []

    def test_groups_with_finds_a_members_groups(self):
        members = [member(i) for i in range(2)]
        registry = registry_with(members)
        assert registry.groups_with(members[0][0]) == ["g1"]
        assert registry.groups_with(str(new_id())) == []


@pytest.fixture
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


class TestGroupsAndActorDeath:
    def test_a_dying_member_breaks_its_group(self, ray):
        """The behaviour that keeps a dead rank from hanging the survivors."""

        @tinyray.remote(max_restarts=0)
        class Rank:
            def ping(self):
                return "pong"

            def crash(self):
                os._exit(3)

        first, second = Rank.remote(), Rank.remote()
        context = tinyray.api._require_context()

        # Register a group by hand: without GPUs the real admission rules would
        # (correctly) refuse, and the behaviour under test is what happens when
        # a member dies.
        group_id = "manual-group"
        context.collective.create(
            group_id,
            [
                (first.actor_id, 1.0, "n0", [0], True),
                (second.actor_id, 1.0, "n1", [0], True),
            ],
        )
        assert context.collective.info(group_id)["state"] == "FORMING"

        with pytest.raises(tinyray.TinyrayError):
            ray.get(first.crash.remote(), timeout=5.0)

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if context.collective.info(group_id)["state"] == "BROKEN":
                break
            time.sleep(0.2)
        else:
            pytest.fail(
                "a dead rank left the group usable; the survivors would hang "
                "inside NCCL on the next collective"
            )

    def test_running_on_a_broken_group_fails_fast(self, ray):
        @tinyray.remote
        class Rank:
            def sync(self, **kwargs):
                return kwargs["rank"]

        handles = [Rank.remote() for _ in range(2)]
        context = tinyray.api._require_context()
        group_id = "manual-group-2"
        context.collective.create(
            group_id,
            [(h.actor_id, 1.0, f"n{i}", [0], True) for i, h in enumerate(handles)],
        )
        group = tinyray.CollectiveGroup(context, group_id, handles)

        # FORMING, not READY: running now would deadlock some ranks.
        with pytest.raises(tinyray.CollectiveError, match="not READY"):
            group.run("sync")

        context.collective.break_group(group_id, "test")
        with pytest.raises(tinyray.GroupRebuilding, match="must be rebuilt"):
            group.run("sync")


class TestCollectiveRun:
    def test_run_delivers_to_every_rank(self, ray):
        """`run` is a barrier: it reaches all ranks and waits for all of them."""

        @tinyray.remote
        class Rank:
            def sync_weights(self, tag, **kwargs):
                return (tag, kwargs["rank"], kwargs["world_size"], kwargs["src_rank"])

        handles = [Rank.remote() for _ in range(4)]
        context = tinyray.api._require_context()
        group_id = "run-group"
        context.collective.create(
            group_id,
            [(h.actor_id, 1.0, f"n{i}", [0], True) for i, h in enumerate(handles)],
        )
        for handle in handles:
            context.collective.acknowledge(group_id, handle.actor_id, 0)
        assert context.collective.info(group_id)["state"] == "READY"

        group = tinyray.CollectiveGroup(context, group_id, handles)
        results = group.run("sync_weights", "iteration-7", src_rank=0)

        assert [r[0] for r in results] == ["iteration-7"] * 4
        assert [r[1] for r in results] == [0, 1, 2, 3]
        assert all(r[2] == 4 for r in results)
        assert all(r[3] == 0 for r in results)


class TestActorSideHelpers:
    def test_join_requires_torch(self, ray):
        # No torch in this environment, so joining must fail with something a
        # user can act on rather than an obscure ImportError deep in a thread.
        @tinyray.remote
        class Rank:
            def ping(self):
                return "pong"

        handle = Rank.remote()
        with pytest.raises(tinyray.UserCodeError) as excinfo:
            ray.get(
                handle._submit(
                    "__tinyray_join_collective__",
                    (
                        {
                            "group_id": "g",
                            "epoch": 0,
                            "rank": 0,
                            "world_size": 2,
                            "store_host": "127.0.0.1",
                            "store_port": 29500,
                            "backend": "nccl",
                        },
                    ),
                    {},
                )
            )
        assert "torch" in str(excinfo.value)

    def test_unknown_control_method_is_rejected(self, ray):
        @tinyray.remote
        class Rank:
            def ping(self):
                return "pong"

        handle = Rank.remote()
        with pytest.raises(tinyray.UserCodeError, match="unknown tinyray control method"):
            ray.get(handle._submit("__tinyray_nonsense__", (), {}))


def test_nccl_env_includes_async_error_handling():
    # Without this, a dead peer leaves the survivors blocked inside NCCL with no
    # way to abort, and the epoch state machine cannot do its job.
    assert tinyray.collective.NCCL_ENV["NCCL_ASYNC_ERROR_HANDLING"] == "1"
    assert tinyray.collective.NCCL_ENV["TORCH_NCCL_ASYNC_ERROR_HANDLING"] == "1"
