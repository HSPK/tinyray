"""Tests for cluster bookkeeping: placement, gangs, naming and restarts.

The scheduler only runs when an actor is created or dies, so these tests mostly
poke the state machine directly. The few that start real processes are the ones
where the interesting behaviour is the interaction between the head, the node
agent and the driver's routing table.
"""

from __future__ import annotations

import os
import time

import pytest

import tinyray
from tinyray._tinyray import ClusterState, new_id


@pytest.fixture
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


def make_cluster(nodes):
    """Build a cluster of `(cpus, gpus)` tuples."""
    state = ClusterState(heartbeat_timeout_seconds=30.0)
    ids = []
    for index, (cpus, gpus) in enumerate(nodes):
        node_id = str(new_id())
        ids.append(node_id)
        state.register_node(
            node_id=node_id,
            endpoint=f"10.0.0.{index + 1}:6380",
            hostname=f"node{index}",
            num_cpus=cpus,
            num_gpus=float(gpus),
            gpu_ids=list(range(gpus)),
        )
    return state, ids


class TestPlacement:
    def test_place_reserves_resources(self):
        state, (node_id,) = make_cluster([(16.0, 4)])
        placed_node, _endpoint, gpus = state.place(num_cpus=4.0, num_gpus=1.0)
        assert placed_node == node_id
        assert len(gpus) == 1
        node = state.nodes()[0]
        assert node["available_cpus"] == 12.0
        assert node["free_gpu_ids"] == [1, 2, 3]

    def test_spread_uses_distinct_nodes(self):
        state, _ = make_cluster([(16.0, 2), (16.0, 2)])
        first, _, _ = state.place(num_cpus=4.0, num_gpus=1.0, strategy="SPREAD")
        second, _, _ = state.place(num_cpus=4.0, num_gpus=1.0, strategy="SPREAD")
        assert first != second

    def test_pack_fills_one_node(self):
        state, _ = make_cluster([(16.0, 2), (16.0, 2)])
        first, _, _ = state.place(num_cpus=4.0, num_gpus=1.0, strategy="PACK")
        second, _, _ = state.place(num_cpus=4.0, num_gpus=1.0, strategy="PACK")
        assert first == second

    def test_gpus_are_exclusive(self):
        # Two ranks on one physical GPU deadlock NCCL, so this has to be exact.
        state, _ = make_cluster([(64.0, 2)])
        _, _, first = state.place(num_gpus=1.0)
        _, _, second = state.place(num_gpus=1.0)
        assert set(first).isdisjoint(second)
        with pytest.raises(tinyray.TinyrayError):
            state.place(num_gpus=1.0)

    def test_fractional_gpus_share_a_device(self):
        # Hyperparameter trials pack onto one card and must not consume the
        # exclusive slots collective members need.
        state, _ = make_cluster([(64.0, 1)])
        for _ in range(4):
            _, _, gpus = state.place(num_cpus=1.0, num_gpus=0.25)
            assert gpus == []
        assert state.nodes()[0]["free_gpu_ids"] == [0]

    def test_infeasible_placement_says_why(self):
        state, _ = make_cluster([(2.0, 0)])
        with pytest.raises(tinyray.TinyrayError) as excinfo:
            state.place(num_cpus=8.0, num_gpus=2.0)
        message = str(excinfo.value)
        assert "requested 8.00 CPUs and 2 whole GPUs" in message
        assert "best node has" in message

    def test_empty_cluster_is_reported_clearly(self):
        state = ClusterState()
        with pytest.raises(tinyray.TinyrayError, match="no nodes are registered"):
            state.place(num_cpus=1.0)


class TestGangPlacement:
    def test_gang_is_all_or_nothing(self):
        state, _ = make_cluster([(16.0, 2), (16.0, 2)])
        with pytest.raises(tinyray.TinyrayError, match="all or nothing"):
            state.place_gang(8, num_cpus=4.0, num_gpus=1.0)
        # Nothing may be left reserved after a refusal.
        assert all(len(n["free_gpu_ids"]) == 2 for n in state.nodes())

    def test_gang_of_exactly_the_right_size(self):
        state, _ = make_cluster([(16.0, 2), (16.0, 2)])
        placements = state.place_gang(4, num_cpus=4.0, num_gpus=1.0)
        assert len(placements) == 4
        assigned = {(node, gpu) for node, _, gpus in placements for gpu in gpus}
        assert len(assigned) == 4, "a GPU was handed out twice"

    def test_capacity_matches_what_can_be_placed(self):
        state, _ = make_cluster([(16.0, 4), (8.0, 2)])
        capacity = state.gang_capacity(num_cpus=4.0, num_gpus=1.0)
        assert capacity == 6
        assert len(state.place_gang(capacity, num_cpus=4.0, num_gpus=1.0)) == capacity

    def test_strict_spread_refuses_to_co_locate(self):
        state, _ = make_cluster([(64.0, 4), (64.0, 4)])
        with pytest.raises(tinyray.TinyrayError):
            state.place_gang(3, num_gpus=1.0, strategy="STRICT_SPREAD")
        placements = state.place_gang(2, num_gpus=1.0, strategy="STRICT_SPREAD")
        assert placements[0][0] != placements[1][0]


class TestActorRegistry:
    def test_removing_an_actor_frees_resources_at_once(self):
        # A sweep churns actors constantly; waiting for a heartbeat to reclaim
        # would idle the cluster.
        state, (node_id,) = make_cluster([(16.0, 2)])
        _, _, gpus = state.place(num_cpus=4.0, num_gpus=1.0)
        actor_id = str(new_id())
        state.add_actor(
            actor_id=actor_id,
            node_id=node_id,
            endpoint="127.0.0.1:1",
            num_cpus=4.0,
            num_gpus=1.0,
            gpu_ids=gpus,
        )
        state.remove_actor(actor_id)
        node = state.nodes()[0]
        assert node["available_cpus"] == 16.0
        assert len(node["free_gpu_ids"]) == 2

    def test_restart_budget_is_finite(self):
        state, (node_id,) = make_cluster([(16.0, 0)])
        actor_id = str(new_id())
        state.add_actor(
            actor_id=actor_id,
            node_id=node_id,
            endpoint="127.0.0.1:1",
            max_restarts=2,
        )
        assert state.note_actor_died(actor_id) is True
        assert state.note_actor_died(actor_id) is True
        assert state.note_actor_died(actor_id) is False
        assert state.actor(actor_id)["state"] == "DEAD"

    def test_a_dead_node_takes_its_actors(self):
        state, (first, second) = make_cluster([(16.0, 0), (16.0, 0)])
        doomed, survivor = str(new_id()), str(new_id())
        state.add_actor(actor_id=doomed, node_id=first, endpoint="127.0.0.1:1")
        state.add_actor(actor_id=survivor, node_id=second, endpoint="127.0.0.1:2")

        assert state.remove_node(first) == [doomed]
        assert state.actor(doomed)["state"] == "DEAD"
        assert state.actor(survivor)["state"] == "ALIVE"

    def test_heartbeat_timeout_detects_a_silent_node(self):
        state = ClusterState(heartbeat_timeout_seconds=0.05)
        node_id = str(new_id())
        state.register_node(node_id=node_id, endpoint="10.0.0.1:6380", hostname="n", num_cpus=8.0)
        assert state.dead_nodes() == []
        time.sleep(0.1)
        assert state.dead_nodes() == [node_id]
        state.heartbeat(node_id, 8.0, 0.0, [])
        assert state.dead_nodes() == []


class TestGangCreation:
    def test_create_actors_starts_them_all(self, ray):
        @tinyray.remote
        class Worker:
            def __init__(self, tag):
                self.tag = tag

            def whoami(self):
                return self.tag

        handles = tinyray.create_actors(Worker, "w", count=4)
        assert len(handles) == 4
        assert ray.get([h.whoami.remote() for h in handles]) == ["w"] * 4
        # Distinct processes, not four handles onto one.
        assert len({h.pid for h in handles}) == 4

    def test_create_actors_refuses_an_impossible_gang(self, ray):
        @tinyray.remote(num_cpus=1.0)
        class Worker:
            def ping(self):
                return "pong"

        # Far more CPUs than any machine has.
        with pytest.raises(tinyray.PlacementFailed):
            tinyray.create_actors(Worker, count=100_000)


class TestNamedActors:
    def test_named_actor_is_resolvable(self, ray):
        @tinyray.remote(name="parameter-server")
        class ParamServer:
            def __init__(self):
                self.weights = 0

            def set(self, value):
                self.weights = value
                return value

            def get_weights(self):
                return self.weights

        server = ParamServer.remote()
        ray.get(server.set.remote(42))

        # A different handle to the same actor, resolved by name.
        again = tinyray.get_actor("parameter-server")
        assert again.actor_id == server.actor_id
        assert ray.get(again.get_weights.remote()) == 42

    def test_unknown_name_raises(self, ray):
        with pytest.raises(tinyray.NotFound, match="no actor is registered"):
            tinyray.get_actor("nobody-here")

    def test_naming_does_not_require_a_detached_lifetime(self, ray):
        # The two were wrongly coupled: a name is for lookup, a lifetime is for
        # ownership, and wanting one should not force the other.
        @tinyray.remote(name="plain-named-actor")
        class Named:
            def ping(self):
                return "pong"

        Named.remote()
        assert ray.get(tinyray.get_actor("plain-named-actor").ping.remote()) == "pong"

    def test_detached_is_refused_rather_than_silently_ignored(self, ray):
        @tinyray.remote(name="would-be-detached", lifetime="detached")
        class Detached:
            def ping(self):
                return "pong"

        with pytest.raises(NotImplementedError, match="standalone head process"):
            Detached.remote()

    def test_unknown_lifetime_is_rejected(self, ray):
        @tinyray.remote(lifetime="forever")
        class Odd:
            def ping(self):
                return "pong"

        with pytest.raises(ValueError, match="unknown lifetime"):
            Odd.remote()


class TestFaultTolerance:
    def test_actor_is_restarted_and_the_handle_still_works(self, ray):
        @tinyray.remote(max_restarts=2)
        class Fragile:
            def __init__(self):
                self.n = 0

            def ping(self):
                return "pong"

            def crash(self):
                os._exit(7)

        actor = Fragile.remote()
        assert ray.get(actor.ping.remote()) == "pong"
        original_pid = actor.pid

        with pytest.raises(tinyray.TinyrayError):
            ray.get(actor.crash.remote(), timeout=5.0)

        # The supervisor restarts it and the driver re-routes; a handle must
        # survive the move.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if ray.get(actor.ping.remote(), timeout=3.0) == "pong":
                    break
            except tinyray.TinyrayError:
                time.sleep(0.2)
        else:
            pytest.fail("actor never came back after crashing")

        entry = tinyray.api._require_context().head.get_actor(actor.actor_id)
        assert entry["restarts"] == 1
        assert entry["endpoint"] != f"127.0.0.1:{original_pid}"

    def test_actor_stays_dead_once_the_budget_runs_out(self, ray):
        @tinyray.remote(max_restarts=0)
        class Doomed:
            def crash(self):
                os._exit(9)

            def ping(self):
                return "pong"

        actor = Doomed.remote()
        with pytest.raises(tinyray.TinyrayError):
            ray.get(actor.crash.remote(), timeout=5.0)

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if not actor.is_alive():
                break
            time.sleep(0.2)
        else:
            pytest.fail("actor with no restart budget was not reaped")

        # Calls now fail fast rather than hanging on a dead endpoint.
        with pytest.raises(tinyray.TinyrayError):
            ray.get(actor.ping.remote(), timeout=5.0)


class TestClusterIntrospection:
    def test_nodes_reports_local_resources(self, ray):
        info = tinyray.nodes()
        assert len(info) == 1
        assert info[0]["total_cpus"] >= 1

    def test_actors_lists_running_actors(self, ray):
        @tinyray.remote
        class Idle:
            def ping(self):
                return 1

        handle = Idle.remote()
        listed = {entry["actor_id"] for entry in tinyray.actors()}
        assert handle.actor_id in listed


class TestHeartbeat:
    """A node that stops reporting in loses its actors.

    Regression test for the bug that every short test missed: nothing was
    sending heartbeats at all, so any session outliving the timeout had its
    actors torn down underneath it.

    The assertion is deliberately end-to-end -- "the actor still answers" --
    rather than "no node looks dead". An earlier version of this test checked
    `dead_nodes() == []` and passed even with heartbeats disabled, because the
    supervisor had already reaped the dead node and an empty list means both
    "healthy" and "already cleaned up". Mutation testing caught it; nothing
    else would have.
    """

    def test_actors_survive_well_past_the_heartbeat_deadline(self):
        tinyray.shutdown()
        # One second, deliberately shorter than the default reporting interval:
        # the agent only keeps up because the interval is derived from the
        # deadline. A fixed interval larger than the timeout would declare every
        # healthy node dead, so this value is what makes that regression visible.
        tinyray.init(heartbeat_timeout=1.0)
        try:

            @tinyray.remote
            class Survivor:
                def ping(self):
                    return "pong"

            actor = Survivor.remote()
            assert tinyray.get(actor.ping.remote()) == "pong"

            time.sleep(5.0)  # five times the deadline

            assert tinyray.get(actor.ping.remote(), timeout=10.0) == "pong", (
                "the actor died while idle: its node stopped reporting in and "
                "the supervisor tore it down"
            )
            assert len(tinyray.nodes()) == 1
        finally:
            tinyray.shutdown()

    def test_a_silent_node_is_eventually_declared_dead(self):
        # The other half of the contract: the detector must still work.
        state = ClusterState(heartbeat_timeout_seconds=0.2)
        node_id = str(new_id())
        state.register_node(
            node_id=node_id, endpoint="10.0.0.9:6380", hostname="ghost", num_cpus=1.0
        )
        time.sleep(0.4)
        assert state.dead_nodes() == [node_id]

    def test_heartbeat_refreshes_free_resources(self, ray):
        context = tinyray.api._require_context()
        context.head.record_heartbeat(context.agent)
        node = context.head.state.nodes()[0]
        assert node["available_cpus"] > 0
