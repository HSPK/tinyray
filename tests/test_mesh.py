"""Workers addressing each other, without the driver in the middle.

tinyray started as a star: the driver at the centre, workers as leaves, every
message relayed. That is right for a fan-out and wrong for a pipeline, where a
fleet of loader sidecars feeds a fleet of trainer sidecars and the driver has
nothing to contribute between steps.

These tests pin the mesh down. The load-bearing ones are not the happy paths --
they are the two that would have caught the original design going wrong:

* a worker calling a peer must not build a head (it used to);
* the driver must go quiet once the mesh is linked (it never did).
"""

from __future__ import annotations

import pickle
import sys
import textwrap
from pathlib import Path

import pytest

import tinyray as tr
from tinyray import mesh

SIDECAR = textwrap.dedent(
    '''
    import tinyray


    class Sidecar:
        def __init__(self):
            self.inbox = []

        def whoami(self):
            return {
                "group": tinyray.my_group(),
                "rank": tinyray.my_rank(),
                "sizes": {name: tinyray.group_size(name) for name in tinyray.roster()},
            }

        def runtime_shape(self):
            """Did calling a peer drag a whole cluster manager in here?"""
            import tinyray.api as api

            tinyray.peers(tinyray.my_group())
            return {
                "head": api._context is not None,
                "peer_context": api._peer_context_value is not None,
                "in_worker": tinyray.mesh.in_worker(),
            }

        def accept(self, payload):
            self.inbox.append(payload)
            return {"rank": tinyray.my_rank(), "inbox": len(self.inbox)}

        def send_to(self, group, rank, payload):
            return tinyray.get(tinyray.peer(group, rank).accept.remote(payload))

        def relay_handle(self, group, rank, via):
            """Pickle a peer handle, send it to a third worker, have it used."""
            target = tinyray.peer(group, rank)
            courier = tinyray.peer(group, via)
            return tinyray.get(courier.use_handle.remote(target))

        def use_handle(self, handle):
            return tinyray.get(handle.accept.remote("relayed"))

        def fetch_from(self, group, rank):
            """Pull a payload straight from a peer, not through the driver."""
            ref = tinyray.peer(group, rank).make_payload.remote(1 << 20)
            return len(tinyray.get(ref))

        def make_payload(self, size):
            return b"x" * size

        def inbox_size(self):
            return len(self.inbox)


    if __name__ == "__main__":
        tinyray.serve(Sidecar())
    '''
)


@pytest.fixture(scope="module")
def sidecar_script(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("mesh") / "sidecar.py"
    path.write_text(SIDECAR)
    return path


@pytest.fixture(scope="module")
def mesh_cluster(sidecar_script: Path):
    tr.init()
    try:
        alpha = tr.launch_workers(
            [sys.executable, str(sidecar_script)],
            size=2,
            name="alpha",
            gpus_per_worker=0.0,
            cpus_per_worker=0.05,
            startup_timeout=180,
        )
        beta = tr.launch_workers(
            [sys.executable, str(sidecar_script)],
            size=3,
            name="beta",
            gpus_per_worker=0.0,
            cpus_per_worker=0.05,
            startup_timeout=180,
        )
        roster = tr.link(alpha=alpha, beta=beta)
        yield alpha, beta, roster
    finally:
        tr.shutdown()


def driver_bytes() -> int:
    return sum(
        peer["bytes_sent"] + peer["bytes_received"] for peer in tr.transport_stats().values()
    )


class TestDiscovery:
    def test_a_worker_knows_who_it_is(self, mesh_cluster):
        alpha, _beta, _roster = mesh_cluster
        who = tr.get(alpha[1].whoami.remote())
        assert who["group"] == "alpha"
        assert who["rank"] == 1

    def test_a_worker_sees_every_group(self, mesh_cluster):
        alpha, _beta, _roster = mesh_cluster
        who = tr.get(alpha[0].whoami.remote())
        assert who["sizes"] == {"alpha": 2, "beta": 3}, (
            "a worker cannot see the whole topology, so it cannot address a peer "
            "it was not explicitly handed"
        )

    def test_the_roster_the_driver_built_matches(self, mesh_cluster):
        _alpha, _beta, roster = mesh_cluster
        assert sorted(roster) == ["alpha", "beta"]
        assert [entry["rank"] for entry in roster["beta"]] == [0, 1, 2]
        assert all(":" in entry["endpoint"] for entry in roster["beta"])


class TestNoPhantomHead:
    """The bug that proved peer-to-peer was never a designed path.

    ``connect()`` inside a worker used to fall through to ``init()``, standing
    up a second Head -- placement table, supervision loop, node agent -- inside
    the worker. It worked, which is why nothing caught it, and it meant every
    sidecar believed it owned the machine's CPUs and GPUs.
    """

    def test_calling_a_peer_does_not_build_a_cluster_manager(self, mesh_cluster):
        alpha, _beta, _roster = mesh_cluster
        shape = tr.get(alpha[0].runtime_shape.remote())
        assert shape["in_worker"] is True
        assert shape["head"] is False, (
            "a worker built a Head just to call a peer; that is a second cluster "
            "manager per sidecar, each one double-counting the node's resources"
        )
        assert shape["peer_context"] is True, "the worker did not use the client-only peer context"

    def test_a_driver_still_has_a_head(self):
        import tinyray.api as api

        assert api._context is not None
        assert api._context.head is not None


class TestPeerCalls:
    def test_a_worker_calls_a_worker(self, mesh_cluster):
        alpha, beta, _roster = mesh_cluster
        result = tr.get(alpha[1].send_to.remote("beta", 2, "hello"))
        assert result["rank"] == 2
        assert tr.get(beta[2].inbox_size.remote()) >= 1

    def test_peer_traffic_does_not_reach_the_driver(self, mesh_cluster):
        alpha, _beta, _roster = mesh_cluster
        before = driver_bytes()
        # One megabyte, moved from one worker to another.
        size = tr.get(alpha[0].fetch_from.remote("beta", 0))
        moved = driver_bytes() - before
        assert size == 1 << 20
        assert moved < 20_000, (
            f"a 1 MB peer-to-peer fetch cost the driver {moved:,} bytes; it should "
            "cost only the dispatch that started it"
        )

    def test_an_unknown_group_is_named_in_the_error(self, mesh_cluster):
        with pytest.raises(mesh.NotLinked, match="gamma"):
            mesh.peers("gamma")

    def test_a_rank_out_of_range_is_refused(self, mesh_cluster):
        alpha, _beta, _roster = mesh_cluster
        with pytest.raises(tr.RemoteCallError, match=r"ranks 0\.\.2"):
            tr.get(alpha[0].send_to.remote("beta", 9, "nope"))


class TestHandlesTravel:
    """A mesh is only a mesh if a reference to a peer can be passed around."""

    def test_a_peer_handle_survives_a_round_trip(self, mesh_cluster):
        alpha, _beta, _roster = mesh_cluster
        restored = pickle.loads(pickle.dumps(alpha[0]))
        assert restored.endpoint == alpha[0].endpoint
        assert restored.actor_id == alpha[0].actor_id
        assert tr.get(restored.inbox_size.remote()) >= 0

    def test_a_handle_can_be_relayed_through_a_third_worker(self, mesh_cluster):
        alpha, beta, _roster = mesh_cluster
        before = tr.get(beta[1].inbox_size.remote())
        result = tr.get(alpha[0].relay_handle.remote("beta", 1, 2))
        assert result["rank"] == 1
        assert tr.get(beta[1].inbox_size.remote()) == before + 1

    def test_an_actor_handle_degrades_to_a_peer_handle(self):
        @tr.remote(num_cpus=0.05)
        class Counter:
            def value(self):
                return 7

        handle = Counter.remote()
        try:
            restored = pickle.loads(pickle.dumps(handle))
            # It can still be called...
            assert tr.get(restored.value.remote()) == 7
            # ...but it is no longer a management handle, because placement and
            # restart belong to whoever owns the head.
            assert isinstance(restored, tr.RemoteWorker)
            assert not isinstance(restored, type(handle))
        finally:
            tr.kill(handle)

    def test_hasattr_is_meaningless_on_a_handle(self):
        """A trap worth pinning, because it already caused one bug.

        Handles proxy every public name to a remote method, so ``hasattr``
        answers yes to anything. ``mesh._members`` originally dispatched on
        ``hasattr(x, "world_size")`` and therefore treated a single actor as a
        worker group. Underscore names are the only reliable discriminator.
        """

        @tr.remote(num_cpus=0.05)
        class Empty:
            pass

        handle = Empty.remote()
        try:
            assert hasattr(handle, "world_size")
            assert hasattr(handle, "there_is_no_such_method")
            assert not hasattr(handle, "_tinyray_group")
        finally:
            tr.kill(handle)


class TestLinking:
    def test_link_accepts_a_bare_list_of_handles(self, sidecar_script: Path):
        @tr.remote(num_cpus=0.05)
        class Node:
            def group(self):
                return tr.my_group(), tr.my_rank()

        actors = tr.create_actors(Node, count=2)
        try:
            tr.link(ring=actors)
            assert tr.get(actors[0].group.remote()) == ["ring", 0] or tr.get(
                actors[0].group.remote()
            ) == ("ring", 0)
        finally:
            for actor in actors:
                tr.kill(actor)

    def test_an_actor_and_a_served_process_can_share_a_mesh(self, mesh_cluster):
        alpha, beta, _roster = mesh_cluster

        @tr.remote(num_cpus=0.05)
        class Watcher:
            def count_peers(self, group):
                return len(tr.peers(group))

        watcher = Watcher.remote()
        try:
            tr.link(alpha=alpha, watcher=watcher)
            assert tr.get(watcher.count_peers.remote("alpha")) == 2
        finally:
            tr.kill(watcher)
            # Restore the roster the module fixture set up.
            tr.link(alpha=alpha, beta=beta)

    def test_relinking_replaces_the_previous_roster(self, mesh_cluster):
        alpha, beta, _roster = mesh_cluster
        tr.link(alpha=alpha)
        assert tr.get(alpha[0].whoami.remote())["sizes"] == {"alpha": 2}
        tr.link(alpha=alpha, beta=beta)
        assert tr.get(alpha[0].whoami.remote())["sizes"] == {"alpha": 2, "beta": 3}


class TestBeforeLinking:
    def test_a_driver_has_no_identity(self):
        with pytest.raises(mesh.NotLinked):
            mesh.my_group()

    def test_the_error_says_what_to_do(self):
        with pytest.raises(mesh.NotLinked, match=r"tinyray\.link"):
            mesh.my_rank()
