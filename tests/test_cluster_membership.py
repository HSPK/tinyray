"""Membership for a cluster tinyray did not launch.

The scale path. Its claims are specific and each one is asserted here:

* a worker registers itself, so joining costs O(1) per worker instead of O(N)
  from one driver;
* a lookup is scoped, so what crosses the wire is bounded by what you asked for
  and **not** by how big the cluster is -- this is the property that the
  original roster push lacked, and it is why that design stopped at a few
  hundred workers;
* death is noticed by lease expiry, so nothing has to supervise processes it
  did not start;
* replicas need no consensus, because every entry is re-asserted by its owner
  within one lease;
* and the registry going away entirely does not stop a training job.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

import tinyray as tr
from tinyray import serde
from tinyray.cluster import RegistryClient, RegistryUnavailable
from tinyray.registry import Registry

WORKER = textwrap.dedent(
    """
    import os, time
    import tinyray


    class Worker:
        def __init__(self):
            self.calls = 0

        def ping(self):
            self.calls += 1
            return {"rank": int(os.environ.get("RANK", 0)), "calls": self.calls}

        def ask_peer(self, group, rank):
            return tinyray.get(tinyray.group(group)[rank].ping.remote())

        def facts(self):
            m = tinyray.membership()
            return {"group": m.group, "rank": m.rank, "world": m.world_size}


    tinyray.join(Worker(), group=os.environ["GROUP"])
    while True:
        time.sleep(0.5)
    """
)


# --------------------------------------------------------------------------
# The registry on its own -- no network, no processes
# --------------------------------------------------------------------------


class TestRegistry:
    def make(self, ttl: float = 30.0) -> Registry:
        return Registry(ttl=ttl)

    def test_a_worker_announces_itself(self):
        registry = self.make()
        answer = registry.register("trainer", 3, "10.0.0.3:41234", "a" * 32, 8)
        assert answer["lease"] == "trainer/3"
        assert registry.lookup("trainer")["members"][0]["rank"] == 3

    def test_a_restarted_rank_replaces_its_old_address(self):
        registry = self.make()
        registry.register("trainer", 0, "10.0.0.1:41234", "a" * 32)
        answer = registry.register("trainer", 0, "10.0.0.9:55555", "b" * 32)
        assert answer["replaced"] == "10.0.0.1:41234"
        members = registry.lookup("trainer")["members"]
        assert len(members) == 1, "a restart must not leave a second entry for the same rank"
        assert members[0]["endpoint"] == "10.0.0.9:55555"

    def test_a_heartbeat_for_an_unknown_lease_says_so(self):
        registry = self.make()
        answer = registry.heartbeat("trainer/7")
        assert answer["known"] is False, (
            "an unknown lease must be reported, not raised: the worker's correct "
            "response is to register again, and an exception would kill it instead"
        )

    def test_a_silent_worker_is_evicted(self):
        registry = self.make(ttl=0.2)
        registry.register("trainer", 0, "10.0.0.1:41234", "a" * 32)
        assert len(registry.lookup("trainer")["members"]) == 1
        time.sleep(0.3)
        assert registry.lookup("trainer")["members"] == [], (
            "a worker that stopped heartbeating is still listed; peers will "
            "address a process that no longer exists"
        )

    def test_a_heartbeat_keeps_a_worker_alive(self):
        registry = self.make(ttl=0.3)
        registry.register("trainer", 0, "10.0.0.1:41234", "a" * 32)
        for _ in range(4):
            time.sleep(0.1)
            registry.heartbeat("trainer/0")
        assert len(registry.lookup("trainer")["members"]) == 1

    def test_the_version_moves_only_when_membership_does(self):
        registry = self.make()
        registry.register("trainer", 0, "10.0.0.1:41234", "a" * 32)
        settled = registry.lookup("trainer")["version"]
        registry.heartbeat("trainer/0")
        assert registry.lookup("trainer")["version"] == settled, (
            "a heartbeat bumped the version, so every watcher would re-fetch the "
            "whole group once per heartbeat per worker"
        )
        registry.register("trainer", 1, "10.0.0.2:41234", "b" * 32)
        assert registry.lookup("trainer")["version"] > settled

    def test_an_unchanged_lookup_can_skip_the_payload(self):
        registry = self.make()
        registry.register("trainer", 0, "10.0.0.1:41234", "a" * 32)
        version = registry.lookup("trainer")["version"]
        answer = registry.lookup("trainer", since=version)
        assert answer["unchanged"] is True
        assert answer["members"] == []

    def test_deregistration_is_immediate(self):
        registry = self.make()
        registry.register("trainer", 0, "10.0.0.1:41234", "a" * 32)
        assert registry.deregister("trainer/0")["removed"] is True
        assert registry.lookup("trainer")["members"] == []


class TestLookupDoesNotGrowWithTheCluster:
    """The property the original design did not have.

    Pushing a roster from the driver cost O(N) calls and O(N^2) bytes: at 8,192
    workers, 278 KB per push and 2.3 GB in total, out of one process. A scoped
    lookup is bounded by the request.
    """

    def populate(self, registry: Registry, count: int) -> None:
        for rank in range(count):
            registry.register(
                "trainer",
                rank,
                f"10.{rank // 65536}.{rank // 256 % 256}.{rank % 256}:41234",
                f"{rank:032x}",
                count,
            )

    def wire_size(self, payload) -> int:
        body, frames = serde.serialize(payload)
        return len(bytes(body)) + sum(len(bytes(frame)) for frame in frames)

    @pytest.mark.parametrize("cluster_size", [64, 1024, 8192])
    def test_a_scoped_answer_is_the_same_size_at_any_scale(self, cluster_size: int):
        registry = Registry()
        self.populate(registry, cluster_size)
        answer = registry.lookup("trainer", list(range(8)))
        assert len(answer["members"]) == 8
        # ~100 bytes an entry; the point is that it does not track cluster_size.
        assert self.wire_size(answer) < 2_000, (
            f"an 8-rank lookup in a {cluster_size}-worker cluster returned "
            f"{self.wire_size(answer)} bytes; scoping is not working and every "
            "worker's memory would grow with the cluster"
        )

    def test_an_unscoped_lookup_is_the_expensive_one(self):
        registry = Registry()
        self.populate(registry, 4096)
        scoped = self.wire_size(registry.lookup("trainer", list(range(8))))
        everything = self.wire_size(registry.lookup("trainer"))
        assert everything > 100 * scoped, (
            "asking for the whole group is supposed to be visibly expensive, so "
            "that the cheap call is the obvious one"
        )

    def test_group_sizes_are_cheap_at_any_scale(self):
        registry = Registry()
        self.populate(registry, 8192)
        assert self.wire_size(registry.groups()) < 500, (
            "a group listing should carry counts, not members"
        )


# --------------------------------------------------------------------------
# Real processes, started the way a launcher would start them
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def worker_script(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("cluster") / "worker.py"
    path.write_text(WORKER)
    return path


class Fleet:
    """A registry pair plus workers, started as Slurm or torchrun would."""

    def __init__(self, script: Path) -> None:
        self.script = script
        self.registries: list[subprocess.Popen] = []
        self.workers: list[subprocess.Popen] = []
        self.ports: list[int] = []

    @property
    def address(self) -> str:
        return ",".join(f"127.0.0.1:{port}" for port in self.ports)

    def start_registries(self, count: int, ttl: float, tmp: Path) -> None:
        from tinyray.process import free_port

        source = tmp / "registry.py"
        source.write_text(
            "import sys, tinyray\n"
            "tinyray.serve_registry(bind=f'127.0.0.1:{sys.argv[1]}', ttl=float(sys.argv[2]))\n"
        )
        for _ in range(count):
            port = free_port()
            self.ports.append(port)
            self.registries.append(
                subprocess.Popen(
                    [sys.executable, str(source), str(port), str(ttl)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )
        deadline = time.monotonic() + 60
        client = RegistryClient(self.address)
        while time.monotonic() < deadline:
            try:
                client.groups()
                return
            except RegistryUnavailable:
                time.sleep(0.2)
        raise RuntimeError("registries never came up")

    def start_workers(self, count: int, group: str) -> None:
        for rank in range(count):
            env = dict(os.environ)
            env.update(
                RANK=str(rank),
                WORLD_SIZE=str(count),
                LOCAL_RANK=str(rank),
                GROUP=group,
                TINYRAY_REGISTRY=self.address,
            )
            self.workers.append(
                subprocess.Popen(
                    [sys.executable, str(self.script)],
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )

    def stop(self) -> None:
        for process in self.workers + self.registries:
            process.kill()
        for process in self.workers + self.registries:
            process.wait(timeout=30)


@pytest.fixture(scope="module")
def fleet(worker_script: Path, tmp_path_factory):
    running = Fleet(worker_script)
    try:
        running.start_registries(2, ttl=4.0, tmp=tmp_path_factory.mktemp("reg"))
        running.start_workers(3, "trainer")
        cluster = tr.attach(running.address)
        cluster.group("trainer").wait_ready(size=3, timeout=120)
        yield running, cluster
    finally:
        running.stop()


class TestJoining:
    def test_workers_register_themselves(self, fleet):
        _running, cluster = fleet
        assert cluster.groups() == {"trainer": 3}

    def test_rank_comes_from_the_launcher(self, fleet):
        _running, cluster = fleet
        facts = cluster.group("trainer").run("facts")
        assert sorted(entry["rank"] for entry in facts) == [0, 1, 2]
        assert all(entry["world"] == 3 for entry in facts), (
            "world size was not read from the launcher's environment"
        )

    def test_no_resources_are_declared_anywhere(self, fleet):
        _running, cluster = fleet
        member = cluster.group("trainer").members()[0]
        assert "num_gpus" not in member
        assert "num_cpus" not in member
        assert "cuda_visible_devices" in member["meta"], (
            "tinyray should report the devices the launcher assigned, not assign any"
        )

    def test_a_controller_can_call_the_fleet(self, fleet):
        _running, cluster = fleet
        answers = cluster.group("trainer").run("ping")
        assert len(answers) == 3

    def test_a_worker_can_call_a_peer(self, fleet):
        _running, cluster = fleet
        answer = tr.get(cluster.group("trainer")[0].ask_peer.remote("trainer", 2))
        assert answer["rank"] == 2


class TestAvailability:
    def test_losing_a_replica_is_survivable(self, fleet):
        running, _cluster = fleet
        client = RegistryClient(running.address)
        assert client.groups() == {"trainer": 3}
        running.registries[0].kill()
        running.registries[0].wait(timeout=30)
        try:
            assert client.groups() == {"trainer": 3}, (
                "the surviving replica did not have the membership; replicas are "
                "supposed to converge from heartbeats alone"
            )
            assert client.failures >= 1
        finally:
            # The remaining tests still need a registry, and the fixture is
            # module-scoped, so leave the survivor running.
            pass

    def test_a_dead_worker_expires_without_a_supervisor(self, fleet):
        running, cluster = fleet
        group = cluster.group("trainer")
        running.workers[-1].kill()
        running.workers[-1].wait(timeout=30)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if len(group.members(fresh=True)) == 2:
                return
            time.sleep(0.5)
        pytest.fail(
            "the dead rank is still registered; nothing supervises these "
            "processes, so the lease is the only thing that can remove it"
        )

    def test_a_lost_registry_does_not_stop_the_job(self, fleet):
        running, _cluster = fleet
        # A short cache TTL, so the fallback path is genuinely exercised. With
        # the default TTL the second lookup is served from a *fresh* cache,
        # which proves nothing about what happens when the registry is gone --
        # the first version of this test asserted stale fallback while never
        # reaching that code.
        client = RegistryClient(running.address, cache_ttl=0.2)
        warm = client.lookup("trainer")
        assert warm, "cache was never warmed"

        for process in running.registries:
            process.kill()
        for process in running.registries:
            process.wait(timeout=30)
        time.sleep(0.5)

        served = client.lookup("trainer")
        assert served == warm, (
            "with every replica gone the client raised or returned nothing. A "
            "stale endpoint is worth far more than a stopped training job"
        )
        assert client.served_from_stale >= 1

        # And the peers themselves are unaffected: the registry was never in the
        # data path, only in the lookup path.
        from tinyray.attach import connect

        worker = connect(served[0]["endpoint"], served[0]["actor_id"])
        assert tr.get(worker.ping.remote())["rank"] in (0, 1, 2)


class TestClientWithoutARegistry:
    def test_the_error_names_the_variable(self):
        previous = os.environ.pop("TINYRAY_REGISTRY", None)
        try:
            with pytest.raises(RegistryUnavailable, match="TINYRAY_REGISTRY"):
                RegistryClient()
        finally:
            if previous is not None:
                os.environ["TINYRAY_REGISTRY"] = previous

    def test_an_unreachable_registry_with_no_cache_raises(self):
        from tinyray.process import free_port

        client = RegistryClient(f"127.0.0.1:{free_port()}")
        with pytest.raises(RegistryUnavailable):
            client.lookup("trainer")
