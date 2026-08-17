"""A traditional actor-learner RL loop, with tinyray as the control plane only.

The loop every on-policy algorithm runs -- PPO, IMPALA, GRPO, pick one:

    for iteration:
        rollout workers generate trajectories with the current policy
        the learner consumes them and produces a new policy
        the new policy is pushed back to the rollout workers

tinyray's job is the *sequencing*: place the fleet, hand out ranks, watch for
deaths, decide when enough trajectories have arrived, and tell everyone when the
weights have changed. It carries neither the trajectories nor the weights.

    rollout 0..7                             learner rank 0..3
        │                                          │
        │  trajectories (megabytes, direct)        │
        └──────────────────────────────────────────┤
                                                   │
        ◄────────── weights (NCCL / CUDA IPC / disk)┘

        driver: a few hundred bytes per iteration, and the word "go"

Three things this example is really about:

1. **Stragglers.** ``wait(num_returns=k)`` takes the first *k* trajectories and
   leaves the rest. This drops the slow workers' *results*, not their work --
   they are still running, and their outputs still occupy memory on the
   producer until released.
2. **Stragglers still attend the weight sync.** Dropping a result is free;
   dropping a rank from a collective is a hang. Every worker is told about the
   new version, including the ones whose data was thrown away.
3. **The weights never touch tinyray.** Here they go through a file because
   that runs anywhere. In a real stack it is an NCCL broadcast or CUDA IPC, and
   tinyray's part is identical either way: it says *when*, not *how*.

Run with ``python examples/rl_control_plane.py``.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import textwrap
import time
from pathlib import Path

import tinyray as tr

HERE = Path(__file__).resolve().parent

NUM_ROLLOUTS = 8
NEEDED = 6  # trajectories per iteration; the other 2 may straggle
LEARNER_RANKS = 2
ITERATIONS = 4
HORIZON = 256  # steps per trajectory
OBS_DIM = 512

# --------------------------------------------------------------------------
# The rollout worker. Stands in for an SGLang or vLLM engine: it holds a copy
# of the policy, generates experience, and reloads weights when told to.
#
# It never calls init_process_group -- a rollout fleet is not a collective, it
# is N independent engines. tinyray gives them ranks anyway, which is how each
# one knows which slice of the environment seeds it owns.
# --------------------------------------------------------------------------
ROLLOUT = textwrap.dedent(
    '''
    import os, time
    import numpy as np

    import tinyray

    RANK = int(os.environ["RANK"])
    HORIZON = int(os.environ["HORIZON"])
    OBS_DIM = int(os.environ["OBS_DIM"])


    class Rollout:
        def __init__(self):
            self.rng = np.random.default_rng(1234 + RANK)
            self.policy = np.zeros(OBS_DIM, dtype=np.float32)
            self.version = 0
            # Heterogeneous hardware, a long episode, an unlucky seed: real
            # fleets are never uniform, and the last worker is always the
            # problem. Rank 0 is fast, rank 7 is 4x slower.
            self.cost = 0.02 * (1 + RANK / 2)

        def generate(self):
            """Produce one trajectory with the current policy."""
            time.sleep(self.cost)
            obs = self.rng.standard_normal((HORIZON, OBS_DIM), dtype=np.float32)
            obs += self.policy                       # the policy shapes the data
            reward = float(obs.mean()) + self.version * 0.1
            return {
                "rank": RANK,
                "version": self.version,
                "obs": obs,                          # ~0.5 MB, stays on this host
                "reward": reward,
            }

        def reload(self, path, version):
            """Pull in the policy the learner just published.

            A real engine broadcasts over NCCL, maps CUDA IPC handles, or loads
            a checkpoint. All three bypass tinyray. What tinyray provided is the
            only thing it should: the signal that there is something to load.
            """
            self.policy = np.load(path)
            self.version = version
            return {"rank": RANK, "version": version}


    if __name__ == "__main__":
        tinyray.serve(Rollout())
    '''
)

# --------------------------------------------------------------------------
# The learner. An ordinary DDP script that happens to accept references to
# trajectories instead of the trajectories themselves.
# --------------------------------------------------------------------------
LEARNER = textwrap.dedent(
    '''
    import numpy as np
    import torch
    import torch.distributed as dist

    import tinyray

    torch.set_num_threads(1)


    class Learner:
        def __init__(self, obs_dim=512):
            dist.init_process_group(backend="gloo")
            self.rank = dist.get_rank()
            self.world = dist.get_world_size()
            self.policy = torch.zeros(obs_dim)
            self.version = 0

        def update(self, refs, lr):
            """One policy-gradient step over the trajectories that arrived.

            ``refs`` is a nested list, so tinyray passes it through untouched
            and each rank fetches only its own slice. Fetching all of them on
            every rank would move the whole batch `world_size` times.
            """
            mine = refs[self.rank :: self.world]
            grad = torch.zeros_like(self.policy)
            reward = 0.0
            for ref in mine:
                traj = tinyray.get(ref)              # rollout -> here, direct
                advantage = traj["reward"]
                grad += torch.from_numpy(np.array(traj["obs"].mean(axis=0))) * advantage
                reward += traj["reward"]

            # Your collective, on your process group.
            dist.all_reduce(grad, op=dist.ReduceOp.SUM)
            count = torch.tensor([float(len(mine))])
            dist.all_reduce(count, op=dist.ReduceOp.SUM)
            self.policy += lr * grad / max(count.item(), 1.0)
            self.version += 1

            return {
                "rank": self.rank,
                "consumed": len(mine),
                "reward": reward / max(len(mine), 1),
                "version": self.version,
            }

        def publish(self, path):
            """Write the new policy where the rollout fleet can pick it up."""
            if self.rank == 0:
                np.save(path, self.policy.numpy())
            dist.barrier()
            return self.version


    if __name__ == "__main__":
        tinyray.serve(Learner())
    '''
)


def main() -> None:
    rollout_path = HERE / "_generated_rollout.py"
    learner_path = HERE / "_generated_learner.py"
    rollout_path.write_text(ROLLOUT)
    learner_path.write_text(LEARNER)
    checkpoints = Path(tempfile.mkdtemp(prefix="tinyray-rl-"))

    tr.init()
    try:
        # -- the rollout fleet ----------------------------------------------
        #
        # Gang-placed: either all eight fit or none are started. A half-sized
        # fleet is not half a fleet, it is a job that silently trains on the
        # wrong batch size.
        #
        # For SGLang each of these would be a `launch_process` running
        # `python -m sglang.launch_server --port {port} ...`, wrapped by a thin
        # client. The control-plane code below would not change.
        rollouts = tr.launch_workers(
            [sys.executable, str(rollout_path)],
            size=NUM_ROLLOUTS,
            name="rollout",
            gpus_per_worker=0.0,  # 1.0 in a real stack
            cpus_per_worker=0.1,
            env={"HORIZON": str(HORIZON), "OBS_DIM": str(OBS_DIM)},
            startup_timeout=180,
        )
        print(f"rollout fleet: {rollouts.world_size} workers")

        # -- the learner ----------------------------------------------------
        learner = tr.launch_workers(
            [sys.executable, str(learner_path)],
            size=LEARNER_RANKS,
            name="learner",
            gpus_per_worker=0.0,  # 1.0 in a real stack
            cpus_per_worker=0.25,
            startup_timeout=180,
        )
        print(
            f"learner group: {learner.world_size} ranks at "
            f"{learner.master_addr}:{learner.master_port}\n"
        )

        rank_of = {rollouts[rank].endpoint: rank for rank in range(NUM_ROLLOUTS)}
        dropped = 0
        for iteration in range(ITERATIONS):
            started = time.perf_counter()

            # 1. Generate. Every worker is asked; none is waited on yet.
            pending = [rollouts[rank].generate.remote() for rank in range(NUM_ROLLOUTS)]

            # 2. Take the first NEEDED. `wait` returns references, never values,
            #    and asks each owner a yes/no question rather than pulling the
            #    trajectory back to find out.
            ready, stragglers = tr.wait(pending, num_returns=NEEDED, timeout=120)
            collected = time.perf_counter() - started

            # 3. The learner pulls the trajectories it was handed, straight from
            #    the workers that produced them.
            stats = learner.run("update", ready, 0.05)

            # 4. Publish, then tell the fleet. Two separate steps because the
            #    weights must exist before anyone is told to load them.
            #
            #    `run`, not `run_on(0, ...)`, even though only rank 0 writes the
            #    file. `publish` ends in a barrier -- that barrier is what makes
            #    "the checkpoint is complete" true rather than hopeful -- and a
            #    barrier entered by one rank out of two is a permanent hang.
            #    Any method containing a collective goes through `run`.
            checkpoint = str(checkpoints / f"policy_v{iteration + 1}.npy")
            version = learner.run("publish", checkpoint)[0]

            #    Note `rollouts` and not `ready`: the two stragglers get the new
            #    policy as well. Their *data* was discarded; they are still part
            #    of the fleet, and in a stack where this sync is an NCCL
            #    broadcast, skipping one would hang the other seven.
            acks = [
                rollouts[rank].reload.remote(checkpoint, version) for rank in range(NUM_ROLLOUTS)
            ]
            tr.get(acks)

            # 5. The straggling trajectories were never used. Say so -- they are
            #    half a megabyte each, sitting on the workers that made them.
            tr.release(stragglers)
            dropped += len(stragglers)

            # A reference knows which worker owns it, which is how the driver
            # names the stragglers without asking anyone.
            slow = sorted(rank_of[ref.owner_endpoint] for ref in stragglers)
            print(
                f"iteration {iteration}: {len(ready)} trajectories in {collected * 1e3:.0f}ms, "
                f"reward {stats[0]['reward']:+.3f}, policy v{version}, "
                f"straggled {slow}"
            )

        # ------------------------------------------------------------------
        # What actually moved.
        # ------------------------------------------------------------------
        generated = ITERATIONS * NUM_ROLLOUTS
        payload = generated * HORIZON * OBS_DIM * 4
        through_driver = sum(
            peer["bytes_sent"] + peer["bytes_received"] for peer in tr.transport_stats().values()
        )

        print(f"\ntrajectories generated:      {generated}")
        print(f"  consumed by the learner:   {generated - dropped}")
        print(f"  dropped as straggling:     {dropped}")
        print(f"rollout -> learner:          {payload / 1e6:,.1f} MB")
        print(
            f"weights learner -> rollouts: {ITERATIONS * NUM_ROLLOUTS * OBS_DIM * 4 / 1e6:.1f} MB "
            f"(through the filesystem, not tinyray)"
        )
        print(f"through the driver:          {through_driver / 1e3:,.1f} KB")
        print(f"ratio:                       {payload / through_driver:,.0f}x")
        print("\ntinyray decided who was late and when to swap the policy.")
        print("It carried neither the experience nor the weights.")
    finally:
        tr.shutdown()
        rollout_path.unlink(missing_ok=True)
        learner_path.unlink(missing_ok=True)
        shutil.rmtree(checkpoints, ignore_errors=True)


if __name__ == "__main__":
    main()
