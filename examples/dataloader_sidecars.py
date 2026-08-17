"""A real torch DataLoader and a real DDP trainer, wired together by sidecars.

This is the shape the design was originally missing. tinyray is not the
dataloader and not the trainer: each of those is the framework's own object,
built by the framework's own code, in a process the framework owns. tinyray is
the *connector* -- one sidecar beside each loader worker, one beside each
data-parallel rank -- and the sidecars talk to each other.

    loader process 0..3                     trainer process 0..1
    ┌──────────────────────────┐            ┌──────────────────────────┐
    │ torch.utils.data.        │            │ model + optimiser        │
    │   DataLoader(num_workers)│            │ dist.all_reduce          │
    │          ▲ local         │            │          ▲ local         │
    │  ┌───────┴────────┐      │            │  ┌───────┴────────┐      │
    │  │ tinyray sidecar│◄─────┼── direct ──┼─►│ tinyray sidecar│      │
    │  └────────────────┘      │            │  └────────────────┘      │
    └──────────────────────────┘            └──────────────────────────┘

                    driver: starts them, introduces them, then goes quiet

The driver is not in the loop. It places the two fleets, calls ``tr.link`` once
so every sidecar learns its peers, and says "go". After that the trainer sidecar
pulls batches straight from its loaders and pushes epoch boundaries back to
them, and the driver moves **zero bytes** until the run is over.

Both directions are real:

* trainer -> loader: ``set_epoch``, ``prefetch``, and the pull itself
* loader -> trainer: the batches, fetched from the loader that produced them

Contrast with the star topology tinyray started with, where every message went
driver -> worker and back. A pipeline is not a fan-out, and routing a pipeline
through its controller makes the controller the bottleneck at exactly the point
where it has nothing useful to contribute.

Run with ``python examples/dataloader_sidecars.py``.
"""

from __future__ import annotations

import sys
import textwrap
import time
from pathlib import Path

import tinyray as tr

HERE = Path(__file__).resolve().parent

NUM_LOADERS = 4
NUM_RANKS = 2
STEPS = 12

# --------------------------------------------------------------------------
# The loader process. Everything above `tinyray.serve` is ordinary PyTorch --
# a Dataset, a DistributedSampler, a DataLoader with its own worker processes.
# tinyray does not know any of it exists.
# --------------------------------------------------------------------------
LOADER = textwrap.dedent(
    '''
    import os

    import torch
    from torch.utils.data import DataLoader, Dataset

    import tinyray

    SHARD = int(os.environ["RANK"])
    SHARDS = int(os.environ["WORLD_SIZE"])


    class Shard(Dataset):
        """Stands in for webdataset, a memmapped corpus, an index file."""

        def __init__(self, shard, shards, length=4096, dim=1024):
            self.shard, self.shards, self.dim = shard, shards, dim
            self.length = length // shards

        def __len__(self):
            return self.length

        def __getitem__(self, index):
            g = torch.Generator().manual_seed(self.shard * 100_003 + index)
            return torch.randn(self.dim, generator=g) + self.shard


    class LoaderSidecar:
        """A control surface on a DataLoader tinyray did not build."""

        def __init__(self):
            self.dataset = Shard(SHARD, SHARDS)
            # num_workers > 0: the DataLoader forks its own worker processes.
            # They are the framework's, not tinyray's, and nothing here manages
            # them -- which is the whole point.
            self.loader = DataLoader(
                self.dataset, batch_size=64, num_workers=2,
                shuffle=True, drop_last=True, persistent_workers=True,
            )
            self.it = iter(self.loader)
            self.epoch = 0
            self.served = 0

        def next_batch(self):
            """One batch, straight off the real DataLoader."""
            try:
                batch = next(self.it)
            except StopIteration:
                self.epoch += 1
                self.it = iter(self.loader)
                batch = next(self.it)
            self.served += 1
            return batch.numpy()

        # -- control surface, called by the trainer sidecar, not the driver ---

        def set_epoch(self, epoch):
            """What a DistributedSampler needs at an epoch boundary."""
            self.epoch = epoch
            self.it = iter(self.loader)
            return {"shard": SHARD, "epoch": self.epoch}

        def stats(self):
            return {
                "shard": SHARD,
                "served": self.served,
                "epoch": self.epoch,
                "loader_workers": self.loader.num_workers,
            }


    if __name__ == "__main__":
        tinyray.serve(LoaderSidecar())
    '''
)

# --------------------------------------------------------------------------
# The trainer process. An ordinary DDP script. The only tinyray-aware part is
# that its input comes from peers instead of from a local DataLoader.
# --------------------------------------------------------------------------
TRAINER = textwrap.dedent(
    '''
    import numpy as np
    import torch
    import torch.distributed as dist

    import tinyray

    torch.set_num_threads(1)


    class TrainerSidecar:
        def __init__(self, hidden=2048):
            dist.init_process_group(backend="gloo")   # yours
            self.rank = dist.get_rank()
            self.world = dist.get_world_size()
            self.w1 = torch.randn(1024, hidden) * 0.02
            self.w2 = torch.randn(hidden, 1024) * 0.02
            self.seen = 0
            self.mine = []

        def bind_loaders(self):
            """Claim this rank's share of the loader fleet.

            Discovery, not configuration: the sidecar asks the roster who the
            loaders are. Nobody had to pass endpoints down a command line, and
            nothing here knows the driver's address.
            """
            loaders = tinyray.peers("loader")
            self.mine = loaders[self.rank :: self.world]
            return {"rank": self.rank, "loaders": [w.endpoint for w in self.mine]}

        def train(self, steps, lr=0.05, prefetch=2):
            """Own the loop. The driver is not involved in any iteration."""
            inflight = []
            history = []
            for step in range(steps):
                # Pull ahead, straight from the loaders. .remote() returns
                # before the DataLoader has been touched, so decode overlaps
                # with the backward pass.
                while len(inflight) < prefetch:
                    inflight.append([w.next_batch.remote() for w in self.mine])

                for ref in inflight.pop(0):
                    batch = tinyray.get(ref)          # loader -> here, direct
                    history.append(self._step(batch, lr))

                if step and step % 6 == 0:
                    # Trainer -> loader control, peer to peer. A real job does
                    # this at an epoch boundary so the sampler reshuffles.
                    tinyray.get([w.set_epoch.remote(step // 6) for w in self.mine])

            for leftover in inflight:
                tinyray.release(leftover)

            # Each rank alternates between its shards, and the shards sit at
            # different offsets, so a single batch's loss says more about which
            # shard it came from than about the model. Halves average that out.
            half = len(history) // 2
            return {
                "rank": self.rank,
                "samples": self.seen,
                "batches": len(history),
                "loss_first_half": sum(history[:half]) / half,
                "loss_second_half": sum(history[half:]) / (len(history) - half),
            }

        def _step(self, batch, lr):
            x = torch.from_numpy(np.array(batch))
            self.seen += x.shape[0]

            h = torch.relu(x @ self.w1)
            out = h @ self.w2
            loss = (out - x).pow(2).mean()

            g_out = 2.0 * (out - x) / out.numel()
            g_w2 = h.T @ g_out
            g_h = (g_out @ self.w2.T) * (h > 0)
            g_w1 = x.T @ g_h

            dist.all_reduce(g_w1, op=dist.ReduceOp.SUM)   # yours
            dist.all_reduce(g_w2, op=dist.ReduceOp.SUM)
            self.w1 -= lr * g_w1 / self.world
            self.w2 -= lr * g_w2 / self.world
            return float(loss)


    if __name__ == "__main__":
        tinyray.serve(TrainerSidecar())
    '''
)


def driver_bytes() -> int:
    return sum(
        peer["bytes_sent"] + peer["bytes_received"] for peer in tr.transport_stats().values()
    )


def main() -> None:
    loader_path = HERE / "_generated_loader_sidecar.py"
    trainer_path = HERE / "_generated_trainer_sidecar.py"
    loader_path.write_text(LOADER)
    trainer_path.write_text(TRAINER)

    tr.init()
    try:
        # -- two independent fleets ------------------------------------------
        #
        # Neither is "the tinyray dataloader" or "the tinyray trainer". Each is
        # a native script that ends in one `tinyray.serve` line. In production
        # these are separate node pools: CPU boxes for decode, GPU boxes for the
        # model.
        loaders = tr.launch_workers(
            [sys.executable, str(loader_path)],
            size=NUM_LOADERS,
            name="loader",
            gpus_per_worker=0.0,
            cpus_per_worker=0.5,  # the DataLoader forks its own workers too
            startup_timeout=240,
        )
        trainers = tr.launch_workers(
            [sys.executable, str(trainer_path)],
            size=NUM_RANKS,
            name="trainer",
            gpus_per_worker=0.0,  # 1.0 in a real stack
            cpus_per_worker=0.5,
            startup_timeout=240,
        )
        print(f"loader fleet:  {loaders.world_size} processes, each with its own DataLoader")
        print(
            f"trainer group: {trainers.world_size} ranks at "
            f"{trainers.master_addr}:{trainers.master_port}"
        )

        # -- introduce them ---------------------------------------------------
        #
        # The one thing only the driver can do. Endpoints do not exist until
        # every worker has bound a port, so this is a push after startup rather
        # than an environment variable.
        tr.link(loader=loaders, trainer=trainers)
        bound = trainers.run("bind_loaders")
        for entry in bound:
            print(f"  rank {entry['rank']} pulls from {len(entry['loaders'])} loaders")

        # -- go ---------------------------------------------------------------
        before = driver_bytes()
        started = time.perf_counter()
        results = trainers.run("train", STEPS, timeout=600)
        elapsed = time.perf_counter() - started
        during = driver_bytes() - before

        print(f"\ntrained {STEPS} steps in {elapsed:.2f}s")
        for entry in results:
            print(
                f"  rank {entry['rank']}: {entry['samples']} samples over "
                f"{entry['batches']} batches, mean loss "
                f"{entry['loss_first_half']:.3f} -> {entry['loss_second_half']:.3f}"
            )

        # ------------------------------------------------------------------
        # What the driver did while all that happened.
        # ------------------------------------------------------------------
        stats = tr.get([loaders[rank].stats.remote() for rank in range(NUM_LOADERS)])
        served = sum(entry["served"] for entry in stats)
        payload = served * 64 * 1024 * 4
        epochs = max(entry["epoch"] for entry in stats)

        print(f"\nbatches served by the DataLoaders: {served}")
        print(f"  epoch boundaries driven by the trainer: {epochs}")
        forked = sum(entry["loader_workers"] for entry in stats)
        print(f"  torch DataLoader worker processes:      {forked}")
        print(f"loader -> trainer:      {payload / 1e6:,.1f} MB, peer to peer")
        print(f"through the driver:     {during:,} bytes for the entire training loop")
        print("\nThe driver placed two fleets and introduced them. Everything after")
        print("that was one sidecar talking to another.")
    finally:
        tr.shutdown()
        loader_path.unlink(missing_ok=True)
        trainer_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
