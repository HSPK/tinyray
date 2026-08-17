"""A classic dataloader -> trainer pipeline, with tinyray only in the middle.

The oldest shape in the book: some processes produce batches, some processes
consume them, and the consumers must never wait on the producers. What tinyray
adds is *not* a data path -- it is placement, supervision and the names that let
a trainer pull a batch straight from the loader that made it.

    loader 0 ──┐                        driver
    loader 1 ──┤  batches (megabytes)     │  refs (bytes)
    loader 2 ──┤ ───────────────────────► │
    loader 3 ──┘                          ▼
                                     rank 0..3   (native DDP script)

The driver never touches a batch. It hands the trainer a *reference* -- a task
id plus the address of the loader holding the result -- and each rank fetches
its own shard directly. The arrow through the driver carries a few hundred
bytes per step; the one that skips it carries all the data.

Three things this example is really about:

1. **Prefetch.** ``.remote()`` returns immediately, so batch *k+1* is being
   built while the trainer works on *k*. Without it the trainer waits for the
   loader on every step, and both halves run at half speed.
2. **References, not values.** ``tr.get`` is never called in the driver.
3. **Release.** A consumed batch is dropped explicitly rather than waiting for
   the watermark, because a loader holding stale batches is memory you paid for
   twice.

Run with ``python examples/dataloader_to_trainer.py``. It uses gloo and numpy
so it runs anywhere; swapping in a real dataset and a real model changes the
two generated scripts, not the controller.
"""

from __future__ import annotations

import sys
import textwrap
import time
from collections import deque
from pathlib import Path

import numpy as np

import tinyray as tr

HERE = Path(__file__).resolve().parent

WORLD_SIZE = 4
BATCH = (512, 1024)  # 2 MB of float32 per batch per rank
STEPS = 8
PREFETCH = 2  # batches in flight per loader

# --------------------------------------------------------------------------
# The trainer. An ordinary DDP script: it creates its own process group, owns
# its own optimiser, and never imports anything from tinyray except the one
# line at the bottom that gives it a control port.
# --------------------------------------------------------------------------
TRAINER = textwrap.dedent(
    '''
    import numpy as np
    import torch
    import torch.distributed as dist

    import tinyray

    torch.set_num_threads(1)


    class Trainer:
        def __init__(self, hidden=2048):
            dist.init_process_group(backend="gloo")
            self.rank = dist.get_rank()
            self.w1 = torch.randn(1024, hidden) * 0.02
            self.w2 = torch.randn(hidden, 1024) * 0.02
            self.seen = 0

        def train_step(self, batch_refs, lr):
            """Take one step on this rank's shard.

            ``batch_refs`` arrives as a plain list, so tinyray leaves it alone --
            only top-level reference arguments are resolved automatically. That
            is what we want here: each rank fetches *its own* shard and nobody
            pulls the other three.
            """
            batch = tinyray.get(batch_refs[self.rank])   # loader -> here, direct
            self.seen += batch.shape[0]

            # An ordinary forward/backward. Nothing here knows about tinyray.
            x = torch.from_numpy(np.array(batch))
            h = torch.relu(x @ self.w1)
            out = h @ self.w2
            loss = (out - x).pow(2).mean()

            g_out = 2.0 * (out - x) / out.numel()
            g_w2 = h.T @ g_out
            g_h = (g_out @ self.w2.T) * (h > 0)
            g_w1 = x.T @ g_h

            # Your collective, on your process group. tinyray never took it.
            dist.all_reduce(g_w1, op=dist.ReduceOp.SUM)
            dist.all_reduce(g_w2, op=dist.ReduceOp.SUM)
            world = dist.get_world_size()
            self.w1 -= lr * g_w1 / world
            self.w2 -= lr * g_w2 / world

            return {"rank": self.rank, "samples": self.seen, "loss": float(loss)}


    if __name__ == "__main__":
        tinyray.serve(Trainer())
    '''
)


# --------------------------------------------------------------------------
# The loader. This one *is* a tinyray actor, because unlike the trainer it has
# no framework of its own to defer to -- it is just Python holding a shard.
# --------------------------------------------------------------------------
@tr.remote(num_cpus=0.25)
class Loader:
    """One shard of the dataset, plus the epoch bookkeeping."""

    def __init__(self, num_shards: int, seed: int = 0) -> None:
        # `create_actors` is atomic, and atomicity means one set of constructor
        # arguments for the whole gang -- there is no rank to read here, unlike
        # in the trainer, where torchrun's environment supplies one. Per-member
        # identity is a second, cheap call.
        self.num_shards = num_shards
        self.seed = seed
        self.shard = -1
        self.rng = np.random.default_rng(seed)
        self.produced = 0

    def assign_shard(self, shard: int) -> int:
        self.shard = shard
        self.rng = np.random.default_rng(self.seed + shard)
        return shard

    def next_batch(self) -> np.ndarray:
        # Stands in for decode + augment + collate. The sleep is what makes
        # prefetching worth anything: a loader that returns instantly hides the
        # very problem this example exists to show.
        time.sleep(0.05)
        self.produced += 1
        batch = self.rng.standard_normal(BATCH, dtype=np.float32)
        batch += self.shard  # so a rank can tell whose shard it received
        return batch

    def stats(self) -> dict:
        return {"shard": self.shard, "produced": self.produced}


def run_epoch(loaders, trainer, *, prefetch: int) -> float:
    """One pass, with ``prefetch`` batches in flight per loader."""
    inflight: deque[list] = deque()
    started = time.perf_counter()

    for step in range(STEPS):
        # Submit ahead. Every .remote() here returns before the loader has done
        # any work, so the queue fills while the trainer is still busy.
        while len(inflight) < prefetch:
            inflight.append([loader.next_batch.remote() for loader in loaders])

        batch_refs = inflight.popleft()

        # One dispatch to every rank. `run` sends to all of them before
        # awaiting any, which is the only safe way to call a method that
        # contains a collective.
        stats = trainer.run("train_step", batch_refs, 0.01)

        # The batches are consumed. Say so, rather than leaving four loaders
        # holding results until the watermark or the TTL notices.
        tr.release(batch_refs)

        if step == STEPS - 1:
            print(f"    rank 0 saw {stats[0]['samples']} samples, loss {stats[0]['loss']:.4f}")

    elapsed = time.perf_counter() - started

    # Prefetching means batches were built that the epoch never used. They are
    # real memory on the loaders; drop them rather than letting the TTL do it.
    for leftover in inflight:
        tr.release(leftover)

    return elapsed


def main() -> None:
    trainer_path = HERE / "_generated_ddp_trainer.py"
    trainer_path.write_text(TRAINER)

    tr.init()
    try:
        # One loader per rank keeps the mapping obvious. In a real job there are
        # usually more loaders than ranks, and the rank picks from a pool.
        loaders = tr.create_actors(Loader, WORLD_SIZE, count=WORLD_SIZE, strategy="SPREAD")
        tr.get([loader.assign_shard.remote(i) for i, loader in enumerate(loaders)])
        print(f"loaders up: {len(loaders)} shards")

        # The trainer is launched, not written by us: torchrun's environment is
        # injected, every rank is started before any is awaited, and placement
        # is atomic.
        #
        # For a real stack this is your own command line, unchanged:
        #   [sys.executable, "pretrain_gpt.py", "--tensor-model-parallel-size", "4", ...]
        trainer = tr.launch_workers(
            [sys.executable, str(trainer_path)],
            size=WORLD_SIZE,
            name="trainer",
            gpus_per_worker=0.0,  # 1.0 in a real stack
            cpus_per_worker=0.25,
            startup_timeout=180,
        )
        print(
            f"trainer up: world_size={trainer.world_size} "
            f"at {trainer.master_addr}:{trainer.master_port}\n"
        )

        print("serial (prefetch=1): loader and trainer take turns")
        serial = run_epoch(loaders, trainer, prefetch=1)
        print(f"    {serial:.2f}s\n")

        print(f"pipelined (prefetch={PREFETCH}): loader runs ahead")
        pipelined = run_epoch(loaders, trainer, prefetch=PREFETCH)
        print(f"    {pipelined:.2f}s  ({serial / pipelined:.2f}x)\n")

        # The ceiling is set by the slower stage, not by tinyray. Measuring the
        # loader on its own says how much of that ceiling we actually reached.
        #
        # Note `wait`, not `get`. We want to know *when* four batches are ready,
        # not what is in them -- and `get` here would drag 8 MB into the driver
        # to time a stopwatch, which is the exact thing this example is about.
        probe = time.perf_counter()
        pending = [loader.next_batch.remote() for loader in loaders]
        tr.wait(pending, num_returns=len(pending))
        load_step = time.perf_counter() - probe
        tr.release(pending)
        train_step = max(serial / STEPS - load_step, 1e-9)
        ceiling = (load_step + train_step) / max(load_step, train_step)
        print(f"    load {load_step * 1e3:.0f}ms/step, train {train_step * 1e3:.0f}ms/step")
        print(
            f"    perfect overlap would be {ceiling:.2f}x; we reached {serial / pipelined:.2f}x\n"
        )

        # ------------------------------------------------------------------
        # What actually moved.
        # ------------------------------------------------------------------
        produced = sum(tr.get(loader.stats.remote())["produced"] for loader in loaders)
        payload = produced * int(np.prod(BATCH)) * 4

        through_driver = sum(
            peer["bytes_sent"] + peer["bytes_received"] for peer in tr.transport_stats().values()
        )
        print(f"batches produced:            {produced}")
        print(f"data loader -> trainer:      {payload / 1e6:,.1f} MB")
        print(f"data through the driver:     {through_driver / 1e3:,.1f} KB")
        print(f"ratio:                       {payload / through_driver:,.0f}x")
        print("\nThe driver moved names. The batches went straight from the loader")
        print("that built them to the rank that trained on them.")
    finally:
        tr.shutdown()
        trainer_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
