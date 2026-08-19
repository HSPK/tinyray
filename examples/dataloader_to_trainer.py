"""Data workers feeding a data-parallel trainer.

The point of this one is the split between the two planes. Batches are tens of
megabytes, so they never touch tinyray: workers write them to shared storage
and hand over a path. Only the handover note -- a few hundred bytes -- goes
over the control plane.

    dataworker x 4   (churn)      read shards, produce batches
    trainer    x 4   (collective) form a round, consume their own shard

Run it:  python examples/dataloader_to_trainer.py
"""

from __future__ import annotations

import os
import pickle
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import tinyray

REGISTRY = Path(sys.executable).parent / "tinyray"
WORLD = 4
WORKERS = 4
STEPS = 12
BATCH_BYTES = 4 << 20  # 4 MB: four times over the control-plane limit


# --------------------------------------------------------------------------
# Data worker: interchangeable, so it has no seat. Trainers reach it by the
# shard it advertises, which is what published state is for.
# --------------------------------------------------------------------------
def run_dataworker(worker_id: str, shard: int, spool: str) -> None:
    class DataWorker:
        def __init__(self) -> None:
            self.served = 0

        def next_batch(self, step: int) -> dict:
            """Returns where the batch is, not the batch.

            Handing back the array itself would be rejected: the payload limit
            is enforced, so the mistake fails immediately instead of quietly
            making the control plane slow.
            """
            path = os.path.join(spool, f"shard{shard}-step{step}.pkl")
            with open(path, "wb") as fh:
                pickle.dump({"shard": shard, "step": step, "data": b"\0" * BATCH_BYTES}, fh)
            self.served += 1
            return {"path": path, "bytes": BATCH_BYTES, "shard": shard, "step": step}

        def served_count(self) -> int:
            return self.served

    svc = DataWorker()
    with tinyray.join("dataworker", "churn", serves=svc) as me:
        me.ready(worker=worker_id, shard=shard)
        trainers = tinyray.pool("trainer")
        # An empty pool means two opposite things -- not started yet, and
        # finished and gone home -- and the phone book cannot tell them apart,
        # because job lifecycle is not its business. Remembering that we saw
        # them is the one bit that separates the two, and it lives here.
        met = False
        deadline = time.monotonic() + 60
        while True:
            alive = trainers.all()
            if alive:
                met = True
                if all(h.state.get("done") for h in alive):
                    break
            elif met or time.monotonic() > deadline:
                break
            time.sleep(0.05)
        print(f"[dataworker {worker_id}] served {svc.served} batches", flush=True)


# --------------------------------------------------------------------------
# Trainer: a seat is a rank, and every rank must agree on the same list before
# a collective can be built.
# --------------------------------------------------------------------------
def run_trainer(rank: int, spool: str) -> None:
    with tinyray.join("trainer", "collective", slot=rank, size=WORLD) as me:
        me.ready(step=0)

        ep = tinyray.pool("trainer").epoch(timeout=30)
        if rank == 0:
            print(f"[trainer/0] round open with ranks {sorted(h.slot for h in ep)}", flush=True)

        # Checking ep.valid inside the loop would be useless: a rank stuck in a
        # collective never reaches the check. A separate thread can, because
        # NCCL releases the GIL while it blocks.
        broken = threading.Event()

        def watchdog() -> None:
            while ep.valid and not broken.is_set():
                time.sleep(0.05)
            broken.set()

        threading.Thread(target=watchdog, daemon=True).start()

        workers = tinyray.pool("dataworker")
        workers.wait(count=1, timeout=30)

        consumed = 0
        for step in range(STEPS):
            if broken.is_set():
                print(f"[trainer/{rank}] round broke at step {step}", flush=True)
                break
            # Each rank takes its own shard: a data-parallel rank that reads
            # someone else's shard silently trains on the wrong data.
            note = workers.pick(shard=rank).next_batch(step=step)
            with open(note["path"], "rb") as fh:
                batch = pickle.load(fh)
            assert batch["shard"] == rank and len(batch["data"]) == note["bytes"]
            os.unlink(note["path"])
            consumed += 1
            me.ready(step=step + 1)
            time.sleep(0.02)  # stands in for forward/backward

        me.ready(done=True)
        print(f"[trainer/{rank}] consumed {consumed} batches", flush=True)
        time.sleep(0.6)


def show_the_limit_is_real(spool: str) -> None:
    """The rule is enforced by the machinery, not by a line in a document."""
    with tinyray.join("prober", "churn") as me:
        me.ready()
        worker = tinyray.pool("dataworker").wait(count=1, timeout=30)[0]
        note = worker.next_batch(step=999)
        os.unlink(note["path"])
        print(f"[probe] handover note is {len(str(note))} bytes for a "
              f"{note['bytes'] // (1 << 20)} MB batch", flush=True)
        try:
            worker.next_batch(step="x" * (2 << 20))
        except ValueError as exc:
            print(f"[probe] oversized payload refused: {str(exc)[:60]}...", flush=True)
        else:
            raise AssertionError("the payload limit did not fire")


ROLES = {
    "dataworker": lambda a: run_dataworker(a[0], int(a[1]), a[2]),
    "trainer": lambda a: run_trainer(int(a[0]), a[1]),
    "probe": lambda a: show_the_limit_is_real(a[0]),
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] in ROLES:
        ROLES[sys.argv[1]](sys.argv[2:])
        return 0

    if not REGISTRY.exists():
        print(f"install first: maturin develop --release ({REGISTRY} missing)")
        return 1

    port = free_port()
    registry = subprocess.Popen(
        [str(REGISTRY), "--listen", f"127.0.0.1:{port}", "--ttl-ms", "2000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    env = dict(os.environ, TINYRAY_REGISTRY=f"127.0.0.1:{port}")
    spool = tempfile.mkdtemp(prefix="tinyray-batches-")
    time.sleep(0.8)

    procs = [
        subprocess.Popen([sys.executable, __file__, "dataworker", f"dw{i}", str(i), spool], env=env)
        for i in range(WORKERS)
    ]
    time.sleep(0.4)
    procs.append(subprocess.Popen([sys.executable, __file__, "probe", spool], env=env))
    procs[-1].wait(timeout=60)
    procs.pop()

    trainers = [
        subprocess.Popen([sys.executable, __file__, "trainer", str(r), spool], env=env)
        for r in range(WORLD)
    ]
    t0 = time.monotonic()
    rc = 0
    for p in trainers + procs:
        if p.wait(timeout=120) != 0:
            rc = 1
    print(f"\nfinished in {time.monotonic() - t0:.1f}s")
    registry.terminate()
    for leftover in Path(spool).glob("*"):
        leftover.unlink()
    Path(spool).rmdir()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
