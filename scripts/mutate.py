#!/usr/bin/env python3
"""Mutation testing: break an invariant on purpose and see if the suite notices.

A passing test suite proves nothing about the tests. This script deliberately
introduces the *exact* classes of bug that got through review, and reports which
ones the suite catches.

A surviving mutant is a hole in the tests, not a bug in the code.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENV_PY = ROOT / ".venv" / "bin" / "python"
CARGO = Path.home() / ".cargo" / "bin" / "cargo"


@dataclass
class Mutant:
    name: str
    why: str
    path: str
    old: str
    new: str
    command: list[str]


MUTANTS: list[Mutant] = [
    Mutant(
        name="heartbeat-removed",
        why="the bug that killed every session older than the timeout",
        path="python/tinyray/head.py",
        old="        agent.start_heartbeat(self)",
        new="        pass  # MUTANT: node never reports in",
        command=["pytest", "tests/test_cluster.py", "-x", "-q", "-k", "Heartbeat"],
    ),
    Mutant(
        name="constructor-not-replayed",
        why="a restarted actor is an empty process until __init__ is replayed",
        path="python/tinyray/api.py",
        old="            if constructor is not None:",
        new="            if False:  # MUTANT: never reconstruct",
        command=["pytest", "tests/test_cluster.py", "-x", "-q", "-k", "restarted"],
    ),
    Mutant(
        name="prewarm-not-primed",
        why="a pool that fills only on demand is always a step behind",
        path="python/tinyray/head.py",
        old="        self.pool.prime()",
        new="        pass  # MUTANT: lazy fill only",
        command=["pytest", "tests/test_lifecycle.py", "-x", "-q", "-k", "warm_actors"],
    ),
    Mutant(
        name="pickle-protocol-downgrade",
        why="protocol 4 still works, it just copies every tensor through the stream",
        path="python/tinyray/serde.py",
        old="PROTOCOL = 5",
        new="PROTOCOL = 4  # MUTANT: no out-of-band buffers",
        command=["pytest", "tests/test_serde.py", "-x", "-q"],
    ),
    Mutant(
        name="oob-threshold-disabled",
        why="inlining every buffer is correct but doubles the payload",
        path="python/tinyray/serde.py",
        old="            if view.nbytes < min_oob_size:\n                return True",
        new="            if True:  # MUTANT: everything inline\n                return True",
        command=["pytest", "tests/test_serde.py", "-x", "-q"],
    ),
    Mutant(
        name="executor-never-yields",
        why="blocking in Rust forever makes SIGTERM unreachable from Python",
        path="python/tinyray/worker_main.py",
        old="            task = self.runtime.next_task(timeout_seconds=0.2)",
        new="            task = self.runtime.next_task(timeout_seconds=3600.0)  # MUTANT",
        command=["pytest", "tests/test_lifecycle.py", "-x", "-q", "-k", "promptly"],
    ),
    Mutant(
        name="detached-silently-ignored",
        why="accepting an option and doing nothing is worse than refusing it",
        path="python/tinyray/api.py",
        old='            if lifetime == "detached":',
        new="            if False:  # MUTANT: silently accept detached",
        command=["pytest", "tests/test_cluster.py", "-x", "-q", "-k", "detached"],
    ),
    Mutant(
        name="heartbeat-interval-exceeds-deadline",
        why="an interval larger than the timeout declares every healthy node dead",
        path="python/tinyray/head.py",
        old="        self.heartbeat_interval = min(\n            heartbeat_interval, max(heartbeat_timeout / 4.0, 0.05)\n        )",
        new="        self.heartbeat_interval = heartbeat_interval  # MUTANT: ignore the deadline",
        command=["pytest", "tests/test_cluster.py", "-x", "-q", "-k", "survive"],
    ),
    Mutant(
        name="ttl-sweeper-disabled",
        why="results past their TTL are never reclaimed, so the store grows without bound",
        path="python/tinyray/worker_main.py",
        old="            self.runtime.sweep_expired()",
        new="            pass  # MUTANT: never sweep",
        command=["pytest", "tests/test_actors.py", "-x", "-q", "-k", "ttl_seconds"],
    ),
    Mutant(
        name="memory-not-accounted",
        why="an option that does not reach the scheduler is decoration",
        path="python/tinyray/head.py",
        old="                memory_bytes=memory_bytes,\n                strategy=strategy,",
        new="                memory_bytes=0,  # MUTANT: ignore the request\n                strategy=strategy,",
        command=["pytest", "tests/test_actors.py", "-x", "-q", "-k", "memory_bytes"],
    ),
    Mutant(
        name="wait-relays-payloads",
        why="answering a readiness question by shipping the whole result to the driver",
        path="crates/tinyray-runtime/src/client.rs",
        old="        let Ok(message) = build_fetch(reference.task_id, leg, true) else {",
        new="        let Ok(message) = build_fetch(reference.task_id, leg, false) else {",
        command=["cargo", "test", "-p", "tinyray-runtime", "--test", "end_to_end", "wait_"],
    ),
    Mutant(
        name="ordering-ignored",
        why="dispatching on arrival instead of sequence reorders actor calls",
        path="crates/tinyray-runtime/src/queue.rs",
        old="        if task.seq > caller.next_seq {",
        new="        if false {  // MUTANT: dispatch on arrival",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "queue::"],
    ),
    Mutant(
        name="newest-result-evicted",
        why="a result nobody has had a chance to read must not be the victim",
        path="crates/tinyray-runtime/src/store.rs",
        old="                .find(|(_, task_id)| Some(**task_id) != protect)",
        new="                .find(|(_, _task_id)| true)  // MUTANT: evict the newest too",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "store::"],
    ),
    Mutant(
        name="release-without-tombstone",
        why="a released result then looks like it never existed",
        path="crates/tinyray-runtime/src/store.rs",
        old="        if removed {\n            let capacity = self.config.tombstone_capacity;\n            inner.tombstone(task_id, capacity);\n        }",
        new="        if false {\n            let capacity = self.config.tombstone_capacity;\n            inner.tombstone(task_id, capacity);\n        }",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "store::"],
    ),
    Mutant(
        name="backpressure-disabled",
        why="an unbounded queue works until it exhausts the actor's memory",
        path="crates/tinyray-runtime/src/queue.rs",
        old="        if self.is_full() {\n            return Err(RejectReason::Backpressure);\n        }",
        new="        if false {\n            return Err(RejectReason::Backpressure);\n        }",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "queue::"],
    ),
    Mutant(
        name="duplicate-calls-executed",
        why="replaying a stateful call twice silently corrupts actor state",
        path="crates/tinyray-runtime/src/queue.rs",
        old="        if task.seq < caller.next_seq || caller.ahead.contains_key(&task.seq) {\n            return Err(RejectReason::DuplicateSeq);\n        }",
        new="        if false {\n            return Err(RejectReason::DuplicateSeq);\n        }",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "queue::"],
    ),
    Mutant(
        name="fetch-does-not-wait",
        why="returning NotReady immediately turns a slow producer into a failure",
        path="crates/tinyray-runtime/src/store.rs",
        old="            if tokio::time::timeout(deadline - now, notified).await.is_err() {",
        new="            if true {  // MUTANT: never wait for the producer",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "store::"],
    ),
    Mutant(
        name="gang-not-atomic",
        why="a half-started gang deadlocks instead of failing",
        path="crates/tinyray-runtime/src/cluster.rs",
        old="        if capacity < count {",
        new="        if false {  // MUTANT: place what fits and hope",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "cluster::"],
    ),
    Mutant(
        name="shared-gpu-allowed",
        why="two NCCL ranks on one device deadlock rather than erroring",
        path="crates/tinyray-runtime/src/collective.rs",
        old="                if devices.contains(&key) {",
        new="                if false {  // MUTANT: allow two ranks per device",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "collective::"],
    ),
    Mutant(
        name="stale-epoch-accepted",
        why="a stale ack makes a rebuilding group look ready before it is",
        path="crates/tinyray-runtime/src/collective.rs",
        old="        if epoch != group.epoch || !group.contains(actor_id) {",
        new="        if !group.contains(actor_id) {  // MUTANT: ignore the epoch",
        command=["cargo", "test", "-p", "tinyray-runtime", "--lib", "collective::"],
    ),
]


def run(mutant: Mutant) -> bool:
    """Apply the mutation, run its tests, restore. True if the suite caught it."""
    target = ROOT / mutant.path
    original = target.read_text()
    if mutant.old not in original:
        print(f"  SKIP  {mutant.name}: anchor text not found (code moved?)")
        return True

    try:
        target.write_text(original.replace(mutant.old, mutant.new, 1))
        if mutant.command[0] == "cargo":
            command = [str(CARGO), *mutant.command[1:]]
        else:
            command = [str(VENV_PY), "-m", *mutant.command]
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=900,
        )
        return result.returncode != 0
    except subprocess.TimeoutExpired:
        # A hang is a detection too: the mutant broke something badly enough
        # that the tests never finished.
        return True
    finally:
        target.write_text(original)
        # Bump the mtime explicitly. Restoring the bytes is not enough: cargo
        # decides what to rebuild from timestamps, so a restore that preserved
        # the original mtime would leave the *mutated* binary in place and every
        # later test run would silently exercise it.
        os.utime(target, None)


def main() -> int:
    selected = sys.argv[1:]
    mutants = [m for m in MUTANTS if not selected or m.name in selected]

    print(f"Running {len(mutants)} mutants\n")
    survivors = []
    for mutant in mutants:
        caught = run(mutant)
        status = "caught " if caught else "SURVIVED"
        print(f"  {status}  {mutant.name:<28} {mutant.why}")
        if not caught:
            survivors.append(mutant)

    print(f"\n{len(mutants) - len(survivors)}/{len(mutants)} caught")
    if survivors:
        print("\nSurviving mutants are holes in the test suite:")
        for mutant in survivors:
            print(f"  - {mutant.name}: {mutant.why}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
