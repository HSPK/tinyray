"""Routing by seat, where reaching the wrong member corrupts data silently.

A shard owner is not interchangeable. `slot(k)` raises when seat k is empty
rather than handing back somebody else, because a key written to the wrong
shard is a bug you find days later.

    python examples/03_shard_router.py
"""

from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Fleet, role_main  # noqa: E402

import tinyray  # noqa: E402

SHARDS = 4
KEYS = 200


def shard_of(key: str) -> int:
    return int(hashlib.blake2b(key.encode(), digest_size=8).hexdigest(), 16) % SHARDS


def run_shard(argv: list[str]) -> None:
    index = int(argv[0])

    class Shard:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}

        def put(self, key: str, value: str) -> int:
            assert shard_of(key) == index, f"key {key} does not belong to shard {index}"
            self.store[key] = value
            return len(self.store)

        def get(self, key: str) -> str | None:
            return self.store.get(key)

        def size(self) -> int:
            return len(self.store)

    svc = Shard()
    with tinyray.join("kv", "stateful", slot=index, serves=svc) as me:
        me.ready(shard=index)
        clients = tinyray.pool("client")
        met = False
        while True:
            alive = clients.all()
            if alive:
                met = True
                if all(h.state.get("done") for h in alive):
                    break
            elif met:
                break
            time.sleep(0.05)
        print(f"[shard {index}] holds {svc.size()} keys", flush=True)
        time.sleep(0.4)


def run_client(_: list[str]) -> None:
    with tinyray.join("client", "churn") as me:
        me.ready()
        kv = tinyray.pool("kv")
        kv.wait(count=SHARDS, timeout=20)

        for i in range(KEYS):
            key = f"key-{i}"
            kv.slot(shard_of(key)).put(key=key, value=f"v{i}")
        # Reads land on the same seat, so every key comes back.
        misses = sum(1 for i in range(KEYS) if kv.slot(shard_of(f"key-{i}")).get(key=f"key-{i}") is None)
        print(f"[client] wrote {KEYS} keys, {misses} unreadable", flush=True)
        assert misses == 0

        # An empty seat is an error, never a substitution.
        try:
            kv.slot(SHARDS + 1).get(key="key-0")
        except tinyray.NotFound as exc:
            print(f"[client] empty seat refused: {exc}", flush=True)
        else:
            raise AssertionError("a missing shard was silently substituted")
        me.ready(done=True)
        time.sleep(0.5)


def driver() -> int:
    with Fleet() as fleet:
        for i in range(SHARDS):
            fleet.spawn(__file__, "shard", i, label=f"shard{i}")
        time.sleep(0.5)
        fleet.spawn(__file__, "client", label="client")
        return fleet.wait_all(timeout=90)


if __name__ == "__main__":
    raise SystemExit(role_main({"shard": run_shard, "client": run_client}, driver))
