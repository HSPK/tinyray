"""Running many calls across a pool of actors.

The shape almost every hyperparameter sweep and batch inference job ends up
wanting: a fixed set of actors, far more work items than actors, and results
wanted as soon as they exist rather than in order.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, Callable

from .api import ActorHandle, ObjectRef, get, wait


class ActorPool:
    """Round-robins work across a set of actors.

    ``map_unordered`` is the important one: yielding in completion order rather
    than submission order is what stops one slow trial from holding up the
    results of everything behind it.
    """

    def __init__(self, actors: Sequence[ActorHandle], *, max_in_flight_per_actor: int = 2) -> None:
        if not actors:
            raise ValueError("an ActorPool needs at least one actor")
        self._actors = list(actors)
        self._max_in_flight = max(1, max_in_flight_per_actor)

    def __len__(self) -> int:
        return len(self._actors)

    @property
    def actors(self) -> list[ActorHandle]:
        return list(self._actors)

    def map_unordered(
        self,
        fn: Callable[[ActorHandle, Any], ObjectRef],
        items: Iterable[Any],
        *,
        timeout: float = 300.0,
    ) -> Iterator[Any]:
        """Yield results as they arrive, not in submission order.

        `fn` receives `(actor, item)` and must return the `ObjectRef` from a
        `.remote()` call.
        """
        pending: list[ObjectRef] = []
        cycle = itertools.cycle(self._actors)
        capacity = len(self._actors) * self._max_in_flight
        iterator = iter(items)
        exhausted = False

        while True:
            # Keep the actors fed, but bounded: submitting everything up front
            # would just move the queue into the actors' memory.
            while not exhausted and len(pending) < capacity:
                try:
                    item = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                pending.append(fn(next(cycle), item))

            if not pending:
                return

            ready, pending = wait(pending, num_returns=1, timeout=timeout)
            if not ready:
                raise TimeoutError(
                    f"no result within {timeout}s with {len(pending)} calls outstanding"
                )
            for ref in ready:
                yield get(ref)

    def map(
        self,
        fn: Callable[[ActorHandle, Any], ObjectRef],
        items: Iterable[Any],
        *,
        timeout: float = 300.0,
    ) -> list[Any]:
        """Like `map_unordered`, but preserves input order."""
        items = list(items)
        cycle = itertools.cycle(self._actors)
        refs = [fn(next(cycle), item) for item in items]
        return get(refs, timeout=timeout)
