"""Shared fixtures and helpers for the tinyray test suite."""

from __future__ import annotations

import pytest


@pytest.fixture
def numpy():
    """numpy, skipping the test if it is unavailable."""
    return pytest.importorskip("numpy")


def buffer_address(obj) -> int:
    """Address of the first byte backing a buffer-like object, without copying.

    Comparing addresses is how these tests prove a payload was shared rather
    than duplicated.
    """
    import numpy as np

    view = memoryview(obj)
    if view.nbytes == 0:
        raise ValueError("cannot take the address of an empty buffer")
    return np.frombuffer(view, dtype=np.uint8).__array_interface__["data"][0]
