"""Tests for the identifier bindings."""

from __future__ import annotations

import pytest

from tinyray import Id, new_id


def test_hex_roundtrip():
    identifier = new_id()
    assert Id(str(identifier)) == identifier
    assert len(identifier.hex) == 32


def test_nil():
    assert Id.nil().is_nil()
    assert str(Id.nil()) == "0" * 32
    assert not new_id().is_nil()


def test_repr_is_unambiguous():
    identifier = Id("0" * 31 + "1")
    assert repr(identifier) == f"Id('{identifier}')"


@pytest.mark.parametrize(
    "value",
    ["", "abc", "0" * 31, "0" * 33, "g" * 32, "+" + "0" * 31, " " + "0" * 31],
    ids=["empty", "short", "31-chars", "33-chars", "non-hex", "plus-sign", "leading-space"],
)
def test_invalid_ids_are_rejected(value):
    with pytest.raises(ValueError):
        Id(value)


def test_uppercase_parses_to_canonical_lowercase():
    assert str(Id("A" * 32)) == "a" * 32


def test_ids_are_unique():
    assert len({new_id() for _ in range(10_000)}) == 10_000


def test_ids_are_hashable_and_comparable():
    a, b = Id("0" * 31 + "1"), Id("0" * 31 + "2")
    assert a < b
    assert a != b
    assert {a, b, Id("0" * 31 + "1")} == {a, b}
