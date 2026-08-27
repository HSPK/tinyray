"""`tinyray` -- run the phone book. Shipped in the same wheel as the client."""

from __future__ import annotations

import argparse
import sys

from ._tinyray import serve_registry


def _lease_ms(text: str) -> int:
    """A lease is a duration, so a negative one is a typo, not a policy.

    Without this it reached pyo3, which refuses to make a u64 out of it and
    raises OverflowError -- not one of the three below, so it came out as a
    traceback. The one operator typo this file promises not to crash on was
    the only one that did.
    """
    ms = int(text) if text.lstrip("+-").isdigit() else _not_a_length(text)
    if ms < 0:
        _not_a_length(text)
    return ms


def _not_a_length(text: str) -> int:
    raise argparse.ArgumentTypeError(f"{text!r} is not a length of time")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="tinyray", description="Run the tinyray registry.")
    ap.add_argument("--listen", default="127.0.0.1:8760", help="host:port, 0 picks a free port")
    ap.add_argument("--ttl-ms", type=_lease_ms, default=20_000, help="lease length in milliseconds")
    args = ap.parse_args(argv)
    try:
        serve_registry(args.listen, args.ttl_ms)
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError, RuntimeError) as e:
        # A bad --listen or --ttl-ms is an operator typo, not a crash.
        print(f"tinyray: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
