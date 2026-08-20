"""`tinyray` -- run the phone book. Shipped in the same wheel as the client."""

from __future__ import annotations

import argparse
import sys

from ._tinyray import serve_registry


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="tinyray", description="Run the tinyray registry.")
    ap.add_argument("--listen", default="127.0.0.1:8760", help="host:port, 0 picks a free port")
    ap.add_argument("--ttl-ms", type=int, default=20_000, help="lease length in milliseconds")
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
