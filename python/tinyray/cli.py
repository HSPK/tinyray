"""``tinyray`` command line.

The commands exist to answer the question that dominates distributed ML
debugging: *which actor is stuck, and on what?* ``tinyray status`` reads the
same `/introspect` endpoint the runtime serves, so what you see is what the
actor thinks, not a driver-side guess.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

from ._tinyray import ClientRuntime


def _fetch_introspect(client: ClientRuntime, endpoint: str) -> Optional[dict]:
    try:
        return json.loads(client.get_text(endpoint, "/introspect"))
    except Exception:
        return None


def _format_bytes(value: int) -> str:
    scaled = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if scaled < 1024 or unit == "TiB":
            return f"{scaled:.0f}{unit}" if unit == "B" else f"{scaled:.1f}{unit}"
        scaled /= 1024.0
    return f"{scaled:.1f}TiB"


def cmd_status(args: argparse.Namespace) -> int:
    """Print one line per actor, plus anything that looks wrong."""
    client = ClientRuntime(request_timeout_seconds=args.timeout)
    reports = []
    for endpoint in args.endpoints:
        report = _fetch_introspect(client, endpoint)
        reports.append((endpoint, report))

    if not reports:
        print("no endpoints given; pass one or more host:port", file=sys.stderr)
        return 2

    header = (
        f"{'ENDPOINT':<24} {'INFLIGHT':<18} {'SECS':>7} "
        f"{'QUEUED':>7} {'DONE':>7} {'FAILED':>7} {'STORE':>9}"
    )
    print(header)
    print("-" * len(header))

    problems: list[str] = []
    durations = []
    for endpoint, report in reports:
        if report is None:
            print(f"{endpoint:<24} {'UNREACHABLE':<18}")
            problems.append(f"{endpoint} did not answer /introspect")
            continue
        inflight = report["inflight"] or "-"
        seconds = report["inflight_seconds"]
        durations.append((endpoint, inflight, seconds))
        print(
            f"{endpoint:<24} {inflight:<18} {seconds:>7.1f} {report['queued']:>7} "
            f"{report['completed']:>7} {report['failed']:>7} "
            f"{_format_bytes(report['store']['bytes']):>9}"
        )

        if report["stuck_callers"]:
            for stuck in report["stuck_callers"]:
                problems.append(
                    f"{endpoint} is waiting for sequence {stuck['awaiting_seq']} from caller "
                    f"{stuck['caller'][:8]} with {stuck['buffered']} call(s) buffered behind it "
                    "(a call was lost in flight; this caller is stalled)"
                )
        if report["store"]["evictions"]:
            problems.append(
                f"{endpoint} has evicted {report['store']['evictions']} result(s); "
                "consumers may see ObjectLost. Raise store_max_bytes or fetch sooner."
            )
        if report["rejected_backpressure"]:
            problems.append(
                f"{endpoint} refused {report['rejected_backpressure']} call(s) for backpressure; "
                "it is slower than its callers."
            )

    # Straggler detection. In an RL loop the slowest rank sets the pace, and a
    # collective barrier means you cannot simply skip it.
    running = [(e, m, s) for e, m, s in durations if m != "-"]
    if len(running) >= 3:
        times = sorted(s for _, _, s in running)
        median = times[len(times) // 2]
        for endpoint, method, seconds in running:
            if median > 0 and seconds > median * 3:
                problems.append(
                    f"{endpoint} has been in {method} for {seconds:.1f}s, more than 3x the "
                    f"median of {median:.1f}s: likely straggler"
                )

    if problems:
        print("\nProblems:")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("\nNo problems detected.")
    return 0


def cmd_introspect(args: argparse.Namespace) -> int:
    """Dump one actor's raw report."""
    client = ClientRuntime(request_timeout_seconds=args.timeout)
    report = _fetch_introspect(client, args.endpoint)
    if report is None:
        print(f"{args.endpoint} did not answer /introspect", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def cmd_health(args: argparse.Namespace) -> int:
    client = ClientRuntime(request_timeout_seconds=args.timeout)
    failures = 0
    for endpoint in args.endpoints:
        try:
            print(f"{endpoint}: {client.get_text(endpoint, '/health')}")
        except Exception as exc:
            print(f"{endpoint}: UNREACHABLE ({exc})", file=sys.stderr)
            failures += 1
    return 1 if failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tinyray", description=__doc__)
    parser.add_argument("--timeout", type=float, default=10.0, help="per-request timeout")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser(
        "status", help="one line per actor, with stragglers and stalls called out"
    )
    status.add_argument("endpoints", nargs="+", metavar="HOST:PORT")
    status.set_defaults(func=cmd_status)

    introspect = subparsers.add_parser("introspect", help="dump one actor's raw report")
    introspect.add_argument("endpoint", metavar="HOST:PORT")
    introspect.set_defaults(func=cmd_introspect)

    health = subparsers.add_parser("health", help="liveness probe")
    health.add_argument("endpoints", nargs="+", metavar="HOST:PORT")
    health.set_defaults(func=cmd_health)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    # argparse puts --timeout on the parent, so it is present either way.
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
