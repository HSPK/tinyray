#!/usr/bin/env python3
"""Do the new tests actually catch the things they claim to?

A test that passes is worth nothing on its own -- it has to fail when the
behaviour it describes is broken. Each entry here breaks exactly one thing and
names the test that must go red for it.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent
PY = ROOT / ".venv/bin/python"

PY_INIT = "python/tinyray/__init__.py"
RS_BEAT = "crates/tinyray-client/src/beat.rs"
RS_PROTO = "crates/tinyray-proto/src/lib.rs"
RS_STATE = "crates/tinyray-registry/src/state.rs"
RS_LIB = "crates/tinyray-client/src/lib.rs"
PY_RPC = "python/tinyray/_rpc.py"

# (label, file, find, replace, test that must fail)
MUTANTS = [
    (
        "update() asserts readiness like ready() did",
        PY_INIT,
        "            self._c.set_state_only(raw)\n        return self\n\n    def replace",
        "            self._c.set_state(raw, True)\n        return self\n\n    def replace",
        "tests/test_readiness_owner.py::test_update_publishes_without_touching_readiness",
    ),
    (
        "replace() asserts readiness",
        PY_INIT,
        "            self._state = fresh\n            self._c.set_state_only(raw)",
        "            self._state = fresh\n            self._c.set_state(raw, True)",
        "tests/test_readiness_owner.py::test_replace_takes_keys_back_without_touching_readiness",
    ),
    (
        "close() sets the flag but does not ring the bell",
        PY_INIT,
        "            _live_watches.discard(self)\n            self._c.wake()",
        "            _live_watches.discard(self)",
        "tests/test_watch_lifecycle.py::test_close_releases_a_blocked_watcher",
    ),
    (
        "leave() does not end live watchers",
        PY_INIT,
        "            for w in list(_live_watches):\n                w.close()",
        "            pass",
        "tests/test_watch_lifecycle.py::test_leave_ends_live_watchers",
    ),
    (
        "achanges() goes back to an executor thread",
        PY_INIT,
        "            await bell.wait(ms / 1000)",
        "            await asyncio.to_thread(self._c.wait_revision, self._tick, ms)",
        "tests/test_watch_lifecycle.py::test_async_watchers_hold_no_executor_thread",
    ),
    (
        "wait_replacement returns any occupant, not a new tenure",
        PY_INIT,
        "                if now is not None and now.identity != was:\n                    return now\n        return None\n\n    def all(",
        "                if now is not None:\n                    return now\n        return None\n\n    def all(",
        "tests/test_watch_lifecycle.py::test_wait_replacement_names_the_new_tenure",
    ),
    (
        "republishing the same state nudges the heartbeat anyway",
        RS_LIB,
        "            if cur.state == state && cur.ready == ready {\n                return Ok(false);\n            }",
        "",
        "tests/test_readiness_owner.py::test_republishing_the_same_thing_costs_nothing",
    ),
    (
        "the request replacing a cancelled one is parked like any other",
        RS_BEAT,
        "            let hold = if cancelled_last {\n                0\n            } else {\n                shared.hold_ms.load(Ordering::Relaxed)\n            };",
        "            let hold = shared.hold_ms.load(Ordering::Relaxed);",
        "tests/test_long_poll.py::test_publishing_flat_out_does_not_starve_the_heartbeat",
    ),
    (
        "dedup ignores readiness and only compares state",
        RS_LIB,
        "            if cur.state == state && cur.ready == ready {",
        "            if cur.state == state {",
        "tests/test_readiness_owner.py::test_going_ready_again_is_never_deduplicated_away",
    ),
    (
        "dedup compares raw bytes instead of parsed values",
        RS_LIB,
        "            if cur.state == state && cur.ready == ready {",
        "            if cur.state.to_string() == state_json && cur.ready == ready {",
        "tests/test_readiness_owner.py::test_key_order_is_not_a_change",
    ),
    (
        "the post-beat pause reads the last request's hold, not the loop's intent",
        RS_BEAT,
        "            if shared.hold_ms.load(Ordering::Relaxed) == 0 {\n                let ms = shared.interval_ms",
        "            if hold == 0 {\n                let ms = shared.interval_ms",
        "tests/test_long_poll.py::test_a_superseded_member_finds_out_within_a_round_trip",
    ),
    (
        "the registry does not report its protocol on the ack",
        RS_STATE,
        '            protocol: tinyray_proto::PROTOCOL,\n'
        '            version: env!("CARGO_PKG_VERSION").to_string(),\n'
        "            ttl_ms:",
        "            protocol: 0,\n            version: String::new(),\n            ttl_ms:",
        "tests/test_registry_capability.py::test_a_member_can_ask_what_the_registry_can_do",
    ),
    (
        "a missing protocol field is an error rather than zero",
        RS_PROTO,
        "    #[serde(default)]\n    pub protocol: u32,",
        "    pub protocol: u32,",
        "crates/tinyray-proto/tests/wire.rs",
    ),
    (
        "an unknown feature name answers False instead of raising",
        PY_INIT,
        "            raise ValueError(\n"
        '                f"no such feature {feature!r}; this package knows about '
        '{sorted(self.FEATURES)}"\n'
        "            )",
        "            return False",
        "tests/test_registry_capability.py::test_an_unknown_feature_is_an_error_not_a_false",
    ),
    (
        "joining an out-of-date registry says nothing",
        PY_INIT,
        '    if not seen.supports("long_poll"):',
        "    if False:",
        "tests/test_registry_capability.py"
        "::test_wanting_more_than_the_registry_has_says_so_instead_of_degrading_quietly",
    ),
    (
        "await_fenced goes back to an executor thread",
        PY_INIT,
        "        self._mine()\n        bell = _loop_bell(self._c)\n        deadline = None if timeout is None else time.monotonic() + timeout\n        while True:\n            if not self._c.accepted:\n                return True\n            ms = _left_ms(deadline)\n            if ms is None:\n                return False\n            await bell.wait(ms / 1000)",
        "        return await asyncio.to_thread(self.wait_fenced, timeout)",
        "tests/test_watch_lifecycle.py::test_await_fenced_holds_no_executor_thread_either",
    ),
    (
        "await_fenced never notices the takeover",
        PY_INIT,
        "        while True:\n            if not self._c.accepted:\n                return True\n"
        "            ms = _left_ms(deadline)\n            if ms is None:\n                return False\n"
        "            await bell.wait(ms / 1000)",
        "        while True:\n            ms = _left_ms(deadline)\n            if ms is None:\n"
        "                return False\n            await bell.wait(ms / 1000)",
        "tests/test_watch_lifecycle.py::test_await_fenced_still_reports_a_takeover",
    ),
    (
        "a bell timeout escapes instead of ending the stream",
        PY_INIT,
        "        except asyncio.TimeoutError:\n            pass",
        "        except asyncio.TimeoutError:\n            raise",
        "tests/test_watch_lifecycle.py::test_achanges_with_a_timeout_ends_rather_than_raises",
    ),
    (
        "a fenced stream ends quietly, like a timeout",
        PY_INIT,
        "        if self._closed:\n            return None, 0\n        if not self._c.accepted:",
        "        if self._closed or not self._c.accepted:\n            return None, 0\n        if False:",
        "tests/test_watch_lifecycle.py::test_the_three_ways_a_stream_ends_are_told_apart",
    ),
    (
        "the async stream ends quietly when fenced",
        PY_INIT,
        "        if self._closed:\n            return None, 0\n        if not self._c.accepted:",
        "        if self._closed or not self._c.accepted:\n            return None, 0\n        if False:",
        "tests/test_watch_lifecycle.py::test_the_async_stream_ends_the_same_three_ways",
    ),
    (
        "close() raises Fenced too, instead of ending quietly",
        PY_INIT,
        "        if self._closed:\n            return None, 0",
        "        if False:\n            return None, 0",
        "tests/test_watch_lifecycle.py::test_the_three_ways_a_stream_ends_are_told_apart",
    ),
    (
        "every call reuses one request id",
        PY_RPC,
        'return f"{_identity or \'anon\'}-{next(_seq)}"',
        'return f"{_identity or \'anon\'}-1"',
        "tests/test_identity_and_fencing.py::test_every_call_carries_a_request_id_that_names_that_attempt",
    ),
]


def build() -> bool:
    if subprocess.run(["cargo", "build", "-q", "-p", "tinyray-client"], cwd=ROOT).returncode:
        return False
    return not subprocess.run(
        [str(PY.parent / "maturin"), "develop", "-q", "--release"], cwd=ROOT
    ).returncode


def main() -> int:
    bad = []
    for label, rel, find, repl, test in MUTANTS:
        path = ROOT / rel
        original = path.read_text()
        if find not in original:
            print(f"SKIP  {label}\n      anchor not found in {rel}")
            bad.append(label)
            continue
        path.write_text(original.replace(find, repl, 1))
        try:
            if rel.endswith(".rs") and not build():
                # A mutant that will not compile is caught too: the compiler
                # is the thing that noticed.
                print(f"CAUGHT  {label}  (did not compile)")
                continue
            if test.endswith(".rs"):
                r = subprocess.run(
                    ["cargo", "test", "-q", "-p", "tinyray-proto"],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                )
            else:
                r = subprocess.run(
                    [str(PY), "-m", "pytest", test, "-q", "--timeout=180"],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                )
            caught = r.returncode != 0
            print(f"{'CAUGHT' if caught else 'MISSED'}  {label}")
            if not caught:
                bad.append(label)
        finally:
            path.write_text(original)
            if rel.endswith(".rs"):
                build()
    print(f"\n{len(MUTANTS) - len(bad)} of {len(MUTANTS)} caught")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
