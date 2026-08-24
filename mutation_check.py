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

UNTIL_BOOTSTRAP = (
    "        snap = self.snapshot()\n"
    "        if predicate(snap):\n"
    "            return snap\n"
    "        # Hand over the revision this snapshot stood at, so a change that\n"
    "        # landed while the predicate was running is still delivered.\n"
    "        with self.changes("
    "since=snap.revision if since is None else since, timeout=timeout) as w:"
)
AWAIT_READY = (
    "        await self.auntil(\n"
    '            enough, timeout=timeout, describe=f"{count} ready member(s) matching {filt}"\n'
    "        )\n"
    "        return found"
)

# (label, file, find, replace, test that must fail)
MUTANTS = [
    (
        "removals in a delta are ignored",
        RS_BEAT,
        "            for id in &d.removed {\n                c.members.remove(id);\n            }",
        "",
        # test_m1_delta.py does not see this one -- it checks that a delta
        # arrives, not that a departure is applied.
        "tests/test_m1_membership.py",
    ),
    (
        "an incremental delta is applied across a registry restart",
        RS_BEAT,
        "            if restarted && !d.full {\n                continue;\n            }",
        "",
        "tests/test_registry_restart.py",
    ),
    (
        "the cached version is never advanced",
        RS_BEAT,
        "            c.version = d.version;",
        "",
        "tests/test_m1_delta.py",
    ),
    (
        "a departed tenure is forgotten, so a beat in flight revives it",
        RS_STATE,
        "            p.gone\n                .insert(b.id, (b.incarnation, Instant::now() + self.ttl));",
        "",
        "tests/test_review_fixes.py::test_a_beat_still_in_flight_cannot_undo_a_leave",
    ),
    (
        "a stored tenure newer than the beat no longer supersedes it",
        RS_STATE,
        "            || b.incarnation < watermark\n"
        "            || stored.is_some_and(|cur| cur > b.incarnation);",
        "            || b.incarnation < watermark;",
        "tests/test_field_coverage.py",
    ),
    (
        "leaving does not take the member out of the fingerprint",
        RS_STATE,
        "                p.roster ^= r.member.roster_hash();\n                p.bump(b.id);",
        "                p.bump(b.id);",
        "tests/test_roster_fingerprint.py",
    ),
    (
        "the pool's declared shape is never disagreed with",
        RS_STATE,
        "        } else if let Some(why) = disagreement(p, b) {",
        "        } else if let Some(why) = None::<String> {",
        "tests/test_pool_shape.py",
    ),
    (
        "expired members are never swept",
        RS_STATE,
        "                .filter(|(_, r)| r.expires_at <= now)",
        "                .filter(|(_, _r)| false)",
        "tests/test_m1_membership.py",
    ),
    (
        "a frozen round is handed out as an editable list",
        PY_INIT,
        "        self.members = tuple(members)\n        self.roster = roster",
        "        self.members = list(members)\n        self.roster = roster",
        "tests/test_m3_epoch.py::test_a_frozen_round_cannot_be_edited",
    ),
    (
        "a snapshot is handed out as an editable list",
        PY_INIT,
        "        # that can be edited afterwards is not one.\n"
        "        self.members = tuple(members)",
        "        # that can be edited afterwards is not one.\n"
        "        self.members = list(members)",
        "tests/test_m3_epoch.py::test_a_snapshot_cannot_be_edited_either",
    ),
    (
        "a round never notices it has broken",
        PY_INIT,
        "        return info is None or info[1] == self.roster",
        "        return True",
        "tests/test_m3_epoch.py::test_readiness_does_not_break_a_round_but_leaving_does",
    ),
    (
        "any advertise value is accepted whole",
        PY_INIT,
        '        if not host or any(c in host for c in "/: "):',
        "        if False:",
        "tests/test_m2_validation.py::test_an_advertise_value_that_is_not_a_bare_host_is_refused",
    ),
    (
        "surrounding whitespace is left in the advertised host",
        PY_INIT,
        "        host = explicit.strip()",
        "        host = explicit",
        "tests/test_m2_validation.py::test_a_bare_host_is_taken_as_given",
    ),
    (
        "method discovery reads the instance and runs its properties",
        "python/tinyray/_serve.py",
        "        static = inspect.getattr_static(obj, name, _ABSENT)",
        "        static = getattr(obj, name, _ABSENT)",
        "tests/test_m2_validation.py::test_a_property_on_a_served_object_is_never_evaluated",
    ),
    (
        "classmethods stop being found",
        "python/tinyray/_serve.py",
        "        elif callable(static) or isinstance(static, classmethod):",
        "        elif callable(static):",
        "tests/test_m2_validation.py::test_the_kinds_of_method_are_all_still_found",
    ),
    (
        "a __getattr__ proxy loses its methods",
        "python/tinyray/_serve.py",
        "        if static is _ABSENT:",
        "        if False:",
        "tests/test_m2_validation.py::test_a_proxy_that_answers_through_getattr_still_works",
    ),
    (
        "the injected parameter counts as one the caller fills",
        "python/tinyray/_serve.py",
        "        fillable = [p for p in names if p not in injected]",
        "        fillable = list(names)",
        "tests/test_identity_and_fencing.py::test_the_context_can_sit_anywhere_in_the_signature",
    ),
    (
        "positional arguments are left positional when a context is injected",
        "python/tinyray/_serve.py",
        "    if injected and args:",
        "    if False:",
        "tests/test_identity_and_fencing.py::test_the_context_can_sit_anywhere_in_the_signature",
    ),
    (
        "the oversize nudge goes back to a fixed stack depth",
        "python/tinyray/_rpc.py",
        "        stacklevel=_app_stacklevel(),",
        "        stacklevel=4,",
        "tests/test_fork_and_reply_budget.py"
        "::test_the_oversize_nudge_points_at_the_line_that_made_the_call",
    ),
    (
        "an async handle sends its calls synchronously",
        "python/tinyray/_rpc.py",
        "    _send = staticmethod(ainvoke)",
        "    _send = staticmethod(invoke)",
        "tests/test_m2_async.py",
    ),
    (
        "a plain handle hands back coroutines",
        PY_INIT,
        "    _send = staticmethod(_rpc.invoke)",
        "    _send = staticmethod(_rpc.ainvoke)",
        "tests/test_m2_calling.py",
    ),
    (
        "until() waits instead of checking what is already true",
        PY_INIT,
        UNTIL_BOOTSTRAP,
        "        snap = self.snapshot()\n"
        "        with self.changes(since=since, timeout=timeout) as w:",
        "tests/test_waiting.py::test_until_returns_at_once_when_it_is_already_true",
    ),
    (
        "wait_departure watches the seat instead of the tenure",
        PY_INIT,
        "            return snap.get(identity) is None\n\n        try:\n            self.until(departed,",
        "            return snap.slot(0) is None\n\n        try:\n            self.until(departed,",
        "tests/test_waiting.py::test_wait_departure_is_about_the_tenure_not_the_seat",
    ),
    (
        "await_ready blocks the event loop",
        PY_INIT,
        AWAIT_READY,
        "        return self.wait(count, timeout, **filt)",
        "tests/test_waiting.py::test_await_ready_leaves_the_event_loop_turning",
    ),
    (
        "await_ready borrows an executor thread",
        PY_INIT,
        AWAIT_READY,
        "        found = await asyncio.to_thread(self.wait, count, timeout, **filt)\n"
        "        return found",
        "tests/test_waiting.py::test_await_ready_holds_no_executor_thread",
    ),
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
        "            raise StopAsyncIteration\n            await bell.wait(ms / 1000)",
        "            raise StopAsyncIteration\n"
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
        "            if shared.hold_ms.load(Ordering::Relaxed) == 0 {\n"
        "                shared.short_polls",
        "            if hold == 0 {\n                shared.short_polls",
        "tests/test_long_poll.py::test_publishing_never_makes_the_loop_fall_back_to_a_timer",
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
        "a field-scoped watch yields on everything anyway",
        PY_INIT,
        "            digest = self._c.field_digest(self._pool._name, self._fields, False)\n"
        "            if digest != self._digest:\n"
        "                self._digest = digest\n"
        "                return self._pool.snapshot(), 0",
        "            return self._pool.snapshot(), 0",
        "tests/test_watch_lifecycle.py::test_a_watch_on_named_fields_ignores_the_rest",
    ),
    (
        "the digest leaves out who the members are",
        RS_LIB,
        "            m.id.hash(&mut h);\n            m.incarnation.hash(&mut h);",
        "",
        "tests/test_watch_lifecycle.py::test_a_watch_on_fields_notices_a_seat_changing_hands",
    ),
    (
        "the serving side stops counting refusals",
        "python/tinyray/_serve.py",
        "            counters.refuse()\n",
        "",
        "tests/test_stats.py"
        "::test_stats_shows_saturation_rather_than_leaving_it_to_guesswork",
    ),
    (
        "a pinned request id is ignored",
        "python/tinyray/_rpc.py",
        "    fixed = _pinned.get()\n    return fixed if fixed is not None else ",
        "    return ",
        "tests/test_identity_and_fencing.py::test_a_caller_can_pin_one_name_across_retries",
    ),
    (
        "the pinned name is never restored afterwards",
        "python/tinyray/_rpc.py",
        "    finally:\n        _pinned.reset(token)",
        "    finally:\n        pass",
        "tests/test_identity_and_fencing.py::test_a_caller_can_pin_one_name_across_retries",
    ),
    (
        "the caller identity goes out as a str header again",
        PY_RPC,
        '        "x-tinyray-caller": _identity.encode(),',
        '        "x-tinyray-caller": _identity,',
        "tests/test_identity_and_fencing.py::test_a_non_ascii_pool_name_can_still_be_called",
    ),
    (
        "the serving side does not decode what the caller sent",
        "python/tinyray/_serve.py",
        '        return raw.encode("latin-1").decode("utf-8")',
        "        return raw",
        "tests/test_identity_and_fencing.py::test_a_non_ascii_pool_name_can_still_be_called",
    ),
    (
        "a request id with a newline in it is accepted",
        PY_RPC,
        '    if any(c < " " or c == "\\x7f" for c in value):',
        "    if False:",
        "tests/test_identity_and_fencing.py"
        "::test_a_request_id_that_cannot_be_a_header_is_refused_where_it_is_set",
    ),
    (
        "every call reuses one request id",
        PY_RPC,
        'else f"{_identity or \'anon\'}-{next(_seq)}"',
        'else f"{_identity or \'anon\'}-1"',
        "tests/test_identity_and_fencing.py::test_every_call_carries_a_request_id_that_names_that_attempt",
    ),
]


def build() -> bool:
    if subprocess.run(["cargo", "build", "-q", "-p", "tinyray-client"], cwd=ROOT).returncode:
        return False
    return not subprocess.run(
        [str(PY.parent / "maturin"), "develop", "-q", "--release"], cwd=ROOT
    ).returncode


def check_anchors() -> list[str]:
    """An anchor that matches twice patches whichever came first, which may not
    be the code the label names -- and the run still says CAUGHT. Found exactly
    that once: `await bell.wait(...)` started matching `await_fenced` as well
    as the watch it was written for."""
    wrong = []
    for label, rel, find, _, _ in MUTANTS:
        n = (ROOT / rel).read_text().count(find)
        if n != 1:
            wrong.append(f"{label}: anchor matches {n} times in {rel}")
    return wrong


def main() -> int:
    if "--count" in sys.argv:
        print(len(MUTANTS))
        return 0
    ambiguous = check_anchors()
    for line in ambiguous:
        print(f"BROKEN  {line}")
    if ambiguous:
        return 1
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
                # A MISSED that does not say why is a dead end. Usually it is
                # the test having no teeth, but it has also been the mutant
                # never reaching the interpreter, and those need opposite
                # reactions.
                tail = (r.stdout or r.stderr or "").strip().splitlines()[-4:]
                for line in tail:
                    print(f"        | {line}")
                now = path.read_text()
                applied = find not in now and (not repl or repl in now)
                print(f"        | mutant was in the file when the test ran: {applied}")
                bad.append(label)
        finally:
            path.write_text(original)
            if rel.endswith(".rs"):
                build()
    print(f"\n{len(MUTANTS) - len(bad)} of {len(MUTANTS)} caught")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
