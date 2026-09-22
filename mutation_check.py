#!/usr/bin/env python3
"""Do the new tests actually catch the things they claim to?

A test that passes is worth nothing on its own -- it has to fail when the
behaviour it describes is broken. Each entry here breaks exactly one thing and
names the test that must go red for it.
"""

# Exact source anchors are intentionally kept on one line.
# ruff: noqa: E501

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent
PY = ROOT / ".venv/bin/python"

PY_INIT = "python/tinyray/__init__.py"
PY_JSON = "python/tinyray/_json.py"
PY_MSGPACK = "python/tinyray/_msgpack.py"
RS_BEAT = "crates/tinyray-membership/src/lib.rs"
RS_MEMBERSHIP_CACHE = "crates/tinyray-membership/src/cache.rs"
RS_MEMBERSHIP_HEARTBEAT = "crates/tinyray-membership/src/heartbeat.rs"
RS_MEMBERSHIP_SHARED = "crates/tinyray-membership/src/shared.rs"
RS_MEMBERSHIP_WAIT = "crates/tinyray-membership/src/wait.rs"
RS_PROTO = "crates/tinyray-proto/src/lib.rs"
RS_RPC = "crates/tinyray-client/src/rpc.rs"
RS_WIRE = "crates/tinyray-proto/src/wire.rs"
RS_RPC_PROTO = "crates/tinyray-proto/src/rpc.rs"
RS_MEMBERSHIP_MANIFEST = "crates/tinyray-membership/Cargo.toml"
RS_SDK_DISCOVERY = "crates/tinyray/src/discovery.rs"
RS_SDK_MANIFEST = "crates/tinyray/Cargo.toml"
RS_SDK_MEMBER = "crates/tinyray/src/member.rs"
RS_SDK_SERVICE = "crates/tinyray/src/service.rs"
RS_SDK_TRANSPORT = "crates/tinyray/src/transport.rs"
RS_SDK_TRANSPORT_CLIENT = "crates/tinyray/src/transport/client.rs"
RS_SDK_TRANSPORT_SERVER = "crates/tinyray/src/transport/server.rs"
RS_BLOB = "crates/tinyray/src/blob.rs"
RS_CLIENT_BLOB = "crates/tinyray-client/src/blob.rs"
RS_REGISTRY_SERVER = "crates/tinyray-registry/src/server.rs"
RS_STATE = "crates/tinyray-registry/src/state.rs"
RS_LIB = "crates/tinyray-client/src/lib.rs"
PY_RPC = "python/tinyray/_rpc.py"

# 同步和异步两条 until 的开头逐字相同，所以锚点必须带上后面那行才唯一。
UNTIL_BOOTSTRAP = (
    "        snap = self.snapshot()\n"
    "        if predicate(snap):\n"
    "            return snap\n"
    "        # Hand over the revision this snapshot stood at, so a change that\n"
)
AWAIT_READY = "        result = await _await_native(self._c, waiter, deadline)"

# (label, file, find, replace, test that must fail)
# fmt: off
MUTANTS = [
    (
        "removals in a delta are ignored",
        RS_MEMBERSHIP_CACHE,
        "        for id in &d.removed {\n            membership_changed |= self.remove(*id);\n        }",
        "",
        # test_deltas.py does not see this one -- it checks that a delta
        # arrives, not that a departure is applied.
        "tests/membership/test_membership.py",
    ),
    (
        "the cached version is never advanced",
        RS_MEMBERSHIP_CACHE,
        "        self.version = d.version;",
        "",
        "tests/discovery/test_deltas.py",
    ),
    (
        "a departed tenure is forgotten, so a beat in flight revives it",
        RS_STATE,
        "            p.gone\n                .insert(b.id, (b.incarnation, Instant::now() + self.ttl));",
        "",
        "tests/registry/test_departures.py"
        "::test_a_beat_still_in_flight_cannot_undo_a_leave",
    ),
    (
        "an unparked beat gets a flat deadline instead of the interval",
        RS_MEMBERSHIP_HEARTBEAT,
        "    if hold_ms == 0 {",
        "    if false {",
        "cargo:tinyray-membership",
    ),
    (
        "a beat body is read whatever size it announces",
        "crates/tinyray-registry/src/server.rs",
        "const MAX_BODY: usize = 512 << 10;",
        "const MAX_BODY: usize = 1 << 40;",
        "tests/registry/test_admission.py"
        "::test_a_body_too_big_to_be_a_beat_is_refused_before_it_is_read",
    ),
    (
        "every parked watcher is woken at the same instant",
        "crates/tinyray-registry/src/server.rs",
        "    let jitter = beat.id % (budget / 8 + 1);",
        "    let jitter = 0;",
        "tests/registry/test_long_poll.py"
        "::test_parked_watchers_do_not_all_come_back_at_once",
    ),
    (
        "a beat is parked for as long as it asks",
        "crates/tinyray-registry/src/server.rs",
        "    let budget = beat.hold_ms.min(reg.ttl.as_millis() as u64 / 2);",
        "    let budget = beat.hold_ms;",
        "tests/registry/test_long_poll.py"
        "::test_a_beat_is_never_parked_longer_than_half_a_lease",
    ),
    (
        "a departure is remembered for good",
        RS_STATE,
        "            p.gone.retain(|_, (_, forget_at)| *forget_at > now);\n",
        "",
        "tests/registry/test_wire_fields.py"
        "::test_a_departure_stops_mattering_once_its_lease_would_have_run_out",
    ),
    (
        "a position past the end of the log is answered incrementally",
        RS_STATE,
        "let since = seen.filter(|v| *v < self.version && *v + 1 >= oldest);",
        "let since = seen.filter(|v| *v < self.version);",
        "tests/registry/test_wire_fields.py"
        "::test_full_says_to_drop_what_you_had",
    ),
    (
        "a position the registry never issued is answered incrementally",
        RS_STATE,
        "let since = seen.filter(|v| *v < self.version && *v + 1 >= oldest);",
        "let since = seen.filter(|v| *v + 1 >= oldest);",
        "tests/registry/test_restart.py"
        "::test_asking_from_a_version_the_registry_never_issued_gets_the_whole_roster",
    ),
    (
        "a beat is taken whatever is in it",
        RS_STATE,
        "        let Some(state_bytes) = Self::admissible(b) else {",
        "        let Some(state_bytes) = Some(0) else {",
        "tests/registry/test_admission.py"
        "::test_absurd_names_are_refused_rather_than_stored",
    ),
    (
        "the first member through decides the pool serves nothing",
        RS_STATE,
        "        } else if p.methods.is_empty() && !b.methods.is_empty() {",
        "        } else if false {",
        "tests/membership/test_pool_shape.py"
        "::test_the_first_member_through_does_not_decide_the_pool_serves_nothing",
    ),
    (
        "a parked watcher is never handed what changed",
        RS_STATE,
        "    pub(crate) fn deltas_shared_for(&self, b: &Beat) -> SharedPools {\n"
        "        let mut deferred = Deferred::default();",
        "    pub(crate) fn deltas_shared_for(&self, b: &Beat) -> SharedPools {\n"
        "        let b = &Beat { watch: Vec::new(), ..b.clone() };\n"
        "        let mut deferred = Deferred::default();",
        "tests/registry/test_long_poll.py"
        "::test_a_change_arrives_long_before_the_next_beat_would_have",
    ),
    (
        "an exclusive seat is given away while somebody holds it",
        RS_STATE,
        "        let occupied = b.exclusive && stored.is_some_and(|cur| cur != b.incarnation);",
        "        let occupied = false;",
        "tests/membership/test_seats.py"
        "::test_exclusive_refuses_an_occupied_seat",
    ),
    (
        "a tenure below the high-water mark is let back in",
        RS_STATE,
        "            || b.incarnation < watermark",
        "            || false",
        "tests/registry/test_departures.py"
        "::test_a_superseded_tenure_cannot_take_the_seat_back",
    ),
    (
        "a stored tenure newer than the beat no longer supersedes it",
        RS_STATE,
        "            || b.incarnation < watermark\n"
        "            || stored.is_some_and(|cur| cur > b.incarnation);",
        "            || b.incarnation < watermark;",
        "tests/registry/test_wire_fields.py",
    ),
    (
        "leaving does not take the member out of the fingerprint",
        RS_STATE,
        "                p.roster ^= r.member.roster_hash();\n"
        "                p.bump(b.id, &mut deferred);",
        "                p.bump(b.id, &mut deferred);",
        "tests/collectives/test_roster_fingerprint.py",
    ),
    (
        "the pool's declared shape is never disagreed with",
        RS_STATE,
        "        } else if let Some(why) = disagreement(p, b) {",
        "        } else if let Some(why) = None::<String> {",
        "tests/membership/test_pool_shape.py",
    ),
    (
        "expired members are never swept",
        RS_STATE,
        "                .filter(|(_, r)| r.expires_at <= now)",
        "                .filter(|(_, _r)| false)",
        "tests/membership/test_membership.py",
    ),
    (
        "a frozen round is handed out as an editable list",
        PY_INIT,
        "            self._materialized = self._view.materialize(\n"
        "                self._handle_cls._from_native, _StateBatch, immutable=True\n"
        "            )\n"
        "        return self._materialized\n\n"
        "    @property\n"
        "    def valid",
        "            self._materialized = self._view.materialize(\n"
        "                self._handle_cls._from_native, _StateBatch, immutable=False\n"
        "            )\n"
        "        return self._materialized\n\n"
        "    @property\n"
        "    def valid",
        "tests/collectives/test_epochs.py"
        "::test_a_frozen_round_cannot_be_edited",
    ),
    (
        "a snapshot is handed out as an editable list",
        PY_INIT,
        "            self._materialized = self._view.materialize(\n"
        "                self._handle_cls._from_native, _StateBatch, immutable=True\n"
        "            )\n"
        "        return self._materialized\n\n"
        "    def __len__",
        "            self._materialized = self._view.materialize(\n"
        "                self._handle_cls._from_native, _StateBatch, immutable=False\n"
        "            )\n"
        "        return self._materialized\n\n"
        "    def __len__",
        "tests/collectives/test_epochs.py"
        "::test_a_snapshot_cannot_be_edited_either",
    ),
    (
        "a round never notices it has broken",
        PY_INIT,
        "        return self._c.epoch_valid(self.pool, self.roster)",
        "        return True",
        "tests/collectives/test_epochs.py"
        "::test_readiness_does_not_break_a_round_but_leaving_does",
    ),
    (
        "any advertise value is accepted whole",
        PY_INIT,
        '        if not host or any(c in host for c in "/: "):',
        "        if False:",
        "tests/rpc/test_validation.py"
        "::test_an_advertise_value_that_is_not_a_bare_host_is_refused",
    ),
    (
        "surrounding whitespace is left in the advertised host",
        PY_INIT,
        "        host = explicit.strip()",
        "        host = explicit",
        "tests/rpc/test_validation.py"
        "::test_a_bare_host_is_taken_as_given",
    ),
    (
        "the listen backlog goes back to socketserver's default of 5",
        "python/tinyray/_serve.py",
        "    request_queue_size = socket.SOMAXCONN",
        "    request_queue_size = 5",
        "tests/rpc/test_concurrency.py"
        "::test_a_fleet_connecting_at_once_does_not_wait_out_a_syn_retransmit",
    ),
    (
        "a method name that cannot go in a URL is served anyway",
        "python/tinyray/_serve.py",
        "        if not (name.isascii() and name.isidentifier()):",
        "        if False:",
        "tests/rpc/test_validation.py"
        "::test_a_method_name_that_cannot_be_a_url_is_refused",
    ),
    (
        "method discovery reads the instance and runs its properties",
        "python/tinyray/_serve.py",
        "        static = inspect.getattr_static(obj, name, _ABSENT)",
        "        static = getattr(obj, name, _ABSENT)",
        "tests/rpc/test_validation.py"
        "::test_a_property_on_a_served_object_is_never_evaluated",
    ),
    (
        "classmethods stop being found",
        "python/tinyray/_serve.py",
        "        elif callable(static) or isinstance(static, classmethod):",
        "        elif callable(static):",
        "tests/rpc/test_validation.py"
        "::test_the_kinds_of_method_are_all_still_found",
    ),
    (
        "a __getattr__ proxy loses its methods",
        "python/tinyray/_serve.py",
        "        if static is _ABSENT:",
        "        if False:",
        "tests/rpc/test_validation.py"
        "::test_a_proxy_that_answers_through_getattr_still_works",
    ),
    (
        "the injected parameter counts as one the caller fills",
        "python/tinyray/_serve.py",
        "sig.replace(parameters=[p for p in sig.parameters.values() if p.name not in injected])",
        "sig",
        "tests/membership/test_identity_and_fencing.py"
        "::test_the_context_can_sit_anywhere_in_the_signature",
    ),
    (
        "positional arguments are left positional when a context is injected",
        "python/tinyray/_serve.py",
        "    if injected:\n        # Defaults fill positional gaps",
        "    if False:\n        # Defaults fill positional gaps",
        "tests/membership/test_identity_and_fencing.py"
        "::test_the_context_can_sit_anywhere_in_the_signature",
    ),
    (
        "the oversize nudge goes back to a fixed stack depth",
        "python/tinyray/_rpc.py",
        "        stacklevel=_app_stacklevel(),",
        "        stacklevel=4,",
        "tests/rpc/test_payloads.py"
        "::test_the_oversize_nudge_points_at_the_line_that_made_the_call",
    ),
    (
        "an async handle sends its calls synchronously",
        "python/tinyray/_rpc.py",
        "    _send = staticmethod(ainvoke)",
        "    _send = staticmethod(invoke)",
        "tests/rpc/test_async.py",
    ),
    (
        "a plain handle hands back coroutines",
        PY_INIT,
        "    _send = staticmethod(_rpc.invoke)",
        "    _send = staticmethod(_rpc.ainvoke)",
        "tests/rpc/test_calling.py",
    ),
    (
        "until() waits instead of checking what is already true",
        PY_INIT,
        UNTIL_BOOTSTRAP,
        "        snap = self.snapshot()\n"
        "        # Hand over the revision this snapshot stood at, so a change that\n",
        "tests/discovery/test_waiting.py"
        "::test_until_returns_at_once_when_it_is_already_true",
    ),
    (
        "wait_departure watches the seat instead of the tenure",
        PY_INIT,
        "        result = _wait_native(\n"
        "            self._c.departure_waiter(self._name, identity),",
        "        result = _wait_native(\n"
        '            self._c.departure_waiter(self._name, f"{self._name}/0#0"),',
        "tests/discovery/test_waiting.py"
        "::test_wait_departure_says_no_rather_than_hanging",
    ),
    (
        "await_ready blocks the event loop",
        PY_INIT,
        AWAIT_READY,
        "        result = _wait_native(waiter, deadline)",
        "tests/discovery/test_waiting.py"
        "::test_await_ready_leaves_the_event_loop_turning",
    ),
    (
        "await_ready borrows an executor thread",
        PY_INIT,
        AWAIT_READY,
        "        result = await asyncio.to_thread(_wait_native, waiter, deadline)",
        "tests/discovery/test_waiting.py"
        "::test_await_ready_holds_no_executor_thread",
    ),
    (
        "update() asserts readiness like ready() did",
        PY_INIT,
        "            self._c.set_state_only(raw)\n            self._state = merged",
        "            self._c.set_state(raw, True)\n            self._state = merged",
        "tests/membership/test_readiness.py"
        "::test_update_publishes_without_touching_readiness",
    ),
    (
        "replace() asserts readiness",
        PY_INIT,
        "            self._c.set_state_only(raw)\n            self._state = fresh",
        "            self._c.set_state(raw, True)\n            self._state = fresh",
        "tests/membership/test_readiness.py"
        "::test_replace_takes_keys_back_without_touching_readiness",
    ),
    (
        "close() sets the flag but does not ring the bell",
        PY_INIT,
        "            _live_watches.discard(self)\n            self._c.wake()",
        "            _live_watches.discard(self)",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_close_releases_a_blocked_watcher",
    ),
    (
        "leave() does not end live watchers",
        PY_INIT,
        "            for w in list(_live_watches):\n                w.close()",
        "            pass",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_leave_ends_live_watchers",
    ),
    (
        "achanges() goes back to an executor thread",
        PY_INIT,
        "            raise StopAsyncIteration\n            await bell.wait(ms / 1000)",
        "            raise StopAsyncIteration\n"
        "            await asyncio.to_thread(\n"
        "                self._c.wait_revision, self._tick, min(ms, 2000)\n"
        "            )",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_async_watchers_hold_no_executor_thread",
    ),
    (
        "the bell waits through wait_for again, which eats a cancel that lands with it",
        PY_INIT,
        "        try:\n            await fut",
        "        try:\n            await asyncio.wait_for(fut, timeout)",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_a_cancel_beats_a_bell_that_rang_in_the_same_tick",
    ),
    (
        "wait_replacement returns any occupant, not a new tenure",
        RS_MEMBERSHIP_WAIT,
        "                        .is_none_or(|identity| !identity_matches(&self.pool, member, identity))",
        "                        .is_none_or(|_identity| true)",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_wait_replacement_names_the_new_tenure",
    ),
    (
        "republishing the same state nudges the heartbeat anyway",
        RS_LIB,
        "            if cur.state == state && cur.ready == ready {\n                return Ok(false);\n            }",
        "",
        "tests/membership/test_readiness.py"
        "::test_republishing_the_same_thing_costs_nothing",
    ),
    (
        "the request replacing a cancelled one is parked like any other",
        RS_MEMBERSHIP_HEARTBEAT,
        "            let hold = if cancelled_last {\n                0\n            } else {\n                shared.hold_ms.load(Ordering::Relaxed)\n            };",
        "            let hold = shared.hold_ms.load(Ordering::Relaxed);",
        "tests/registry/test_long_poll.py"
        "::test_publishing_flat_out_does_not_starve_the_heartbeat",
    ),
    (
        "dedup ignores readiness and only compares state",
        RS_LIB,
        "            if cur.state == state && cur.ready == ready {",
        "            if cur.state == state {",
        "tests/membership/test_readiness.py"
        "::test_going_ready_again_is_never_deduplicated_away",
    ),
    (
        "dedup compares raw bytes instead of parsed values",
        RS_LIB,
        "            if cur.state == state && cur.ready == ready {",
        "            if cur.state.to_string() == state_json && cur.ready == ready {",
        "tests/membership/test_readiness.py"
        "::test_key_order_is_not_a_change",
    ),
    (
        "the post-beat pause reads the last request's hold, not the loop's intent",
        RS_MEMBERSHIP_HEARTBEAT,
        "            if shared.hold_ms.load(Ordering::Relaxed) == 0 {\n"
        "                shared.short_polls",
        "            if hold == 0 {\n                shared.short_polls",
        "tests/registry/test_long_poll.py"
        "::test_publishing_never_makes_the_loop_fall_back_to_a_timer",
    ),
    (
        "the registry does not report its protocol on the ack",
        RS_STATE,
        "            protocol: tinyray_proto::PROTOCOL,\n"
        '            version: env!("CARGO_PKG_VERSION").to_string(),\n'
        "            ttl_ms:",
        "            protocol: 0,\n            version: String::new(),\n            ttl_ms:",
        "tests/registry/test_capabilities.py"
        "::test_a_member_can_ask_what_the_registry_can_do",
    ),
    (
        "a missing protocol field is an error rather than zero",
        RS_PROTO,
        "    #[serde(default)]\n    pub protocol: u32,",
        "    pub protocol: u32,",
        "cargo:tinyray-proto",
    ),
    (
        "an unknown feature name answers False instead of raising",
        PY_INIT,
        "            raise ValueError(\n"
        '                f"no such feature {feature!r}; this package knows about '
        '{sorted(self.FEATURES)}"\n'
        "            )",
        "            return False",
        "tests/registry/test_capabilities.py"
        "::test_an_unknown_feature_is_an_error_not_a_false",
    ),
    (
        "joining an out-of-date registry says nothing",
        PY_INIT,
        "        if missing:\n            required = max",
        "        if False:\n            required = max",
        "tests/registry/test_capabilities.py"
        "::test_wanting_more_than_the_registry_has_says_so_instead_of_degrading_quietly",
    ),
    (
        "await_fenced goes back to an executor thread",
        PY_INIT,
        "        while True:\n            bell = _loop_bell(self._c)\n"
        "            if not self._c.accepted:\n                return True\n"
        "            ms = _left_ms(deadline)\n            if ms is None:\n"
        "                return False\n            await bell.wait(ms / 1000)",
        "        return await asyncio.to_thread(self.wait_fenced, timeout)",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_await_fenced_holds_no_executor_thread_either",
    ),
    (
        "await_fenced never notices the takeover",
        PY_INIT,
        "        while True:\n            bell = _loop_bell(self._c)\n"
        "            if not self._c.accepted:\n                return True\n"
        "            ms = _left_ms(deadline)\n            if ms is None:\n                return False\n"
        "            await bell.wait(ms / 1000)",
        "        while True:\n            bell = _loop_bell(self._c)\n"
        "            ms = _left_ms(deadline)\n            if ms is None:\n"
        "                return False\n            await bell.wait(ms / 1000)",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_await_fenced_still_reports_a_takeover",
    ),
    (
        "a bell timeout escapes instead of ending the stream",
        PY_INIT,
        "        if not fut.done():\n            fut.set_result(None)",
        "        if not fut.done():\n            fut.set_exception(asyncio.TimeoutError())",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_achanges_with_a_timeout_ends_rather_than_raises",
    ),
    (
        "a fenced stream ends quietly, like a timeout",
        PY_INIT,
        "        if self._closed:\n            return None, 0\n        if not self._c.accepted:",
        "        if self._closed or not self._c.accepted:\n            return None, 0\n        if False:",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_the_three_ways_a_stream_ends_are_told_apart",
    ),
    (
        "the async stream ends quietly when fenced",
        PY_INIT,
        "        if self._closed:\n            return None, 0\n        if not self._c.accepted:",
        "        if self._closed or not self._c.accepted:\n            return None, 0\n        if False:",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_the_async_stream_ends_the_same_three_ways",
    ),
    (
        "close() raises Fenced too, instead of ending quietly",
        PY_INIT,
        "        if self._closed:\n            return None, 0",
        "        if False:\n            return None, 0",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_the_three_ways_a_stream_ends_are_told_apart",
    ),
    (
        "a field-scoped watch yields on everything anyway",
        PY_INIT,
        "            digest = self._c.field_digest(self._pool._name, self._fields)\n"
        "            if digest != self._digest:\n"
        "                self._digest = digest\n"
        "                return self._pool.snapshot(), 0",
        "            return self._pool.snapshot(), 0",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_a_watch_on_named_fields_ignores_the_rest",
    ),
    (
        "the per-loop cache is touched without a lock",
        PY_RPC,
        "_per_loop_lock = threading.Lock()\n\n\ndef per_loop",
        "_per_loop_lock = contextlib.nullcontext()\n\n\ndef per_loop",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_loops_in_many_threads_do_not_close_each_others_pipes",
    ),
    (
        "async work goes to the join-time loop even if it stopped",
        "python/tinyray/_serve.py",
        "                if loop is None or not loop.is_running():",
        "                if loop is None:",
        "tests/rpc/test_async.py"
        "::test_a_member_still_answers_after_the_loop_it_joined_on_stops",
    ),
    (
        "the first lookup does not wait for the first answer",
        PY_INIT,
        "        deadline = time.monotonic() + _FIRST_ANSWER_S\n        while True:",
        "        deadline = time.monotonic() + _FIRST_ANSWER_S\n        while False:",
        "tests/discovery/test_cold_cache.py"
        "::test_first_lookup_does_not_call_a_full_pool_empty",
    ),
    (
        "a silent registry is waited on anyway",
        PY_INIT,
        "            if self._c.silence_ms > self._lease_ms() // 2:",
        "            if False:",
        "tests/discovery/test_cold_cache.py"
        "::test_a_dead_registry_does_not_stall_lookups",
    ),
    (
        "saying goodbye only takes effect when the lease lapses",
        RS_STATE,
        "        } else if b.leaving {",
        "        } else if false {",
        "tests/membership/test_cleanup.py"
        "::test_the_registry_ends_a_round_where_it_started",
    ),
    (
        "a call is counted only after its answer is on the wire",
        "python/tinyray/_serve.py",
        "            counters.answered(failed)\n            counted = True\n"
        "            self._send_raw(code, encoded)",
        "            self._send_raw(code, encoded)\n            counters.answered(failed)\n"
        "            counted = True",
        "tests/rpc/test_stats.py"
        "::test_a_call_you_have_the_answer_to_is_already_counted",
    ),
    (
        "closing leaves the handler threads parked",
        "python/tinyray/_serve.py",
        "        for conn in list(self._srv.live):",
        "        for conn in []:",
        "tests/membership/test_cleanup.py"
        "::test_a_round_of_membership_leaves_nothing_behind",
    ),
    (
        "a closed server keeps its lookup table",
        "python/tinyray/_serve.py",
        "        self.dispatch.clear()\n        self._srv.dispatch = {}\n",
        "",
        "tests/membership/test_membership.py"
        "::test_leaving_lets_go_of_what_it_was_serving",
    ),
    (
        "the shape cache is keyed by the bound method",
        "python/tinyray/_serve.py",
        '    key = getattr(fn, "__func__", fn)\n    got = _SHAPES.get(key)',
        "    key = fn\n    got = _SHAPES.get(key)",
        "tests/membership/test_membership.py"
        "::test_leaving_lets_go_of_what_it_was_serving",
    ),
    (
        "a member told it lost the seat can be told otherwise later",
        RS_MEMBERSHIP_SHARED,
        "            changed |= self.accepted.swap(false, Ordering::Relaxed);",
        "            changed |= self.accepted.swap(true, Ordering::Relaxed);",
        "tests/registry/test_restart.py"
        "::test_a_frozen_owner_waking_after_a_restart_does_not_take_the_seat_back",
    ),
    (
        "a forked child runs the parent's exit hook",
        PY_INIT,
        "    atexit.register(member._leave_at_exit)",
        "    atexit.register(member.leave)",
        "tests/membership/test_fork.py"
        "::test_a_forked_child_exiting_normally_says_nothing",
    ),
    (
        "leaving stays registered with atexit",
        PY_INIT,
        "            atexit.unregister(self._leave_at_exit)\n",
        "",
        "tests/membership/test_membership.py"
        "::test_leaving_lets_go_of_what_it_was_serving",
    ),
    (
        "missing required arguments pass through signature binding",
        "python/tinyray/_serve.py",
        "public.bind(*args, **{k: v for k, v in kwargs.items() if k not in injected})",
        "public.bind_partial(*args, **{k: v for k, v in kwargs.items() if k not in injected})",
        "tests/rpc/test_validation.py"
        "::test_arguments_that_do_not_fit_are_the_callers_mistake",
    ),
    (
        "a dispatch that came apart is never counted",
        "python/tinyray/_serve.py",
        "            if not counted:",
        "            if False:",
        "tests/rpc/test_http.py"
        "::test_a_body_the_parser_gives_up_on_still_counts_as_a_call",
    ),
    (
        "any path at all reaches a method",
        "python/tinyray/_serve.py",
        '        if not batching and not (raw_result or self.path.startswith("/call/")):',
        "        if False:",
        "tests/rpc/test_http.py"
        "::test_only_the_call_path_reaches_a_method",
    ),
    (
        "a body that timed out leaves the connection open",
        "python/tinyray/_serve.py",
        "                self.close_connection = True\n                return self._send(408",
        "                return self._send(408",
        "tests/rpc/test_http.py"
        "::test_a_body_the_server_gave_up_on_takes_the_connection_with_it",
    ),
    (
        "an unreadable content-length leaves the connection open",
        "python/tinyray/_serve.py",
        "            self.close_connection = True\n"
        '            return self._send(400, {"error": "content-length is not a number"})',
        '            return self._send(400, {"error": "content-length is not a number"})',
        "tests/rpc/test_http.py"
        "::test_a_body_the_server_gave_up_on_takes_the_connection_with_it",
    ),
    (
        "a negative content-length leaves the connection open",
        "python/tinyray/_serve.py",
        "            self.close_connection = True\n"
        '            return self._send(400, {"error": "content-length is negative"})',
        '            return self._send(400, {"error": "content-length is negative"})',
        "tests/rpc/test_http.py"
        "::test_a_body_the_server_gave_up_on_takes_the_connection_with_it",
    ),
    (
        "a payload refused as too large is not the caller's fault",
        PY_RPC,
        "    if status == 413:",
        "    if False:",
        "tests/rpc/test_outcomes.py"
        "::test_every_status_lands_in_the_right_class",
    ),
    (
        "a status nobody agreed on is read as a good answer",
        PY_RPC,
        "    if status != 200:\n        raise OutcomeUnknown(f\"{target} returned HTTP {status}\")",
        "    if False:\n        raise OutcomeUnknown(f\"{target} returned HTTP {status}\")",
        "tests/rpc/test_outcomes.py"
        "::test_every_status_lands_in_the_right_class",
    ),
    (
        "typed RPC returns are handed back as raw JSON",
        PY_RPC,
        "        return convert_json(raw, want)",
        "        return loads(raw)",
        "tests/rpc/test_calling.py"
        "::test_returns_restores_a_nested_named_tuple",
    ),
    (
        "legacy typed RPC returns stop coercing JSON values",
        PY_JSON,
        "    return msgspec.convert(value, want, strict=False)",
        "    return msgspec.convert(value, want, strict=True)",
        "tests/rpc/test_models.py"
        "::test_legacy_result_fallback_restores_json_object_keys",
    ),
    (
        "setting a timeout forgets the requested return type",
        PY_RPC,
        "        return BoundMethod(self._handle, self._name, seconds, self._send, self._return_type)",
        "        return BoundMethod(self._handle, self._name, seconds, self._send)",
        "tests/rpc/test_calling.py"
        "::test_returns_and_timeout_compose_in_either_order",
    ),
    (
        "returns() records no return type",
        PY_RPC,
        "        return BoundMethod(self._handle, self._name, self._timeout, self._send, return_type)",
        "        return BoundMethod(self._handle, self._name, self._timeout, self._send)",
        "tests/rpc/test_calling.py"
        "::test_returns_restores_a_nested_named_tuple",
    ),
    (
        "an async typed RPC return is not restored",
        PY_RPC,
        "        return self._send(\n"
        "            self._handle,\n"
        "            self._name,\n"
        "            payload,\n"
        "            self._timeout,\n"
        "            _return_type=self._return_type,\n"
        "        )",
        "        return self._send(self._handle, self._name, payload, self._timeout)",
        "tests/rpc/test_calling.py"
        "::test_returns_restores_the_async_result_too",
    ),
    (
        "a typed return mismatch omits which call and type were wrong",
        PY_RPC,
        '        raise TypeError(f"{target} returned JSON that does not match {type_name}: {e}") from e',
        "        raise TypeError(str(e)) from e",
        "tests/rpc/test_calling.py"
        "::test_returns_names_the_call_and_json_path_when_the_shape_is_wrong",
    ),
    (
        "a handle with no address is posted to anyway",
        PY_RPC,
        "    if handle.url is None:",
        "    if False:",
        "tests/rpc/test_outcomes.py"
        "::test_no_address_never_left_this_process",
    ),
    (
        "a method the far side does not have is called a maybe",
        PY_RPC,
        "    if status == 404:",
        "    if False:",
        "tests/rpc/test_outcomes.py"
        "::test_a_method_the_far_side_does_not_have_is_not_a_maybe",
    ),
    (
        "a request the callee never read is called maybe-ran",
        PY_RPC,
        "    if status in (400, 408, 411):",
        "    if status == 411:",
        "tests/rpc/test_http.py"
        "::test_a_request_the_callee_never_read_whole_is_safe_to_send_again",
    ),
    (
        "a forked child keeps the parent's shared connection",
        PY_RPC,
        "    _sync = None\n",
        "",
        "tests/membership/test_fork.py"
        "::test_a_forked_child_does_not_share_the_synchronous_connection",
    ),
    (
        "a forked child keeps the parent's transports",
        PY_RPC,
        "    _loops.clear()\n",
        "",
        "tests/membership/test_fork.py"
        "::test_a_forked_child_does_not_talk_down_the_parents_sockets",
    ),
    (
        "a forked child inherits the lock still held",
        PY_INIT,
        "    _live_watches.clear()\n"
        "    _live_native_waits.clear()\n"
        "    _rpc.reset_after_fork()",
        "    _live_watches.clear()\n"
        "    _live_native_waits.clear()",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_a_child_forked_while_the_lock_was_held_can_still_watch",
    ),
    (
        "the blocking replacement wait only sees what comes next",
        PY_INIT,
        "        waiter = self._c.replacement_waiter(self._name, seat, was, capture)\n"
        "        result = _wait_native(waiter, deadline)\n"
        "        if result[0] == _WAIT_FENCED:\n"
        "            _fenced_wait(self._name)\n"
        "        if result[0] != _WAIT_READY or result[1] is None:\n"
        "            return None\n"
        "        return result[1].slot(seat, self._handle_cls._from_native)",
        "        with self.changes(timeout=timeout) as w:\n"
        "            for snap in w:\n"
        "                now = snap.slot(seat)\n"
        "                if now is not None and now.identity != was:\n"
        "                    return now\n"
        "        return None",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_a_replacement_that_already_happened_is_not_missed",
    ),
    (
        "the async replacement wait only sees what comes next",
        PY_INIT,
        "        result = await _await_native(\n"
        "            self._c,\n"
        "            self._c.replacement_waiter(self._name, seat, was, capture),\n"
        "            deadline,\n"
        "        )\n"
        "        if result[0] == _WAIT_FENCED:\n"
        "            _fenced_wait(self._name)\n"
        "        if result[0] != _WAIT_READY or result[1] is None:\n"
        "            return None\n"
        "        return result[1].slot(seat, self._handle_cls._from_native)",
        "        async with self.achanges(timeout=timeout) as w:\n"
        "            async for snap in w:\n"
        "                now = snap.slot(seat)\n"
        "                if now is not None and now.identity != was:\n"
        "                    return now\n"
        "        return None",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_a_replacement_that_already_happened_is_not_missed",
    ),
    (
        "a bell outlives the loop it belongs to",
        PY_RPC,
        "            if got is None or got.is_closed():",
        "            if got is None:",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_each_event_loop_leaves_nothing_behind",
    ),
    (
        "the digest leaves out who the members are",
        RS_MEMBERSHIP_CACHE,
        "            m.id.hash(&mut h);\n            m.incarnation.hash(&mut h);",
        "",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_a_watch_on_fields_notices_a_seat_changing_hands",
    ),
    (
        "the serving side stops counting refusals",
        "python/tinyray/_serve.py",
        "            counters.refuse()\n",
        "",
        "tests/rpc/test_stats.py"
        "::test_stats_shows_saturation_rather_than_leaving_it_to_guesswork",
    ),
    (
        "a pinned request id is ignored",
        "python/tinyray/_rpc.py",
        "    fixed = _pinned.get()\n    return fixed if fixed is not None else ",
        "    return ",
        "tests/membership/test_identity_and_fencing.py"
        "::test_a_caller_can_pin_one_name_across_retries",
    ),
    (
        "the pinned name is never restored afterwards",
        "python/tinyray/_rpc.py",
        "    finally:\n        _pinned.reset(token)",
        "    finally:\n        pass",
        "tests/membership/test_identity_and_fencing.py"
        "::test_a_caller_can_pin_one_name_across_retries",
    ),
    (
        "a world of zero seats is accepted",
        PY_INIT,
        "    if size is not None and not 1 <= size <= _MAX_SEAT:",
        "    if False:",
        "tests/membership/test_pool_shape.py"
        "::test_a_world_of_zero_seats_is_refused",
    ),
    (
        "a seat outside the world is accepted",
        PY_INIT,
        "    if slot is not None and size is not None and slot >= size:",
        "    if False:",
        "tests/membership/test_pool_shape.py"
        "::test_a_seat_outside_the_world_is_refused",
    ),
    (
        "a launcher variable is taken whatever its value",
        PY_INIT,
        "        if not 0 <= got <= _MAX_SEAT:",
        "        if False:",
        "tests/membership/test_pool_shape.py"
        "::test_a_launcher_variable_that_cannot_be_a_seat_says_which_one",
    ),
    (
        "a pool name is accepted whatever is in it",
        PY_INIT,
        '    if not name.isascii() or any(c < " " or c == "\\x7f" for c in name):',
        "    if False:",
        "tests/membership/test_identity_and_fencing.py"
        "::test_a_pool_name_that_cannot_be_a_header_is_refused",
    ),
    (
        "pool() takes a name join() would have refused",
        PY_INIT,
        "    return Pool(_checked_pool_name(name), _require_client())",
        "    return Pool(name, _require_client())",
        "tests/membership/test_identity_and_fencing.py"
        "::test_a_pool_name_that_cannot_be_a_header_is_refused",
    ),
    (
        "a request id that cannot be a header is accepted",
        PY_RPC,
        '    if not value.isascii() or any(c < " " or c == "\\x7f" for c in value):',
        "    if False:",
        "tests/membership/test_identity_and_fencing.py"
        "::test_a_request_id_that_cannot_be_a_header_is_refused_where_it_is_set",
    ),
    (
        "every call reuses one request id",
        PY_RPC,
        'else _generated_request_id(_identity or "anon", next(_seq))',
        'else _generated_request_id(_identity or "anon", 1)',
        "tests/membership/test_identity_and_fencing.py"
        "::test_every_call_carries_a_request_id_that_names_that_attempt",
    ),
    (
        "a cancelled attempt is terminal even if the kill never landed",
        "examples/agent_pool/pool.py",
        '        if record.state in ("completed", "cancelled"):\n            # A terminal state is terminal.',
        "        if False:\n            # A terminal state is terminal.",
        "tests/examples/test_agent_pool.py"
        "::test_a_cancelled_attempt_cannot_be_finished_by_a_survivor",
    ),
    (
        "a watcher resuming from since= baselines its digest on today",
        PY_INIT,
        "            self._digest = _NO_DIGEST",
        "            self._digest = self._c.field_digest(pool._name, self._fields)",
        "tests/discovery/test_watch_lifecycle.py"
        "::test_a_watch_on_fields_does_not_lose_what_happened_before_since",
    ),
    (
        "until subscribes from now instead of from the snapshot it looked at",
        PY_INIT,
        "        with self.changes(\n            since=snap.revision if since is None else since,",
        "        with self.changes(\n            since=None if since is None else since,",
        "tests/discovery/test_waiting.py"
        "::test_until_hands_the_revision_over_without_leaving_a_gap",
    ),
    (
        "auntil subscribes from now instead of from the snapshot it looked at",
        PY_INIT,
        "        watch = self.achanges(\n"
        "            since=snap.revision if since is None else since,",
        "        watch = self.achanges(\n            since=None if since is None else since,",
        "tests/discovery/test_waiting.py"
        "::test_auntil_hands_the_revision_over_as_well",
    ),
    (
        "flush blames the registry for a seat that was taken",
        PY_INIT,
        "        if not accepted:\n"
        '            raise SeatTaken(f"{self.pool} seat {self.slot} was taken while publishing")',
        '        if False:\n            raise SeatTaken("")',
        "tests/membership/test_seats.py"
        "::test_flush_says_the_seat_was_taken_rather_than_blaming_the_registry",
    ),
    (
        "a wait parks even when the cache has already moved past it",
        RS_LIB,
        "            if *rev != since {\n                return *rev;\n            }",
        "            if false {\n                return *rev;\n            }",
        "tests/discovery/test_events.py"
        "::test_a_wait_handed_a_revision_already_passed_returns_at_once",
    ),
    (
        "a superseded member keeps beating",
        RS_MEMBERSHIP_HEARTBEAT,
        "                    if !alive {\n"
        "                        // Superseded. Beating on would only be waiting for the\n"
        "                        // replacement to die so we could take the seat back.\n"
        "                        return;\n"
        "                    }",
        "                    if false {\n                        return;\n                    }",
        "tests/membership/test_seats.py"
        "::test_a_superseded_member_stops_beating_instead_of_hammering_the_registry",
    ),
    (
        "empty heartbeat acknowledgements broadcast cache changes",
        RS_MEMBERSHIP_HEARTBEAT,
        "                    shared.note_beat();\n"
        "                    if changed {\n"
        "                        shared.ring();\n"
        "                    }",
        "                    shared.note_beat();\n"
        "                    if true {\n"
        "                        shared.ring();\n"
        "                    }",
        "tests/registry/test_long_poll.py"
        "::test_idle_heartbeats_do_not_broadcast_cache_changes",
    ),
    (
        "publication acknowledgements do not wake publication waiters",
        RS_MEMBERSHIP_HEARTBEAT,
        "                    shared.acked.notify_one();\n"
        "                    shared.note_beat();",
        "                    shared.acked.notify_one();",
        "tests/membership/test_identity_and_fencing.py"
        "::test_flush_is_released_by_the_exact_publication_ack",
    ),
    (
        "Rust registration returns to 100ms sliced waits",
        RS_SDK_MEMBER,
        "        if !shared.wait_registered(timeout) {",
        "        if !shared.wait_registered(Duration::from_millis(100)) {",
        "tests/project/test_api.py"
        "::test_rust_member_registration_waits_on_one_event_deadline",
    ),
    (
        "a filter compares big integers as doubles",
        RS_PROTO,
        "(false, false) => x == y,",
        "(false, false) => x.as_f64() == y.as_f64(),",
        "cargo:tinyray-proto",
    ),
    (
        "the number rule stops at the top level",
        RS_PROTO,
        "        (Value::Array(x), Value::Array(y)) => {\n"
        "            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same_value(p, q))\n"
        "        }\n"
        "        (Value::Object(x), Value::Object(y)) => {\n"
        "            x.len() == y.len()\n"
        "                && x.iter()\n"
        "                    .all(|(k, v)| y.get(k).is_some_and(|w| same_value(v, w)))\n"
        "        }\n",
        "",
        "cargo:tinyray-proto",
    ),
    (
        "a negative lease reaches pyo3 and comes out as a traceback",
        "python/tinyray/registry.py",
        '    ap.add_argument("--ttl-ms", type=_lease_ms, default=20_000,',
        '    ap.add_argument("--ttl-ms", type=int, default=20_000,',
        "tests/registry/test_leases.py"
        "::test_a_lease_that_is_not_a_length_of_time_is_refused_cleanly",
    ),
    (
        "wait() runs its own loop and cannot report a lost seat",
        PY_INIT,
        "        result = _wait_native(waiter, deadline)\n"
        "        if result[0] == _WAIT_FENCED:\n"
        "            _fenced_wait(self._name)\n"
        "        if result[0] == _WAIT_READY and result[1] is not None:",
        "        result = _wait_native(waiter, deadline)\n"
        "        if result[0] == _WAIT_READY and result[1] is not None:",
        "tests/membership/test_seats.py"
        "::test_every_wait_says_it_was_fenced_rather_than_blaming_the_pool",
    ),
    (
        "join's budget starts only after the first beat has spent its own",
        PY_INIT,
        "    if not c.start(int(min(timeout, _FIRST_BEAT_S) * 1000)):",
        "    if not c.start():",
        "tests/membership/test_join.py"
        "::test_the_budget_covers_reaching_the_registry_not_just_the_wait",
    ),
    (
        "leave() says goodbye for a member that was never there",
        RS_LIB,
        "            if s.beats_ok.load(Ordering::Relaxed) > 0 {\n"
        "                py.allow_threads(|| beat_once(&rt, &s, Duration::from_secs(5), false));\n"
        "            }",
        "            py.allow_threads(|| beat_once(&rt, &s, Duration::from_secs(5), false));",
        "tests/membership/test_join.py"
        "::test_the_budget_covers_reaching_the_registry_not_just_the_wait",
    ),
    (
        "until hands the watch a fresh budget instead of what is left",
        PY_INIT,
        "            timeout=None if deadline is None else max(0.0, deadline - time.monotonic()),\n"
        "        ) as w:",
        "            timeout=timeout,\n        ) as w:",
        "tests/discovery/test_waiting.py"
        "::test_the_budget_is_a_deadline_not_an_allowance_on_top",
    ),
    (
        "auntil hands the watch a fresh budget instead of what is left",
        PY_INIT,
        "        watch = self.achanges(\n"
        "            since=snap.revision if since is None else since,\n"
        "            timeout=None if deadline is None else max(0.0, deadline - time.monotonic()),\n"
        "        )",
        "        watch = self.achanges(\n"
        "            since=snap.revision if since is None else since, timeout=timeout\n"
        "        )",
        "tests/discovery/test_waiting.py"
        "::test_the_budget_is_a_deadline_not_an_allowance_on_top",
    ),
    (
        "a connection that says nothing keeps its thread for good",
        "python/tinyray/_serve.py",
        "        self.connection.settimeout(BODY_TIMEOUT)\n\n    def log_message",
        "        pass\n\n    def log_message",
        "tests/rpc/test_http.py"
        "::test_a_connection_that_says_nothing_at_all_releases_its_thread",
    ),
    (
        "the client's own fingerprint is not the registry's hash",
        RS_MEMBERSHIP_CACHE,
        "let roster = members.iter().fold(0, |h, m| h ^ m.roster_hash());",
        "let roster = members.iter().fold(0u64, |h, m| h.wrapping_add(m.roster_hash()));",
        "tests/collectives/test_roster_fingerprint.py"
        "::test_the_clients_own_fingerprint_agrees_with_the_registrys",
    ),
    (
        "the header names a different tenure than the member holds",
        PY_INIT,
        "        _rpc.set_identity(member.identity)",
        "        _rpc.set_identity(_identity(pool, slot, ident, incarnation - 1))",
        "tests/membership/test_identity_and_fencing.py"
        "::test_one_spelling_of_the_fencing_token",
    ),
    (
        "taking a seat does not release the parked beat of the member losing it",
        RS_STATE,
        "                    bell.notify_one();",
        "                    let _ = bell;",
        "tests/membership/test_seats.py"
        "::test_a_superseded_process_stops_answering",
    ),
    (
        "the first beat is handed the whole budget instead of a slice",
        PY_INIT,
        "    if not c.start(int(min(timeout, _FIRST_BEAT_S) * 1000)):",
        "    if not c.start(int(timeout * 1000)):",
        "tests/registry/test_network_faults.py"
        "::test_ctrl_c_during_join_is_not_swallowed_until_the_timeout",
    ),
    (
        "never joined and already left give the same message",
        PY_INIT,
        '    raise RuntimeError("call tinyray.join(...) before looking anyone up")',
        "    raise RuntimeError(\n"
        '        "this process has left; a lookup after leave() cannot work. "\n'
        '        "Background threads outliving leave() are the usual cause."\n'
        "    )",
        "tests/membership/test_membership.py"
        "::test_lookup_before_join_is_explicit",
    ),
    (
        "the async replacement wait names the synchronous one in its error",
        PY_INIT,
        'self._replacement_target(slot, identity, "await_replacement")',
        'self._replacement_target(slot, identity, "wait_replacement")',
        "tests/discovery/test_watch_lifecycle.py"
        "::test_wait_replacement_wants_exactly_one_of_slot_or_identity",
    ),
    (
        "a class bound on the served object is published as a method",
        "python/tinyray/_serve.py",
        "        if not callable(attr) or isinstance(attr, type):",
        "        if not callable(attr):",
        "tests/rpc/test_validation.py"
        "::test_a_class_on_the_served_object_is_not_a_method",
    ),
    (
        "dataclass RPC values are rejected again",
        PY_JSON,
        "        if is_dataclass(value) and not isinstance(value, type):",
        "        if False:",
        "tests/rpc/test_codecs.py"
        "::test_dataclasses_are_normalized_without_widening_unrelated_values",
    ),
    (
        "typed calls stop using the raw result path",
        PY_RPC,
        "    path = \"/_batch\" if batching else f\"{RAW_RESULT_PATH if raw_result else '/call/'}{name}\"",
        '    path = "/_batch" if batching else f"/call/{name}"',
        "tests/rpc/test_models.py"
        "::test_typed_calls_fall_back_to_a_legacy_result_envelope",
    ),
    (
        "the server wraps a negotiated raw result again",
        "python/tinyray/_serve.py",
        '                if raw_result and code == 200 and not failed and set(body) == {"result"}:',
        '                if False and code == 200 and not failed and set(body) == {"result"}:',
        "tests/rpc/test_models.py"
        "::test_raw_result_negotiation_returns_the_value_as_the_whole_body",
    ),
    (
        "model arguments rebuild the whole generic request graph",
        "python/tinyray/_serve.py",
        "        if raw and _takes_model(fn):",
        "        if False:",
        "tests/rpc/test_models.py"
        "::test_model_rpc_skips_both_generic_object_graphs",
    ),
    (
        "plain dataclass JSON rebuilds a generic object graph",
        PY_JSON,
        "    return msgspec.json.decode(raw, type=want, strict=False)",
        "    return convert(loads(bytes(raw)), want)",
        "tests/rpc/test_models.py"
        "::test_model_rpc_skips_both_generic_object_graphs",
    ),
    (
        "direct dataclass JSON drops legacy NaN and surrogate support",
        PY_JSON,
        "    except msgspec.DecodeError as direct_error:",
        "    except ():",
        "tests/rpc/test_models.py"
        "::test_direct_dataclass_json_keeps_legacy_nan_and_surrogate_semantics",
    ),
    (
        "only the first of a *args run gets its annotation",
        "python/tinyray/_serve.py",
        "tuple(_coerce_value(v, want) for v in value)",
        "tuple(_coerce_value(v, want) if i == 0 else v "
        "for i, v in enumerate(value))",
        "tests/rpc/test_validation.py"
        "::test_the_annotation_covers_every_value_it_names",
    ),
    (
        "an envelope that cannot be unpacked comes back as maybe-ran",
        "python/tinyray/_serve.py",
        "        if given_args is not None and not isinstance(given_args, list):",
        "        if False:",
        "tests/rpc/test_http.py"
        "::test_a_malformed_envelope_is_the_callers_fault_not_a_maybe",
    ),
    (
        "the first beat ignores the loop's ack and waits out its own budget",
        RS_MEMBERSHIP_HEARTBEAT,
        "            _ = s.acked.notified(), if stop_when_registered => None,",
        "            _ = s.acked.notified(), if false => None,",
        "tests/registry/test_network_faults.py"
        "::test_join_returns_when_the_loop_registers_not_when_the_first_beat_gives_up",
    ),
    (
        "flush counts beats instead of asking whether its state was acked",
        PY_INIT,
        "        mine, _ = self._c.publish_versions()",
        '        mine = self._c.stats()["beats_ok"] + 2',
        "tests/registry/test_long_poll.py"
        "::test_flush_waits_for_its_own_state_not_for_two_more_beats",
    ),
    (
        "flush is satisfied by an ack for the state before the one it published",
        PY_INIT,
        "        confirmed, accepted = self._c.wait_publication(mine, 0 if ms is None else ms)",
        "        confirmed, accepted = self._c.wait_publication(mine - 1, 0 if ms is None else ms)",
        "tests/membership/test_identity_and_fencing.py"
        "::test_flush_is_released_by_the_exact_publication_ack",
    ),
    (
        "the beat connection leaves Nagle on, so a close-following beat stalls",
        RS_BEAT,
        "    c.set_nodelay(true);",
        "    c.set_nodelay(false);",
        "tests/registry/test_long_poll.py"
        "::test_a_beat_that_follows_close_on_the_last_one_is_not_held_by_nagle",
    ),
    (
        "a refused beat counts as confirmation of the state it carried",
        RS_MEMBERSHIP_HEARTBEAT,
        "                    if alive {\n"
        "                        shared.confirmed.fetch_max(showing, Ordering::Relaxed);\n"
        "                    }",
        "                    shared.confirmed.fetch_max(showing, Ordering::Relaxed);",
        "tests/membership/test_seats.py"
        "::test_a_refused_beat_confirms_nothing",
    ),
    (
        "constructing a Pool does not subscribe, so priming buys nothing",
        PY_INIT,
        "    def __init__(self, name: str, client: _Client):\n"
        "        self._name = name\n"
        "        self._c = client\n"
        "        client.watch([name])",
        "    def __init__(self, name: str, client: _Client):\n"
        "        self._name = name\n"
        "        self._c = client",
        "tests/discovery/test_async_lookup_cost.py"
        "::test_priming_a_pool_keeps_the_first_async_lookup_off_the_loop",
    ),
    (
        "registry_url is accepted and then ignored for the environment",
        PY_INIT,
        "    endpoint = _endpoint(registry_url)",
        "    endpoint = _endpoint()",
        "tests/membership/test_join.py"
        "::test_registry_url_beats_the_environment_and_does_not_touch_it",
    ),
    (
        "the unreachable message re-reads the environment instead of what was dialled",
        PY_INIT,
        'f"no answer from the registry at {endpoint} after "',
        'f"no answer from the registry at {_endpoint()} after "',
        "tests/membership/test_join.py"
        "::test_the_unreachable_message_names_the_address_it_actually_dialled",
    ),
    (
        "a list of registry addresses is composed into a URL instead of refused",
        PY_INIT,
        '    if "," in raw:',
        "    if False:",
        "tests/membership/test_join.py"
        "::test_a_list_of_registries_is_refused_instead_of_dialled",
    ),
    (
        "a fenced epoch trusts its unchanged cached fingerprint",
        PY_INIT,
        "return self._c.epoch_valid(self.pool, self.roster)",
        "return True",
        "tests/collectives/test_epochs.py"
        "::test_fencing_invalidates_epochs_even_without_a_final_cache_refresh",
    ),
    (
        "a fenced member may open another epoch",
        PY_INIT,
        "    def _check_fenced(self) -> None:\n        if not self._c.accepted:",
        "    def _check_fenced(self) -> None:\n        if False:",
        "tests/collectives/test_epochs.py"
        "::test_fencing_invalidates_epochs_even_without_a_final_cache_refresh",
    ),
    (
        "a rejoin keeps the old client's event-loop bell",
        PY_INIT,
        "reuse=lambda bell: bell._client is client,",
        "reuse=lambda bell: True,",
        "tests/discovery/test_async_watches.py"
        "::test_async_rejoin_replaces_the_bell_and_releases_old_waiters",
    ),
    (
        "replacing a bell abandons its outstanding waiters",
        PY_INIT,
        "            self._fire()\n            self._loop.remove_reader(self._r)",
        "            self._loop.remove_reader(self._r)",
        "tests/discovery/test_async_watches.py"
        "::test_async_rejoin_replaces_the_bell_and_releases_old_waiters",
    ),
    (
        "nonfinite state reaches the native JSON parser",
        PY_INIT,
        "raw = json.dumps(state, allow_nan=False)",
        "raw = json.dumps(state)",
        "tests/membership/test_state.py"
        "::test_nonfinite_state_is_rejected_without_poisoning_the_member",
    ),
    (
        "ready commits Python state before native validation",
        PY_INIT,
        "            self._c.set_state(raw, True)\n            self._state = merged",
        "            self._state = merged\n            self._c.set_state(raw, True)",
        "tests/membership/test_state.py"
        "::test_native_state_rejection_also_preserves_python_state",
    ),
    (
        "update commits Python state before native validation",
        PY_INIT,
        "            self._c.set_state_only(raw)\n            self._state = merged",
        "            self._state = merged\n            self._c.set_state_only(raw)",
        "tests/membership/test_state.py"
        "::test_native_state_rejection_also_preserves_python_state",
    ),
    (
        "replace commits Python state before native validation",
        PY_INIT,
        "            self._c.set_state_only(raw)\n            self._state = fresh",
        "            self._state = fresh\n            self._c.set_state_only(raw)",
        "tests/membership/test_state.py"
        "::test_native_state_rejection_also_preserves_python_state",
    ),
    (
        "set_ready commits Python state before native validation",
        PY_INIT,
        "            self._c.set_state(raw, True)\n            self._state = fresh",
        "            self._state = fresh\n            self._c.set_state(raw, True)",
        "tests/membership/test_state.py"
        "::test_native_state_rejection_also_preserves_python_state",
    ),
    (
        "an available snapshot bypasses the watch deadline",
        PY_INIT,
        "        if _left_ms(self._deadline) is None:\n            return None, 0\n",
        "",
        "tests/discovery/test_waiting.py"
        "::test_watch_deadline_wins_over_available_changes",
    ),
    (
        "failed initialization leaves its method server running",
        PY_INIT,
        "cleanup.callback(server.close)",
        "cleanup.callback(lambda: None)",
        "tests/membership/test_join.py"
        "::test_failed_join_releases_every_acquired_resource",
    ),
    (
        "context binding permits a duplicate argument",
        "python/tinyray/_serve.py",
        "bound = public.bind(*args, **{k: v for k, v in kwargs.items() if k not in injected})",
        "bound = public.bind(*args, **{k: v for k, v in kwargs.items() "
        "if k not in injected and k not in list(public.parameters)[:len(args)]})",
        "tests/rpc/test_validation.py"
        "::test_context_injection_preserves_python_binding_and_conversion",
    ),
    (
        "retired clients may replace the active membership's bell",
        PY_INIT,
        "    if client is not _client:",
        "    if False:",
        "tests/discovery/test_async_watches.py"
        "::test_retired_async_operations_cannot_replace_the_current_bell",
    ),
    (
        "an async watch checks state before its first subscription",
        PY_INIT,
        "            bell = _loop_bell(self._c)\n            snap, ms = self._step()",
        "            snap, ms = self._step()\n            bell = _loop_bell(self._c)",
        "tests/discovery/test_async_watches.py"
        "::test_first_async_subscription_cannot_miss_fencing",
    ),
    (
        "await_fenced checks state before its first subscription",
        PY_INIT,
        "            bell = _loop_bell(self._c)\n            if not self._c.accepted:\n"
        "                return True",
        "            if not self._c.accepted:\n                return True\n"
        "            bell = _loop_bell(self._c)",
        "tests/discovery/test_async_watches.py"
        "::test_first_async_subscription_cannot_miss_fencing",
    ),
    (
        "old publications overwrite newer state",
        RS_STATE,
        "            if newer {",
        "            if true {",
        "cargo:tinyray-registry",
    ),
    (
        "the client omits publication ordering",
        RS_MEMBERSHIP_SHARED,
        "publication: Some(published.version),",
        "publication: None,",
        "cargo:tinyray-membership",
    ),
    (
        "an old acknowledgment rolls the cache back",
        RS_MEMBERSHIP_SHARED,
        "            if d.version < c.version {\n                continue;\n            }\n",
        "",
        "cargo:tinyray-membership",
    ),
    (
        "the short-lease request budget outlives the lease",
        RS_MEMBERSHIP_HEARTBEAT,
        "hold_ms + hold_ms / 2 + (hold_ms / 2).min(200)",
        "hold_ms + hold_ms / 2 + 200",
        "cargo:tinyray-membership",
    ),
    (
        "reading the heartbeat body starts another full timeout",
        RS_BEAT,
        "tokio::time::timeout_at(deadline, resp.into_body().collect())",
        "tokio::time::timeout(budget, resp.into_body().collect())",
        "tests/registry/test_leases.py"
        "::test_headers_and_body_share_one_heartbeat_deadline",
    ),
    (
        "mixed numeric filters round the integer to a float",
        RS_PROTO,
        "(false, true) => integer_matches_float(x, y.as_f64().unwrap()),",
        "(false, true) => x.as_f64() == y.as_f64(),",
        "cargo:tinyray-proto",
    ),
    (
        "pull requests no longer run the code quality gates",
        ".github/workflows/release.yml",
        "  pull_request:\n",
        "",
        "tests/project/test_ci.py"
        "::test_code_quality_runs_for_pull_requests_and_main",
    ),
    (
        "slot lookup ignores readiness",
        RS_MEMBERSHIP_CACHE,
        "            (!require_ready || member.ready).then(|| member.clone())",
        "            Some(member.clone())",
        "tests/discovery/test_fast_lookups.py"
        "::test_duplicate_slots_choose_the_lowest_eligible_wire_id",
    ),
    (
        "native snapshots survive a changed publication",
        RS_MEMBERSHIP_CACHE,
        "            *self.snapshots.get_mut().unwrap() = Default::default();",
        "",
        "cargo:tinyray-membership",
    ),
    (
        "field digest memoization survives a changed publication",
        RS_MEMBERSHIP_CACHE,
        "            *self.digest.get_mut().unwrap() = None;",
        "",
        "cargo:tinyray-membership",
    ),
    (
        "shared registry deltas survive a pool change",
        RS_STATE,
        "        self.cache.get_mut().unwrap().clear(&mut deferred.retired);",
        "",
        "cargo:tinyray-registry",
    ),
    (
        "a batch does not recheck fencing between items",
        "python/tinyray/_serve.py",
        "            fenced = self._fenced()\n            name = item[\"method\"]",
        "            fenced = None\n            name = item[\"method\"]",
        "tests/rpc/test_batch.py::test_takeover_between_items_fences_the_remaining_prefix",
    ),
    (
        "a batch continues after an item failed",
        "python/tinyray/_serve.py",
        "            if failed or encoding_failed:",
        "            if False:",
        "tests/rpc/test_batch.py::test_first_failure_stops_execution_with_completed_results",
    ),
    (
        "a missing performance metric is silently skipped",
        "bench.py",
        "        if key not in is_:\n            worse.append(f\"{key}: missing from current results\")",
        "        if key not in is_:\n            pass",
        "tests/project/test_bench.py::test_missing_current_metrics_fail_instead_of_disappearing",
    ),
    (
        "benchmark cleanup bypasses the member-owned server",
        "bench.py",
        "    _registries[-1].members.callback(member.leave)",
        "    _registries[-1].members.callback(member._c.leave)",
        "tests/project/test_bench.py::test_benchmark_teardown_closes_members_and_restores_environment",
    ),
]

# HTTP/JSON method RPC was removed in 0.18. Keep the historical entries above
# readable, but replace their dead anchors with invariants of the native
# framed transport. Filtering by label also makes duplicate old entries
# impossible to leave active accidentally.
_REPLACED_METHOD_RPC_MUTANTS = {
    "a beat body is read whatever size it announces",
    "the listen backlog goes back to socketserver's default of 5",
    "positional arguments are left positional when a context is injected",
    "a call is counted only after its answer is on the wire",
    "closing leaves the handler threads parked",
    "a closed server keeps its lookup table",
    "missing required arguments pass through signature binding",
    "a dispatch that came apart is never counted",
    "any path at all reaches a method",
    "a body that timed out leaves the connection open",
    "an unreadable content-length leaves the connection open",
    "a negative content-length leaves the connection open",
    "a payload refused as too large is not the caller's fault",
    "a status nobody agreed on is read as a good answer",
    "typed RPC returns are handed back as raw JSON",
    "legacy typed RPC returns stop coercing JSON values",
    "a typed return mismatch omits which call and type were wrong",
    "a handle with no address is posted to anyway",
    "a method the far side does not have is called a maybe",
    "a request the callee never read is called maybe-ran",
    "a forked child keeps the parent's shared connection",
    "a forked child keeps the parent's transports",
    "the serving side stops counting refusals",
    "a connection that says nothing keeps its thread for good",
    "typed calls stop using the raw result path",
    "the server wraps a negotiated raw result again",
    "only the first of a *args run gets its annotation",
    "the beat connection leaves Nagle on, so a close-following beat stalls",
    "context binding permits a duplicate argument",
    "reading the heartbeat body starts another full timeout",
    "a batch does not recheck fencing between items",
    "a batch continues after an item failed",
    "a method name that cannot go in a URL is served anyway",
    "a pool name is accepted whatever is in it",
    "pool() takes a name join() would have refused",
    "a request id that cannot be a header is accepted",
    "dataclass RPC values are rejected again",
    "plain dataclass JSON rebuilds a generic object graph",
    "direct dataclass JSON drops legacy NaN and surrogate support",
    "an envelope that cannot be unpacked comes back as maybe-ran",
    "model arguments rebuild the whole generic request graph",
}
MUTANTS = [
    m
    for m in MUTANTS
    if m[0] not in _REPLACED_METHOD_RPC_MUTANTS and m[1] != PY_JSON
]
MUTANTS.extend(
    [
        (
            "a frame length is trusted before allocation",
            RS_WIRE,
            "    if length > maximum {\n"
            "        return Err(FrameError::FrameTooLarge { length, maximum });\n"
            "    }",
            "    if false {\n"
            "        return Err(FrameError::FrameTooLarge { length, maximum });\n"
            "    }",
            "cargo:tinyray-proto",
        ),
        (
            "a method request with the wrong protocol version is dispatched",
            RS_SDK_TRANSPORT_SERVER,
            "    if request.protocol != RPC_PROTOCOL {",
            "    if false {",
            "tests/rpc/test_transport.py"
            "::test_invalid_protocol_metadata_is_correlated_and_never_dispatched",
        ),
        (
            "a reply with the wrong protocol version is accepted",
            RS_SDK_TRANSPORT_CLIENT,
            "    if reply.protocol != RPC_PROTOCOL {",
            "    if false {",
            "tests/rpc/test_transport.py"
            "::test_reply_protocol_mismatch_is_unknown_and_discards_the_connection",
        ),
        (
            "multiplexed replies are routed by arrival order instead of request id",
            RS_SDK_TRANSPORT_CLIENT,
            "        if let Some(pending) = state.pending.remove(&reply.request_id) {",
            "        let arrived_first = state.pending.keys().min().cloned();\n"
            "        if let Some(pending) = arrived_first\n"
            "            .as_ref()\n"
            "            .and_then(|request_id| state.pending.remove(request_id))\n"
            "        {",
            "tests/rpc/test_transport.py"
            "::test_one_multiplexed_connection_routes_mixed_sync_and_async_replies_out_of_order",
        ),
        (
            "the shared listener ignores the Python service target fencing token",
            RS_SDK_TRANSPORT_SERVER,
            "    if request.target != state.identity {",
            "    if false {",
            "tests/rpc/test_transport.py"
            "::test_every_complete_reply_leaves_the_connection_correlated",
        ),
        (
            "a partial request write is called outcome-unknown",
            RS_SDK_TRANSPORT_CLIENT,
            "                connection.poison(\n"
            "                    CallError::NotDelivered(format!(\n"
            '                        "the request was not completely written to {endpoint} before timeout"\n'
            "                    )),\n"
            "                    CallError::OutcomeUnknown(format!(\n"
            '                        "{endpoint} failed after a complete request was written"\n'
            "                    )),\n"
            "                    false,\n"
            "                );",
            "                connection.poison(\n"
            "                    CallError::NotDelivered(format!(\n"
            '                        "the request was not completely written to {endpoint} before timeout"\n'
            "                    )),\n"
            "                    CallError::OutcomeUnknown(format!(\n"
            '                        "{endpoint} failed after a complete request was written"\n'
            "                    )),\n"
            "                    true,\n"
            "                );",
            "tests/rpc/test_transport.py"
            "::test_timeout_during_a_partial_write_is_not_delivered",
        ),
        (
            "a timeout after the complete write is called not-delivered",
            RS_SDK_TRANSPORT_CLIENT,
            "            CallError::OutcomeUnknown(format!(\"{endpoint} did not answer before the call timeout\"))",
            "            CallError::NotDelivered(format!(\"{endpoint} did not answer before the call timeout\"))",
            "tests/rpc/test_transport.py"
            "::test_timeout_after_complete_write_is_outcome_unknown",
        ),
        (
            "async cancellation retains its multiplexed waiter",
            RS_SDK_TRANSPORT_CLIENT,
            "            if let Some(connection) = target.connection.upgrade() {\n"
            "                let _ = connection.abandon(&target.request_id);\n"
            "            }",
            "",
            "cargo:tinyray:external_client_cancellation_removes_the_waiter_synchronously",
        ),
        (
            "overloaded method calls wait in a queue",
            RS_SDK_TRANSPORT_SERVER,
            "        Some(admission) => match admission.clone().try_acquire_owned() {",
            "        Some(admission) => match admission.clone().acquire_owned().await {",
            "tests/rpc/test_outcomes.py"
            "::test_going_over_the_concurrency_limit_is_refused_not_queued",
        ),
        (
            "served calls disappear from native statistics",
            RS_SDK_TRANSPORT_SERVER,
            "        flight.answered(reply.status != RpcStatus::Success);",
            "",
            "tests/rpc/test_transport.py"
            "::test_answer_is_counted_before_the_client_can_observe_it",
        ),
        (
            "a complete sync response is never returned to the pool",
            RS_SDK_TRANSPORT_CLIENT,
            "            let _ = sender.send(Ok(ReceivedRpcReply { reply, _ack: ack }));\n"
            "            if close {",
            "            let _ = sender.send(Ok(ReceivedRpcReply { reply, _ack: ack }));\n"
            "            if true {",
            "tests/rpc/test_transport.py"
            "::test_sync_calls_return_a_complete_connection_to_the_native_pool",
        ),
        (
            "the idle connection cap becomes per-endpoint and unbounded",
            RS_SDK_TRANSPORT,
            "const MAX_IDLE_CONNECTIONS: usize = 64;",
            "const MAX_IDLE_CONNECTIONS: usize = usize::MAX;",
            "tests/rpc/test_transport.py"
            "::test_idle_connection_cap_is_process_global_not_per_endpoint",
        ),
        (
            "closing a local listener leaves its client pool behind",
            "python/tinyray/_serve.py",
            "        for endpoint in self._endpoints:\n"
            "            _native.rpc_drop_endpoint(endpoint)\n",
            "",
            "tests/membership/test_cleanup.py"
            "::test_a_round_of_membership_leaves_nothing_behind",
        ),
        (
            "closing a native listener keeps the served object",
            "python/tinyray/_serve.py",
            "        self.dispatch.clear()\n",
            "",
            "tests/rpc/test_transport.py"
            "::test_closing_a_native_listener_releases_the_served_object",
        ),
        (
            "legacy HTTP method endpoints reach the native dialer",
            PY_RPC,
            '    if "://" in endpoint:\n',
            "    if False:\n",
            "tests/rpc/test_transport.py"
            "::test_endpoint_is_bare_and_legacy_http_is_rejected_explicitly",
        ),
        (
            "a handle with no native endpoint is dialled anyway",
            PY_RPC,
            "    if endpoint is None:\n",
            "    if False:\n",
            "tests/rpc/test_outcomes.py::test_no_address_never_left_this_process",
        ),
        (
            "method-not-found is reported as outcome-unknown",
            PY_RPC,
            "    if status == _native.RPC_STATUS_METHOD_NOT_FOUND:\n",
            "    if False:\n",
            "tests/rpc/test_outcomes.py"
            "::test_a_method_the_far_side_does_not_have_is_not_a_maybe",
        ),
        (
            "an unknown native status is treated as success",
            PY_RPC,
            '    raise OutcomeUnknown(f"{target} returned unknown native RPC status {status!r}")',
            "    return",
            "tests/rpc/test_outcomes.py::test_every_native_status_lands_in_the_right_class",
        ),
        (
            "a typed return error omits the remote call and expected type",
            PY_RPC,
            "    except msgspec.ValidationError as exc:\n"
            '        label = getattr(want, "__qualname__", repr(want))\n'
            '        raise TypeError(f"{call} returned MessagePack that does not match {label}: {exc}") from exc',
            "    except msgspec.ValidationError as exc:\n"
            '        label = getattr(want, "__qualname__", repr(want))\n'
            "        raise TypeError(str(exc)) from exc",
            "tests/rpc/test_calling.py"
            "::test_returns_names_the_call_and_messagepack_path_when_the_shape_is_wrong",
        ),
        (
            "missing arguments pass partial signature binding",
            "python/tinyray/_serve.py",
            "        bound = public.bind(*args, **public_kwargs)",
            "        bound = public.bind_partial(*args, **public_kwargs)",
            "tests/rpc/test_validation.py"
            "::test_arguments_that_do_not_fit_are_the_callers_mistake",
        ),
        (
            "only the first variadic argument is coerced",
            "python/tinyray/_serve.py",
            "            bound.arguments[name] = tuple(_coerce_value(item, want) for item in value)",
            "            bound.arguments[name] = tuple(\n"
            "                _coerce_value(item, want) if index == 0 else item\n"
            "                for index, item in enumerate(value)\n"
            "            )",
            "tests/rpc/test_validation.py"
            "::test_the_annotation_covers_every_value_it_names",
        ),
        (
            "a caller can forge an injected CallContext argument",
            "python/tinyray/_serve.py",
            "        public_kwargs = {key: value for key, value in kwargs.items() if key not in injected}",
            "        public_kwargs = kwargs",
            "tests/rpc/test_validation.py"
            "::test_context_injection_preserves_python_binding_and_conversion",
        ),
        (
            "a batch stops checking ownership between items",
            "python/tinyray/_serve.py",
            "            if not self.still_ours():",
            "            if False:",
            "tests/rpc/test_batch.py"
            "::test_takeover_between_items_fences_the_remaining_prefix",
        ),
        (
            "a batch continues after the first failed item",
            "python/tinyray/_serve.py",
            "            if result[0] != _native.RPC_STATUS_SUCCESS:",
            "            if False:",
            "tests/rpc/test_batch.py"
            "::test_first_failure_stops_execution_with_completed_results",
        ),
        (
            "dataclass arguments rebuild a generic MessagePack object graph",
            "python/tinyray/_serve.py",
            "        if raw and _takes_model(fn):",
            "        if False:",
            "tests/rpc/test_models.py"
            "::test_typed_dataclass_rpc_skips_generic_object_graphs",
        ),
        (
            "arbitrary-size integers stop using the reserved extension",
            PY_MSGPACK,
            "        except OverflowError:\n"
            "            encoded = _encoder.encode(_prepare_bigints(value, set()))",
            "        except ():\n"
            "            encoded = _encoder.encode(_prepare_bigints(value, set()))",
            "tests/rpc/test_codecs.py"
            "::test_arbitrary_size_python_integers_round_trip_as_values_and_keys",
        ),
        (
            "ordinary MessagePack encoding always opens BlobRef tracking",
            PY_MSGPACK,
            "    try:\n"
            "        return _fast_encoder.encode(value), ()\n"
            "    except (_BlobScopeRequired, OverflowError):\n"
            "        return _encode_with_blob_scope(value)",
            "    return _encode_with_blob_scope(value)",
            "tests/rpc/test_codecs.py"
            "::test_ordinary_values_skip_blob_tracking_scopes",
        ),
        (
            "ordinary MessagePack decoding always opens BlobRef tracking",
            PY_MSGPACK,
            "    try:\n"
            "        return fast_decoder.decode(raw)\n"
            "    except _BlobScopeRequired:\n"
            "        return _decode_with_scope(decoder, raw)",
            "    return _decode_with_scope(decoder, raw)",
            "tests/rpc/test_codecs.py"
            "::test_ordinary_values_skip_blob_tracking_scopes",
        ),
        (
            "framework dataclasses are silently encoded as standard dataclasses",
            PY_MSGPACK,
            "def dumps(value: Any) -> bytes:\n"
            '    """Encode one application value with native MessagePack semantics."""\n'
            "    _reject_pydantic_values(value)\n"
            "    return _encode_prepared(value)[0]",
            "def dumps(value: Any) -> bytes:\n"
            '    """Encode one application value with native MessagePack semantics."""\n'
            "    return _encode_prepared(value)[0]",
            "tests/rpc/test_codecs.py"
            "::test_framework_model_objects_and_types_are_explicitly_unsupported",
        ),
        (
            "unsupported framework return types execute the remote method",
            PY_RPC,
            "        validate_type(return_type)\n"
            "        return BoundMethod(self._handle, self._name, self._timeout, self._send, return_type)",
            "        return BoundMethod(self._handle, self._name, self._timeout, self._send, return_type)",
            "tests/rpc/test_codecs.py"
            "::test_framework_model_objects_and_types_are_explicitly_unsupported",
        ),
        (
            "the registry beat leaves Nagle enabled",
            RS_MEMBERSHIP_HEARTBEAT,
            "    stream\n"
            "        .set_nodelay(true)\n"
            '        .map_err(|e| format!("the connection came up but TCP_NODELAY failed: {e}"))\n',
            "    stream\n"
            "        .set_nodelay(false)\n"
            '        .map_err(|e| format!("the connection came up but TCP_NODELAY failed: {e}"))\n',
            "cargo:tinyray-membership",
        ),
        (
            "the heartbeat reconnects for every beat",
            RS_MEMBERSHIP_HEARTBEAT,
            "            let sending = post(shared.clone(), &beat, budget, connection.take());",
            "            let sending = post(shared.clone(), &beat, budget, None);",
            "tests/registry/test_persistent_connections.py"
            "::test_idle_heartbeats_reuse_one_clean_connection",
        ),
        (
            "a stale registry reply is accepted on the reusable stream",
            RS_MEMBERSHIP_HEARTBEAT,
            "    if header.request_id != request_id {",
            "    if false {",
            "cargo:tinyray-membership",
        ),
        (
            "member changes leave the scalar filter index stale",
            RS_MEMBERSHIP_CACHE,
            "            self.filter_index.get_mut().unwrap().invalidate();",
            "",
            "cargo:tinyray-membership",
        ),
        (
            "the scalar filter index conflates booleans with integers",
            RS_MEMBERSHIP_CACHE,
            "            serde_json::Value::Bool(value) => Some(Self::Bool(*value)),",
            "            serde_json::Value::Bool(value) => {\n"
            "                Some(Self::I64(if *value { 1 } else { 0 }))\n"
            "            }",
            "cargo:tinyray-membership",
        ),
        (
            "the registry reply body gets a fresh timeout budget",
            RS_MEMBERSHIP_HEARTBEAT,
            "    tokio::time::timeout_at(deadline, read_frame_body(reader, length))",
            "    tokio::time::timeout(budget, read_frame_body(reader, length))",
            "cargo:tinyray-membership",
        ),
        (
            "a silent method socket gets a fresh first-frame deadline",
            RS_SDK_TRANSPORT_SERVER,
            "    tokio::time::timeout_at(deadline, reader.readable()).await",
            "    tokio::time::timeout_at(deadline + SERVER_FRAME_TIMEOUT, reader.readable()).await",
            "cargo:tinyray:transport::server::tests::silent_first_frame_readiness_uses_the_absolute_deadline",
        ),
        (
            "the method frame body starts a fresh timeout budget",
            RS_SDK_TRANSPORT_SERVER,
            "    let bytes = tokio::time::timeout_at(deadline, read_frame_body(reader, length))\n"
            "        .await\n"
            "        .map_err(|_| ServerFrameError::TimedOut)?\n"
            "        .map_err(ServerFrameError::Frame)?;\n"
            "    Ok(AdmittedFrame {",
            "    let bytes = tokio::time::timeout(SERVER_FRAME_TIMEOUT, read_frame_body(reader, length))\n"
            "        .await\n"
            "        .map_err(|_| ServerFrameError::TimedOut)?\n"
            "        .map_err(ServerFrameError::Frame)?;\n"
            "    Ok(AdmittedFrame {",
            "cargo:tinyray:transport::server::tests::prefix_and_body_share_the_original_absolute_deadline",
        ),
        (
            "method connections ignore their per-server admission limit",
            RS_SDK_TRANSPORT_SERVER,
            "    let server = server.clone().try_acquire_owned().ok()?;",
            "    let server = Arc::new(Semaphore::new(1))\n"
            "        .try_acquire_owned()\n"
            "        .ok()?;",
            "cargo:tinyray:transport::server::tests::connection_admission_is_global_and_per_server",
        ),
        (
            "bulk method frames stop charging the per-server byte budget",
            RS_SDK_TRANSPORT,
            "            endpoint\n"
            "                .bulk_bytes\n"
            "                .clone()\n"
            "                .try_acquire_many_owned(permits)\n"
            "                .ok()?,",
            "            endpoint\n"
            "                .bulk_bytes\n"
            "                .clone()\n"
            "                .try_acquire_many_owned(1)\n"
            "                .ok()?,",
            "cargo:tinyray:transport::server::tests::bulk_server_frames_cannot_consume_the_small_control_frame_reserve",
        ),
        (
            "bulk method frames stop charging the process byte budget",
            RS_SDK_TRANSPORT,
            "            global\n"
            "                .bulk_bytes\n"
            "                .clone()\n"
            "                .try_acquire_many_owned(permits)\n"
            "                .ok()?,",
            "            global\n"
            "                .bulk_bytes\n"
            "                .clone()\n"
            "                .try_acquire_many_owned(1)\n"
            "                .ok()?,",
            "cargo:tinyray:transport::server::tests::bulk_server_frame_bytes_are_bounded_globally",
        ),
        (
            "bulk method reservations can consume the small control-frame reserve",
            RS_SDK_TRANSPORT,
            "    let (global_bytes, endpoint_bytes) = if length <= SMALL_FRAME_MAX_BYTES {",
            "    let (global_bytes, endpoint_bytes) = if false {",
            "cargo:tinyray:transport::server::tests::bulk_server_frames_cannot_consume_the_small_control_frame_reserve",
        ),
        (
            "method RPC accepts 256 MiB frames again",
            RS_RPC_PROTO,
            "pub const MAX_RPC_FRAME_BYTES: usize = 32 << 20;",
            "pub const MAX_RPC_FRAME_BYTES: usize = 256 << 20;",
            "cargo:tinyray-proto:method_frames_keep_the_documented_control_plane_cap",
        ),
        (
            "malformed typed method envelopes lose their request ID",
            RS_SDK_TRANSPORT_SERVER,
            "                    .write(RpcReply::error(\n"
            "                        header.request_id,\n"
            "                        RpcStatus::MalformedProtocol,",
            "                    .write(RpcReply::error(\n"
            "                        String::new(),\n"
            "                        RpcStatus::MalformedProtocol,",
            "tests/rpc/test_transport.py"
            "::test_malformed_typed_envelopes_keep_the_minimal_request_id",
        ),
        (
            "composite large-integer map keys lose their hashable shape",
            PY_MSGPACK,
            "            _encoded_item(_prepare_hashable(key, active)),",
            "            _encoded_item(_prepare_bigints(key, active)),",
            "tests/rpc/test_codecs.py"
            "::test_composite_large_integer_keys_preserve_hashable_shapes",
        ),
        (
            "a malformed extension map key escapes as an internal listener failure",
            "python/tinyray/_serve.py",
            "                except (BlobError, TypeError) as exc:\n"
            "                    return _reply(\n"
            "                        _native.RPC_STATUS_CALLER_FAULT,\n"
            '                        error_type="TypeError",\n'
            '                        message=f"{name}(): malformed MessagePack: {exc}",\n'
            "                    )\n",
            "",
            "tests/rpc/test_codecs.py"
            "::test_unhashable_extension_map_key_is_a_correlated_type_error",
        ),
        (
            "a malformed extension batch key escapes as an internal listener failure",
            "python/tinyray/_serve.py",
            "        except (BlobError, TypeError) as exc:\n"
            "            return _reply(\n"
            "                _native.RPC_STATUS_CALLER_FAULT,\n"
            '                error_type="TypeError",\n'
            '                message=f"malformed batch MessagePack: {exc}",\n'
            "            )\n",
            "",
            "tests/rpc/test_codecs.py"
            "::test_unhashable_extension_map_key_is_a_correlated_type_error",
        ),
        (
            "generated request IDs use an overlong caller identity verbatim",
            PY_RPC,
            "    if len(direct) <= _MAX_REQUEST_ID:\n"
            "        return direct",
            "    if True:\n"
            "        return direct",
            "tests/membership/test_identity_and_fencing.py"
            "::test_generated_request_ids_bound_long_identities_without_colliding",
        ),
        (
            "registry typed-envelope errors lose a recovered request ID",
            RS_REGISTRY_SERVER,
            "                send_error(\n"
            "                    &mut writer,\n"
            "                    request_id,\n"
            '                    "malformed_request",\n'
            "                    error.to_string(),\n"
            "                )",
            "                send_error(\n"
            "                    &mut writer,\n"
            "                    0,\n"
            '                    "malformed_request",\n'
            "                    error.to_string(),\n"
            "                )",
            "cargo:tinyray-registry",
        ),
        (
            "registry envelope headers require a payload again",
            RS_WIRE,
            "pub struct RegistryEnvelopeHeader {\n"
            "    pub request_id: u64,\n"
            "    pub operation: String,\n"
            "}",
            "pub struct RegistryEnvelopeHeader {\n"
            "    pub request_id: u64,\n"
            "    pub operation: String,\n"
            '    #[serde(rename = "payload")]\n'
            "    _payload: serde::de::IgnoredAny,\n"
            "}",
            "cargo:tinyray-registry",
        ),
        (
            "registry beat sockets are absent from the fork descriptor tracker",
            RS_MEMBERSHIP_HEARTBEAT,
            "            let fd = stream.as_raw_fd();\n"
            "            shared.registry_fds.register(fd);",
            "",
            "tests/membership/test_fork.py"
            "::test_a_forked_child_closes_inherited_registry_sockets_only",
        ),
        (
            "a forked child forgets the heartbeat runtime before closing its sockets",
            RS_LIB,
            "        self.shared.registry_fds.close_all();\n"
            "        if let Some(rt) = self.rt.lock().unwrap().take() {",
            "        if let Some(rt) = self.rt.lock().unwrap().take() {",
            "tests/membership/test_fork.py"
            "::test_a_forked_child_closes_inherited_registry_sockets_only",
        ),
        (
            "native listeners return to a fixed backlog of 128",
            RS_WIRE,
            "pub const OS_MAX_LISTEN_BACKLOG: i32 = i32::MAX;",
            "pub const OS_MAX_LISTEN_BACKLOG: i32 = 128;",
            "cargo:tinyray-proto",
        ),
        (
            "native roster views survive changed member data",
            RS_MEMBERSHIP_CACHE,
            "            *self.native.get_mut().unwrap() = Default::default();",
            "",
            "cargo:tinyray-membership",
        ),
        (
            "snapshot construction eagerly materializes every Handle",
            PY_INIT,
            "        snapshot._view = view\n"
            "        snapshot._materialized = None\n"
            "        snapshot._handle_cls = handle_cls",
            "        snapshot._view = view\n"
            "        snapshot._materialized = view.materialize(\n"
            "            handle_cls._from_native, immutable=True\n"
            "        )\n"
            "        snapshot._handle_cls = handle_cls",
            "tests/discovery/test_native_views.py"
            "::test_snapshot_and_epoch_materialize_handles_only_when_requested",
        ),
        (
            "native Handles eagerly copy state during construction",
            PY_INIT,
            "        handle._native = member\n"
            "        handle._methods = methods\n"
            "        return handle",
            "        handle._native = member\n"
            "        handle._methods = methods\n"
            "        handle._state = member.materialize_state() or {}\n"
            "        return handle",
            "tests/discovery/test_native_views.py"
            "::test_native_handles_keep_fields_and_state_lazy_while_proxying_methods",
        ),
        (
            "native Handle state is never installed into its direct slot",
            PY_INIT,
            "            state = self._native.materialize_state() or {}\n"
            "            self.state = state\n"
            "            self._state = state\n"
            "            return state",
            "            state = self._native.materialize_state() or {}\n"
            "            self._state = state\n"
            "            return state",
            "tests/discovery/test_native_views.py"
            "::test_snapshot_and_epoch_materialize_handles_only_when_requested",
        ),
        (
            "a roster state batch decodes again for every Handle",
            PY_INIT,
            "            self._states = states\n"
            "            self._native = None",
            "",
            "tests/discovery/test_fast_lookups.py"
            "::test_roster_state_batch_decodes_only_once",
        ),
        (
            "small serialized state batches are never cached",
            RS_MEMBERSHIP_CACHE,
            "        if encoded.len() <= SNAPSHOT_BYTES {\n"
            "            let _ = self.states.set(encoded.clone());\n"
            "        }\n"
            "        encoded",
            "        encoded",
            "cargo:tinyray-membership:tests::serialized_state_batches_are_cached_and_invalidated",
        ),
        (
            "native count waits accept fewer members than requested",
            RS_MEMBERSHIP_WAIT,
            "                let ready = cached.is_some() && (matched as i128) >= *target;",
            "                let ready = true;",
            "tests/discovery/test_native_views.py"
            "::test_built_in_waits_bypass_python_predicate_and_roster_loops",
        ),
        (
            "native snapshots lose the cache revision they froze at",
            RS_MEMBERSHIP_CACHE,
            "    pub fn frozen(&self, require_ready: bool) -> FrozenPool {\n"
            "        FrozenPool {\n"
            "            members: self.native(require_ready),\n"
            "            roster: self.roster,\n"
            "            version: self.version,",
            "    pub fn frozen(&self, require_ready: bool) -> FrozenPool {\n"
            "        FrozenPool {\n"
            "            members: self.native(require_ready),\n"
            "            roster: self.roster,\n"
            "            version: 0,",
            "tests/discovery/test_native_views.py"
            "::test_native_views_remain_frozen_and_state_copies_stay_isolated",
        ),
        (
            "sync RPC eagerly extracts Python bytes into a Vec",
            RS_RPC,
            "    // PyBackedBytes avoids PyO3's eager Vec extraction copy at the FFI boundary.\n"
            "    payload: PyBackedBytes,",
            "    // PyBackedBytes avoids PyO3's eager Vec extraction copy at the FFI boundary.\n"
            "    payload: Vec<u8>,",
            "tests/project/test_bench.py"
            "::test_native_bytes_boundaries_keep_zero_copy_pybacked_extraction",
        ),
        (
            "async RPC eagerly extracts Python bytes into a Vec",
            RS_RPC,
            "    // Keep the borrowed Python buffer until the one owned async request copy.\n"
            "    payload: PyBackedBytes,",
            "    // Keep the borrowed Python buffer until the one owned async request copy.\n"
            "    payload: Vec<u8>,",
            "tests/project/test_bench.py"
            "::test_native_bytes_boundaries_keep_zero_copy_pybacked_extraction",
        ),
        (
            "RPC replies eagerly extract Python bytes into a Vec",
            RS_RPC,
            "                            u8,\n"
            "                            PyBackedBytes,\n"
            "                            String,",
            "                            u8,\n"
            "                            Vec<u8>,\n"
            "                            String,",
            "tests/project/test_bench.py"
            "::test_native_bytes_boundaries_keep_zero_copy_pybacked_extraction",
        ),
        (
            "the Python RPC adapter creates a second Tokio client runtime",
            RS_RPC,
            "            tinyray::Client::from_current().map_err(|error| error.to_string())?",
            "            tinyray::Client::new(tinyray::ClientConfig::default())\n"
            "                .map_err(|error| error.to_string())?",
            "tests/project/test_bench.py"
            "::test_native_rpc_binding_is_a_thin_public_transport_adapter",
        ),
        (
            "Python async cancellation aborts without synchronously removing its waiter",
            RS_RPC,
            "    fn stop(&self) {\n"
            "        self.cancellation.cancel();\n"
            "        if let Some(task) = self.task.lock().unwrap().take() {",
            "    fn stop(&self) {\n"
            "        if let Some(task) = self.task.lock().unwrap().take() {",
            "cargo:tinyray-client:rpc::tests::rpc_ticket_cancels_before_aborting_the_task",
        ),
        (
            "the public Rust service SDK acquires a Python runtime dependency",
            RS_SDK_MANIFEST,
            "async-trait.workspace = true\nsha2.workspace = true",
            'async-trait.workspace = true\npyo3 = "0.22"\nsha2.workspace = true',
            "tests/project/test_api.py"
            "::test_public_rust_sdk_has_no_python_runtime_dependency",
        ),
        (
            "Rust service calls lose their request context",
            RS_SDK_TRANSPORT_SERVER,
            "                request_id: Arc::from(request_id.clone()),",
            '                request_id: Arc::from(""),',
            "cargo:tinyray",
        ),
        (
            "Rust service target fencing is skipped",
            RS_SDK_TRANSPORT_SERVER,
            "        if request.target != state.identity {",
            "        if false {",
            "cargo:tinyray",
        ),
        (
            "Rust service task completion cancels a partially read frame",
            RS_SDK_TRANSPORT_SERVER,
            "        let frame = tokio::select! {\n"
            "            _ = state.shutdown.notified() => break,\n"
            "            _ = connection.shutdown.notified() => break,\n"
            "            frame = read_server_frame_after_first(\n"
            "                &mut reader,\n"
            "                first[0],\n"
            "                &state.global_frames,\n"
            "                &state.frames,\n"
            "                deadline,\n"
            "            ) => frame,\n"
            "        };",
            "        let frame = tokio::select! {\n"
            "            _ = state.shutdown.notified() => break,\n"
            "            _ = connection.shutdown.notified() => break,\n"
            "            _ = requests.join_next(), if !requests.is_empty() => continue,\n"
            "            frame = read_server_frame_after_first(\n"
            "                &mut reader,\n"
            "                first[0],\n"
            "                &state.global_frames,\n"
            "                &state.frames,\n"
            "                deadline,\n"
            "            ) => frame,\n"
            "        };",
            "cargo:tinyray:blob_capable_rust_transport_preserves_128_concurrent_raw_calls",
        ),
        (
            "Rust batch items are dispatched out of order",
            RS_SDK_SERVICE,
            "        for (index, item) in envelope.calls.into_iter().enumerate() {",
            "        for (index, item) in envelope.calls.into_iter().rev().enumerate() {",
            "cargo:tinyray",
        ),
        (
            "a delivered Rust call timeout is classified as not-delivered",
            RS_SDK_TRANSPORT_CLIENT,
            "        AbandonedCall::Written | AbandonedCall::Completed => {\n"
            "            CallError::OutcomeUnknown(format!(\"{endpoint} did not answer before the call timeout\"))\n"
            "        }",
            "        AbandonedCall::Written | AbandonedCall::Completed => {\n"
            "            CallError::NotDelivered(format!(\"{endpoint} did not answer before the call timeout\"))\n"
            "        }",
            "cargo:tinyray",
        ),
        (
            "a reply that becomes ready after the absolute client deadline is accepted",
            RS_SDK_TRANSPORT_CLIENT,
            "    let result = match tokio::time::timeout_at(deadline, &mut receive).await {",
            "    let result = match tokio::time::timeout_at(\n"
            "        deadline + Duration::from_secs(60),\n"
            "        &mut receive,\n"
            "    )\n"
            "    .await\n"
            "    {",
            "cargo:tinyray:late_abandoned_blob_replies_are_acked_and_connection_stays_reusable",
        ),
        (
            "BlobRef descriptors skip the Linux boot identity check",
            RS_BLOB,
            "        if self.boot != boot_fingerprint()? {",
            "        if false {",
            "tests/rpc/test_blobref.py"
            "::test_descriptor_rejects_boot_inode_fd_size_and_reuse",
        ),
        (
            "BlobRef descriptors skip the inode reuse check",
            RS_BLOB,
            "            if metadata.ino() != self.inode {",
            "            if false {",
            "tests/rpc/test_blobref.py"
            "::test_descriptor_rejects_boot_inode_fd_size_and_reuse",
        ),
        (
            "BlobRef descriptors skip the mapping size bound",
            RS_BLOB,
            "        check_size(self.size, maximum)?;",
            "",
            "tests/rpc/test_blobref.py"
            "::test_descriptor_rejects_boot_inode_fd_size_and_reuse",
        ),
        (
            "BlobRef receivers accept an unsealed descriptor",
            RS_BLOB,
            "            if seals < 0 || seals & REQUIRED_SEALS != REQUIRED_SEALS {",
            "            if false {",
            "tests/rpc/test_blobref.py"
            "::test_descriptor_rejects_boot_inode_fd_size_and_reuse",
        ),
        (
            "BlobRef mappings request write access",
            RS_BLOB,
            "                libc::PROT_READ,",
            "                libc::PROT_READ | libc::PROT_WRITE,",
            "tests/rpc/test_blobref.py"
            "::test_blobref_is_read_only_zero_copy_and_explicit_bytes_copy",
        ),
        (
            "BlobRef MessagePack decoding eagerly copies payload bytes",
            PY_MSGPACK,
            "        blob = BlobRef.from_descriptor(raw)\n"
            "        if state is not None:\n"
            '            state["cache"][raw] = blob\n'
            "        return blob",
            "        blob = BlobRef.from_descriptor(raw)\n"
            "        if state is not None:\n"
            '            state["cache"][raw] = blob\n'
            "        return bytes(blob)",
            "tests/rpc/test_blobref.py"
            "::test_blobref_messagepack_is_explicit_and_regular_bytes_are_unchanged",
        ),
        (
            "outgoing RPC drops temporary BlobRef lifetime retention",
            PY_RPC,
            "    body, keepalive = dumps_with_blob_refs(payload)",
            "    body, _ = dumps_with_blob_refs(payload)\n"
            "    keepalive = ()",
            "tests/rpc/test_blobref.py"
            "::test_async_cancellation_retains_temporary_blob_until_delayed_decode",
        ),
        (
            "Python BlobRef decoding skips the per-message count limit",
            PY_MSGPACK,
            '            if state["refs"] > _MAX_BLOB_REFS_PER_MESSAGE:',
            "            if False:",
            "tests/rpc/test_blobref.py"
            "::test_decoder_deduplicates_descriptors_and_bounds_count_and_bytes",
        ),
        (
            "Python BlobRef decoding skips the aggregate mapped-byte limit",
            PY_MSGPACK,
            "                if mapped > _MAX_BLOB_MAPPED_BYTES_PER_MESSAGE:",
            "                if False:",
            "tests/rpc/test_blobref.py"
            "::test_decoder_deduplicates_descriptors_and_bounds_count_and_bytes",
        ),
        (
            "Python BlobRef decoding maps duplicate descriptors repeatedly",
            PY_MSGPACK,
            '            cached = state["cache"].get(raw)\n'
            "            if cached is not None:\n"
            "                return cached._clone()",
            "            cached = None\n"
            "            if cached is not None:\n"
            "                return cached._clone()",
            "tests/rpc/test_blobref.py"
            "::test_decoder_deduplicates_descriptors_and_bounds_count_and_bytes",
        ),
        (
            "Rust BlobRef decoding skips the per-message count limit",
            RS_BLOB,
            "        if state.refs > state.limits.max_refs {",
            "        if false {",
            "cargo:tinyray",
        ),
        (
            "Rust BlobRef decoding skips the aggregate mapped-byte limit",
            RS_BLOB,
            "        if state.mapped_bytes > state.limits.max_mapped_bytes {",
            "        if false {",
            "cargo:tinyray",
        ),
        (
            "Rust BlobRef decoding maps duplicate descriptors repeatedly",
            RS_BLOB,
            "            if let Some(inner) = resources.cache.get(&key).and_then(Weak::upgrade) {\n"
            "                return Ok(BlobRef {\n"
            "                    inner: Some(inner),\n"
            "                    decoded_handle,\n"
            "                });\n"
            "            }",
            "            if let Some(inner) = None::<Arc<BlobInner>> {\n"
            "                return Ok(BlobRef {\n"
            "                    inner: Some(inner),\n"
            "                    decoded_handle,\n"
            "                });\n"
            "            }",
            "cargo:tinyray",
        ),
        (
            "public BlobRef descriptor opens skip the process handle bound",
            RS_BLOB,
            "    if resources.handles >= MAX_DECODED_BLOB_HANDLES {",
            "    if false {",
            "tests/rpc/test_blobref.py"
            "::test_public_from_descriptor_is_process_bounded",
        ),
        (
            "Python RPC abandons delivered BlobRef owners",
            RS_SDK_TRANSPORT_CLIENT,
            "            state.abandoned.insert(request_id.to_owned(), blob_owners);",
            "            state.abandoned.insert(\n"
            "                request_id.to_owned(),\n"
            "                RpcBlobOwners::track(Vec::new()).unwrap(),\n"
            "            );",
            "tests/rpc/test_blobref.py"
            "::test_async_cancellation_retains_temporary_blob_until_delayed_decode",
        ),
        (
            "Rust RPC abandons delivered BlobRef owners",
            RS_SDK_TRANSPORT_CLIENT,
            "            state.abandoned.insert(request_id.to_owned(), blob_owners);",
            "            state.abandoned.insert(\n"
            "                request_id.to_owned(),\n"
            "                RpcBlobOwners::track(Vec::new()).unwrap(),\n"
            "            );",
            "cargo:tinyray",
        ),
        (
            "forwarded BlobRef descriptors keep the original owner fd",
            RS_BLOB,
            "        #[cfg(target_os = \"linux\")]\n"
            "        {\n"
            "            let mut descriptor = inner.descriptor.clone();\n"
            "            descriptor.owner_pid = std::process::id();\n"
            "            descriptor.fd = inner.file.as_raw_fd();\n"
            "            Ok(descriptor)\n"
            "        }",
            "        #[cfg(target_os = \"linux\")]\n"
            "        {\n"
            "            Ok(inner.descriptor.clone())\n"
            "        }",
            "tests/rpc/test_blobref.py"
            "::test_exported_child_blobref_serializes_child_owned_descriptor",
        ),
        (
            "BlobRef tokens use deterministic zero bytes instead of getrandom",
            RS_BLOB,
            "            fill_token(&mut token)?;",
            "            token.fill(0);",
            "cargo:tinyray",
        ),
        (
            "BlobRef descriptor validation skips the header token",
            RS_BLOB,
            "        && header[8..24] == descriptor.token",
            "        && true",
            "tests/rpc/test_blobref.py"
            "::test_descriptor_rejects_boot_inode_fd_size_and_reuse",
        ),
        (
            "Python serialization misses nested BlobRef owners",
            PY_MSGPACK,
            '            state["owners"].append(value)',
            "            pass",
            "tests/rpc/test_blobref.py"
            "::test_async_cancellation_retains_temporary_blob_until_delayed_decode",
        ),
        (
            "native BlobRef construction skips fork registration",
            RS_CLIENT_BLOB,
            "        states.push(Arc::downgrade(&state));",
            "",
            "tests/rpc/test_blobref.py"
            "::test_public_blobref_constructors_are_registered_for_fork_cleanup",
        ),
        (
            "service replies drop BlobRef owners before acknowledgement",
            RS_SDK_TRANSPORT_SERVER,
            "            connection_for_request.admit_blob_response(&mut reply, blob_owners);",
            "            connection_for_request.admit_blob_response(&mut reply, Vec::new());",
            "tests/rpc/test_blobref.py"
            "::test_blob_response_ack_releases_temporary_service_owners",
        ),
        (
            "BlobRef replies skip the per-reply owner-count limit",
            RS_SDK_TRANSPORT_SERVER,
            "        if unique.len() > self.max_blob_refs_per_reply {",
            "        if false {",
            "cargo:tinyray",
        ),
        (
            "BlobRef replies skip the per-reply byte limit",
            RS_SDK_TRANSPORT_SERVER,
            "        if bytes > self.max_blob_bytes_per_reply {",
            "        if false {",
            "cargo:tinyray",
        ),
        (
            "outstanding BlobRef replies skip count admission",
            RS_SDK_TRANSPORT,
            "        if state.refs.checked_add(refs)? > self.max_refs",
            "        if false",
            "cargo:tinyray",
        ),
        (
            "outstanding BlobRef replies skip byte admission",
            RS_SDK_TRANSPORT,
            "            || state.bytes.checked_add(bytes)? > self.max_bytes",
            "            || false",
            "cargo:tinyray",
        ),
        (
            "Rust raw replies acknowledge BlobRefs before caller decoding",
            RS_SDK_TRANSPORT_CLIENT,
            "            _ack: self._ack,",
            "            _ack: None,",
            "cargo:tinyray",
        ),
        (
            "fork reset leaves native-only RPC BlobRef owners alive",
            RS_RPC,
            "    if let Some(inherited) = slot.take() {\n"
            "        inherited.abandon_after_fork();\n"
            "        std::mem::forget(inherited);\n"
            "    }",
            "    if let Some(inherited) = slot.take() {\n"
            "        std::mem::forget(inherited);\n"
            "    }",
            "tests/rpc/test_blobref.py"
            "::test_fork_clears_native_pending_blob_owners_before_runtime_forget",
        ),
        (
            "forked BlobRef serialization keeps the parent pid",
            RS_BLOB,
            "        #[cfg(target_os = \"linux\")]\n"
            "        {\n"
            "            let mut descriptor = inner.descriptor.clone();\n"
            "            descriptor.owner_pid = std::process::id();\n"
            "            descriptor.fd = inner.file.as_raw_fd();\n"
            "            Ok(descriptor)\n"
            "        }",
            "        #[cfg(target_os = \"linux\")]\n"
            "        {\n"
            "            Ok(inner.descriptor.clone())\n"
            "        }",
            "cargo:tinyray",
        ),
        (
            "Rust MessagePack decoding accepts trailing bytes",
            RS_BLOB,
            "    if decoder.get_ref().position() != bytes.len() as u64 {",
            "    if false {",
            "cargo:tinyray",
        ),
        (
            "BlobRef file ingestion mutates the caller cursor",
            RS_BLOB,
            "    file.read_at(data, offset)",
            "    let mut shared = file.try_clone()?;\n"
            "    shared.seek(SeekFrom::Start(offset))?;\n"
            "    std::io::Read::read(&mut shared, data)",
            "cargo:tinyray",
        ),
        (
            "BlobRef file ingestion misses truncation",
            RS_BLOB,
            "        if count == 0 {\n"
            "            return Err(BlobError::Invalid(\n"
            '                "source file changed while creating BlobRef".into(),\n'
            "            ));\n"
            "        }",
            "        if count == 0 {\n"
            "            break;\n"
            "        }",
            "cargo:tinyray",
        ),
        (
            "BlobRef response acknowledgements do not release owners",
            RS_SDK_TRANSPORT_SERVER,
            "        self.blob_responses.lock().unwrap().remove(request_id);",
            "        let _ = request_id;",
            "tests/rpc/test_blobref.py"
            "::test_blob_response_ack_releases_temporary_service_owners",
        ),
        (
            "Rust client idle eviction ignores guarded BlobRef replies",
            RS_SDK_TRANSPORT_CLIENT,
            "            if reply.blob_refs {\n"
            "                state.guarded_replies.insert(reply.request_id.clone());\n"
            "            }",
            "",
            "cargo:tinyray:raw_reply_guard_holds_blob_owner_until_decode_or_drop",
        ),
        (
            "Python client idle eviction ignores guarded BlobRef replies",
            RS_SDK_TRANSPORT_CLIENT,
            "            if reply.blob_refs {\n"
            "                state.guarded_replies.insert(reply.request_id.clone());\n"
            "            }",
            "",
            "tests/rpc/test_blobref.py"
            "::test_native_raw_reply_guard_survives_client_and_server_idle_deadlines",
        ),
        (
            "server idle expiry ignores unacknowledged BlobRef replies",
            RS_SDK_TRANSPORT_SERVER,
            "        responses\n"
            "            .values()\n"
            "            .map(|lease| lease.expires_at)\n"
            "            .min()\n"
            "            .map(TokioInstant::from_std)\n"
            "            .unwrap_or_else(|| TokioInstant::now() + SERVER_FRAME_TIMEOUT)",
            "        TokioInstant::now() + SERVER_FRAME_TIMEOUT",
            "cargo:tinyray",
        ),
        (
            "Python partial batch missing-method drops completed BlobRef owners",
            "python/tinyray/_serve.py",
            '                    message=f"no method {name!r}",\n'
            "                    batch_index=index,\n"
            "                    completed=index,\n"
            "                    blob_owners=tuple(blob_owners),",
            '                    message=f"no method {name!r}",\n'
            "                    batch_index=index,\n"
            "                    completed=index,",
            "tests/rpc/test_blobref.py"
            "::test_partial_batch_missing_method_keeps_completed_blob",
        ),
        (
            "Python partial batch fencing drops completed BlobRef owners",
            "python/tinyray/_serve.py",
            '                    message=f"{self.identity} is held by a later tenure",\n'
            "                    batch_index=index,\n"
            "                    completed=index,\n"
            "                    blob_owners=tuple(blob_owners),",
            '                    message=f"{self.identity} is held by a later tenure",\n'
            "                    batch_index=index,\n"
            "                    completed=index,",
            "tests/rpc/test_blobref.py"
            "::test_partial_batch_fencing_keeps_completed_blob",
        ),
        (
            "Rust non-success conversion drops the BlobRef ACK guard",
            RS_SDK_TRANSPORT_CLIENT,
            "        _blob_ack: ack,",
            "        _blob_ack: None,",
            "cargo:tinyray",
        ),
        (
            "Python late abandoned BlobRef replies omit their ACK",
            RS_SDK_TRANSPORT_CLIENT,
            "            if reply.blob_refs {\n"
            "                self.queue_blob_ack(&reply.request_id);\n"
            "            }\n"
            "            if close {",
            "            if close {",
            "tests/rpc/test_blobref.py"
            "::test_late_blob_replies_are_acked_after_timeout_and_async_cancellation",
        ),
        (
            "Rust late abandoned BlobRef replies omit their ACK",
            RS_SDK_TRANSPORT_CLIENT,
            "            if reply.blob_refs {\n"
            "                self.queue_blob_ack(&reply.request_id);\n"
            "            }\n"
            "            if close {",
            "            if close {",
            "cargo:tinyray:late_abandoned_blob_replies_are_acked_and_connection_stays_reusable",
        ),
        (
            "Python pending connects are not registered before await",
            RS_SDK_TRANSPORT_CLIENT,
            "        client.fds.register(fd);\n"
            "        Self {\n"
            "            socket: Some(socket),\n"
            "            fd,\n"
            "            pid: client.pid,\n"
            "            client: Arc::downgrade(client),\n"
            "            transferred: false,\n"
            "        }",
            "        Self {\n"
            "            socket: Some(socket),\n"
            "            fd,\n"
            "            pid: client.pid,\n"
            "            client: Arc::downgrade(client),\n"
            "            transferred: false,\n"
            "        }",
            "tests/membership/test_fork.py"
            "::test_fork_closes_a_connect_in_progress_rpc_socket_and_parent_continues",
        ),
        (
            "Rust pending connects are not registered before await",
            RS_SDK_TRANSPORT_CLIENT,
            "        client.fds.register(fd);\n"
            "        Self {\n"
            "            socket: Some(socket),\n"
            "            fd,\n"
            "            pid: client.pid,\n"
            "            client: Arc::downgrade(client),\n"
            "            transferred: false,\n"
            "        }",
            "        Self {\n"
            "            socket: Some(socket),\n"
            "            fd,\n"
            "            pid: client.pid,\n"
            "            client: Arc::downgrade(client),\n"
            "            transferred: false,\n"
            "        }",
            "cargo:tinyray:fork_closes_a_connect_in_progress_rust_client_socket",
        ),
        (
            "Rust service readiness wait consumes the first frame byte",
            RS_SDK_TRANSPORT_SERVER,
            "async fn wait_server_readable(\n"
            "    reader: &mut OwnedReadHalf,\n"
            "    deadline: TokioInstant,\n"
            ") -> Result<std::io::Result<()>, tokio::time::error::Elapsed> {\n"
            "    tokio::time::timeout_at(deadline, reader.readable()).await\n"
            "}",
            "async fn wait_server_readable(\n"
            "    reader: &mut OwnedReadHalf,\n"
            "    deadline: TokioInstant,\n"
            ") -> Result<std::io::Result<()>, tokio::time::error::Elapsed> {\n"
            "    tokio::time::timeout_at(deadline, async {\n"
            "        let mut consumed = [0u8; 1];\n"
            "        reader.read(&mut consumed).await.map(|_| ())\n"
            "    })\n"
            "    .await\n"
            "}",
            "cargo:tinyray:ordinary_raw_128_way_multiplexing_keeps_frame_boundaries",
        ),
        (
            "Rust discovery snapshots ignore their filter",
            RS_SDK_DISCOVERY,
            "            .map(|pool| Snapshot::new(self.name.clone(), pool.filtered(&filter, require_ready))))",
            "            .map(|pool| Snapshot::new(self.name.clone(), pool.frozen(require_ready))))",
            "cargo:tinyray:public_discovery_views_filter_freeze_wait_and_track_replacements",
        ),
        (
            "Rust discovery count ignores its filter",
            RS_SDK_DISCOVERY,
            "            .map(|pool| pool.count(&filter, require_ready))",
            "            .map(|pool| pool.ids(require_ready).len())",
            "cargo:tinyray:public_discovery_views_filter_freeze_wait_and_track_replacements",
        ),
        (
            "Rust async discovery waits are never notified",
            RS_MEMBERSHIP_SHARED,
            "        self.revision_notify.notify_waiters();",
            "",
            "cargo:tinyray:public_discovery_views_filter_freeze_wait_and_track_replacements",
        ),
        (
            "Rust discovery epochs never invalidate",
            RS_SDK_DISCOVERY,
            "    pub fn valid(&self) -> bool {\n"
            "        self.shared\n"
            "            .epoch_valid(self.snapshot.pool(), self.snapshot.roster())\n"
            "    }",
            "    pub fn valid(&self) -> bool {\n"
            "        true\n"
            "    }",
            "cargo:tinyray:public_discovery_views_filter_freeze_wait_and_track_replacements",
        ),
        (
            "Rust server copies the decoded RPC payload twice",
            RS_SDK_TRANSPORT_SERVER,
            "        payload: Arc::from(request.payload),",
            "        payload: Arc::from(request.payload.to_vec()),",
            "tests/project/test_bench.py"
            "::test_rust_server_borrows_rpc_payload_before_its_single_owned_copy",
        ),
        (
            "Rust MemberBuilder creates a second client runtime",
            RS_SDK_MEMBER,
            "        let client = rpc_runtime.client();",
            "        let client = Client::new(crate::ClientConfig {\n"
            "            worker_threads: self.rpc_worker_threads,\n"
            "        })?;",
            "cargo:tinyray:member_builder_shares_one_rpc_worker_pool",
        ),
        (
            "Rust MemberBuilder creates a second server runtime",
            RS_SDK_MEMBER,
            "            let server = rpc_runtime.start_server(config, owned)?;",
            "            let server = Server::start(config, owned)?;",
            "cargo:tinyray:member_builder_shares_one_rpc_worker_pool",
        ),
        (
            "membership owns the fork-safe FD utility again",
            RS_MEMBERSHIP_MANIFEST,
            'tinyray-core = { path = "../tinyray-core" }\n',
            "",
            "tests/project/test_api.py"
            "::test_rust_workspace_keeps_core_membership_and_sdk_layers_separate",
        ),
    ]
)
# fmt: on


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


def check_selectors() -> list[str]:
    """A named test that no longer exists reads as CAUGHT, for good.

    `caught = returncode != 0`, and pytest answers 4 for a selector that picks
    nothing -- the same 4 it gives a module the mutant broke on import, so the
    exit code cannot tell those apart afterwards. Measured: a misspelt name,
    a missing file, a syntax error and an import-time raise all come back 4,
    while a test that genuinely fails comes back 1.

    So the check has to happen here, before anything is mutated, where the
    question is simply whether the name exists. One collect of the whole suite
    answers it for every entry at once: 442 tests in 0.15s.
    """
    named = {t for *_, t in MUTANTS if not t.startswith("cargo:")}
    if not named:
        return []
    out = subprocess.run(
        [
            str(PY),
            "-m",
            "pytest",
            "tests/",
            "--collect-only",
            "-q",
            "-p",
            "no:randomly",
            "-o",
            "addopts=",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    ids = {line.strip() for line in out.stdout.splitlines() if "::" in line}
    if not ids:
        return ["could not collect the suite, so no selector could be checked"]
    missing = []
    for label, *_, test in MUTANTS:
        if test.startswith("cargo:"):
            continue
        # Three shapes are all valid: a whole file, one test, or one test whose
        # cases collect as `file::name[case]`.
        if not (
            test in ids
            or any(i.startswith(test + "[") for i in ids)
            or any(i.startswith(test + "::") for i in ids)
        ):
            missing.append(f"{label}: no test named {test}")
    return missing + check_cargo_packages()


def check_cargo_packages() -> list[str]:
    """The same trap on the Rust side: `cargo test -p nosuch-package` exits
    101, which the run reads as CAUGHT. Measured against the real one, which
    exits 0 with five tests passing. Renaming a crate would leave every entry
    aimed at it green for ever.

    `cargo metadata --no-deps` answers in 0.021s, so this is free.
    """
    wanted = {t.split(":", 2)[1] for *_, t in MUTANTS if t.startswith("cargo:")}
    if not wanted:
        return []
    out = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if out.returncode:
        return ["cargo metadata failed, so no crate name could be checked"]
    have = {p["name"] for p in json.loads(out.stdout)["packages"]}
    return [f"no crate named {w}" for w in sorted(wanted - have)]


def main() -> int:
    if "--count" in sys.argv:
        print(len(MUTANTS))
        return 0
    ambiguous = check_anchors() + check_selectors()
    for line in ambiguous:
        print(f"BROKEN  {line}")
    if ambiguous:
        return 1
    bad = []
    for label, rel, find, repl, test in MUTANTS:
        path = ROOT / rel
        original = path.read_text()
        # No "anchor not found" branch here: check_anchors() flags anything
        # other than exactly one match, zero included, and main() has already
        # returned by then. Restores are exact -- the finally below writes back
        # the text read at the top of this iteration -- so no earlier entry can
        # take an anchor away from a later one. The branch that used to stand
        # here could not fire.
        path.write_text(original.replace(find, repl, 1))
        # Two mutants that shorten the same file by the same number of bytes,
        # written inside one second, are indistinguishable to the bytecode
        # cache: it keys on (mtime seconds, size), so the second one runs the
        # first one's code. Measured -- with the mutant on disk, a plain
        # `import tinyray` still handed back the unmutated class, and the run
        # said MISSED for a test that fails in 0.2s on its own. The reverse is
        # the dangerous one: a mutant called CAUGHT because some *other*
        # mutant's bytecode broke the test would leave a toothless test looking
        # covered. Not worth reasoning about the invalidation rules; just make
        # sure there is nothing to load.
        #
        # It cleared only python/tinyray, while the list also mutates
        # examples/agent_pool/pool.py and several files under tests/, each with
        # a __pycache__ of its own. Measured on pool.py: write one mutant, run
        # it, then inside the same second write a second mutant of exactly the
        # same length -- Python ran the *first* one's bytecode, and the run
        # would have reported on a mutant that was not on disk. Clearing next
        # to whatever is being mutated costs nothing and needs no case analysis
        # about which files import which.
        for stale in (path.parent / "__pycache__").glob("*.pyc"):
            stale.unlink()
        try:
            if rel.endswith(".rs") and not build():
                # A mutant that will not compile is caught too: the compiler
                # is the thing that noticed.
                print(f"CAUGHT  {label}  (did not compile)")
                continue
            if test.startswith("cargo:"):
                # Some things are only visible from the Rust side. A beat's
                # deadline is one: every registry the Python suite talks to is
                # on loopback, so a deadline that stopped following the
                # interval would cost nothing there and everything on a real
                # link.
                cargo_target = test.split(":", 2)
                command = ["cargo", "test", "-q", "-p", cargo_target[1]]
                if len(cargo_target) == 3:
                    command.extend([cargo_target[2], "--", "--exact", "--test-threads=1"])
                r = subprocess.run(
                    command,
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
    # Named at the end, not only where they happened. A run is long enough that
    # it gets watched through `| tail`, and a count on its own is a dead end:
    # 117 of 118 once, 118 of 118 on the next run, and no way back to which one
    # flaked. A flaky entry is as useless as a toothless one, so it has to be
    # possible to say which.
    for label in bad:
        print(f"  MISSED  {label}")
    print(f"\n{len(MUTANTS) - len(bad)} of {len(MUTANTS)} caught")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
