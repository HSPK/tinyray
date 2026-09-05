"""Opt-in batches share a transport request, not a transaction or a retry policy."""

from __future__ import annotations

import asyncio
import http.client
import inspect
import json
import socket
import threading
import time
import warnings

import httpx
import pytest
import tinyray
from tinyray import _rpc, _serve
from tinyray._errors import BatchError, Fenced, NotDelivered, OutcomeUnknown, RemoteError
from tinyray._rpc import MAX_BATCH, Call, abatch, batch
from tinyray._serve import CallContext, MethodServer


class Service:
    def __init__(self):
        self.events = []
        self.contexts = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.owned = True
        self.shared_value = {"count": 0}

    def record(self, value: int, ctx: CallContext):
        self.events.append(value)
        self.contexts.append((ctx.identity, ctx.request_id))
        return value

    def echo(self, value):
        return value

    def signature(self, first: int, /, ctx: CallContext, *rest: int, flag: int, **extras: int):
        self.events.append("signature")
        return [first, list(rest), flag, extras, ctx.identity, ctx.request_id]

    async def async_record(self, value: int, ctx: CallContext):
        await asyncio.sleep(0)
        return self.record(value, ctx)

    def boom(self):
        self.events.append("boom")
        raise ValueError("expected business failure")

    def unserializable(self):
        self.events.append("unserializable")
        return object()

    def cycle(self):
        self.events.append("cycle")
        value = {}
        value["self"] = value
        return value

    def shared(self):
        self.shared_value["count"] += 1
        return self.shared_value

    def hold(self, value: int):
        self.events.append(value)
        self.entered.set()
        assert self.release.wait(10), "the test did not release its blocked method"
        return value

    def takeover(self):
        self.events.append("takeover")
        self.owned = False
        return "old tenure finished"

    def _private(self):
        self.events.append("private")


@pytest.fixture
def served(monkeypatch):
    service = Service()
    server = MethodServer(service, "batch/0#1", host="127.0.0.1", max_concurrency=1)
    server.still_ours = lambda: service.owned
    handle = tinyray.Handle(
        "batch",
        {"id": 0, "slot": 0, "incarnation": 1, "url": server.url("127.0.0.1"), "ready": True},
        server.methods,
    )
    monkeypatch.setattr(_rpc, "_identity", "caller/7#42")
    try:
        yield service, server, handle
    finally:
        service.release.set()
        server.close()


@pytest.fixture(params=[batch, abatch], ids=["sync", "async"])
def send_batch(request):
    def send(*args, **kwargs):
        result = request.param(*args, **kwargs)
        return asyncio.run(result) if inspect.isawaitable(result) else result

    return send


def _settled(server):
    until = time.monotonic() + 5
    while server.counters.snapshot()["in_flight"]:
        assert time.monotonic() < until, "a batch leaked its admission slot"
        time.sleep(0.005)
    return server.counters.snapshot()


def _item(method="record", args=None, kwargs=None):
    return {"method": method, "args": [1] if args is None else args, "kwargs": kwargs or {}}


def _raw(server, body, *, extra=b"", length=None, shutdown=False):
    length = str(len(body)).encode() if length is None else length
    head = (
        b"POST /_batch HTTP/1.1\r\nHost: localhost\r\nContent-Length: "
        + length
        + b"\r\n"
        + extra
        + b"\r\n"
    )
    with socket.create_connection(("127.0.0.1", server.port), timeout=5) as connection:
        connection.sendall(head + body)
        if shutdown:
            connection.shutdown(socket.SHUT_WR)
        response = http.client.HTTPResponse(connection)
        response.begin()
        return response.status, response.read()


def test_public_exports_exist():
    assert tinyray.Call is Call
    assert tinyray.batch is batch
    assert tinyray.abatch is abatch
    assert tinyray.BatchError is BatchError


def test_ordered_results_and_one_admitted_request(served, send_batch):
    service, server, handle = served
    assert send_batch(handle, [Call("record", (1,)), Call("record", kwargs={"value": 2})]) == [1, 2]
    assert service.events == [1, 2]
    stats = _settled(server)
    assert stats["calls"] == 1
    assert stats["failed"] == 0
    assert stats["peak_in_flight"] == 1
    assert handle.echo.timeout(2).returns(tuple[int, int])([3, 4]) == (3, 4)


@pytest.mark.parametrize(
    "call,cause,events",
    [
        (Call("boom"), RemoteError, [1, "boom"]),
        (Call("record"), TypeError, [1]),
        (Call("record", ("not an int",)), TypeError, [1]),
        (Call("record", (2,), {"value": 3}), TypeError, [1]),
        (Call("does_not_exist"), AttributeError, [1]),
        (Call("unserializable"), RemoteError, [1, "unserializable"]),
        (Call("cycle"), RemoteError, [1, "cycle"]),
    ],
)
def test_first_failure_stops_execution_with_completed_results(
    served, send_batch, call, cause, events
):
    service, server, handle = served
    with pytest.raises(BatchError) as caught:
        send_batch(handle, [Call("record", (1,)), call, Call("record", (99,))])
    error = caught.value
    assert error.failed_index == 1
    assert error.completed_results == [1]
    assert isinstance(error.cause, cause)
    assert error.__cause__ is error.cause
    assert not isinstance(error, tinyray.Unreachable)
    assert service.events == events
    if call.method == "boom":
        assert error.cause.type == "ValueError"
        assert "expected business failure" in error.cause.traceback
    if call.method in ("unserializable", "cycle"):
        assert "cannot be sent as JSON" in str(error.cause)
    stats = _settled(server)
    assert stats["calls"] == stats["failed"] == 1
    assert handle.echo("still usable") == "still usable"


def test_results_are_snapshotted_before_the_next_item(served, send_batch):
    _, _, handle = served
    assert send_batch(handle, [Call("shared"), Call("shared")]) == [{"count": 1}, {"count": 2}]


def test_real_signatures_and_stable_derived_context_ids(served, send_batch):
    service, _, handle = served
    with tinyray.request_id("reconcile-42"):
        result = send_batch(
            handle,
            [
                Call("record", (1,)),
                Call("signature", ("3", "4", 5), {"flag": "6", "extra": "7", "ctx": "forged"}),
                Call("async_record", (2,)),
            ],
        )
    assert result == [
        1,
        [3, [4, 5], 6, {"extra": 7}, "caller/7#42", "reconcile-42:1"],
        2,
    ]
    assert service.contexts == [
        ("caller/7#42", "reconcile-42:0"),
        ("caller/7#42", "reconcile-42:2"),
    ]
    send_batch(handle, [Call("record", (3,))])
    assert service.contexts[-1][1] not in {"reconcile-42:0", "reconcile-42:2"}


@pytest.mark.parametrize("length", [196, 197, 199, 200])
def test_derived_ids_fit_the_pin_limit_are_unique_and_repeat_stably(length):
    root = "r" * length
    ids = [_rpc._batch_request_id(root, index) for index in range(MAX_BATCH)]
    assert len(set(ids)) == MAX_BATCH
    assert ids == [_rpc._batch_request_id(root, index) for index in range(MAX_BATCH)]
    for index, derived in enumerate(ids):
        assert len(derived) <= 200
        assert derived.endswith(f":{index}")
        with tinyray.request_id(derived):
            assert _rpc._request_id() == derived
        if len(root) + len(f":{index}") <= 200:
            assert derived == f"{root}:{index}"


def test_long_roots_with_a_shared_prefix_have_distinct_derived_ids():
    first = "r" * 199 + "a"
    second = "r" * 199 + "b"
    assert _rpc._batch_request_id(first, 0) != _rpc._batch_request_id(second, 0)


def test_max_length_batch_ids_can_be_forwarded_as_single_call_ids(served, send_batch):
    service, _, handle = served
    root = "r" * 200
    with tinyray.request_id(root):
        assert send_batch(handle, [Call("record", (1,)), Call("record", (2,))]) == [1, 2]
    derived = [request_id for _, request_id in service.contexts]
    assert derived == [_rpc._batch_request_id(root, index) for index in range(2)]
    for request_id in derived:
        with tinyray.request_id(request_id):
            handle.record(3)
            assert service.contexts[-1][1] == request_id
    service.contexts.clear()
    with tinyray.request_id(root):
        assert send_batch(handle, [Call("record", (1,)), Call("record", (2,))]) == [1, 2]
    assert [request_id for _, request_id in service.contexts] == derived


def test_positional_only_signature_is_not_weakened(served, send_batch):
    service, _, handle = served
    with pytest.raises(BatchError) as caught:
        send_batch(handle, [Call("signature", kwargs={"first": 1, "flag": 2})])
    assert caught.value.failed_index == 0
    assert caught.value.completed_results == []
    assert isinstance(caught.value.cause, TypeError)
    assert service.events == []


def test_stale_handle_refuses_the_whole_batch(served, send_batch):
    service, server, handle = served
    stale = tinyray.Handle(
        "batch",
        {"id": 0, "slot": 0, "incarnation": 0, "url": handle.url, "ready": True},
        server.methods,
    )
    with pytest.raises(Fenced):
        send_batch(stale, [Call("record", (1,))])
    assert service.events == []
    assert server.counters.snapshot()["calls"] == 0
    assert send_batch(handle, [Call("record", (2,))]) == [2]


def test_takeover_between_items_fences_the_remaining_prefix(served, send_batch):
    service, server, handle = served
    with pytest.raises(BatchError) as caught:
        send_batch(handle, [Call("record", (1,)), Call("takeover"), Call("record", (3,))])
    assert caught.value.failed_index == 2
    assert caught.value.completed_results == [1, "old tenure finished"]
    assert isinstance(caught.value.cause, Fenced)
    assert service.events == [1, "takeover"]
    assert _settled(server)["failed"] == 1


def test_overload_refuses_the_entire_batch_before_execution(served, send_batch):
    service, server, handle = served
    result = []
    errors = []

    def hold_slot():
        try:
            result.extend(batch(handle, [Call("hold", (1,)), Call("record", (2,))]))
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=hold_slot)
    thread.start()
    try:
        assert service.entered.wait(5)
        with pytest.raises(NotDelivered, match="concurrency"):
            send_batch(handle, [Call("record", (3,)), Call("record", (4,))])
        with pytest.raises(NotDelivered):
            handle.record(5)
        assert service.events == [1]
    finally:
        service.release.set()
        thread.join(5)
    assert not thread.is_alive()
    assert not errors
    assert result == [1, 2]
    stats = _settled(server)
    assert stats["calls"] == 1
    assert stats["refused"] == 2
    assert send_batch(handle, [Call("record", (6,))]) == [6]


def test_timeout_is_unknown_for_the_whole_batch_and_never_retries(served, send_batch):
    service, server, handle = served
    try:
        with pytest.raises(OutcomeUnknown):
            send_batch(handle, [Call("hold", (1,)), Call("record", (2,))], timeout=0.2)
        assert service.entered.is_set()
        assert service.events == [1]
    finally:
        service.release.set()
    stats = _settled(server)
    assert service.events == [1, 2]
    assert stats["calls"] == 1
    assert handle.echo(3) == 3


def test_async_cancellation_does_not_claim_remote_items_were_cancelled(served):
    service, server, handle = served

    async def run():
        pending = asyncio.create_task(abatch(handle, [Call("hold", (1,)), Call("record", (2,))]))
        try:
            assert await asyncio.to_thread(service.entered.wait, 5)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert service.events == [1]
        finally:
            service.release.set()

    asyncio.run(run())
    assert _settled(server)["calls"] == 1
    assert service.events == [1, 2]
    assert handle.echo(3) == 3


def test_async_methods_stay_on_the_joining_loop():
    async def run():
        loop = asyncio.get_running_loop()

        class OnLoop:
            async def check(self, ctx: CallContext):
                assert asyncio.get_running_loop() is loop
                return ctx.request_id

        server = MethodServer(OnLoop(), "loop/0#1", host="127.0.0.1")
        handle = tinyray.Handle(
            "loop",
            {"id": 0, "incarnation": 1, "url": server.url("127.0.0.1"), "ready": True},
            server.methods,
        )
        try:
            with tinyray.request_id("loop-batch"):
                assert await abatch(handle, [Call("check"), Call("check")]) == [
                    "loop-batch:0",
                    "loop-batch:1",
                ]
        finally:
            await asyncio.to_thread(server.close)

    asyncio.run(run())


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"method": 1}, TypeError),
        ({"method": ""}, ValueError),
        ({"method": "_private"}, ValueError),
        ({"method": "echo/other"}, ValueError),
        ({"method": "处理"}, ValueError),
        ({"method": "echo", "args": "abc"}, TypeError),
        ({"method": "echo", "args": None}, TypeError),
        ({"method": "echo", "kwargs": []}, TypeError),
        ({"method": "echo", "kwargs": {1: "value"}}, TypeError),
    ],
)
def test_invalid_call_descriptors_fail_locally(kwargs, error):
    with pytest.raises(error):
        Call(**kwargs)


def test_invalid_or_oversized_batches_do_not_contact_a_handle(send_batch):
    with pytest.raises(TypeError, match="Call"):
        send_batch(object(), [Call("echo"), {}])
    with pytest.raises(ValueError, match=str(MAX_BATCH)):
        send_batch(object(), (Call("echo") for _ in range(MAX_BATCH + 1)))
    assert send_batch(object(), []) == []


def test_the_batch_limit_is_inclusive_and_empty_batches_are_local(served, send_batch):
    service, server, handle = served
    assert send_batch(handle, []) == []
    assert server.counters.snapshot()["calls"] == 0
    assert send_batch(handle, [Call("record", (i,)) for i in range(MAX_BATCH)]) == list(
        range(MAX_BATCH)
    )
    assert service.events == list(range(MAX_BATCH))
    assert _settled(server)["calls"] == 1


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"calls": None},
        {"calls": [], "extra": True},
        {"calls": [{}]},
        {"calls": [_item(), {"method": "record"}]},
        {"calls": [_item(), _item(method="_private")]},
        {"calls": [_item(), _item(method="处理")]},
        {"calls": [_item(), _item(args="bad")]},
        {"calls": [_item(), {"method": "record", "args": [], "kwargs": []}]},
        {"calls": [_item(), {"method": "record", "args": None, "kwargs": {}}]},
    ],
)
def test_malformed_envelopes_refuse_before_any_item(served, payload):
    service, server, handle = served
    status, raw = _raw(server, json.dumps(payload).encode())
    assert status == 400
    with pytest.raises(NotDelivered):
        _rpc._decode_batch(status, raw, handle.identity, 2)
    assert service.events == []
    stats = _settled(server)
    assert stats["calls"] == stats["failed"] == 1
    assert handle.echo("next") == "next"


@pytest.mark.parametrize("body", [b"not json", b'{"calls":[', b'{"calls":"\xff"}'])
def test_malformed_json_is_not_delivered(served, body):
    service, server, _ = served
    assert _raw(server, body)[0] == 400
    assert service.events == []
    assert _settled(server)["failed"] == 1


def test_wire_batch_limit_and_wire_empty_batch(served):
    service, server, handle = served
    status, raw = _raw(server, json.dumps({"calls": [_item()] * (MAX_BATCH + 1)}).encode())
    assert status == 413
    with pytest.raises(ValueError, match=str(MAX_BATCH)):
        _rpc._decode_batch(status, raw, handle.identity, MAX_BATCH + 1)
    assert service.events == []
    status, raw = _raw(server, b'{"calls":[]}')
    assert status == 200
    assert json.loads(raw) == {"items": []}
    stats = _settled(server)
    assert stats["calls"] == 2
    assert stats["failed"] == 1


@pytest.mark.parametrize(
    "extra,length,status",
    [
        (b"Transfer-Encoding: chunked\r\n", b"2", 411),
        (b"", b"abc", 400),
        (b"", b"-1", 400),
    ],
)
def test_bad_framing_never_dispatches_a_batch(served, extra, length, status):
    service, server, handle = served
    got, raw = _raw(server, b"{}", extra=extra, length=length)
    assert got == status
    with pytest.raises(NotDelivered):
        _rpc._decode_batch(got, raw, handle.identity, 1)
    assert service.events == []
    assert server.counters.snapshot()["calls"] == 0


def test_short_body_is_refused_even_if_its_prefix_is_valid_json(served):
    service, server, _ = served
    body = json.dumps({"calls": [_item()]}).encode()
    assert _raw(server, body, length=str(len(body) + 10).encode(), shutdown=True)[0] == 400
    assert service.events == []


def test_stalled_batch_body_is_refused_and_releases_its_connection(served, monkeypatch):
    service, server, handle = served
    monkeypatch.setattr(_serve, "BODY_TIMEOUT", 0.05)
    status, raw = _raw(server, b"{", length=b"100")
    assert status == 408
    with pytest.raises(NotDelivered):
        _rpc._decode_batch(status, raw, handle.identity, 1)
    assert service.events == []
    assert handle.echo("next") == "next"


@pytest.mark.parametrize("status", [404, 405, 501])
def test_legacy_peers_fail_explicitly_without_single_call_replay(
    served, send_batch, monkeypatch, status
):
    service, _, handle = served
    original = _serve._Handler.do_POST
    attempts = []

    def legacy(handler):
        attempts.append(handler.path)
        if handler.path == "/_batch":
            handler.close_connection = True
            handler._send(status, {})
        else:
            original(handler)

    monkeypatch.setattr(_serve._Handler, "do_POST", legacy)
    with pytest.raises(NotDelivered, match="does not support RPC batching"):
        send_batch(handle, [Call("record", (1,)), Call("record", (2,))])
    assert attempts == ["/_batch"]
    assert service.events == []
    assert handle.record(3) == 3


@pytest.mark.parametrize(
    "exception,expected",
    [(httpx.ConnectError("refused"), NotDelivered), (httpx.ReadError("reset"), OutcomeUnknown)],
)
def test_batch_transport_errors_cover_the_entire_request(
    served, send_batch, monkeypatch, exception, expected
):
    service, _, handle = served
    attempts = []

    def fail(request):
        attempts.append(request.url.path)
        raise exception

    transport = httpx.MockTransport(fail)
    with httpx.Client(transport=transport) as sync:
        asynchronous = httpx.AsyncClient(transport=transport)
        monkeypatch.setattr(_rpc, "_sync_client", lambda: sync)
        monkeypatch.setattr(_rpc, "_async_client", lambda: asynchronous)
        try:
            with pytest.raises(expected):
                send_batch(handle, [Call("record", (1,))])
        finally:
            asyncio.run(asynchronous.aclose())
    assert attempts == ["/_batch"]
    assert service.events == []


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {"result": 1},
        {"items": []},
        {"items": [{"status": True, "body": {"result": 1}}]},
        {"items": [{"status": 200, "body": {"error": "broken"}}]},
        {"items": [{"status": 200, "body": {"result": 1}}]},
        {"items": [{"status": 500, "body": {"error": "broken"}}]},
        {
            "items": [
                {"status": 404, "body": {"error": "no method"}},
                {"status": 200, "body": {"result": 1}},
            ]
        },
    ],
)
def test_malformed_responses_never_claim_a_known_batch_outcome(payload):
    with pytest.raises(OutcomeUnknown):
        _rpc._decode_batch(200, json.dumps(payload).encode(), "peer/0#1", 2)


def test_reply_write_failure_still_releases_the_admission_slot(served, monkeypatch):
    service, server, handle = served
    original = _serve._Handler._send_raw
    broken = False

    def fail_once(handler, code, raw):
        nonlocal broken
        if handler.path == "/_batch" and not broken:
            broken = True
            raise OSError("simulated failed response write")
        original(handler, code, raw)

    monkeypatch.setattr(_serve._Handler, "_send_raw", fail_once)
    responses = []
    with httpx.Client(event_hooks={"response": [responses.append]}) as client:
        monkeypatch.setattr(_rpc, "_sync_client", lambda: client)
        with pytest.raises(OutcomeUnknown):
            batch(handle, [Call("record", (1,))])
        assert responses[0].headers["connection"] == "close"
        assert service.events == [1]
        assert _settled(server)["calls"] == 1
        assert handle.record(2) == 2


def test_batch_oversize_warnings_retain_application_attribution(served, send_batch, monkeypatch):
    _, _, handle = served
    monkeypatch.setattr(_rpc, "SOFT_BODY", 1024)
    blob = "x" * 4096
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", tinyray.OversizeWarning)
        assert send_batch(handle, [Call("echo", (blob,))]) == [blob]
    nudges = [entry for entry in caught if issubclass(entry.category, tinyray.OversizeWarning)]
    assert len(nudges) == 2
    assert all(entry.filename == __file__ for entry in nudges)
    assert "sending" in str(nudges[0].message)
    assert "returned" in str(nudges[1].message)
