"""Batches share one framed request, not a transaction or retry policy."""

from __future__ import annotations

import asyncio
import inspect
import socket
import struct
import threading
import time
import warnings
from types import SimpleNamespace

import pytest
import tinyray
from tinyray import _rpc
from tinyray._errors import BatchError, Fenced, NotDelivered, OutcomeUnknown, RemoteError
from tinyray._msgpack import dumps, loads
from tinyray._rpc import MAX_BATCH, Call, abatch, batch
from tinyray._serve import CallContext, MethodServer

from tests.support.rpc_wire import frame, recv_reply
from tests.support.rpc_wire import request as wire_request


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
        {
            "id": 0,
            "slot": 0,
            "incarnation": 1,
            "url": server.url("127.0.0.1"),
            "ready": True,
        },
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


def _connect(endpoint: str) -> socket.socket:
    host, port = endpoint.rsplit(":", 1)
    return socket.create_connection((host, int(port)), timeout=5)


def _wire_batch(handle, payload, *, batch_len=None, request_id="wire-batch"):
    if batch_len is None:
        calls = payload.get("calls") if isinstance(payload, dict) else None
        batch_len = len(calls) if isinstance(calls, list) else 1
    envelope = wire_request(
        request_id=request_id,
        target=handle.identity,
        method=None,
        batch_len=batch_len,
        body=dumps(payload),
    )
    with _connect(handle.url) as connection:
        connection.sendall(frame(envelope))
        return recv_reply(connection)


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
        assert "cannot be sent as MessagePack" in str(error.cause)
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
    reply = _wire_batch(handle, payload)
    assert reply["status"] == "caller_fault"
    assert service.events == []
    stats = _settled(server)
    assert stats["calls"] == stats["failed"] == 1
    assert handle.echo("next") == "next"


@pytest.mark.parametrize("body", [b"\xc1", b"\x81\xa5calls", b"\x92\x01"])
def test_malformed_messagepack_batch_is_a_caller_fault(served, body):
    service, server, handle = served
    envelope = wire_request(
        request_id="bad-body",
        target=handle.identity,
        method=None,
        batch_len=1,
        body=body,
    )
    with _connect(handle.url) as connection:
        connection.sendall(frame(envelope))
        reply = recv_reply(connection)
    assert reply["status"] == "caller_fault"
    assert service.events == []
    assert _settled(server)["failed"] == 1


def test_wire_batch_metadata_limit_and_empty_batch(served):
    service, server, handle = served
    reply = _wire_batch(
        handle,
        {"calls": [_item()] * (MAX_BATCH + 1)},
        batch_len=MAX_BATCH + 1,
    )
    assert reply["status"] == "malformed_protocol"
    assert service.events == []
    assert server.counters.snapshot()["calls"] == 0

    reply = _wire_batch(handle, {"calls": []}, batch_len=0, request_id="empty")
    assert reply["status"] == "success"
    assert loads(reply["body"]) == []
    stats = _settled(server)
    assert stats["calls"] == 1
    assert stats["failed"] == 0


def test_truncated_batch_frame_never_dispatches(served):
    service, server, handle = served
    with _connect(handle.url) as connection:
        connection.sendall(struct.pack(">I", 100) + b"\x80")
        connection.shutdown(socket.SHUT_WR)
        assert recv_reply(connection)["status"] == "malformed_protocol"
    assert service.events == []
    assert server.counters.snapshot()["calls"] == 0


def test_legacy_batch_endpoint_is_refused_without_single_call_replay(send_batch):
    handle = tinyray.Handle(
        "legacy",
        {"id": 0, "incarnation": 1, "url": "http://legacy:80", "ready": True},
        ("record",),
    )
    with pytest.raises(NotDelivered, match="hard cutover"):
        send_batch(handle, [Call("record", (1,))])


def test_transport_failure_applies_to_the_entire_batch(send_batch):
    handle = tinyray.Handle(
        "gone",
        {"id": 0, "incarnation": 1, "url": "127.0.0.1:1", "ready": True},
        ("record",),
    )
    with pytest.raises(NotDelivered):
        send_batch(handle, [Call("record", (1,))])


@pytest.mark.parametrize(
    "outcome",
    [
        SimpleNamespace(
            kind=tinyray._tinyray.RPC_OUTCOME_REPLY,
            status=tinyray._tinyray.RPC_STATUS_SUCCESS,
            payload=b"\x01",
            batch_index=None,
            completed=None,
            message="",
            error_type="",
            traceback="",
        ),
        SimpleNamespace(
            kind=tinyray._tinyray.RPC_OUTCOME_REPLY,
            status=tinyray._tinyray.RPC_STATUS_REMOTE_ERROR,
            payload=dumps([1]),
            batch_index=0,
            completed=1,
            message="broken",
            error_type="ValueError",
            traceback="",
        ),
    ],
)
def test_malformed_responses_never_claim_a_known_batch_outcome(outcome):
    with pytest.raises(OutcomeUnknown):
        _rpc._decode_batch(outcome, "peer/0#1", 2)


def test_reply_write_failure_still_releases_the_admission_slot(served):
    service, server, handle = served
    envelope = wire_request(
        request_id="drop-reply",
        target=handle.identity,
        method=None,
        batch_len=1,
        body=dumps({"calls": [_item("hold", [1])]}),
    )
    connection = _connect(handle.url)
    connection.sendall(frame(envelope))
    assert service.entered.wait(5)
    connection.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_LINGER,
        struct.pack("ii", 1, 0),
    )
    connection.close()
    service.release.set()
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
