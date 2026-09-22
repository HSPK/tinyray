"""Native method RPC framing, connection state, and listener lifecycle."""

from __future__ import annotations

import asyncio
import gc
import itertools
import os
import socket
import struct
import threading
import time
import warnings
import weakref
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing

import msgspec
import pytest
import tinyray
from tinyray._msgpack import dumps, loads
from tinyray._serve import MethodServer

from tests.support.rpc_wire import MAX_FRAME, frame, recv_frame, recv_reply, request


class Service:
    def __init__(self) -> None:
        self.calls = 0

    def ping(self) -> str:
        self.calls += 1
        return "pong"

    def echo(self, value: int) -> int:
        self.calls += 1
        return value

    def delayed(self, value: str, seconds: float) -> str:
        self.calls += 1
        time.sleep(seconds)
        return value

    def boom(self) -> None:
        self.calls += 1
        raise ValueError("expected")

    def unserializable(self):
        self.calls += 1
        return object()

    def ran(self) -> int:
        return self.calls


@pytest.fixture
def served():
    service = Service()
    server = MethodServer(service, "service/0#1", host="127.0.0.1")
    handle = tinyray.Handle(
        "service",
        {
            "id": 0,
            "slot": 0,
            "incarnation": 1,
            "url": server.url("127.0.0.1"),
            "ready": True,
        },
        server.methods,
    )
    try:
        yield service, server, handle
    finally:
        server.close()


def _connect(endpoint: str, timeout: float = 5.0) -> socket.socket:
    host, port = endpoint.rsplit(":", 1)
    return socket.create_connection((host, int(port)), timeout=timeout)


def test_closing_a_native_listener_releases_the_served_object():
    service = Service()
    held = weakref.ref(service)
    server = MethodServer(service, "release/0#1", host="127.0.0.1")
    del service

    server.close()
    gc.collect()

    assert held() is None


def _valid(
    *,
    request_id: str = "raw-1",
    target: str = "service/0#1",
    method: str = "ping",
    payload=None,
):
    if payload is None:
        payload = {"args": [], "kwargs": {}}
    return request(
        request_id=request_id,
        target=target,
        method=method,
        body=dumps(payload),
    )


def test_endpoint_is_bare_and_legacy_http_is_rejected_explicitly(served):
    _, _, handle = served
    assert handle.url.startswith("127.0.0.1:")
    assert "://" not in handle.url

    legacy = tinyray.Handle(
        "service",
        {
            "id": 0,
            "slot": 0,
            "incarnation": 1,
            "url": f"http://{handle.url}",
            "ready": True,
        },
        ("ping",),
    )
    with pytest.raises(tinyray.NotDelivered, match="hard cutover"):
        legacy.ping()


def test_http_bytes_are_protocol_data_not_an_http_listener(served):
    service, _, handle = served
    with _connect(handle.url) as connection:
        connection.sendall(b"POST /call/ping HTTP/1.1\r\nHost: x\r\n\r\n")
        reply = recv_reply(connection)
    assert reply["status"] == "malformed_protocol"
    assert service.calls == 0


@pytest.mark.parametrize(
    "raw",
    [
        b"\x00\x00",
        b"\x00\x00\x00\x00",
        struct.pack(">I", MAX_FRAME + 1),
        struct.pack(">I", 1) + b"\xc1",
        struct.pack(">I", 20) + b"\x80",
    ],
    ids=["partial-prefix", "empty", "oversized", "malformed-msgpack", "truncated-body"],
)
def test_bad_frames_get_a_protocol_error_and_close(served, raw):
    service, _, handle = served
    with _connect(handle.url) as connection:
        connection.sendall(raw)
        connection.shutdown(socket.SHUT_WR)
        reply = recv_reply(connection)
        assert reply["status"] == "malformed_protocol"
        assert connection.recv(1) == b""
    assert service.calls == 0


@pytest.mark.parametrize(
    "envelope",
    [
        _valid(request_id="bad-version") | {"v": 999},
        _valid(request_id="missing-method") | {"method": None},
        _valid(request_id="mixed") | {"batch": 1},
        request(
            request_id="bad-batch",
            target="service/0#1",
            method=None,
            batch_len=129,
            body=dumps({"calls": []}),
        ),
    ],
)
def test_invalid_protocol_metadata_is_correlated_and_never_dispatched(served, envelope):
    service, _, handle = served
    with _connect(handle.url) as connection:
        connection.sendall(frame(envelope))
        reply = recv_reply(connection)
        assert reply["id"] == envelope["id"]
        assert reply["status"] == "malformed_protocol"
        assert connection.recv(1) == b""
    assert service.calls == 0


@pytest.mark.parametrize(
    "envelope",
    [
        _valid(request_id="unknown-op") | {"op": "teleport"},
        {key: value for key, value in _valid(request_id="missing-body").items() if key != "body"},
        _valid(request_id="wrong-version-type") | {"v": "one"},
        _valid(request_id="wrong-caller-type") | {"from": 7},
        _valid(request_id="wrong-body-type") | {"body": {"not": "bytes"}},
    ],
)
def test_malformed_typed_envelopes_keep_the_minimal_request_id(served, envelope):
    service, _, handle = served
    with _connect(handle.url) as connection:
        connection.sendall(frame(envelope))
        reply = recv_reply(connection)
        assert reply["id"] == envelope["id"]
        assert reply["status"] == "malformed_protocol"
        assert connection.recv(1) == b""
    assert service.calls == 0


def test_application_validation_failure_keeps_the_framed_connection_usable(served):
    service, _, handle = served
    with _connect(handle.url) as connection:
        bad = _valid(
            request_id="bad-app",
            method="echo",
            payload={"args": 5, "kwargs": {}},
        )
        connection.sendall(frame(bad))
        assert recv_reply(connection)["status"] == "caller_fault"

        good = _valid(
            request_id="good-app",
            method="echo",
            payload={"args": [7], "kwargs": {}},
        )
        connection.sendall(frame(good))
        reply = recv_reply(connection)
        assert reply["status"] == "success"
        assert loads(reply["body"]) == 7
    assert service.calls == 1


def test_every_complete_reply_leaves_the_connection_correlated(served):
    service, _, handle = served
    with _connect(handle.url) as connection:
        cases = [
            (_valid(request_id="missing", method="nope"), "method_not_found"),
            (_valid(request_id="fenced", target="service/0#0"), "fenced"),
            (_valid(request_id="raises", method="boom"), "remote_error"),
            (_valid(request_id="bad-return", method="unserializable"), "remote_error"),
            (
                _valid(
                    request_id="good",
                    method="echo",
                    payload={"args": [9], "kwargs": {}},
                ),
                "success",
            ),
        ]
        for envelope, status in cases:
            connection.sendall(frame(envelope))
            reply = recv_reply(connection)
            assert reply["id"] == envelope["id"]
            assert reply["status"] == status
        assert loads(reply["body"]) == 9
    assert service.calls == 3


def test_one_server_connection_replies_out_of_order_by_request_id(served):
    service, _, handle = served
    with _connect(handle.url) as connection:
        slow = _valid(
            request_id="slow-first",
            method="delayed",
            payload={"args": ["slow", 0.15], "kwargs": {}},
        )
        fast = _valid(
            request_id="fast-second",
            method="delayed",
            payload={"args": ["fast", 0.0], "kwargs": {}},
        )
        connection.sendall(frame(slow) + frame(fast))
        first = recv_reply(connection)
        second = recv_reply(connection)
    assert [first["id"], second["id"]] == ["fast-second", "slow-first"]
    assert [loads(first["body"]), loads(second["body"])] == ["fast", "slow"]
    assert service.calls == 2


def test_answer_is_counted_before_the_client_can_observe_it(served):
    _, server, handle = served
    assert handle.ping() == "pong"
    stats = server.counters.snapshot()
    assert stats["calls"] == 1
    assert stats["failed"] == 0
    assert stats["in_flight"] == 0


def test_sync_calls_return_a_complete_connection_to_the_native_pool(served):
    _, _, handle = served
    tinyray._tinyray.rpc_debug_clear_pools()
    before = tinyray._tinyray.rpc_debug_state()["idle_connections"]
    assert handle.ping() == "pong"
    after_first = tinyray._tinyray.rpc_debug_state()["idle_connections"]
    assert after_first == before + 1
    assert handle.ping() == "pong"
    assert tinyray._tinyray.rpc_debug_state()["idle_connections"] == after_first


@pytest.mark.parametrize(("callers", "connections"), [(8, 4), (128, 4)])
def test_concurrent_calls_scale_to_a_bounded_number_of_connections(served, callers, connections):
    _, server, handle = served
    tinyray._tinyray.rpc_debug_clear_pools()
    before = tinyray._tinyray.rpc_debug_state()
    before_fds = len(os.listdir("/proc/self/fd"))
    gate = threading.Barrier(callers + 1)

    def call(index: int) -> str:
        gate.wait()
        return handle.delayed(str(index), 0.02)

    with ThreadPoolExecutor(max_workers=callers) as workers:
        pending = [workers.submit(call, index) for index in range(callers)]
        gate.wait()
        assert [future.result(timeout=30) for future in pending] == [
            str(index) for index in range(callers)
        ]

    after = tinyray._tinyray.rpc_debug_state()
    after_fds = len(os.listdir("/proc/self/fd"))
    assert after["connections_opened"] == before["connections_opened"] + connections
    assert after["connections"] == after["idle_connections"] == connections
    assert after["in_flight"] == 0
    assert server.counters.snapshot()["connections"] == connections
    assert after_fds - before_fds <= connections * 2 + 1


def test_idle_connection_cap_is_process_global_not_per_endpoint():
    servers = []
    tinyray._tinyray.rpc_debug_clear_pools()
    try:
        for index in range(70):
            server = MethodServer(Service(), f"cap/{index}#1", host="127.0.0.1")
            servers.append(server)
            handle = tinyray.Handle(
                "cap",
                {
                    "id": index,
                    "incarnation": 1,
                    "url": server.url("127.0.0.1"),
                    "ready": True,
                },
                server.methods,
            )
            assert handle.ping() == "pong"
        assert tinyray._tinyray.rpc_debug_state()["idle_connections"] == 64
    finally:
        for server in servers:
            server.close()


def test_idle_connections_close_with_the_listener(served):
    _, server, handle = served
    connection = _connect(handle.url)
    server.close()
    connection.settimeout(2)
    try:
        assert connection.recv(1) == b""
    except ConnectionResetError:
        pass


def test_silent_and_partial_prefix_connections_do_not_block_an_ordinary_call(served):
    _, server, handle = served
    attackers = []
    try:
        for index in range(64):
            connection = _connect(handle.url)
            if index % 2:
                connection.sendall(b"\x00\x00")
            attackers.append(connection)
        deadline = time.monotonic() + 2
        while server.counters.snapshot()["connections"] < len(attackers):
            assert time.monotonic() < deadline, "the listener did not admit the attack sockets"
            time.sleep(0.005)
        assert handle.ping.timeout(2)() == "pong"
    finally:
        for connection in attackers:
            connection.close()


def test_bulk_frame_budget_refuses_before_body_read_and_preserves_small_calls(served):
    service, server, handle = served
    attackers = []
    try:
        for _ in range(2):
            connection = _connect(handle.url)
            connection.sendall(struct.pack(">I", MAX_FRAME))
            attackers.append(connection)
        deadline = time.monotonic() + 2
        while server.counters.snapshot()["bulk_frame_bytes"] != 2 * MAX_FRAME:
            assert time.monotonic() < deadline, "maximum frames never reserved their bounded budget"
            time.sleep(0.005)

        with _connect(handle.url) as refused:
            refused.sendall(struct.pack(">I", MAX_FRAME))
            reply = recv_reply(refused)
            assert reply["id"] == ""
            assert reply["status"] == "concurrency_refused"
            assert "byte budgets" in reply["error"]["message"]
            assert refused.recv(1) == b""

        assert handle.ping.timeout(2)() == "pong"
        assert service.calls == 1
        stats = server.counters.snapshot()
        assert stats["bulk_frame_bytes"] == 2 * MAX_FRAME
        assert stats["small_frame_bytes"] == 0
    finally:
        for connection in attackers:
            connection.close()


class FakePeer:
    def __init__(self, handler):
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.endpoint = f"127.0.0.1:{self.listener.getsockname()[1]}"
        self.accepted = 0
        self.error: BaseException | None = None

        def run() -> None:
            try:
                handler(self)
            except BaseException as exc:
                self.error = exc
            finally:
                self.listener.close()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def accept(self):
        connection, _ = self.listener.accept()
        self.accepted += 1
        return closing(connection)

    def join(self) -> None:
        self.thread.join(5)
        assert not self.thread.is_alive()
        if self.error is not None:
            raise self.error


def _fake_handle(endpoint: str) -> tinyray.Handle:
    return tinyray.Handle(
        "fake",
        {"id": 0, "slot": 0, "incarnation": 1, "url": endpoint, "ready": True},
        ("ping", "echo"),
    )


def _read_request(connection: socket.socket) -> dict:
    return msgspec.msgpack.decode(recv_frame(connection))


def _success(request_id: str, value) -> bytes:
    return frame({"v": 1, "id": request_id, "status": "success", "body": dumps(value)})


def test_request_id_mismatch_is_unknown_and_discards_the_connection():
    def handler(peer: FakePeer) -> None:
        with peer.accept() as first:
            request1 = _read_request(first)
            first.sendall(_success(request1["id"] + "-wrong", "wrong"))
            first.settimeout(2)
            assert first.recv(1) == b""
        with peer.accept() as second:
            request2 = _read_request(second)
            second.sendall(_success(request2["id"], "ok"))

    peer = FakePeer(handler)
    handle = _fake_handle(peer.endpoint)
    with pytest.raises(tinyray.OutcomeUnknown, match="not"):
        handle.ping()
    assert handle.ping() == "ok"
    peer.join()
    assert peer.accepted == 2


def test_reply_protocol_mismatch_is_unknown_and_discards_the_connection():
    def handler(peer: FakePeer) -> None:
        with peer.accept() as first:
            request1 = _read_request(first)
            first.sendall(
                frame(
                    {
                        "v": 999,
                        "id": request1["id"],
                        "status": "success",
                        "body": dumps("wrong"),
                    }
                )
            )
            first.settimeout(2)
            assert first.recv(1) == b""
        with peer.accept() as second:
            request2 = _read_request(second)
            second.sendall(_success(request2["id"], "ok"))

    peer = FakePeer(handler)
    handle = _fake_handle(peer.endpoint)
    with pytest.raises(tinyray.OutcomeUnknown, match="protocol"):
        handle.ping()
    assert handle.ping() == "ok"
    peer.join()
    assert peer.accepted == 2


@pytest.mark.parametrize(
    "bad_reply",
    [
        b"\x00\x00",
        struct.pack(">I", 20) + b"\x80",
        struct.pack(">I", 1) + b"\xc1",
        struct.pack(">I", MAX_FRAME + 1),
    ],
    ids=["truncated-prefix", "truncated-body", "malformed", "oversized"],
)
def test_bad_replies_are_unknown_and_never_reused(bad_reply):
    def handler(peer: FakePeer) -> None:
        with peer.accept() as first:
            _read_request(first)
            first.sendall(bad_reply)
            first.shutdown(socket.SHUT_WR)
            first.settimeout(2)
            assert first.recv(1) == b""
        with peer.accept() as second:
            request2 = _read_request(second)
            second.sendall(_success(request2["id"], "ok"))

    peer = FakePeer(handler)
    handle = _fake_handle(peer.endpoint)
    with pytest.raises(tinyray.OutcomeUnknown):
        handle.ping()
    assert handle.ping() == "ok"
    peer.join()
    assert peer.accepted == 2


def test_timeout_after_complete_write_is_outcome_unknown():
    received = threading.Event()

    def handler(peer: FakePeer) -> None:
        with peer.accept() as connection:
            _read_request(connection)
            received.set()
            time.sleep(0.5)

    peer = FakePeer(handler)
    handle = _fake_handle(peer.endpoint)
    with pytest.raises(tinyray.OutcomeUnknown):
        handle.ping.timeout(0.05)()
    assert received.wait(1)
    peer.join()


def test_sync_timeout_removes_only_its_waiter_and_discards_the_late_reply():
    def handler(peer: FakePeer) -> None:
        with peer.accept() as connection:
            first = _read_request(connection)
            second = _read_request(connection)
            connection.sendall(_success(second["id"], "second"))
            connection.sendall(_success(first["id"], "late"))
            third = _read_request(connection)
            connection.sendall(_success(third["id"], "third"))

    peer = FakePeer(handler)
    handle = _fake_handle(peer.endpoint)
    with pytest.raises(tinyray.OutcomeUnknown):
        handle.ping.timeout(0.05)()
    assert handle.ping() == "second"
    assert handle.ping() == "third"
    peer.join()
    assert peer.accepted == 1


def test_async_cancellation_discards_only_its_waiter_and_late_reply():
    received = threading.Event()

    def handler(peer: FakePeer) -> None:
        with peer.accept() as connection:
            first = _read_request(connection)
            received.set()
            second = _read_request(connection)
            connection.sendall(_success(second["id"], "second"))
            connection.sendall(_success(first["id"], "late"))
            third = _read_request(connection)
            connection.sendall(_success(third["id"], "third"))

    peer = FakePeer(handler)

    async def drive() -> None:
        handle = tinyray.AsyncHandle(
            "fake",
            {
                "id": 0,
                "slot": 0,
                "incarnation": 1,
                "url": peer.endpoint,
                "ready": True,
            },
            ("ping",),
        )
        pending = asyncio.create_task(handle.ping())
        assert await asyncio.to_thread(received.wait, 1)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert tinyray._tinyray.rpc_debug_state()["in_flight"] == 0
        assert await handle.ping() == "second"
        assert await handle.ping() == "third"

    asyncio.run(drive())
    peer.join()
    assert peer.accepted == 1


def test_one_multiplexed_connection_routes_mixed_sync_and_async_replies_out_of_order():
    def handler(peer: FakePeer) -> None:
        with peer.accept() as connection:
            requests = [_read_request(connection) for _ in range(2)]
            for request_ in reversed(requests):
                payload = loads(request_["body"])
                connection.sendall(_success(request_["id"], payload["args"][0]))

    peer = FakePeer(handler)
    sync_handle = _fake_handle(peer.endpoint)
    async_handle = tinyray.AsyncHandle(
        "fake",
        {
            "id": 0,
            "slot": 0,
            "incarnation": 1,
            "url": peer.endpoint,
            "ready": True,
        },
        ("echo",),
    )

    async def drive() -> list[str]:
        with tinyray.request_id("route-a"):
            async_call = asyncio.create_task(async_handle.echo("async"))
        with tinyray.request_id("route-z"):
            sync_call = asyncio.create_task(asyncio.to_thread(sync_handle.echo, "sync"))
        return await asyncio.gather(
            async_call,
            sync_call,
        )

    got = asyncio.run(drive())
    assert got == ["async", "sync"]
    peer.join()
    assert peer.accepted == 1


def test_ordinary_raw_128_way_multiplexing_keeps_frame_boundaries(served):
    _, _, handle = served
    tinyray._tinyray.rpc_debug_clear_pools()
    body = dumps({"args": [], "kwargs": {}})
    gate = threading.Barrier(129)
    sequence = itertools.count()

    def call_many():
        gate.wait()
        for _ in range(32):
            request_id = f"frame-{next(sequence)}"
            outcome = tinyray._tinyray.rpc_call_sync(
                handle.url,
                request_id,
                "caller/0#1",
                handle.identity,
                body,
                5_000,
                method="ping",
            )
            assert outcome.status == tinyray._tinyray.RPC_STATUS_SUCCESS
            assert loads(bytes(outcome.payload)) == "pong"

    with ThreadPoolExecutor(max_workers=128) as workers:
        futures = [workers.submit(call_many) for _ in range(128)]
        gate.wait()
        for future in futures:
            future.result(timeout=60)
    assert tinyray._tinyray.rpc_debug_state()["connections"] <= 4


def test_malformed_reply_poisons_every_in_flight_request_and_reconnects():
    total = 2

    def handler(peer: FakePeer) -> None:
        with peer.accept() as first:
            for _ in range(total):
                _read_request(first)
            first.sendall(struct.pack(">I", 1) + b"\xc1")
            first.shutdown(socket.SHUT_WR)
        with peer.accept() as second:
            request2 = _read_request(second)
            second.sendall(_success(request2["id"], "ok"))

    peer = FakePeer(handler)
    handle = _fake_handle(peer.endpoint)
    errors: list[BaseException] = []

    def call(index: int) -> None:
        try:
            handle.echo(index)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=call, args=(index,)) for index in range(total)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert len(errors) == total
    assert all(isinstance(error, tinyray.OutcomeUnknown) for error in errors)
    assert handle.ping() == "ok"
    peer.join()
    assert peer.accepted == 2


def test_endpoint_multiplexing_limit_refuses_locally_without_polling():
    accepted = threading.Event()

    def handler(peer: FakePeer) -> None:
        with ExitStack() as stack:
            connections = [stack.enter_context(peer.accept()) for _ in range(4)]
            count = 0
            lock = threading.Lock()

            def drain(connection) -> None:
                nonlocal count
                while True:
                    try:
                        _read_request(connection)
                    except (EOFError, OSError):
                        return
                    with lock:
                        count += 1
                        if count == 256:
                            accepted.set()

            for connection in connections:
                threading.Thread(target=drain, args=(connection,), daemon=True).start()
            assert accepted.wait(2)
            time.sleep(0.2)

    peer = FakePeer(handler)
    handle = tinyray.AsyncHandle(
        "fake",
        {
            "id": 0,
            "slot": 0,
            "incarnation": 1,
            "url": peer.endpoint,
            "ready": True,
        },
        ("ping",),
    )

    async def drive() -> list[BaseException]:
        outcomes = await asyncio.gather(
            *(handle.ping.timeout(2)() for _ in range(257)),
            return_exceptions=True,
        )
        return [outcome for outcome in outcomes if isinstance(outcome, BaseException)]

    errors = asyncio.run(drive())
    assert accepted.wait(2)
    assert len(errors) == 257
    refused = [error for error in errors if isinstance(error, tinyray.NotDelivered)]
    unknown = [error for error in errors if isinstance(error, tinyray.OutcomeUnknown)]
    assert len(refused) == 1
    assert len(unknown) == 256
    peer.join()
    assert peer.accepted == 4


@pytest.mark.parametrize("kind", ["unknown", "duplicate"])
def test_unknown_or_duplicate_reply_ids_poison_remaining_requests(kind):
    def handler(peer: FakePeer) -> None:
        with peer.accept() as connection:
            first = _read_request(connection)
            second = _read_request(connection)
            if kind == "duplicate":
                connection.sendall(_success(first["id"], "first"))
                connection.sendall(_success(first["id"], "duplicate"))
            else:
                connection.sendall(_success("never-sent", "unknown"))
            connection.settimeout(2)
            assert connection.recv(1) == b""
            assert first["id"] != second["id"]

    peer = FakePeer(handler)
    handle = _fake_handle(peer.endpoint)
    outcomes: list[object] = []

    def call(value: str) -> None:
        try:
            outcomes.append(handle.echo(value))
        except BaseException as exc:  # noqa: BLE001
            outcomes.append(exc)

    threads = [threading.Thread(target=call, args=(value,)) for value in ("first", "second")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    if kind == "duplicate":
        assert "first" in outcomes
        errors = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
        assert len(errors) == 1 and isinstance(errors[0], tinyray.OutcomeUnknown)
    else:
        assert all(isinstance(outcome, tinyray.OutcomeUnknown) for outcome in outcomes)
    peer.join()
    assert peer.accepted == 1


def test_timeout_during_a_partial_write_is_not_delivered():
    accepted = threading.Event()

    def handler(peer: FakePeer) -> None:
        with peer.accept() as connection:
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            accepted.set()
            time.sleep(0.5)

    peer = FakePeer(handler)
    handle = _fake_handle(peer.endpoint)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", tinyray.OversizeWarning)
        with pytest.raises(tinyray.NotDelivered, match="not completely written"):
            handle.echo.timeout(0.02)("x" * (8 << 20))
    assert accepted.wait(1)
    peer.join()
