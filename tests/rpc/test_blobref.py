"""Explicit same-host BlobRef transport over sealed Linux memfd objects."""

from __future__ import annotations

import asyncio
import base64
import gc
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import NamedTuple

import msgspec
import pytest
import tinyray
from tinyray import _msgpack

from tests.support.rpc_wire import frame, recv_frame

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="BlobRef is Linux-only")


class BlobService:
    def __init__(self):
        self.retained = None
        self.mapped = threading.Event()
        self.calls = 0
        self.server = None

    def blob_len(self, value: tinyray.BlobRef) -> int:
        self.calls += 1
        return len(value)

    def echo_blob(self, value: tinyray.BlobRef) -> tinyray.BlobRef:
        return value

    def retain_blob(self, value: tinyray.BlobRef) -> int:
        self.retained = value
        self.mapped.set()
        return len(value)

    def retain_slow(self, value: tinyray.BlobRef, seconds: float) -> int:
        self.retained = value
        self.mapped.set()
        time.sleep(seconds)
        return len(value)

    def retained_bytes(self) -> bytes:
        return bytes(self.retained)

    def retained_blob(self) -> tinyray.BlobRef:
        return self.retained

    def make_blob(self) -> tinyray.BlobRef:
        self.calls += 1
        return tinyray.blob(b"made-by-handler")

    def delayed_blob(self, seconds: float) -> tinyray.BlobRef:
        self.calls += 1
        self.mapped.set()
        time.sleep(seconds)
        return tinyray.blob(b"late-blob")

    def count_blobs(self, values: list[tinyray.BlobRef]) -> int:
        self.calls += 1
        return len(values)

    def typed_blob(self, value: BlobBox) -> int:
        return len(value.data)


@dataclass
class BlobBox:
    name: str
    data: tinyray.BlobRef


class BlobTuple(NamedTuple):
    name: str
    data: tinyray.BlobRef


class _DelayedBlobPeer:
    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.endpoint = f"127.0.0.1:{self.listener.getsockname()[1]}"
        self.received = threading.Event()
        self.release = threading.Event()
        self.done = threading.Event()
        self.descriptor: bytes | None = None
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            with self.listener.accept()[0] as connection:
                request = msgspec.msgpack.decode(recv_frame(connection))
                encoded = msgspec.msgpack.decode(request["body"])
                self.descriptor = _find_ext(encoded).data
                self.received.set()
                assert self.release.wait(5)
                decoded = _msgpack.loads(request["body"])
                blob = _find_blob(decoded)
                assert bytes(blob) == b"delayed-owner"
                blob.close()
                connection.sendall(
                    frame(
                        {
                            "v": 1,
                            "id": request["id"],
                            "status": "success",
                            "body": _msgpack.dumps(None),
                        }
                    )
                )
        except BaseException as exc:
            self.error = exc
        finally:
            self.done.set()
            self.listener.close()

    def join(self):
        assert self.done.wait(5)
        self.thread.join(5)
        assert not self.thread.is_alive()
        if self.error is not None:
            raise self.error


def _find_ext(value):
    if isinstance(value, msgspec.msgpack.Ext) and value.code == 124:
        return value
    if isinstance(value, dict):
        values = (*value.keys(), *value.values())
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        values = ()
    for item in values:
        found = _find_ext(item)
        if found is not None:
            return found
    return None


def _find_blob(value):
    if type(value) is tinyray.BlobRef:
        return value
    if isinstance(value, dict):
        values = (*value.keys(), *value.values())
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        values = ()
    for item in values:
        found = _find_blob(item)
        if found is not None:
            return found
    return None


def _assert_descriptor_eventually_stale(descriptor):
    deadline = time.monotonic() + 2
    while True:
        try:
            opened = tinyray.BlobRef.from_descriptor(descriptor)
        except tinyray.BlobError:
            return
        opened.close()
        assert time.monotonic() < deadline
        time.sleep(0.01)


@pytest.fixture
def blob_peer(registry):
    service = BlobService()
    with tinyray.join(
        "blob-service",
        "stateful",
        slot=0,
        size=1,
        serves=service,
        coalesce_ms=0,
    ) as member:
        service.server = member._server
        member.ready().flush(timeout=5)
        yield service, tinyray.pool(member.pool).slot(0)


@pytest.mark.parametrize("size", [64 << 10, 1 << 20, 16 << 20])
def test_blobref_is_read_only_zero_copy_and_explicit_bytes_copy(size):
    source = bytearray(index % 251 for index in range(size))
    before = source[:32]
    with tinyray.blob(source) as blob:
        source[:32] = b"x" * 32
        assert len(blob) == size
        view = blob.view()
        assert view.obj is blob
        assert view.readonly
        assert bytes(view[:32]) == bytes(before)
        with pytest.raises(TypeError):
            view[0] = 1
        copied = bytes(blob)
        assert copied == view
        assert copied is not view
        with pytest.raises(BufferError):
            blob.close()
        view.release()
    assert blob.closed
    with pytest.raises(tinyray.BlobError):
        bytes(blob)


def test_blobref_messagepack_is_explicit_and_regular_bytes_are_unchanged():
    blob = tinyray.blob(b"blob")
    encoded = _msgpack.dumps({"blob": blob, "bytes": b"bytes"})
    decoded = _msgpack.loads(encoded)
    assert type(decoded["blob"]) is tinyray.BlobRef
    assert bytes(decoded["blob"]) == b"blob"
    assert type(decoded["bytes"]) is bytes and decoded["bytes"] == b"bytes"
    unknown = _msgpack.loads(msgspec.msgpack.encode(msgspec.msgpack.Ext(120, b"x")))
    assert isinstance(unknown, msgspec.msgpack.Ext)
    with pytest.raises(tinyray.BlobError):
        _msgpack.loads(msgspec.msgpack.encode(msgspec.msgpack.Ext(124, b"forged")))
    with pytest.raises(tinyray.BlobError, match="4096"):
        _msgpack.loads(msgspec.msgpack.encode(msgspec.msgpack.Ext(124, b"x" * 4097)))
    with pytest.raises(msgspec.DecodeError):
        _msgpack.loads(b"\x01\x02")
    decoded["blob"].close()
    blob.close()

    stale = tinyray.blob(b"closed-before-map")
    encoded_stale = _msgpack.dumps(stale)
    stale.close()
    with pytest.raises(tinyray.BlobError):
        _msgpack.loads(encoded_stale)


def _resource_counts():
    gc.collect()
    return (
        len(os.listdir("/proc/self/fd")),
        sum("memfd:tinyray-blob" in line for line in open("/proc/self/maps")),
    )


def _memfd_counts():
    fds = 0
    for name in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink(f"/proc/self/fd/{name}")
        except FileNotFoundError:
            continue
        fds += "memfd:tinyray-blob" in target
    maps = sum("memfd:tinyray-blob" in line for line in open("/proc/self/maps"))
    return fds, maps


def test_decoder_deduplicates_descriptors_and_bounds_count_and_bytes(monkeypatch):
    source = tinyray.blob(b"deduplicated")
    encoded = _msgpack.dumps([source] * 8)
    before = _resource_counts()
    opens = 0
    native_blob_ref = _msgpack.BlobRef

    class CountingBlobRef:
        @classmethod
        def from_descriptor(cls, descriptor):
            nonlocal opens
            opens += 1
            return native_blob_ref.from_descriptor(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(_msgpack, "BlobRef", CountingBlobRef)
        decoded = _msgpack.loads(encoded)
    assert opens == 1
    after = _resource_counts()
    assert after == (before[0] + 1, before[1] + 1)
    assert all(bytes(value) == b"deduplicated" for value in decoded)
    decoded[0].close()
    assert bytes(decoded[-1]) == b"deduplicated"
    for value in decoded[1:]:
        value.close()
    assert _resource_counts() == before

    too_many = _msgpack.dumps([source] * 512)
    with pytest.raises(tinyray.BlobError, match="BlobRef values"):
        _msgpack.loads(too_many)
    assert _resource_counts() == before

    other = tinyray.blob(b"aggregate")
    aggregate = _msgpack.dumps([source, other])
    monkeypatch.setattr(
        _msgpack,
        "_MAX_BLOB_MAPPED_BYTES_PER_MESSAGE",
        32 + len(source) + 32 + len(other) - 1,
    )
    with pytest.raises(tinyray.BlobError, match="aggregate limit"):
        _msgpack.loads(aggregate)
    source.close()
    other.close()


def test_decoder_budget_rejects_before_handler_invocation(blob_peer, monkeypatch):
    service, handle = blob_peer
    source = tinyray.blob(b"bounded")
    before = service.calls
    with pytest.raises(TypeError, match="BlobRef values"):
        handle.count_blobs([source] * 512)
    assert service.calls == before
    other = tinyray.blob(b"also-bounded")
    monkeypatch.setattr(
        _msgpack,
        "_MAX_BLOB_MAPPED_BYTES_PER_MESSAGE",
        32 + len(source) + 32 + len(other) - 1,
    )
    with pytest.raises(TypeError, match="aggregate limit"):
        handle.count_blobs([source, other])
    assert service.calls == before
    source.close()
    other.close()


def test_public_from_descriptor_is_process_bounded():
    checked = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import tinyray;"
                "n=tinyray._tinyray.BLOB_MAX_DECODED_HANDLES;"
                "source=tinyray.BlobRef.create(b'x');"
                "items=[tinyray.BlobRef.from_descriptor(source.descriptor()) for _ in range(n)];"
                "\ntry:\n"
                " tinyray.BlobRef.from_descriptor(source.descriptor())\n"
                "except tinyray.BlobError:\n"
                " print('bounded')\n"
                "else:\n"
                " raise SystemExit('descriptor opens were not bounded')\n"
            ),
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert checked.returncode == 0, checked.stderr
    assert checked.stdout.strip() == "bounded"


def _mutated_descriptor(blob, **changes):
    descriptor = msgspec.msgpack.decode(blob.descriptor())
    descriptor.update(changes)
    return msgspec.msgpack.encode(descriptor)


def test_descriptor_rejects_boot_inode_fd_size_and_reuse():
    blob = tinyray.blob(b"verified")
    descriptor = msgspec.msgpack.decode(blob.descriptor())
    boot = list(descriptor["boot"])
    boot[0] ^= 1
    with pytest.raises(tinyray.BlobError, match="different"):
        tinyray.BlobRef.from_descriptor(_mutated_descriptor(blob, boot=boot))
    with pytest.raises(tinyray.BlobError, match="closed|reused|device"):
        tinyray.BlobRef.from_descriptor(_mutated_descriptor(blob, inode=descriptor["inode"] ^ 1))
    with pytest.raises(tinyray.BlobError, match="closed|reused|device"):
        tinyray.BlobRef.from_descriptor(_mutated_descriptor(blob, device=descriptor["device"] ^ 1))
    token = list(descriptor["token"])
    token[0] ^= 1
    with pytest.raises(tinyray.BlobError, match="header|token"):
        tinyray.BlobRef.from_descriptor(_mutated_descriptor(blob, token=token))
    with pytest.raises(tinyray.BlobError):
        tinyray.BlobRef.from_descriptor(_mutated_descriptor(blob, fd=2**31 - 1))
    with pytest.raises(tinyray.BlobError, match="limit"):
        tinyray.BlobRef.from_descriptor(_mutated_descriptor(blob, size=tinyray.MAX_BLOB_BYTES + 1))
    with pytest.raises(tinyray.BlobError, match="protocol"):
        tinyray.BlobRef.from_descriptor(
            _mutated_descriptor(blob, version=descriptor["version"] + 1)
        )
    with pytest.raises(tinyray.BlobError, match="4096"):
        tinyray.BlobRef.from_descriptor(b"x" * 4097)
    with pytest.raises(tinyray.BlobError, match="trailing"):
        tinyray.BlobRef.from_descriptor(blob.descriptor() + b"\x00")
    with pytest.raises(tinyray.BlobError, match="limit"):
        tinyray.BlobRef.from_descriptor(blob.descriptor(), max_bytes=len(blob) - 1)
    payload = b"unsealed"
    with tempfile.TemporaryFile() as file:
        file.write(
            b"TRBLOB01" + bytes(descriptor["token"]) + struct.pack(">Q", len(payload)) + payload
        )
        file.flush()
        metadata = os.fstat(file.fileno())
        with pytest.raises(tinyray.BlobError, match="sealed"):
            tinyray.BlobRef.from_descriptor(
                _mutated_descriptor(
                    blob,
                    fd=file.fileno(),
                    size=len(payload),
                    device=metadata.st_dev,
                    inode=metadata.st_ino,
                )
            )

    stale = tinyray.blob(b"stale")
    stale_descriptor = stale.descriptor()
    old_fd = msgspec.msgpack.decode(stale_descriptor)["fd"]
    stale.close()
    reused = []
    try:
        while True:
            fd = os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)
            reused.append(fd)
            if fd == old_fd or len(reused) > 128:
                break
        with pytest.raises(tinyray.BlobError):
            tinyray.BlobRef.from_descriptor(stale_descriptor)
    finally:
        for fd in reused:
            os.close(fd)
        blob.close()


def test_invalid_blob_descriptor_is_rejected_before_method_invocation(blob_peer):
    service, handle = blob_peer
    before = service.calls
    malformed = msgspec.msgpack.encode(
        {
            "args": [msgspec.msgpack.Ext(124, b"not-a-descriptor")],
            "kwargs": {},
        }
    )
    outcome = tinyray._tinyray.rpc_call_sync(
        handle.url,
        "bad-blob",
        "caller/0#1",
        handle.identity,
        malformed,
        1000,
        method="blob_len",
    )
    assert outcome.status == tinyray._tinyray.RPC_STATUS_CALLER_FAULT
    assert service.calls == before


def test_prepared_request_retains_blob_until_delivery(blob_peer):
    _, handle = blob_peer
    value = tinyray.blob(b"prepared")
    descriptor = value.descriptor()
    payload = {"args": [value], "kwargs": {}}
    endpoint, body, request, keepalive = tinyray._rpc._prepare(handle, "blob_len", payload)
    del payload, value
    gc.collect()

    opened = tinyray.BlobRef.from_descriptor(descriptor)
    opened.close()
    outcome = tinyray._rpc._native_sync(
        handle,
        "blob_len",
        body,
        request,
        1.0,
        None,
    )
    assert endpoint == handle.url
    assert outcome.status == tinyray._tinyray.RPC_STATUS_SUCCESS
    assert len(keepalive) == 1
    keepalive[0].close()


def test_serialization_collects_supported_nested_container_subclasses():
    blobs = [tinyray.blob(bytes([index])) for index in range(6)]
    payload = {
        "named": BlobTuple("named", blobs[0]),
        "dataclass": BlobBox("data", blobs[1]),
        "dict": {"blob": blobs[2]},
        "tuple": (blobs[3],),
        "list": [blobs[4]],
        "sets": ({blobs[5]}, frozenset({blobs[5]})),
    }
    _, owners = _msgpack.dumps_with_blob_refs(payload)
    assert {id(owner) for owner in owners} == {id(blob) for blob in blobs}

    cycle = [blobs[0]]
    cycle.append(cycle)
    assert _msgpack.blob_refs(cycle) == (blobs[0],)
    with pytest.raises((ValueError, RecursionError)):
        _msgpack.dumps_with_blob_refs(cycle)
    for blob in blobs:
        blob.close()


def _delayed_handle(peer):
    return tinyray.Handle(
        "delayed-blob",
        {
            "id": 0,
            "slot": 0,
            "incarnation": 1,
            "url": peer.endpoint,
            "ready": True,
        },
        ("consume",),
    )


def _temporary_blob_payload():
    return {
        "args": [
            {
                "named": BlobTuple(
                    "temporary",
                    tinyray.blob(b"delayed-owner"),
                )
            }
        ],
        "kwargs": {},
    }


def test_sync_timeout_retains_temporary_blob_until_delayed_decode():
    peer = _DelayedBlobPeer()
    handle = _delayed_handle(peer)
    with pytest.raises(tinyray.OutcomeUnknown):
        tinyray._rpc.invoke(handle, "consume", _temporary_blob_payload(), 0.05)
    assert peer.received.wait(1)
    gc.collect()
    peer.release.set()
    peer.join()
    assert peer.descriptor is not None
    _assert_descriptor_eventually_stale(peer.descriptor)


def test_async_cancellation_retains_temporary_blob_until_delayed_decode():
    peer = _DelayedBlobPeer()
    handle = _delayed_handle(peer)

    async def drive():
        pending = asyncio.create_task(
            tinyray._rpc.ainvoke(handle, "consume", _temporary_blob_payload(), 5)
        )
        assert await asyncio.to_thread(peer.received.wait, 1)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        del pending
        gc.collect()
        peer.release.set()
        assert await asyncio.to_thread(peer.done.wait, 5)

    asyncio.run(drive())
    peer.join()
    assert peer.descriptor is not None
    _assert_descriptor_eventually_stale(peer.descriptor)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
def test_fork_clears_native_pending_blob_owners_before_runtime_forget():
    gc.collect()
    assert _memfd_counts() == (0, 0)
    parent_blob = tinyray.BlobRef.create(b"parent-stays-valid")
    parent_view = parent_blob.view()
    peer = _DelayedBlobPeer()
    handle = _delayed_handle(peer)
    result = []

    def call():
        result.append(tinyray._rpc.invoke(handle, "consume", _temporary_blob_payload(), 5))

    caller = threading.Thread(target=call)
    caller.start()
    assert peer.received.wait(1)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        before_release = _memfd_counts()
        parent_view.release()
        after_release = _memfd_counts()
        os.write(write_fd, repr((before_release, after_release)).encode())
        os._exit(0)
    os.close(write_fd)
    assert os.read(read_fd, 128) == b"((1, 1), (0, 0))"
    os.waitpid(pid, 0)
    assert bytes(parent_view) == b"parent-stays-valid"
    peer.release.set()
    caller.join(5)
    assert not caller.is_alive()
    assert result == [None]
    peer.join()
    parent_view.release()
    parent_blob.close()


def test_python_rpc_sync_async_batch_retention_and_cancellation(blob_peer):
    service, handle = blob_peer
    assert handle.blob_len(tinyray.blob(b"temporary")) == len(b"temporary")
    blob = tinyray.blob(b"a" * (1 << 20))
    assert handle.blob_len(blob) == len(blob)
    assert handle.typed_blob(BlobBox("one", blob)) == len(blob)
    received = handle.echo_blob(blob)
    assert type(received) is tinyray.BlobRef
    assert bytes(received) == bytes(blob)
    assert tinyray.batch(
        handle,
        [
            tinyray.Call("blob_len", (blob,)),
            tinyray.Call("retain_blob", (blob,)),
        ],
    ) == [len(blob), len(blob)]
    batch_blobs = tinyray.batch(
        handle,
        [
            tinyray.Call("echo_blob", (blob,)),
            tinyray.Call("make_blob"),
        ],
    )
    assert [bytes(value) for value in batch_blobs] == [
        b"a" * (1 << 20),
        b"made-by-handler",
    ]
    for value in batch_blobs:
        value.close()
    blob.close()
    assert bytes(received) == b"a" * (1 << 20)
    assert handle.retained_bytes() == b"a" * (1 << 20)
    forwarded = handle.retained_blob()
    assert bytes(forwarded) == b"a" * (1 << 20)
    forwarded.close()

    async def cancel_after_mapping():
        source = tinyray.blob(b"cancelled")
        task = asyncio.create_task(tinyray.apool("blob-service").slot(0).retain_slow(source, 0.2))
        assert await asyncio.to_thread(service.mapped.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        source.close()
        await asyncio.sleep(0.25)
        assert await tinyray.apool("blob-service").slot(0).retained_bytes() == b"cancelled"

    service.mapped.clear()
    asyncio.run(cancel_after_mapping())
    received.close()


def test_concurrent_readers_share_mapping(blob_peer):
    _, handle = blob_peer
    source = tinyray.blob(bytes(range(251)) * 4096)
    retained = handle.echo_blob(source)

    def checksum():
        return sum(retained.view())

    with ThreadPoolExecutor(max_workers=32) as workers:
        results = list(workers.map(lambda _: checksum(), range(128)))
    assert len(set(results)) == 1
    source.close()
    assert checksum() == results[0]
    retained.close()


def test_blob_response_ack_releases_temporary_service_owners(blob_peer):
    _, handle = blob_peer
    made = handle.make_blob()
    assert bytes(made) == b"made-by-handler"
    made.close()
    assert handle.blob_len(tinyray.blob(b"warm")) == 4
    deadline = time.monotonic() + 2
    baseline = _resource_counts()
    for _ in range(32):
        made = handle.make_blob()
        assert bytes(made) == b"made-by-handler"
        made.close()
    while _resource_counts() != baseline:
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_native_raw_reply_guard_survives_client_and_server_idle_deadlines(blob_peer):
    _, handle = blob_peer
    tinyray._tinyray.rpc_debug_clear_pools()
    outcome = tinyray._tinyray.rpc_call_sync(
        handle.url,
        "held-raw-blob",
        "caller/0#1",
        handle.identity,
        _msgpack.dumps({"args": [], "kwargs": {}}),
        30_000,
        method="make_blob",
    )
    assert outcome.status == tinyray._tinyray.RPC_STATUS_SUCCESS
    time.sleep(16)
    assert tinyray._tinyray.rpc_debug_state()["connections"] == 1
    value = _msgpack.loads(bytes(outcome.payload))
    assert bytes(value) == b"made-by-handler"
    value.close()
    del outcome
    gc.collect()

    deadline = time.monotonic() + 12
    while tinyray._tinyray.rpc_debug_state()["connections"] != 0:
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_late_blob_replies_are_acked_after_timeout_and_async_cancellation(blob_peer):
    service, handle = blob_peer
    for _ in range(130):
        service.mapped.clear()
        with pytest.raises(tinyray.OutcomeUnknown):
            handle.delayed_blob.timeout(0.002)(0.01)
        assert service.mapped.wait(1)
        time.sleep(0.012)

    async def cancel_repeatedly():
        async_handle = tinyray.apool("blob-service").slot(0)
        for _ in range(130):
            service.mapped.clear()
            pending = asyncio.create_task(async_handle.delayed_blob(0.01))
            assert await asyncio.to_thread(service.mapped.wait, 1)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            await asyncio.sleep(0.012)

    asyncio.run(cancel_repeatedly())
    deadline = time.monotonic() + 3
    while service.server.counters.snapshot()["unacked_blob_refs"] != 0:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    probe = tinyray.blob(b"still-reusable")
    assert handle.blob_len(probe) == len(probe)
    probe.close()


def test_partial_batch_missing_method_keeps_completed_blob(blob_peer):
    _, handle = blob_peer
    with pytest.raises(tinyray.BatchError) as caught:
        tinyray.batch(
            handle,
            [
                tinyray.Call("make_blob"),
                tinyray.Call("missing"),
            ],
        )
    assert isinstance(caught.value.cause, AttributeError)
    completed = caught.value.completed_results
    assert len(completed) == 1
    assert bytes(completed[0]) == b"made-by-handler"
    completed[0].close()


def test_partial_batch_fencing_keeps_completed_blob():
    service = BlobService()
    server = tinyray._serve.MethodServer(service, "fenced-blob/0#1", host="127.0.0.1")
    server.still_ours = lambda: service.calls == 0
    handle = tinyray.Handle(
        "fenced-blob",
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
        with pytest.raises(tinyray.BatchError) as caught:
            tinyray.batch(
                handle,
                [
                    tinyray.Call("make_blob"),
                    tinyray.Call("make_blob"),
                ],
            )
        assert isinstance(caught.value.cause, tinyray.Fenced)
        completed = caught.value.completed_results
        assert len(completed) == 1
        assert bytes(completed[0]) == b"made-by-handler"
        completed[0].close()
    finally:
        server.close()


OWNER = """
import base64, os, sys, tinyray
b = tinyray.blob(b'owner-crash')
print(base64.b64encode(b.descriptor()).decode(), flush=True)
if len(sys.argv) > 1:
    sys.stdin.readline()
os._exit(0)
"""


def test_owner_exit_and_crash_cleanup():
    exited = subprocess.run(
        [sys.executable, "-c", OWNER],
        capture_output=True,
        text=True,
        timeout=10,
    )
    descriptor = base64.b64decode(exited.stdout.strip())
    with pytest.raises(tinyray.BlobError):
        tinyray.BlobRef.from_descriptor(descriptor)

    owner = subprocess.Popen(
        [sys.executable, "-c", OWNER, "wait"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    descriptor = base64.b64decode(owner.stdout.readline().strip())
    received = tinyray.BlobRef.from_descriptor(descriptor)
    owner.stdin.write("\n")
    owner.stdin.flush()
    owner.wait(timeout=5)
    assert bytes(received) == b"owner-crash"
    received.close()


def test_forwarded_descriptor_uses_receiver_fd_while_original_owner_is_live():
    owner = subprocess.Popen(
        [sys.executable, "-c", OWNER, "wait"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    original = base64.b64decode(owner.stdout.readline().strip())
    received = tinyray.BlobRef.from_descriptor(original)
    forwarded = base64.b64encode(received.descriptor()).decode()
    owner.stdin.write("\n")
    owner.stdin.flush()
    owner.wait(timeout=5)

    opened = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import base64,sys,tinyray;"
                "b=tinyray.BlobRef.from_descriptor(base64.b64decode(sys.argv[1]));"
                "print(bytes(b).decode())"
            ),
            forwarded,
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert opened.returncode == 0, opened.stderr
    assert opened.stdout.strip() == "owner-crash"
    received.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
def test_forked_child_closes_blob_handles_without_harming_parent():
    blob = tinyray.blob(b"fork")
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        os.write(write_fd, str(blob.closed).encode())
        os._exit(0)
    os.close(write_fd)
    assert os.read(read_fd, 32) == b"True"
    os.waitpid(pid, 0)
    assert bytes(blob) == b"fork"
    blob.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
def test_public_blobref_constructors_are_registered_for_fork_cleanup():
    created = tinyray.BlobRef.create(b"created")
    source = tinyray.BlobRef.create(b"opened")
    opened = tinyray.BlobRef.from_descriptor(source.descriptor())
    view = opened.view()
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        before_release = (created.closed, opened.closed, bytes(view))
        view.release()
        after_release = opened.closed
        os.write(
            write_fd,
            repr((before_release, after_release)).encode(),
        )
        os._exit(0)
    os.close(write_fd)
    child = os.read(read_fd, 256)
    os.waitpid(pid, 0)
    assert child == b"((True, False, b'opened'), True)"
    assert bytes(created) == b"created"
    assert bytes(view) == b"opened"
    view.release()
    created.close()
    opened.close()
    source.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
def test_exported_child_blobref_serializes_child_owned_descriptor():
    source = tinyray.BlobRef.create(b"forward-from-child")
    opened = tinyray.BlobRef.from_descriptor(source.descriptor())
    view = opened.view()
    descriptor_pipe = os.pipe()
    release_pipe = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(descriptor_pipe[0])
        os.close(release_pipe[1])
        descriptor = opened.descriptor()
        os.write(
            descriptor_pipe[1],
            struct.pack(">I", len(descriptor)) + descriptor,
        )
        os.read(release_pipe[0], 1)
        view.release()
        os._exit(0)

    os.close(descriptor_pipe[1])
    os.close(release_pipe[0])
    with os.fdopen(descriptor_pipe[0], "rb") as reader:
        length = struct.unpack(">I", reader.read(4))[0]
        descriptor = reader.read(length)
    view.release()
    opened.close()
    source.close()
    forwarded = tinyray.BlobRef.from_descriptor(descriptor)
    assert bytes(forwarded) == b"forward-from-child"
    forwarded.close()
    os.write(release_pipe[1], b"x")
    os.close(release_pipe[1])
    os.waitpid(pid, 0)


def test_blobref_fds_and_mappings_are_released():
    def counts():
        gc.collect()
        fds = len(os.listdir("/proc/self/fd"))
        maps = sum("memfd:tinyray-blob" in line for line in open("/proc/self/maps"))
        return fds, maps

    before = counts()
    blob = tinyray.blob(b"x" * (1 << 20))
    received = tinyray.BlobRef.from_descriptor(blob.descriptor())
    assert counts()[0] >= before[0] + 2
    received.close()
    blob.close()
    del received, blob
    assert counts() == before
