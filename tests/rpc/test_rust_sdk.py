"""The public Rust SDK shares membership and method RPC with Python."""

from __future__ import annotations

import asyncio
import json
import pathlib
import subprocess

import pytest
import tinyray

ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def rust_examples():
    subprocess.run(
        [
            "cargo",
            "build",
            "-q",
            "-p",
            "tinyray",
            "--example",
            "rust_service",
            "--example",
            "rust_client",
            "--example",
            "rust_blob_client",
        ],
        cwd=ROOT,
        check=True,
        timeout=180,
    )
    return (
        ROOT / "target/debug/examples/rust_service",
        ROOT / "target/debug/examples/rust_client",
        ROOT / "target/debug/examples/rust_blob_client",
    )


class PythonPeer:
    def ping_raw(self):
        return "pong"

    def ping(self):
        return "pong"

    def echo(self, value):
        return value

    def echo_bytes(self, value: bytes):
        return value

    def blob_len(self, value: tinyray.BlobRef):
        return len(value)

    def bytes_len(self, value: bytes):
        return len(value)

    def echo_blob(self, value: tinyray.BlobRef):
        return value

    def make_blob(self):
        return tinyray.blob(b"made-by-python")

    def retain_blob(self, value: tinyray.BlobRef):
        self.retained = value
        return len(value)

    def retained_len(self):
        return len(self.retained)

    def retained_blob(self):
        return self.retained

    def job(self, value):
        return value

    def context(self, context: tinyray.CallContext):
        return {"caller": context.identity, "request_id": context.request_id}

    def sleep_ms(self, millis: int):
        return millis

    def fail(self):
        raise RuntimeError("expected Python failure")


def test_python_handle_calls_rust_service_and_mixed_pool(registry, rust_examples):
    rust_service, _, rust_blob_client = rust_examples
    proc = subprocess.Popen(
        [str(rust_service), registry.endpoint, "mixed-sdk", "0", "2"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout.readline().startswith("READY ")
        with tinyray.join(
            "mixed-sdk",
            "stateful",
            slot=1,
            size=2,
            serves=PythonPeer(),
            coalesce_ms=0,
        ) as python_member:
            python_member.ready(language="python").flush(timeout=5)
            pool = tinyray.pool("mixed-sdk")
            members = pool.wait(count=2, timeout=15)
            assert {member.state["language"] for member in members} == {"python", "rust"}
            assert pool.slot(0).echo({"from": "python"}) == {"from": "python"}
            assert pool.slot(1).echo({"from": "python"}) == {"from": "python"}
            with tinyray.request_id("python-to-rust"):
                context = pool.slot(0).context()
            assert context == {
                "caller": python_member.identity,
                "request_id": "python-to-rust",
            }
            with pytest.raises(tinyray.RemoteError) as caught:
                pool.slot(0).fail()
            assert caught.value.type == "RustError"

            async def call_async():
                return await tinyray.apool("mixed-sdk").slot(0).sleep_ms(7)

            assert asyncio.run(call_async()) == 7
            source = tinyray.blob(b"python-to-rust")
            assert pool.slot(0).blob_len(source) == len(source)
            received = pool.slot(0).echo_blob(source)
            assert pool.slot(0).retain_blob(source) == len(source)
            batch_blobs = tinyray.batch(
                pool.slot(0),
                [
                    tinyray.Call("echo_blob", (source,)),
                    tinyray.Call("make_blob"),
                ],
            )
            assert [bytes(value) for value in batch_blobs] == [
                b"python-to-rust",
                b"made-by-rust",
            ]
            for value in batch_blobs:
                value.close()
            source.close()
            assert bytes(received) == b"python-to-rust"
            received.close()
            retained = pool.slot(0).retained_blob()
            assert bytes(retained) == b"python-to-rust"
            retained.close()
            rust_round_trip = subprocess.run(
                [
                    str(rust_blob_client),
                    pool.slot(0).url,
                    pool.slot(0).identity,
                    str(1 << 20),
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            assert rust_round_trip.returncode == 0, rust_round_trip.stderr
            assert int(rust_round_trip.stdout) == 1 << 20
            assert set(pool.slot(0)._methods) == {
                "context",
                "blob_len",
                "bytes_len",
                "echo",
                "echo_blob",
                "echo_bytes",
                "fail",
                "job",
                "make_blob",
                "ping",
                "ping_raw",
                "retain_blob",
                "retained_blob",
                "retained_len",
                "sleep_ms",
            }
    finally:
        if proc.stdin is not None:
            proc.stdin.write("\n")
            proc.stdin.flush()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        assert proc.returncode == 0, proc.stderr.read()[-1000:]


def test_rust_client_calls_python_service(registry, rust_examples):
    _, rust_client, rust_blob_client = rust_examples
    with tinyray.join(
        "python-for-rust",
        "stateful",
        slot=0,
        size=1,
        serves=PythonPeer(),
        coalesce_ms=0,
    ) as member:
        member.ready(language="python").flush(timeout=5)
        handle = tinyray.pool(member.pool).slot(0)
        completed = subprocess.run(
            [
                str(rust_client),
                handle.url,
                handle.identity,
                json.dumps({"from": "rust"}),
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert completed.returncode == 0, completed.stderr
        assert json.loads(completed.stdout) == {"from": "rust"}
        blob = subprocess.run(
            [str(rust_blob_client), handle.url, handle.identity, str(1 << 20)],
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert blob.returncode == 0, blob.stderr
        assert int(blob.stdout) == 1 << 20
