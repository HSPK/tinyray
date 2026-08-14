"""Supervising processes tinyray did not write.

The capability that makes "control plane" more than a claim. An SGLang or vLLM
server is an ordinary process: it needs GPUs assigned, an environment injected,
readiness detected, logs labelled and its resources reclaimed when it stops.

Readiness is the part worth testing hardest. "The process is running" and "the
server can answer" are minutes apart for an inference engine, and a driver that
starts sending requests in that window sees connection refused and concludes
the cluster is broken.
"""

from __future__ import annotations

import sys
import textwrap
import time
import urllib.error
import urllib.request

import pytest

import tinyray
from tinyray.process import HttpOk, LogMatch, PortOpen, ProcessAlive, ready_when

PYTHON = sys.executable

#: A stand-in for an inference server: prints, waits, binds late, then serves.
SERVER = textwrap.dedent(
    """
    import http.server, sys, time
    port = int(sys.argv[1]); delay = float(sys.argv[2])
    print("loading model", flush=True)
    time.sleep(delay)
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if self.path == "/health" else 404)
            self.end_headers(); self.wfile.write(b"ok")
        def log_message(self, *a): pass
    print("server ready", flush=True)
    http.server.HTTPServer(("127.0.0.1", port), H).serve_forever()
    """
)


@pytest.fixture
def ray():
    tinyray.init()
    yield tinyray
    tinyray.shutdown()


def get(url: str, timeout: float = 5.0) -> int:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return int(response.status)


class TestReadiness:
    def test_http_readiness_waits_until_the_server_answers(self, ray):
        """Not merely until the process exists.

        The server sleeps before binding, so a check that only looked at
        liveness would report ready roughly a second too early and the caller's
        first request would fail.
        """
        started = time.perf_counter()
        server = tinyray.launch_process(
            [PYTHON, "-c", SERVER, "{port}", "1.0"],
            name="late-binder",
            ready_when="http:/health",
            startup_timeout=60,
        )
        elapsed = time.perf_counter() - started
        try:
            assert elapsed >= 1.0, (
                f"reported ready after {elapsed:.2f}s, before the server had bound; "
                "readiness is checking the wrong thing"
            )
            assert get(f"http://{server.endpoint}/health") == 200
        finally:
            tinyray.stop_process("late-binder")

    def test_port_readiness(self, ray):
        server = tinyray.launch_process(
            [PYTHON, "-c", SERVER, "{port}", "0.2"],
            name="port-ready",
            ready_when="port",
            startup_timeout=60,
        )
        try:
            assert server.port is not None
            assert get(f"http://{server.endpoint}/health") == 200
        finally:
            tinyray.stop_process("port-ready")

    def test_log_readiness(self, ray):
        server = tinyray.launch_process(
            [PYTHON, "-c", SERVER, "{port}", "0.3"],
            name="log-ready",
            ready_when="log:server ready",
            startup_timeout=60,
        )
        try:
            assert any("server ready" in line for line in server.recent_log())
        finally:
            tinyray.stop_process("log-ready")

    def test_alive_readiness_is_honest_about_being_weak(self, ray):
        # Returns almost immediately: it only claims the process started.
        started = time.perf_counter()
        process = tinyray.launch_process(
            [PYTHON, "-c", "import time; time.sleep(30)"],
            name="just-alive",
            ready_when="alive",
            allocate_port=False,
        )
        try:
            assert time.perf_counter() - started < 1.0
            assert process.is_alive()
        finally:
            tinyray.stop_process("just-alive")

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            ("port", PortOpen),
            ("http", HttpOk),
            ("http:/ready", HttpOk),
            ("log:started", LogMatch),
            ("alive", ProcessAlive),
        ],
    )
    def test_readiness_shorthands(self, spec, expected):
        assert isinstance(ready_when(spec), expected)

    def test_unknown_readiness_spec_is_refused(self):
        with pytest.raises(ValueError, match="unknown readiness spec"):
            ready_when("whenever")


class TestStartupFailures:
    def test_a_process_that_dies_reports_its_output(self, ray):
        """The log is the only useful artefact when a server dies on boot."""
        with pytest.raises(tinyray.ProcessStartupError) as excinfo:
            tinyray.launch_process(
                [PYTHON, "-c", "import sys; print('CUDA out of memory'); sys.exit(3)"],
                name="doomed",
                ready_when="http",
                startup_timeout=30,
            )
        message = str(excinfo.value)
        assert "exited with code 3" in message
        assert "CUDA out of memory" in message, "the child's output was swallowed"

    def test_a_process_that_never_becomes_ready_times_out(self, ray):
        with pytest.raises(tinyray.ProcessStartupError, match="did not become ready"):
            tinyray.launch_process(
                [PYTHON, "-c", "import time; time.sleep(60)"],
                name="never-ready",
                ready_when="port",
                startup_timeout=2,
            )

    def test_a_missing_executable_says_so(self, ray):
        with pytest.raises(tinyray.ProcessStartupError, match="was not found on PATH"):
            tinyray.launch_process(
                ["definitely-not-a-real-binary"], name="missing", ready_when="alive"
            )

    def test_a_failed_start_returns_its_resources(self, ray):
        # A process that never came up must not keep its GPUs reserved; a
        # sweep would otherwise leak the cluster away one failure at a time.
        before = tinyray.nodes()[0]["available_cpus"]
        with pytest.raises(tinyray.ProcessStartupError):
            tinyray.launch_process(
                [PYTHON, "-c", "import sys; sys.exit(1)"],
                name="leaky",
                num_cpus=2.0,
                ready_when="port",
                startup_timeout=5,
            )
        assert tinyray.nodes()[0]["available_cpus"] == before


class TestPortAndEnvironment:
    def test_the_port_placeholder_is_substituted(self, ray):
        server = tinyray.launch_process(
            [PYTHON, "-c", SERVER, "{port}", "0.1"],
            name="substituted",
            ready_when="http:/health",
            startup_timeout=60,
        )
        try:
            assert str(server.port) in server.command
            assert server.endpoint == f"127.0.0.1:{server.port}"
        finally:
            tinyray.stop_process("substituted")

    def test_environment_is_injected_and_substituted(self, ray):
        reporter = textwrap.dedent(
            """
            import os, sys, time
            sys.stderr.write(f"MODEL={os.environ.get('MODEL')} "
                             f"URL={os.environ.get('URL')} "
                             f"CUDA={os.environ.get('CUDA_VISIBLE_DEVICES')!r}\\n")
            sys.stderr.flush()
            time.sleep(5)
            """
        )
        process = tinyray.launch_process(
            [PYTHON, "-c", reporter],
            name="env-report",
            env={"MODEL": "llama", "URL": "http://127.0.0.1:{port}"},
            ready_when="log:MODEL=",
            startup_timeout=30,
        )
        try:
            log = "".join(process.recent_log())
            assert "MODEL=llama" in log
            assert f"URL=http://127.0.0.1:{process.port}" in log
            # Set even with no GPUs, so the child never inherits the driver's.
            assert "CUDA=''" in log
        finally:
            tinyray.stop_process("env-report")


class TestResourceAccounting:
    def test_processes_and_actors_share_one_scheduler(self, ray):
        """The reason this lives in the head rather than in a helper.

        A trainer actor and an inference server must not be handed the same
        GPU, and only a single scheduler can promise that.
        """
        before = tinyray.nodes()[0]["available_cpus"]
        process = tinyray.launch_process(
            [PYTHON, "-c", "import time; time.sleep(30)"],
            name="accounted",
            num_cpus=2.0,
            ready_when="alive",
            allocate_port=False,
        )
        try:
            assert tinyray.nodes()[0]["available_cpus"] == before - 2.0
            assert process.name in [p.name for p in tinyray.processes()]
        finally:
            tinyray.stop_process("accounted")
        assert tinyray.nodes()[0]["available_cpus"] == before

    def test_an_impossible_request_is_refused(self, ray):
        with pytest.raises(tinyray.PlacementFailed):
            tinyray.launch_process(
                [PYTHON, "-c", "pass"], name="greedy", num_cpus=1e9, ready_when="alive"
            )

    def test_stopping_removes_it_from_the_registry(self, ray):
        tinyray.launch_process(
            [PYTHON, "-c", "import time; time.sleep(30)"],
            name="transient",
            ready_when="alive",
            allocate_port=False,
        )
        assert "transient" in [p.name for p in tinyray.processes()]
        tinyray.stop_process("transient")
        assert "transient" not in [p.name for p in tinyray.processes()]


class TestLifecycle:
    def test_shutdown_stops_supervised_processes(self):
        tinyray.shutdown()
        tinyray.init()
        process = tinyray.launch_process(
            [PYTHON, "-c", "import time; time.sleep(120)"],
            name="outlives-nothing",
            ready_when="alive",
            allocate_port=False,
        )
        pid = process.pid
        tinyray.shutdown()

        # An inference server holds GPU memory until it exits; leaving one
        # behind strands a card until somebody notices.
        deadline = time.monotonic() + 20
        import os

        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.1)
        else:
            pytest.fail("a supervised process outlived tinyray.shutdown()")

    def test_reap_reports_exits(self, ray):
        process = tinyray.launch_process(
            [PYTHON, "-c", "import sys; sys.exit(0)"],
            name="short-lived",
            ready_when="alive",
            allocate_port=False,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and process.is_alive():
            time.sleep(0.05)
        assert not process.is_alive()
        assert process.exit_code() == 0
