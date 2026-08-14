"""Driving a native trainer and a native inference server from one controller.

The shape an RL post-training loop actually has, with tinyray doing only what a
control plane should:

* the trainer is an ordinary script with its own ``init_process_group``;
* the inference server is a process tinyray did not write at all;
* weights move between them however the frameworks prefer -- NCCL, CUDA IPC, a
  checkpoint on disk -- and never through tinyray.

Run with ``python examples/native_stack.py``. It uses gloo and a toy HTTP server
so it runs anywhere; the SGLang and Megatron equivalents differ only in the
command lines, which are noted in comments.
"""

from __future__ import annotations

import sys
import textwrap
import urllib.request
from pathlib import Path

import tinyray as tr

HERE = Path(__file__).resolve().parent

# --------------------------------------------------------------------------
# The trainer. In a real stack this file is your Megatron or DeepSpeed script,
# unchanged apart from the last two lines.
# --------------------------------------------------------------------------
TRAINER = textwrap.dedent(
    '''
    import torch
    import torch.distributed as dist


    class Trainer:
        def __init__(self):
            # Entirely yours. tinyray never calls this, and never takes the
            # default process group you are about to create.
            dist.init_process_group(backend="gloo")
            self.rank = dist.get_rank()
            self.weights = torch.zeros(16)
            self.version = 0

        def train_step(self, lr):
            grad = torch.full((16,), float(self.rank + 1) * lr)
            dist.all_reduce(grad, op=dist.ReduceOp.SUM)   # your collective
            self.weights += grad
            self.version += 1
            return {"rank": self.rank, "version": self.version}

        def export_weights(self):
            """Hand weights to the inference engine.

            Real stacks broadcast over NCCL, use CUDA IPC, or write a
            checkpoint. All of them bypass tinyray, which is the point: the
            control plane says *when*, the frameworks decide *how*.
            """
            return {"version": self.version, "checksum": float(self.weights.sum())}


    if __name__ == "__main__":
        import tinyray

        tinyray.serve(Trainer())
    '''
)

# --------------------------------------------------------------------------
# The inference server. Stands in for `python -m sglang.launch_server`, which
# tinyray supervises without knowing anything about it.
# --------------------------------------------------------------------------
SERVER = textwrap.dedent(
    """
    import http.server, json, sys, time

    port = int(sys.argv[1])
    print("loading model", flush=True)
    time.sleep(1.0)          # a real engine takes minutes

    state = {"version": 0}


    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
            elif self.path == "/generate":
                self.send_response(200); self.end_headers()
                self.wfile.write(json.dumps(state).encode())
            else:
                self.send_response(404); self.end_headers()

        def log_message(self, *a):
            pass


    print("server ready", flush=True)
    http.server.HTTPServer(("127.0.0.1", port), Handler).serve_forever()
    """
)


def main() -> None:
    trainer_path = HERE / "_generated_trainer.py"
    server_path = HERE / "_generated_server.py"
    trainer_path.write_text(TRAINER)
    server_path.write_text(SERVER)

    tr.init()
    try:
        # -- the trainer: a native script, given ranks and a control port ----
        #
        # For Megatron this becomes:
        #   [sys.executable, "pretrain_gpt.py", "--tensor-model-parallel-size", "8", ...]
        trainer = tr.launch_workers(
            [sys.executable, str(trainer_path)],
            size=4,
            name="trainer",
            gpus_per_worker=0.0,  # 1.0 in a real stack
            cpus_per_worker=0.1,
            startup_timeout=120,
        )
        print(f"trainer up: {trainer}")

        # -- the inference server: supervised, never imported ----------------
        #
        # For SGLang this becomes:
        #   ["python", "-m", "sglang.launch_server",
        #    "--model-path", MODEL, "--port", "{port}", "--tp", "4"]
        rollout = tr.launch_process(
            [sys.executable, str(server_path), "{port}"],
            name="rollout",
            num_gpus=0.0,  # 4.0 in a real stack
            num_cpus=0.1,
            # Readiness is observed. An engine binds its port long before it can
            # answer, and a controller that assumes otherwise fails its first
            # request.
            ready_when="http:/health",
            startup_timeout=120,
        )
        print(f"rollout up: {rollout.endpoint}")

        # -- the loop -------------------------------------------------------
        for iteration in range(3):
            # Dispatched to every rank, then awaited: a collective inside the
            # method only returns once all ranks have entered it.
            results = trainer.run("train_step", 0.1)
            exported = trainer.run_on(0, "export_weights")

            with urllib.request.urlopen(
                f"http://{rollout.endpoint}/generate", timeout=5
            ) as response:
                rollout_state = response.read().decode()

            print(
                f"iteration {iteration}: version={results[0]['version']} "
                f"checksum={exported['checksum']:.1f} rollout={rollout_state}"
            )

        moved = sum(peer["bytes_received"] for peer in tr.transport_stats().values())
        print(f"\ncontrol traffic through the driver: {moved:,} bytes")
        print("gradients and weights went nowhere near it.")
    finally:
        tr.shutdown()
        trainer_path.unlink(missing_ok=True)
        server_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
