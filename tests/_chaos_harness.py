"""Shared harness for the real-RNS chaos tests (``test_chaos.py``).

Spawns ICN nodes as separate OS processes (each its own Reticulum instance) over
localhost TCP, the same way ``test_integration.py`` does — the parent test
process never initialises RNS, because ``RNS.Reticulum`` is a hard process
singleton. On top of that this adds what chaos testing needs: killing and
restarting a node, and driving the interactive client over stdin.

A background reader thread drains each child's stdout into a queue, so the test
can wait for a tagged line (``ROUTER_READY``, ``FETCH_RESULT`` …) with a real
timeout while harmlessly skipping interleaved RNS log lines — and never
deadlocks on a full pipe.
"""

from __future__ import annotations

import json
import os
import queue
import socket
import subprocess
import threading
import time

REPO_ROOT = os.path.dirname(os.path.dirname(__file__))

ORIGIN_SCRIPT = os.path.join(os.path.dirname(__file__), "_chaos_origin.py")
ROUTER_SCRIPT = os.path.join(os.path.dirname(__file__), "_chaos_router.py")
CLIENT_SCRIPT = os.path.join(os.path.dirname(__file__), "_chaos_client.py")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def expected_content(label: str) -> bytes:
    """Mirror of ``_chaos_origin.content_for`` for the test side."""
    return b"chaos-content-" + label.encode()


class Node:
    """A child ICN process with a background stdout-draining reader.

    ``read_until`` waits for the next line beginning with one of ``prefixes``
    (returning the JSON payload that follows it), raising on an ``*_ERROR`` line,
    early process exit, or timeout.
    """

    def __init__(self, argv: list[str]):
        env = dict(os.environ)
        env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        self.argv = argv
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            text=True,
            bufsize=1,
        )
        self._lines: queue.Queue[str | None] = queue.Queue()
        self._reader = threading.Thread(target=self._drain, daemon=True)
        self._reader.start()

    def _drain(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self._lines.put(line)
        self._lines.put(None)  # EOF sentinel

    def read_until(self, *prefixes: str, timeout: float = 60.0) -> dict:
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"timed out waiting for {prefixes!r} from {self.argv[1]!r}"
                )
            try:
                line = self._lines.get(timeout=remaining)
            except queue.Empty:
                continue
            if line is None:
                raise RuntimeError(
                    f"process exited before {prefixes!r} ({self.argv[1]!r})"
                )
            if "_ERROR" in line:
                raise RuntimeError(f"node error: {line.strip()}")
            for prefix in prefixes:
                if line.startswith(prefix):
                    return json.loads(line[len(prefix):])

    def send(self, line: str) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(line.rstrip("\n") + "\n")
        self.proc.stdin.flush()

    def fetch(self, label: str, lifetime_ms: int = 15000,
              timeout: float | None = None) -> dict:
        """Drive an interactive FETCH and return its FETCH_RESULT payload."""
        self.send(f"FETCH {label} {lifetime_ms}")
        # Allow the Interest its full lifetime plus slack to come back.
        return self.read_until(
            "FETCH_RESULT ", timeout=timeout or (lifetime_ms / 1000 + 15)
        )

    def is_alive(self) -> bool:
        return self.proc.poll() is None

    def stop(self, timeout: float = 10.0) -> None:
        """Graceful terminate, escalating to kill."""
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=timeout)

    def kill(self) -> None:
        """Hard kill — simulates a crash with no graceful shutdown."""
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
