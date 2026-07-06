"""End-to-end integration test over a real two-node RNS mesh.

Spawns a separate ICN origin server in its own process and Reticulum instance
(``tests/_icn_origin.py``), which connects over localhost TCP into this
process's shared Reticulum instance (the ``shared_rns`` fixture), then fetches
content end-to-end. This exercises the real RNS stack — interfaces, announces,
path resolution, Links, Channels, LinkFace and the Forwarder — rather than the
in-memory ``TestFace``.

In-process loopback is impossible: RNS has no path to a destination living in
the same instance, so two genuine instances (two processes) are required.

Gated behind ``RNS_INTEGRATION=1`` because it spins up Reticulum and is slow::

    RNS_INTEGRATION=1 python -m pytest tests/test_integration.py -v
"""

import asyncio
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile

import pytest
import RNS

from rns_icn.config import KnownPeer, ServerConfig
from rns_icn.name import Name
from rns_icn.packet import Interest
from rns_icn.rns_server import ICNServer

APP_NAME = "icn"
ASPECT = "default"
EXPECTED = b"Hello from ICN server A!"

ORIGIN_SCRIPT = os.path.join(os.path.dirname(__file__), "_icn_origin.py")
ROUTER_SCRIPT = os.path.join(os.path.dirname(__file__), "_icn_router.py")
CLIENT_SCRIPT = os.path.join(os.path.dirname(__file__), "_icn_client.py")
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.skipif(
    not os.environ.get("RNS_INTEGRATION"),
    reason="Set RNS_INTEGRATION=1 to run integration tests",
)
class TestRNSIntegration:
    """Two RNS instances (two processes) over TCP exchange Interest/Data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmproot = tempfile.mkdtemp(prefix="rns_icn_itest_")
        self.origin_cfg = os.path.join(self.tmproot, "origin")
        self.client_cfg = os.path.join(self.tmproot, "client")
        os.makedirs(self.client_cfg)
        self.proc = None
        yield
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        shutil.rmtree(self.tmproot, ignore_errors=True)

    def _start_origin(self, port: int) -> dict:
        """Launch the origin subprocess (dialling into this process's shared
        Reticulum on ``port``) and wait for its ORIGIN_READY line."""
        env = dict(os.environ)
        env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        self.proc = subprocess.Popen(
            [sys.executable, ORIGIN_SCRIPT, self.origin_cfg, str(port), "--connect"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            text=True,
        )
        # Read stdout until the handshake line appears (or process dies).
        while True:
            line = self.proc.stdout.readline()
            if line == "":
                raise RuntimeError("Origin subprocess exited before becoming ready")
            if line.startswith("ORIGIN_READY "):
                return json.loads(line[len("ORIGIN_READY "):])

    @pytest.mark.asyncio
    async def test_fetch_over_tcp_mesh(self, shared_rns):
        # The origin dials into this process's shared Reticulum instance —
        # RNS.Reticulum() is a process singleton, so the test process can
        # never bring up a per-test instance of its own.
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, self._start_origin, shared_rns.port)
        origin_hexhash = info["hexhash"]
        origin_identity_path = info["identity_path"]

        origin_identity = RNS.Identity.from_file(origin_identity_path)
        assert origin_identity is not None, "Could not load origin identity"
        origin_addr = origin_identity.hash

        server = ICNServer(
            ServerConfig(
                identity_path=os.path.join(self.client_cfg, "client_identity"),
                app_name=APP_NAME,
                aspect=ASPECT,
                cs_path=os.path.join(self.client_cfg, "client_cs.db"),
                announce_interval=5.0,
                http_enabled=False,
                known_peers=[
                    KnownPeer(
                        name="origin",
                        destination_hash=origin_hexhash,
                        identity_path=origin_identity_path,
                    )
                ],
            )
        )
        await server.start()
        try:
            # Establish a Link to the origin over the TCP mesh.
            face_id = await server.connect(origin_hexhash)
            assert face_id is not None, "Failed to establish link to origin"

            server.forwarder.add_route(Name(origin_addr), face_id, 10)

            # Manifest fetch (discovery path).
            manifest = await server.forwarder.express(
                Interest(name=Name(origin_addr, [b"manifest"]))
                .with_can_be_prefix()
                .with_lifetime(15000),
                0,
            )
            assert manifest is not None, "Failed to fetch manifest from origin"

            # Content fetch over the real Link/Channel.
            hello = Name(origin_addr, [b"hello"])
            result = await server.forwarder.express(
                Interest(name=hello, lifetime_ms=15000), 0
            )
            assert result is not None, "Failed to receive Data over RNS link"
            assert result.content == EXPECTED
            assert result.name == hello
        finally:
            await server.shutdown()


@pytest.mark.skipif(
    not os.environ.get("RNS_INTEGRATION"),
    reason="Set RNS_INTEGRATION=1 to run integration tests",
)
class TestRNSMultiHop:
    """Three RNS instances (three processes): client → router → origin.

    Proves ICN multi-hop forwarding over the real stack. The client only ever
    establishes a Link to the *router* — it has no RNS or ICN path to the
    origin. The router forwards the client's Interest upstream to the origin
    via its FIB, returns the reverse-path Data, and caches it at the hop.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmproot = tempfile.mkdtemp(prefix="rns_icn_mhop_")
        self.origin_cfg = os.path.join(self.tmproot, "origin")
        self.router_cfg = os.path.join(self.tmproot, "router")
        self.client_cfg = os.path.join(self.tmproot, "client")
        self.procs: list[subprocess.Popen] = []
        yield
        for proc in self.procs:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
        shutil.rmtree(self.tmproot, ignore_errors=True)

    def _spawn(self, argv: list[str], ready_prefix: str) -> dict:
        """Launch a subprocess and block until its handshake/result line.

        Skips interleaved RNS log lines, raises on an ``*_ERROR`` line or
        early exit, and returns the JSON payload following ``ready_prefix``.
        """
        env = dict(os.environ)
        env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            text=True,
        )
        self.procs.append(proc)
        while True:
            line = proc.stdout.readline()
            if line == "":
                raise RuntimeError(f"Subprocess exited before {ready_prefix!r}")
            if "ROUTER_ERROR" in line:
                raise RuntimeError(f"Subprocess error: {line.strip()}")
            if line.startswith(ready_prefix):
                return json.loads(line[len(ready_prefix):])

    @pytest.mark.asyncio
    async def test_fetch_through_router(self):
        # All three nodes run as separate processes (separate Reticulum
        # instances). The parent test process never initialises RNS — RNS is a
        # hard process singleton owned by the shared_rns fixture in conftest.
        origin_port = _free_port()
        router_port = _free_port()
        loop = asyncio.get_running_loop()

        # 1. Origin: TCP server, publishes content.
        origin = await loop.run_in_executor(
            None,
            self._spawn,
            [sys.executable, ORIGIN_SCRIPT, self.origin_cfg, str(origin_port)],
            "ORIGIN_READY ",
        )

        # 2. Router: TCP server (for the client) + TCP client (to the origin).
        #    Links upstream and installs the origin's FIB route before READY.
        router = await loop.run_in_executor(
            None,
            self._spawn,
            [
                sys.executable, ROUTER_SCRIPT, self.router_cfg,
                str(router_port), str(origin_port),
                origin["hexhash"], origin["identity_path"],
            ],
            "ROUTER_READY ",
        )

        # 3. Client: TCP client to the router only. Routes the origin's prefix
        #    via the router, then fetches manifest + blob through two hops.
        result = await loop.run_in_executor(
            None,
            self._spawn,
            [
                sys.executable, CLIENT_SCRIPT, self.client_cfg,
                str(router_port), router["hexhash"], router["identity_path"],
                origin["identity_path"],
            ],
            "CLIENT_RESULT ",
        )

        assert result["linked"], "Client failed to link to router"
        assert result["manifest"], "Failed to fetch manifest through router"
        assert result["content_hex"] is not None, "Failed to fetch content through router"
        assert bytes.fromhex(result["content_hex"]) == EXPECTED
        assert result["name_ok"], "Returned Data name did not match the Interest"

        # Cache-at-the-hop: the origin's content is now stored in the router's
        # ContentStore (read its SQLite DB directly — shared localhost fs).
        conn = sqlite3.connect(router["cs_path"])
        try:
            cached = {row[0] for row in conn.execute("SELECT content_bytes FROM content")}
        finally:
            conn.close()
        assert EXPECTED in cached, "Router did not cache forwarded content"
