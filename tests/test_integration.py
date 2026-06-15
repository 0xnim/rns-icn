"""End-to-end integration test over a real two-node RNS mesh.

Spawns a separate ICN origin server in its own process and Reticulum instance
(``tests/_icn_origin.py``), connects this process to it over a localhost
TCPInterface, then fetches content end-to-end. This exercises the real RNS
stack — interfaces, announces, path resolution, Links, Channels, LinkFace and
the Forwarder — rather than the in-memory ``TestFace``.

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
import subprocess
import sys
import tempfile

import pytest
import RNS

from rns_icn.config import KnownPeer, ServerConfig
from rns_icn.rns_server import ICNServer
from rns_icn.name import Name
from rns_icn.packet import Interest

APP_NAME = "icn"
ASPECT = "default"
EXPECTED = b"Hello from ICN server A!"

ORIGIN_SCRIPT = os.path.join(os.path.dirname(__file__), "_icn_origin.py")
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _write_client_config(configdir: str, connect_port: int) -> None:
    os.makedirs(configdir, exist_ok=True)
    config = f"""[reticulum]
  enable_transport = Yes
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = 3

[interfaces]
  [[TCP Client Interface]]
    type = TCPClientInterface
    interface_enabled = yes
    target_host = 127.0.0.1
    target_port = {connect_port}
"""
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(config)


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
        """Launch the origin subprocess and wait for its ORIGIN_READY line."""
        env = dict(os.environ)
        env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        self.proc = subprocess.Popen(
            [sys.executable, ORIGIN_SCRIPT, self.origin_cfg, str(port)],
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
    async def test_fetch_over_tcp_mesh(self):
        port = _free_port()

        # Origin handshake must complete before we bring up our Reticulum,
        # since RNS.Reticulum() is a process singleton.
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, self._start_origin, port)
        origin_hexhash = info["hexhash"]
        origin_identity_path = info["identity_path"]

        # Bring up this process's Reticulum as a TCP client to the origin.
        _write_client_config(self.client_cfg, port)
        RNS.Reticulum(configdir=self.client_cfg)

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
