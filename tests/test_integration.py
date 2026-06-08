"""Integration test: Two RNSICNServers over RNS Shared Instance (loopback).

Requires an RNS config that has a Shared Instance interface configured.
If no RNS config exists, creates a temporary one.
"""

import asyncio
import os
import tempfile

import pytest
import RNS

from rns_icn.rns_server import RNSICNServer
from rns_icn.name import Name
from rns_icn.packet import Interest


@pytest.mark.skipif(
    not os.environ.get("RNS_INTEGRATION"),
    reason="Set RNS_INTEGRATION=1 to run integration tests",
)
class TestRNSIntegration:
    """Runs two RNSICNServers on SharedInstance and exchanges Interest/Data."""

    @pytest.fixture(autouse=True)
    def setup_rns(self):
        """Ensure RNS is initialized with SharedInstance."""
        if not RNS.Reticulum._running:
            # Use a temp config dir to avoid interfering with the user's config
            self.configdir = tempfile.mkdtemp(prefix="rns_icn_test_")
            os.environ["RNS_CONFIGDIR"] = self.configdir
            RNS.Reticulum(configdir=self.configdir)
        yield
        # Cleanup
        if hasattr(self, "configdir"):
            import shutil
            shutil.rmtree(self.configdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_server_to_server(self):
        """Create two servers, connect them, exchange content."""
        identity_a = RNS.Identity()
        identity_b = RNS.Identity()

        server_a = RNSICNServer(identity_a, app_name="icn", aspect="test_a")
        server_b = RNSICNServer(identity_b, app_name="icn", aspect="test_b")

        server_a.start()
        server_b.start()

        # Publish content on server A
        hello_name = Name(identity_a.hash, [b"hello"])
        server_a.publish_content(hello_name, b"Hello from ICN server A!")

        # Publish manifest on server A
        server_a.publish_manifest()

        # Server B: establish link to server A
        face_id = await server_b.connect(server_a.hexhash)
        assert face_id is not None, "Failed to establish link"

        # Add route to server A's namespace via the new face
        server_b.forwarder.add_route(Name(identity_a.hash), face_id, 10)

        # Express Interest for the hello content
        interest = Interest(name=hello_name, lifetime_ms=5000)
        result = await server_b.forwarder.express(interest, 0)

        assert result is not None, "Failed to receive Data"
        assert result.content == b"Hello from ICN server A!"
        assert result.name == hello_name

        # Cleanup
        server_a.stop()
        server_b.stop()
