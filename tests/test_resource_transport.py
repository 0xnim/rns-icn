"""Tests for RNS.Resource transport for large ICN content chunks.

Test plan:
  1. Unit-level: wrap/unwrap helpers
  2. ResourceTransportError hierarchy
  3. ResourcePublisher construction (runs without RNS, just tests init)
  4. LargeContentPublisher construction and threshold behaviour
  5. Integration: ResourcePublisher ↔ ResourceListener over two real RNS
     instances (publisher subprocess → this process, localhost TCP)
"""

import asyncio
import base64
import hashlib
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile

import pytest

from rns_icn.chunker import chunk_content
from rns_icn.manifest import ContentManifest
from rns_icn.name import Name
from rns_icn.packet import Data
from rns_icn.resource_transport import (
    DEFAULT_RESOURCE_THRESHOLD,
    RESOURCE_TYPE_ICN_DATA,
    LargeContentPublisher,
    ResourceListener,
    ResourcePublisher,
    ResourceTimeoutError,
    ResourceTransportError,
    _unwrap_payload,
    _wrap_payload,
)

# ── Unit tests: wrap/unwrap helpers ──


class TestWrapUnwrap:
    def test_wrap_adds_type_tag(self):
        """_wrap_payload prepends the ICN data type byte."""
        raw = b"hello"
        wrapped = _wrap_payload(raw)
        assert len(wrapped) == len(raw) + 1
        assert wrapped[0] == RESOURCE_TYPE_ICN_DATA
        assert wrapped[1:] == raw

    def test_unwrap_valid(self):
        """_unwrap_payload strips the type tag for tagged data."""
        wrapped = struct.pack(">B", RESOURCE_TYPE_ICN_DATA) + b"hello"
        result = _unwrap_payload(wrapped)
        assert result == b"hello"

    def test_unwrap_wrong_type_returns_none(self):
        """_unwrap_payload returns None when the tag doesn't match."""
        wrapped = struct.pack(">B", 0x00) + b"hello"
        assert _unwrap_payload(wrapped) is None

    def test_unwrap_too_short_returns_none(self):
        """_unwrap_payload returns None for empty data."""
        assert _unwrap_payload(b"") is None

    def test_round_trip(self):
        """Round-trip preserves raw bytes."""
        raw = b"\x01\x02\x03\xff\xfe" * 100
        assert _unwrap_payload(_wrap_payload(raw)) == raw


# ── Unit tests: error hierarchy ──


class TestErrors:
    def test_resource_transport_error(self):
        assert issubclass(ResourceTransportError, Exception)
        assert issubclass(ResourceTimeoutError, ResourceTransportError)

    def test_resource_timeout_error_message(self):
        err = ResourceTimeoutError("too slow")
        assert "too slow" in str(err)


# ── Unit tests: LargeContentPublisher threshold ──


class TestLargeContentPublisher:
    def test_default_threshold(self):
        """Default threshold is 100 KB."""
        assert DEFAULT_RESOURCE_THRESHOLD == 1024 * 100

    def test_threshold_get_set(self):
        """Threshold property can be read and changed."""
        pub = LargeContentPublisher.__new__(LargeContentPublisher)
        pub._threshold = 100 * 1024
        assert pub.threshold == 100 * 1024
        pub.threshold = 200 * 1024
        assert pub.threshold == 200 * 1024


# ── Unit tests: ResourcePublisher construction ──


def test_resource_publisher_init():
    """ResourcePublisher can be constructed with a MagicMock link (no RNS needed)."""
    from unittest.mock import MagicMock
    mock_link = MagicMock()
    publisher = ResourcePublisher(mock_link)
    assert publisher._link is mock_link


# ── Integration tests: Resource transport between two real RNS instances ──


PEER_SCRIPT = os.path.join(os.path.dirname(__file__), "_resource_peer.py")
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))

APP_NAME = "icn"


@pytest.mark.skipif(
    not os.environ.get("RNS_INTEGRATION"),
    reason="Set RNS_INTEGRATION=1 to run integration tests",
)
class _ResourceIntegrationBase:
    """Receive side of the resource-transport round-trip.

    This process (the shared in-process Reticulum, see ``conftest.py``) owns
    the IN destination and the ResourceListener; the publishing side runs in
    a subprocess (``_resource_peer.py``) dialling in over localhost TCP,
    because RNS has no path to a destination living in the same instance.
    """

    @pytest.fixture(autouse=True)
    def _peer_process(self):
        self.tmproot = tempfile.mkdtemp(prefix="rns_icn_resource_")
        self.proc = None
        yield
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        shutil.rmtree(self.tmproot, ignore_errors=True)

    def _listening_destination(self, aspect: str, received: list[Data]):
        """An IN destination collecting every ICN Data arriving via Resource."""
        import RNS

        identity = RNS.Identity()
        dest = RNS.Destination(
            identity, RNS.Destination.IN, RNS.Destination.SINGLE, APP_NAME, aspect
        )

        def on_incoming_link(link: RNS.Link):
            listener = ResourceListener(link)
            listener.set_on_data(received.append)

        dest.set_link_established_callback(on_incoming_link)
        return identity, dest

    def _spawn_peer(self, port: int, spec: dict) -> None:
        spec_path = os.path.join(self.tmproot, "spec.json")
        with open(spec_path, "w") as f:
            json.dump(spec, f)
        env = dict(os.environ)
        env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        self.proc = subprocess.Popen(
            [
                sys.executable, PEER_SCRIPT,
                os.path.join(self.tmproot, "peer"), str(port), spec_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            text=True,
        )

    @staticmethod
    def _item(labels: list[str], content: bytes) -> dict:
        return {"labels": labels, "content_b64": base64.b64encode(content).decode()}

    async def _receive(
        self, dest, received: list[Data], count: int, timeout: float = 120.0
    ) -> None:
        """Wait until the peer has delivered ``count`` Data packets.

        Re-announces the destination while waiting: the peer resolves it via
        path requests, which only reach us once its TCP link is up.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        next_announce = loop.time()
        while len(received) < count:
            if self.proc.poll() is not None:
                pytest.fail(f"peer subprocess exited with code {self.proc.returncode}")
            if loop.time() >= deadline:
                pytest.fail(f"received {len(received)}/{count} Data packets before timeout")
            if loop.time() >= next_announce:
                dest.announce()
                next_announce = loop.time() + 2.0
            await asyncio.sleep(0.2)


class TestResourceTransportIntegration(_ResourceIntegrationBase):
    """Publish Data as RNS.Resource in one instance, receive it in another."""

    @pytest.mark.asyncio
    async def test_publish_small_data(self, shared_rns):
        """Small Data packet sent via Resource arrives and is parsed correctly."""
        received: list[Data] = []
        identity, dest = self._listening_destination("resource_test_a", received)

        content = b"Hello via RNS.Resource!"
        self._spawn_peer(shared_rns.port, {
            "app_name": APP_NAME,
            "aspect": "resource_test_a",
            "dest_hexhash": dest.hash.hex(),
            "name_root_hex": identity.hash.hex(),
            "mode": "data",
            "items": [self._item(["resource_test"], content)],
        })
        await self._receive(dest, received, 1)

        data = received[0]
        assert data.name == Name(identity.hash, [b"resource_test"])
        assert data.content == content
        assert data.metadata.content_hash is not None

    @pytest.mark.asyncio
    async def test_publish_large_chunk(self, shared_rns):
        """Large Data packet (~120 KB) sent via Resource is handled correctly.

        Tests segmentation support at the RNS level.
        """
        received: list[Data] = []
        identity, dest = self._listening_destination("resource_test_large", received)

        large_content = b"X" * (120 * 1024)
        self._spawn_peer(shared_rns.port, {
            "app_name": APP_NAME,
            "aspect": "resource_test_large",
            "dest_hexhash": dest.hash.hex(),
            "name_root_hex": identity.hash.hex(),
            "mode": "data",
            "items": [self._item(["large_resource_test"], large_content)],
        })
        await self._receive(dest, received, 1)

        data = received[0]
        assert data.name == Name(identity.hash, [b"large_resource_test"])
        assert len(data.content) == 120 * 1024
        assert data.content == large_content

    @pytest.mark.asyncio
    async def test_multiple_chunks_via_resource(self, shared_rns):
        """Multiple Data packets sent as separate resources arrive in order."""
        received: list[Data] = []
        identity, dest = self._listening_destination("resource_test_multi", received)

        self._spawn_peer(shared_rns.port, {
            "app_name": APP_NAME,
            "aspect": "resource_test_multi",
            "dest_hexhash": dest.hash.hex(),
            "name_root_hex": identity.hash.hex(),
            "mode": "data",
            "items": [
                self._item(["multi_test", str(i)], f"chunk_{i}".encode())
                for i in range(3)
            ],
        })
        await self._receive(dest, received, 3)

        assert len(received) == 3
        for i in range(3):
            assert f"chunk_{i}".encode() in received[i].content


class TestLargeContentPublisherIntegration(_ResourceIntegrationBase):
    """LargeContentPublisher sending chunked content via Resource."""

    @pytest.mark.asyncio
    async def test_large_content_publisher_threshold_behaviour(self, shared_rns):
        """Every chunk of chunked content plus its manifest arrives intact."""
        received: list[Data] = []
        identity, dest = self._listening_destination("lcp_test", received)

        content = b"Hello from chunked content over RNS.Resource! " * 50
        name = Name(identity.hash, [b"chunked_over_resource"])
        # Chunk locally with the same parameters the peer uses: chunking is
        # deterministic, so this yields the expected packet count and chunk
        # hashes without comparing whole packets (timestamps differ).
        expected = chunk_content(content, name, chunk_size=200)

        self._spawn_peer(shared_rns.port, {
            "app_name": APP_NAME,
            "aspect": "lcp_test",
            "dest_hexhash": dest.hash.hex(),
            "name_root_hex": identity.hash.hex(),
            "mode": "chunked",
            "chunk_size": 200,
            "resource_threshold": 128,
            "items": [self._item(["chunked_over_resource"], content)],
        })
        await self._receive(dest, received, len(expected.data_packets) + 1)

        by_name = {str(d.name): d for d in received}
        manifest = ContentManifest.from_data(by_name.pop(str(name)))
        assert manifest.chunks == expected.manifest.chunks
        assert manifest.content_hash == expected.manifest.content_hash

        received_hashes = {
            hashlib.blake2b(d.content, digest_size=32).digest()
            for d in by_name.values()
        }
        assert received_hashes == {c.content_hash for c in manifest.chunks}
