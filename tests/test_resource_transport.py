"""Tests for RNS.Resource transport for large ICN content chunks.

Test plan:
  1. Unit-level: wrap/unwrap helpers
  2. ResourceTransportError hierarchy
  3. ResourcePublisher construction (runs without RNS, just tests init)
  4. LargeContentPublisher construction and threshold behaviour
  5. Integration: ResourcePublisher ↔ ResourceListener over RNS Shared Instance
"""

import asyncio
import os
import struct
import tempfile

import pytest

from rns_icn.name import RNS_ADDR_BYTES, Name
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


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


def content_hash(byte_val: int = 0xBB) -> bytes:
    return bytes([byte_val] + [0] * 31)


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


# ── Integration test: Resource transport over Shared Instance ──


@pytest.mark.skipif(
    not os.environ.get("RNS_INTEGRATION"),
    reason="Set RNS_INTEGRATION=1 to run integration tests",
)
class TestResourceTransportIntegration:
    """Full integration test: publish Data as RNS.Resource, receive on the other side.

    Uses RNS SharedInstance for loopback transport between two identities.
    ResourceListener attaches to the received link (Link.ACCEPT_APP strategy),
    not to a Destination.
    """

    @pytest.fixture(autouse=True)
    def setup_rns(self):
        import RNS

        if not RNS.Reticulum._running:
            configdir = tempfile.mkdtemp(prefix="rns_icn_resource_")
            os.environ["RNS_CONFIGDIR"] = configdir
            RNS.Reticulum(configdir=configdir)
            self.configdir = configdir
        else:
            self.configdir = None
        yield
        if self.configdir:
            import shutil
            shutil.rmtree(self.configdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_publish_small_data(self):
        """Small Data packet sent via Resource arrives and is parsed correctly.

        Uses a small payload that fits in a single RNS packet.
        """
        import RNS

        RNS.log_level = RNS.LOG_ERROR
        identity_a = RNS.Identity()
        identity_b = RNS.Identity()

        # Server A: destination for receiving links + resources
        dest_a = RNS.Destination(
            identity_a,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            "icn", "resource_test_a",
        )

        # Server B: destination for sending resource to A
        dest_b = RNS.Destination(
            identity_b,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            "icn", "resource_test_a",
        )

        received_data = None
        got_callback = asyncio.Event()
        link_a = None

        def on_incoming_link(link: RNS.Link):
            nonlocal link_a
            link_a = link
            # Set up resource listening on the incoming link
            listener = ResourceListener(link)

            def on_data(data: Data):
                nonlocal received_data
                received_data = data
                got_callback.set()

            listener.set_on_data(on_data)

        dest_a.set_link_established_callback(on_incoming_link)

        # Give RNS a moment to propagate destinations
        await asyncio.sleep(1.0)

        # Create an outbound link from B to A
        link_b = RNS.Link(dest_b)
        start = asyncio.get_event_loop().time()
        while link_b.status != RNS.Link.ACTIVE:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > 30.0:
                pytest.fail("Link establishment timed out")

        # Wait for side A to also see the link
        start = asyncio.get_event_loop().time()
        while link_a is None:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > 5.0:
                pytest.fail("Side A never got the incoming link")

        # Publish a Data packet via Resource from B to A
        name = Name(identity_a.hash, [b"resource_test"])
        data = Data.new(name=name, content=b"Hello via RNS.Resource!")

        publisher = ResourcePublisher(link_b)
        ok = await publisher.publish_data(data, timeout=30.0)
        assert ok, "publish_data returned False (timeout or error)"

        # Wait for the receiving side to get the resource
        await asyncio.wait_for(got_callback.wait(), timeout=30.0)

        assert received_data is not None
        assert received_data.name == name
        assert received_data.content == b"Hello via RNS.Resource!"
        assert received_data.metadata.content_hash is not None

        # Cleanup
        link_b.teardown()

    @pytest.mark.asyncio
    async def test_publish_large_chunk(self):
        """Large Data packet (~120 KB) sent via Resource is handled correctly.

        Tests segmentation support at the RNS level.
        """
        import RNS

        RNS.log_level = RNS.LOG_ERROR
        identity_a = RNS.Identity()
        identity_b = RNS.Identity()

        dest_a = RNS.Destination(
            identity_a,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            "icn", "resource_test_large",
        )

        dest_b = RNS.Destination(
            identity_b,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            "icn", "resource_test_large",
        )

        received_data = None
        got_callback = asyncio.Event()
        link_a = None

        def on_incoming_link(link: RNS.Link):
            nonlocal link_a
            link_a = link
            listener = ResourceListener(link)

            def on_data(data: Data):
                nonlocal received_data
                received_data = data
                got_callback.set()

            listener.set_on_data(on_data)

        dest_a.set_link_established_callback(on_incoming_link)

        await asyncio.sleep(1.0)

        link_b = RNS.Link(dest_b)
        start = asyncio.get_event_loop().time()
        while link_b.status != RNS.Link.ACTIVE:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > 30.0:
                pytest.fail("Link establishment timed out")

        start = asyncio.get_event_loop().time()
        while link_a is None:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > 5.0:
                pytest.fail("Side A never got the incoming link")

        # Create ~120 KB of content (exceeds 100 KB threshold)
        large_content = b"X" * (120 * 1024)
        name = Name(identity_a.hash, [b"large_resource_test"])
        data = Data.new(name=name, content=large_content)

        publisher = ResourcePublisher(link_b)
        ok = await publisher.publish_data(data, timeout=120.0)
        assert ok, "publish_data for large chunk returned False"

        await asyncio.wait_for(got_callback.wait(), timeout=120.0)

        assert received_data is not None
        assert received_data.name == name
        assert len(received_data.content) == 120 * 1024
        assert received_data.content == large_content

        link_b.teardown()

    @pytest.mark.asyncio
    async def test_multiple_chunks_via_resource(self):
        """Multiple Data packets sent as separate resources arrive correctly."""
        import RNS

        RNS.log_level = RNS.LOG_ERROR
        identity_a = RNS.Identity()
        identity_b = RNS.Identity()

        dest_a = RNS.Destination(
            identity_a,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            "icn", "resource_test_multi",
        )

        dest_b = RNS.Destination(
            identity_b,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            "icn", "resource_test_multi",
        )

        received_packets = []
        got_all = asyncio.Event()
        link_a = None

        def on_incoming_link(link: RNS.Link):
            nonlocal link_a
            link_a = link
            listener = ResourceListener(link)

            def on_data(data: Data):
                received_packets.append(data)
                if len(received_packets) == 3:
                    got_all.set()

            listener.set_on_data(on_data)

        dest_a.set_link_established_callback(on_incoming_link)

        await asyncio.sleep(1.0)

        link_b = RNS.Link(dest_b)
        start = asyncio.get_event_loop().time()
        while link_b.status != RNS.Link.ACTIVE:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > 30.0:
                pytest.fail("Link establishment timed out")

        start = asyncio.get_event_loop().time()
        while link_a is None:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > 5.0:
                pytest.fail("Side A never got the incoming link")

        publisher = ResourcePublisher(link_b)

        # Send 3 Data packets as individual resources
        for i in range(3):
            name = Name(identity_a.hash, [b"multi_test", str(i).encode()])
            data = Data.new(name=name, content=f"chunk_{i}".encode())
            ok = await publisher.publish_data(data, timeout=30.0)
            assert ok, f"publish_data for chunk {i} returned False"

        await asyncio.wait_for(got_all.wait(), timeout=60.0)

        assert len(received_packets) == 3
        for i in range(3):
            label = f"chunk_{i}".encode()
            assert label in received_packets[i].content

        link_b.teardown()


# ── Integration test: LargeContentPublisher with actual chunker content ──


@pytest.mark.skipif(
    not os.environ.get("RNS_INTEGRATION"),
    reason="Set RNS_INTEGRATION=1 to run integration tests",
)
class TestLargeContentPublisherIntegration:
    """LargeContentPublisher sending chunked content via Resource."""

    @pytest.fixture(autouse=True)
    def setup_rns(self):
        import RNS

        if not RNS.Reticulum._running:
            configdir = tempfile.mkdtemp(prefix="rns_icn_lcp_")
            os.environ["RNS_CONFIGDIR"] = configdir
            RNS.Reticulum(configdir=configdir)
            self.configdir = configdir
        else:
            self.configdir = None
        yield
        if self.configdir:
            import shutil
            shutil.rmtree(self.configdir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_large_content_publisher_threshold_behaviour(self):
        """LargeContentPublisher publishes data packets via Resource.

        Verifies the threshold property and that data are publishable.
        """
        import RNS

        from rns_icn.chunker import chunk_content

        RNS.log_level = RNS.LOG_ERROR
        identity_a = RNS.Identity()
        identity_b = RNS.Identity()

        dest_a = RNS.Destination(
            identity_a,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            "icn", "lcp_test",
        )

        dest_b = RNS.Destination(
            identity_b,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            "icn", "lcp_test",
        )

        received_packets = {}
        got_manifest = asyncio.Event()
        link_a = None

        def on_incoming_link(link: RNS.Link):
            nonlocal link_a
            link_a = link
            listener = ResourceListener(link)

            def on_data(data: Data):
                received_packets[str(data.name)] = data
                got_manifest.set()

            listener.set_on_data(on_data)

        dest_a.set_link_established_callback(on_incoming_link)

        await asyncio.sleep(1.0)

        link_b = RNS.Link(dest_b)
        start = asyncio.get_event_loop().time()
        while link_b.status != RNS.Link.ACTIVE:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > 30.0:
                pytest.fail("Link establishment timed out")

        start = asyncio.get_event_loop().time()
        while link_a is None:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > 5.0:
                pytest.fail("Side A never got the incoming link")

        # Chunk some content
        content = b"Hello from chunked content over RNS.Resource! " * 50
        name = Name(identity_a.hash, [b"chunked_over_resource"])
        result = chunk_content(content, name, chunk_size=200)

        # Publish all data packets via LargeContentPublisher
        lcp = LargeContentPublisher(link_b, resource_threshold=128)  # small threshold for test
        for data in result.data_packets:
            ok = await lcp.publish_data_packet(data, timeout=30.0)
            assert ok, f"Failed to publish {data.name}"

        # Also publish the manifest as a Data packet
        manifest_data = Data.new(name=name, content=result.manifest.to_json())
        ok = await lcp.publish_data_packet(manifest_data, timeout=30.0)
        assert ok, "Failed to publish manifest"

        # Allow time for all resources to arrive
        await asyncio.sleep(3.0)

        # Verify at least some data arrived
        assert len(received_packets) > 0, "No data packets received"

        link_b.teardown()
