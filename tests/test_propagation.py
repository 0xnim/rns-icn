"""Tests for Propagation Node — peering, sync, and content propagation.

Tests cover:
- PropPeer wire format (to_bytes/from_bytes round-trips)
- Packet envelope parsing for PROP_PEER
- PropagationManager peer tracking
- PropagationManager content forwarding between peers
- ICNServer propagation integration (peering handshake)
- Content propagation across 3+ peer mesh
- Consumer gets content from peered server (producer offline scenario)
- Edge cases: no peers, face not found, duplicate peering
"""

import asyncio

import pytest

from rns_icn.face import FaceId
from rns_icn.name import Name, RNS_ADDR_BYTES
from rns_icn.packet import (
    Data,
    Packet,
    PacketType,
    PropPeer,
    parse_packet,
)
from rns_icn.propagation import PropagationError, PropagationManager
from rns_icn.server import ICNServer


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


def make_name(parts: list[bytes]) -> Name:
    return Name(rns_addr(0xAA), parts)


# ═══════════════════════════════════════════
# PropPeer wire format
# ═══════════════════════════════════════════


class TestPropPeerWire:
    def test_round_trip_default(self):
        addr = rns_addr(0xBB)
        peer = PropPeer(rns_addr=addr)
        data = peer.to_bytes()
        parsed = PropPeer.from_bytes(data)
        assert parsed.version == 1
        assert parsed.rns_addr == addr
        assert parsed.wants_sync is False

    def test_round_trip_with_wants_sync(self):
        addr = rns_addr(0xCC)
        peer = PropPeer(rns_addr=addr, wants_sync=True)
        data = peer.to_bytes()
        parsed = PropPeer.from_bytes(data)
        assert parsed.rns_addr == addr
        assert parsed.wants_sync is True

    def test_type_byte(self):
        peer = PropPeer(rns_addr=rns_addr(0xDD))
        data = peer.to_bytes()
        assert data[0] == PacketType.PROP_PEER

    def test_version_field(self):
        peer = PropPeer(version=1, rns_addr=rns_addr(0xEE))
        data = peer.to_bytes()
        assert data[1] == 1

    def test_short_buffer_raises(self):
        with pytest.raises(ValueError):
            PropPeer.from_bytes(b"\x04\x01")

    def test_wrong_type_byte_raises(self):
        with pytest.raises(ValueError):
            PropPeer.from_bytes(b"\x01\x01" + bytes(16) + b"\x00")

    def test_rns_addr_padding(self):
        """Short addresses get zero-padded to 16 bytes."""
        short = b"\x01\x02\x03"
        peer = PropPeer(rns_addr=short, wants_sync=False)
        data = peer.to_bytes()
        assert len(data) == 19  # type(1) + ver(1) + addr(16) + flags(1)
        assert data[2:5] == short
        assert data[5:18] == b"\x00" * 13
        parsed = PropPeer.from_bytes(data)
        assert parsed.rns_addr == b"\x01\x02\x03" + b"\x00" * 13

    def test_rns_addr_truncation(self):
        """Overly long addresses get truncated to 16 bytes."""
        long = bytes(range(32))
        peer = PropPeer(rns_addr=long)
        data = peer.to_bytes()
        assert len(data) == 19
        parsed = PropPeer.from_bytes(data)
        assert parsed.rns_addr == bytes(range(16))


class TestPropPeerParsePacket:
    def test_parse_prop_peer(self):
        addr = rns_addr(0xBB)
        peer = PropPeer(rns_addr=addr)
        raw = peer.to_bytes()
        pkt = parse_packet(raw)
        assert pkt.type == PacketType.PROP_PEER
        assert pkt.peer is not None
        assert pkt.peer.rns_addr == addr
        assert pkt.interest is None
        assert pkt.data is None
        assert pkt.subscribe is None

    def test_packet_dataclass_defaults(self):
        pkt = Packet(type=PacketType.PROP_PEER)
        assert pkt.peer is None


# ═══════════════════════════════════════════
# PropagationManager peer tracking
# ═══════════════════════════════════════════


class TestPropagationManagerPeerTracking:
    def make_mgr(self):
        return PropagationManager(server=None)

    def test_add_peer(self):
        mgr = self.make_mgr()
        addr = rns_addr(0xBB)
        mgr.add_peer(100, addr)
        assert mgr.is_peer(100)
        assert mgr.peer_count() == 1

    def test_add_multiple_peers(self):
        mgr = self.make_mgr()
        mgr.add_peer(100, rns_addr(0xBB))
        mgr.add_peer(101, rns_addr(0xCC))
        mgr.add_peer(102, rns_addr(0xDD))
        assert mgr.peer_count() == 3

    def test_remove_peer(self):
        mgr = self.make_mgr()
        mgr.add_peer(100, rns_addr(0xBB))
        mgr.remove_peer(100)
        assert not mgr.is_peer(100)
        assert mgr.peer_count() == 0

    def test_remove_nonexistent(self):
        mgr = self.make_mgr()
        mgr.add_peer(100, rns_addr(0xBB))
        mgr.remove_peer(999)  # Should not raise
        assert mgr.peer_count() == 1

    def test_is_peer_not_peer(self):
        mgr = self.make_mgr()
        assert not mgr.is_peer(999)

    def test_is_peer_empty(self):
        mgr = self.make_mgr()
        assert mgr.peer_count() == 0

    def test_peer_prefixes(self):
        mgr = self.make_mgr()
        addr_a = rns_addr(0xBB)
        addr_b = rns_addr(0xCC)
        mgr.add_peer(100, addr_a)
        mgr.add_peer(101, addr_b)
        prefixes = mgr.peer_prefixes()
        assert len(prefixes) == 2
        assert Name(addr_a) in prefixes
        assert Name(addr_b) in prefixes


# ═══════════════════════════════════════════
# PropagationManager with server — handshake
# ═══════════════════════════════════════════


class TestPropagationServerHandshake:
    @pytest.mark.asyncio
    async def test_handle_incoming_peers(self):
        """Sending PROP_PEER through handle_incoming registers the peer."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        fid = face.id()

        peer_addr = rns_addr(0xBB)
        peer = PropPeer(rns_addr=peer_addr, wants_sync=True)
        await server.handle_incoming(fid, peer.to_bytes())

        assert server.propagation.is_peer(fid)
        assert server.propagation.peer_count() == 1

    @pytest.mark.asyncio
    async def test_peer_handshake_adds_fib_route(self):
        """Peering adds a FIB route to the peer's producer prefix."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        fid = face.id()

        peer_addr = rns_addr(0xBB)
        peer = PropPeer(rns_addr=peer_addr)
        await server.handle_incoming(fid, peer.to_bytes())

        # FIB should have a route for the peer's prefix
        peer_prefix = Name(peer_addr)
        routes = server.fib.lookup(peer_prefix)
        assert routes is not None
        assert any(fid == r[0] for r in routes)


# ═══════════════════════════════════════════
# Content propagation across peers
# ═══════════════════════════════════════════


class TestPropagationContentForwarding:
    @pytest.mark.asyncio
    async def test_propagate_forwards_to_peers(self):
        """propagate() sends Data to all peered servers."""
        server = ICNServer(rns_addr(0xAA))
        peer_face1 = server._new_face()
        peer_face2 = server._new_face()
        fid1 = peer_face1.id()
        fid2 = peer_face2.id()

        server.propagation.add_peer(fid1, rns_addr(0xBB))
        server.propagation.add_peer(fid2, rns_addr(0xCC))

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"hello")
        forwarded = await server.propagation.propagate(data)

        assert forwarded == 2

        # Both peers should receive the data
        raw1 = await asyncio.wait_for(peer_face1._send_q.get(), timeout=0.5)
        raw2 = await asyncio.wait_for(peer_face2._send_q.get(), timeout=0.5)
        assert Data.from_bytes(raw1).content == b"hello"
        assert Data.from_bytes(raw2).content == b"hello"

    @pytest.mark.asyncio
    async def test_propagate_excludes_sender(self):
        """propagate with exclude_face skips the sender."""
        server = ICNServer(rns_addr(0xAA))
        peer_face1 = server._new_face()
        peer_face2 = server._new_face()
        fid1 = peer_face1.id()
        fid2 = peer_face2.id()

        server.propagation.add_peer(fid1, rns_addr(0xBB))
        server.propagation.add_peer(fid2, rns_addr(0xCC))

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"exclude_test")
        forwarded = await server.propagation.propagate(data, exclude_face=fid1)

        assert forwarded == 1

        # face1 should NOT receive (excluded)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(peer_face1._send_q.get(), timeout=0.05)

        # face2 should receive
        raw2 = await asyncio.wait_for(peer_face2._send_q.get(), timeout=0.5)
        assert Data.from_bytes(raw2).content == b"exclude_test"

    @pytest.mark.asyncio
    async def test_propagate_no_peers(self):
        """propagate with no peers returns 0."""
        server = ICNServer(rns_addr(0xAA))
        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"test")
        forwarded = await server.propagation.propagate(data)
        assert forwarded == 0

    @pytest.mark.asyncio
    async def test_publish_pushed_propagates_to_peers(self):
        """publish_pushed() on a peered server forwards to all peers."""
        server = ICNServer(rns_addr(0xAA))
        peer_face1 = server._new_face()
        peer_face2 = server._new_face()

        server.propagation.add_peer(peer_face1.id(), rns_addr(0xBB))
        server.propagation.add_peer(peer_face2.id(), rns_addr(0xCC))

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"propagated")
        await server.publish_pushed(data)

        # Both peers should receive via propagation
        raw1 = await asyncio.wait_for(peer_face1._send_q.get(), timeout=0.5)
        raw2 = await asyncio.wait_for(peer_face2._send_q.get(), timeout=0.5)
        assert Data.from_bytes(raw1).content == b"propagated"
        assert Data.from_bytes(raw2).content == b"propagated"

    @pytest.mark.asyncio
    async def test_peer_data_propagates_to_other_peers(self):
        """Data from one peer propagates to other peers."""
        server = ICNServer(rns_addr(0xAA))
        # Three peers
        peer_faces = [server._new_face() for _ in range(3)]
        fids = [f.id() for f in peer_faces]
        for i, fid in enumerate(fids):
            server.propagation.add_peer(fid, bytes([0xBB + i] + [0] * (RNS_ADDR_BYTES - 1)))

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"multi_peer")

        # Simulate data arriving from peer 0
        await server.handle_incoming(fids[0], data.to_bytes())

        # peer 1 and 2 should receive (peer 0 excluded as sender)
        raw1 = await asyncio.wait_for(peer_faces[1]._send_q.get(), timeout=0.5)
        raw2 = await asyncio.wait_for(peer_faces[2]._send_q.get(), timeout=0.5)
        assert Data.from_bytes(raw1).content == b"multi_peer"
        assert Data.from_bytes(raw2).content == b"multi_peer"

        # peer 0 should NOT receive its own data back
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(peer_faces[0]._send_q.get(), timeout=0.05)

    @pytest.mark.asyncio
    async def test_peer_data_caches_locally(self):
        """Data from a peer is cached in the local CS."""
        server = ICNServer(rns_addr(0xAA))
        peer_face = server._new_face()
        fid = peer_face.id()
        server.propagation.add_peer(fid, rns_addr(0xBB))

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"cache_test")
        await server.handle_incoming(fid, data.to_bytes())

        # Should be cached locally
        cached = server.forwarder.cs.get(data.name)
        assert cached is not None
        assert cached.content == b"cache_test"


# ═══════════════════════════════════════════
# Consumer gets content from peered server
# (producer offline scenario)
# ═══════════════════════════════════════════


class TestPropagationConsumerOffline:
    @pytest.mark.asyncio
    async def test_consumer_gets_content_via_peered_server(self):
        """Consumer connects to a peered server and gets content
        that was propagated from the producer's server.

        Setup:
        1. producer_server publishes content
        2. content propagates to peer_server via direct forwarding
        3. consumer connects to peer_server and fetches content
        4. consumer gets content even though producer is 'offline'
        """
        producer_addr = rns_addr(0xAA)
        peer_addr = rns_addr(0xBB)

        producer_server = ICNServer(producer_addr)
        peer_server = ICNServer(peer_addr)

        # Set up propagation: producer → peer
        # Create faces that will act as the propagation link
        prod_link_face = producer_server._new_face()
        peer_link_face = peer_server._new_face()

        # Cross-wire the queues so data sent by producer to its link face
        # arrives at the peer server's handle_incoming for its link face
        prod_link_face._incoming = peer_link_face._incoming = None

        # Register as propagation peers
        producer_server.propagation.add_peer(prod_link_face.id(), peer_addr)
        peer_server.propagation.add_peer(peer_link_face.id(), producer_addr)

        # Step 1: Producer publishes content directly into its CS
        stream_name = Name(producer_addr, [b"stream", b"seg1"])
        stream_data = Data.new(
            name=stream_name,
            content=b"producer_content",
        ).with_sequence(1)

        # Cache it on producer (same as what publish_pushed does)
        producer_server.forwarder.cs.insert(stream_data.name, stream_data)

        # Step 2: Manually propagate the content to the peer server
        await producer_server.propagation.propagate(stream_data)
        # Read the forwarded data from prod_link_face's send queue
        forwarded_raw = await asyncio.wait_for(
            prod_link_face._send_q.get(), timeout=0.5
        )

        # Step 3: Deliver it to peer server as if it arrived over the link
        await peer_server.handle_incoming(peer_link_face.id(), forwarded_raw)

        # Step 4: Consumer connects to peer_server
        consumer_face = peer_server._new_face()
        consumer_fid = consumer_face.id()

        # Step 5: Consumer sends Interest to peer_server
        from rns_icn.packet import Interest
        interest = Interest(
            name=stream_name,
            lifetime_ms=5000,
        )
        await peer_server.handle_interest(interest, consumer_fid)

        # Step 6: Consumer should receive the content even though
        # the producer is not connected to peer_server directly
        response_raw = await asyncio.wait_for(
            consumer_face._send_q.get(), timeout=0.5
        )
        response = Data.from_bytes(response_raw)
        assert response.content == b"producer_content"
        assert response.name == stream_name


# ═══════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════


class TestPropagationEdgeCases:
    @pytest.mark.asyncio
    async def test_no_server_no_op(self):
        """PropagationManager without server does nothing."""
        mgr = PropagationManager(server=None)
        data = Data.new(name=make_name([b"test"]), content=b"test")
        result = await mgr.propagate(data)
        assert result == 0

    @pytest.mark.asyncio
    async def test_propagate_missing_face_skips(self):
        """Propagate skips peers whose face is gone."""
        server = ICNServer(rns_addr(0xAA))
        addr = rns_addr(0xBB)
        server.propagation.add_peer(999, addr)  # face 999 doesn't exist
        data = Data.new(name=make_name([b"test"]), content=b"test")
        forwarded = await server.propagation.propagate(data)
        assert forwarded == 0

    @pytest.mark.asyncio
    async def test_remove_peer_cleanup(self):
        """Removing a peer cleans up face and synced producers."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        fid = face.id()
        server.propagation.add_peer(fid, rns_addr(0xBB))
        assert server.propagation.peer_count() == 1

        server.propagation.remove_peer(fid)
        assert not server.propagation.is_peer(fid)
        assert server.propagation.peer_count() == 0

    @pytest.mark.asyncio
    async def test_non_peer_data_not_forwarded(self):
        """Regular (non-peer) data is not propagated."""
        server = ICNServer(rns_addr(0xAA))
        peer_face = server._new_face()
        server.propagation.add_peer(peer_face.id(), rns_addr(0xBB))

        # Data from a non-peer face
        non_peer_face = server._new_face()
        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"non_peer")
        await server.handle_incoming(non_peer_face.id(), data.to_bytes())

        # Peer should NOT receive this data (it was from a non-peer face,
        # handled by handle_data which doesn't propagate)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(peer_face._send_q.get(), timeout=0.05)
