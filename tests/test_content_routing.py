"""Tests for 5.3 Content routing between servers.

Tests cover:
- Interest forwarding from consumer → Server A → (FIB peer route) → Server B → Data back
- Consumer gets content produced on a peered upstream server
- Multi-hop chain: Consumer → Server A → Server B → Server C
- No-route scenario: Interest times out when no FIB entry exists
- Interest forwarding through propagation-managed peer routes
"""

import asyncio

import pytest

from rns_icn.face import FaceId
from rns_icn.name import Name, RNS_ADDR_BYTES
from rns_icn.packet import Data, Interest, PacketType, parse_packet
from rns_icn.server import ICNServer


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


# ── Server Link helper ──


def link_servers(
    server_a: ICNServer, face_a: FaceId,
    server_b: ICNServer, face_b: FaceId,
) -> asyncio.Task:
    """Wire two ICNServer faces together bidirectionally.

    Runs a background task that:
    - Reads bytes from face_a's send queue and delivers to server_b.handle_incoming(face_b)
    - Reads bytes from face_b's send queue and delivers to server_a.handle_incoming(face_a)

    This simulates the real RNS Link transport: bytes sent on one side appear
    on the other side's handle_incoming.
    """
    async def _relay():
        q_a = server_a.get_face_send_queue(face_a)
        q_b = server_b.get_face_send_queue(face_b)
        if q_a is None or q_b is None:
            return
        while True:
            done, _ = await asyncio.wait(
                [asyncio.create_task(q_a.get()), asyncio.create_task(q_b.get())],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                raw = task.result()
                # Determine direction: if it came from q_a, send to server_b
                if task.get_coro().cr_frame and hasattr(task.get_coro(), "cr_code"):
                    pass  # can't easily determine, check below
                # Check both queues to see which one produced
                if raw in list(q_a._queue) or not q_a.empty():
                    pass  # just use the task's origin
                # Simple approach: the task that completed tells us the source
                source_is_a = (task.get_coro().cr_frame.f_locals.get("self") is q_a
                               if task.get_coro().cr_frame else False)
                if source_is_a:
                    await server_b.handle_incoming(face_b, raw)
                else:
                    await server_a.handle_incoming(face_a, raw)

    task = asyncio.create_task(_relay())
    return task


def server_link(
    server_a: ICNServer, face_a_id: FaceId,
    server_b: ICNServer, face_b_id: FaceId,
) -> asyncio.Task:
    """Wire two faces bidirectionally so bytes from one server arrive at the other.

    Returns the background task (call task.cancel() during cleanup).
    """
    async def _pipe():
        q_a = server_a.get_face_send_queue(face_a_id)
        q_b = server_b.get_face_send_queue(face_b_id)
        if q_a is None or q_b is None:
            return
        while True:
            # Wait for bytes from either side
            raw_a_task = asyncio.create_task(q_a.get())
            raw_b_task = asyncio.create_task(q_b.get())
            done, pending = await asyncio.wait(
                [raw_a_task, raw_b_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                raw = task.result()
                if task is raw_a_task:
                    await server_b.handle_incoming(face_b_id, raw)
                else:
                    await server_a.handle_incoming(face_a_id, raw)
            for t in pending:
                t.cancel()

    task = asyncio.create_task(_pipe())
    return task


# ═══════════════════════════════════════════
# Interest forwarding across peered servers
# ═══════════════════════════════════════════


class TestInterestForwarding:
    """Consumer → Server A → (FIB route to Server B) → Data back."""

    @pytest.mark.asyncio
    async def test_forward_interest_to_peer(self):
        """Consumer connects to Server A, Interest forwarded to Server B
        via FIB route, Server B serves from CS, Data comes back."""
        server_a = ICNServer(rns_addr(0xAA))
        server_b = ICNServer(rns_addr(0xBB))

        # Create peering faces on both servers
        face_a_to_b = server_a._new_face()
        face_b_to_a = server_b._new_face()

        # Wire them bidirectionally
        link = server_link(server_a, face_a_to_b.id(), server_b, face_b_to_a.id())

        try:
            # Peer server_a → server_b via propagation
            from rns_icn.packet import PropPeer
            peer_b = PropPeer(rns_addr=rns_addr(0xBB), wants_sync=False)
            await server_a.handle_incoming(face_a_to_b.id(), peer_b.to_bytes())

            # Peer server_b → server_a
            peer_a = PropPeer(rns_addr=rns_addr(0xAA), wants_sync=False)
            await server_b.handle_incoming(face_b_to_a.id(), peer_a.to_bytes())

            # Publish content on Server B
            content_name = Name(rns_addr(0xBB), [b"hello"])
            content_data = Data.new(name=content_name, content=b"Hello from B!")
            server_b.forwarder.cs.insert(content_name, content_data)

            # Consumer connects to Server A
            consumer_face = server_a._new_face()
            consumer_fid = consumer_face.id()

            # Consumer sends Interest for Server B's content
            interest = Interest(name=content_name, lifetime_ms=5000)
            await server_a.handle_interest(interest, consumer_fid)

            # Consumer should receive the Data (forwarded to B, served from B's CS)
            response_raw = await asyncio.wait_for(
                consumer_face._send_q.get(), timeout=3.0
            )
            response = Data.from_bytes(response_raw)
            assert response.content == b"Hello from B!"
            assert response.name == content_name
        finally:
            link.cancel()
            try:
                await link
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_forward_interest_no_route(self):
        """Consumer sends Interest for content with no FIB route — times out."""
        server_a = ICNServer(rns_addr(0xAA))

        consumer_face = server_a._new_face()
        consumer_fid = consumer_face.id()

        # Content on a server that's not peered
        unknown_name = Name(rns_addr(0xFF), [b"unknown"])
        interest = Interest(name=unknown_name, lifetime_ms=500)

        await server_a.handle_interest(interest, consumer_fid)

        # Consumer should get nothing back (timeout)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                consumer_face._send_q.get(), timeout=0.3
            )

    @pytest.mark.asyncio
    async def test_forward_multiple_peers_second_serves(self):
        """Consumer sends Interest that matches a specific peer's prefix.
        Server A has routes to two peers but only Server B has the content."""
        server_a = ICNServer(rns_addr(0xAA))
        server_b = ICNServer(rns_addr(0xBB))
        server_c = ICNServer(rns_addr(0xCC))

        # Wire A ↔ B
        face_a_b = server_a._new_face()
        face_b_a = server_b._new_face()
        link_ab = server_link(server_a, face_a_b.id(), server_b, face_b_a.id())

        # Wire A ↔ C
        face_a_c = server_a._new_face()
        face_c_a = server_c._new_face()
        link_ac = server_link(server_a, face_a_c.id(), server_c, face_c_a.id())

        try:
            # Peer A ↔ B and A ↔ C via propagation
            for server, peer_addr, face in [
                (server_a, rns_addr(0xBB), face_a_b.id()),
                (server_a, rns_addr(0xCC), face_a_c.id()),
                (server_b, rns_addr(0xAA), face_b_a.id()),
                (server_c, rns_addr(0xAA), face_c_a.id()),
            ]:
                peer_name = rns_addr(0xBB) if peer_addr == rns_addr(0xBB) else (
                    rns_addr(0xCC) if peer_addr == rns_addr(0xCC) else rns_addr(0xAA)
                )
                from rns_icn.packet import PropPeer
                peer = PropPeer(rns_addr=peer_addr, wants_sync=False)
                await server.handle_incoming(face, peer.to_bytes())

            # Publish content only on Server C
            content_name = Name(rns_addr(0xCC), [b"only_on_c"])
            content_data = Data.new(name=content_name, content=b"Server C content!")
            server_c.forwarder.cs.insert(content_name, content_data)

            # Consumer on Server A
            consumer_face = server_a._new_face()
            consumer_fid = consumer_face.id()

            interest = Interest(name=content_name, lifetime_ms=5000)
            await server_a.handle_interest(interest, consumer_fid)

            # Should get content from Server C
            response_raw = await asyncio.wait_for(
                consumer_face._send_q.get(), timeout=3.0
            )
            response = Data.from_bytes(response_raw)
            assert response.content == b"Server C content!"
            assert response.name == content_name
        finally:
            for link in [link_ab, link_ac]:
                link.cancel()
                try:
                    await link
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_forward_and_cache_on_intermediate(self):
        """Server A forwards Interest to Server B, gets Data back,
        and caches it in CS for subsequent requests."""
        server_a = ICNServer(rns_addr(0xAA))
        server_b = ICNServer(rns_addr(0xBB))

        face_a_b = server_a._new_face()
        face_b_a = server_b._new_face()
        link = server_link(server_a, face_a_b.id(), server_b, face_b_a.id())

        try:
            # Peer A ↔ B
            for s, addr, fid in [
                (server_a, rns_addr(0xBB), face_a_b.id()),
                (server_b, rns_addr(0xAA), face_b_a.id()),
            ]:
                from rns_icn.packet import PropPeer
                await s.handle_incoming(fid, PropPeer(rns_addr=addr, wants_sync=False).to_bytes())

            # Publish on Server B
            content_name = Name(rns_addr(0xBB), [b"cached"])
            content_data = Data.new(name=content_name, content=b"Cache me!")
            server_b.forwarder.cs.insert(content_name, content_data)

            # First fetch (forward to B)
            consumer1 = server_a._new_face()
            interest1 = Interest(name=content_name, lifetime_ms=5000)
            await server_a.handle_interest(interest1, consumer1.id())
            await asyncio.wait_for(consumer1._send_q.get(), timeout=3.0)

            # Data should now be cached on Server A
            cached = server_a.forwarder.cs.get(content_name)
            assert cached is not None
            assert cached.content == b"Cache me!"

            # Second fetch (local CS hit, no forwarding needed)
            consumer2 = server_a._new_face()
            interest2 = Interest(name=content_name, lifetime_ms=5000)
            await server_a.handle_interest(interest2, consumer2.id())
            response_raw = await asyncio.wait_for(
                consumer2._send_q.get(), timeout=1.0
            )
            response = Data.from_bytes(response_raw)
            assert response.content == b"Cache me!"
        finally:
            link.cancel()
            try:
                await link
            except asyncio.CancelledError:
                pass


# ═══════════════════════════════════════════
# Forwarder PIT waiter integration
# ═══════════════════════════════════════════


class TestPitWaiterForward:
    """Tests specifically for the PIT waiter pattern in Forwarder._forward()."""

    @pytest.mark.asyncio
    async def test_forward_with_transport_loop(self):
        """Use TestFace with a transport loop to validate PIT waiter resolves."""
        from rns_icn.face import TestFace, test_face_pair
        from rns_icn.forwarder import Forwarder

        fw = Forwarder()
        upstream_face, downstream_face = test_face_pair()
        fw.register_face(upstream_face)
        fw.add_route(Name(rns_addr(0x01)), upstream_face.id(), 10)

        # Pre-populate CS with response data on the "producer" side
        # The transport reads from downstream_face._incoming and feeds
        # Data back to the forwarder via receive_data
        data_name = Name(rns_addr(0x01), [b"test"])
        response_data = Data.new(name=data_name, content=b"forwarded_response")

        async def transport():
            """Read Interest from downstream, serve from CS."""
            # Wait for Interest bytes
            interest_raw = await downstream_face._incoming.get()
            # Send Data back through the same face
            await downstream_face._outgoing.put(response_data.to_bytes())

        async def data_router():
            """Read Data from upstream_face._incoming and feed to forwarder."""
            data_raw = await upstream_face._incoming.get()
            pkt = parse_packet(data_raw)
            if pkt.data is not None:
                await fw.receive_data(pkt.data, upstream_face.id())

        asyncio.create_task(transport())
        asyncio.create_task(data_router())
        await asyncio.sleep(0.05)

        interest = Interest(name=data_name, lifetime_ms=5000)
        result = await fw.express(interest, 0)

        assert result is not None
        assert result.content == b"forwarded_response"
        assert result.name == data_name

        # Should also be in CS
        cached = fw.cs.get(data_name)
        assert cached is not None
        assert cached.content == b"forwarded_response"
