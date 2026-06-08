"""Tests for APS Subscribe — push delivery.

Tests cover:
- APSubscribe wire format (to_bytes/from_bytes round-trips)
- Packet envelope parsing for APS_SUBSCRIBE
- APSManager subscription tracking and prefix matching
- APSManager publish delivery to subscribers
- ICNServer handle_subscribe integration
- ICNServer publish_pushed integration
- Full subscribe-then-push integration test
- Edge cases: start_from_now, empty name, duplicate subscriptions
"""

import asyncio

import pytest

from rns_icn.aps import APSManager
from rns_icn.face import FaceId, TestFace, test_face_pair
from rns_icn.name import Name, RNS_ADDR_BYTES
from rns_icn.packet import (
    APSubscribe,
    Data,
    Packet,
    PacketType,
    SubscribeError,
    parse_packet,
)
from rns_icn.server import ICNServer


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


def make_name(parts: list[bytes]) -> Name:
    return Name(rns_addr(0xAA), parts)


# ═════════════════════════════════════════════════
# APSubscribe wire format
# ═════════════════════════════════════════════════


class TestAPSubscribe:
    def test_round_trip(self):
        name = make_name([b"stream", b"chat"])
        sub = APSubscribe(name=name)
        data = sub.to_bytes()
        parsed = APSubscribe.from_bytes(data)
        assert parsed.name == name
        assert parsed.start_from_now is False

    def test_start_from_now(self):
        name = make_name([b"stream"])
        sub = APSubscribe(name=name, start_from_now=True)
        data = sub.to_bytes()
        parsed = APSubscribe.from_bytes(data)
        assert parsed.name == name
        assert parsed.start_from_now is True

    def test_complex_name(self):
        name = make_name([b"alice", b"sensor", b"temperature"])
        sub = APSubscribe(name=name)
        data = sub.to_bytes()
        parsed = APSubscribe.from_bytes(data)
        assert parsed.name == name
        assert parsed.name.rns_addr == rns_addr(0xAA)
        assert len(parsed.name.components) == 4  # rns_addr + 3 path
        assert parsed.name.components[1:] == [b"alice", b"sensor", b"temperature"]

    def test_type_byte(self):
        sub = APSubscribe(name=make_name([b"t"]))
        data = sub.to_bytes()
        assert data[0] == PacketType.APS_SUBSCRIBE

    def test_short_buffer_raises(self):
        with pytest.raises(SubscribeError):
            APSubscribe.from_bytes(b"\x03")

    def test_wrong_type_byte_raises(self):
        with pytest.raises(SubscribeError):
            APSubscribe.from_bytes(b"\x01\x02" + b"test")


class TestAPSubscribeParsePacket:
    def test_parse_aps_subscribe(self):
        sub = APSubscribe(name=make_name([b"stream"]))
        raw = sub.to_bytes()
        pkt = parse_packet(raw)
        assert pkt.type == PacketType.APS_SUBSCRIBE
        assert pkt.subscribe is not None
        assert pkt.subscribe.name == make_name([b"stream"])
        assert pkt.interest is None
        assert pkt.data is None

    def test_parse_aps_subscribe_start_from_now(self):
        sub = APSubscribe(name=make_name([b"stream"]), start_from_now=True)
        raw = sub.to_bytes()
        pkt = parse_packet(raw)
        assert pkt.type == PacketType.APS_SUBSCRIBE
        assert pkt.subscribe.start_from_now is True

    def test_packet_dataclass_defaults(self):
        pkt = Packet(type=PacketType.APS_SUBSCRIBE)
        assert pkt.subscribe is None


# ═════════════════════════════════════════════════
# APSManager unit tests
# ═════════════════════════════════════════════════


class TestAPSManager:
    def make_manager(self) -> APSManager:
        return APSManager(server=None)

    def test_subscribe_basic(self):
        mgr = self.make_manager()
        name = make_name([b"stream"])
        mgr.subscribe(name, 1)
        assert mgr.is_subscribed(name, 1)
        assert mgr.subscription_count() == 1

    def test_subscribe_multiple_faces(self):
        mgr = self.make_manager()
        name = make_name([b"stream"])
        mgr.subscribe(name, 1)
        mgr.subscribe(name, 2)
        mgr.subscribe(name, 3)
        assert mgr.is_subscribed(name, 1)
        assert mgr.is_subscribed(name, 2)
        assert mgr.is_subscribed(name, 3)
        assert mgr.subscription_count() == 3

    def test_subscribe_duplicate_is_idempotent(self):
        mgr = self.make_manager()
        name = make_name([b"stream"])
        mgr.subscribe(name, 1)
        mgr.subscribe(name, 1)
        assert mgr.subscription_count() == 1

    def test_unsubscribe(self):
        mgr = self.make_manager()
        name = make_name([b"stream"])
        mgr.subscribe(name, 1)
        mgr.subscribe(name, 2)
        mgr.unsubscribe(name, 1)
        assert not mgr.is_subscribed(name, 1)
        assert mgr.is_subscribed(name, 2)
        assert mgr.subscription_count() == 1

    def test_unsubscribe_nonexistent(self):
        mgr = self.make_manager()
        name = make_name([b"stream"])
        # Should not raise
        mgr.unsubscribe(name, 99)

    def test_unsubscribe_face(self):
        mgr = self.make_manager()
        mgr.subscribe(make_name([b"a"]), 1)
        mgr.subscribe(make_name([b"b"]), 1)
        mgr.subscribe(make_name([b"c"]), 2)
        mgr.unsubscribe_face(1)
        assert mgr.subscription_count() == 1
        assert mgr.is_subscribed(make_name([b"c"]), 2)

    def test_unsubscribe_face_nonexistent(self):
        mgr = self.make_manager()
        mgr.subscribe(make_name([b"a"]), 1)
        # Should not raise
        mgr.unsubscribe_face(99)
        assert mgr.subscription_count() == 1

    def test_get_subscriber_faces_exact_match(self):
        mgr = self.make_manager()
        name = make_name([b"stream"])
        mgr.subscribe(name, 1)
        mgr.subscribe(name, 2)
        faces = mgr.get_subscriber_faces(name)
        assert 1 in faces
        assert 2 in faces

    def test_get_subscriber_faces_prefix_match(self):
        mgr = self.make_manager()
        stream_name = make_name([b"alice", b"sensor"])
        child_name = make_name([b"alice", b"sensor", b"temp"])
        mgr.subscribe(stream_name, 1)
        faces = mgr.get_subscriber_faces(child_name)
        assert 1 in faces

    def test_get_subscriber_faces_no_match(self):
        mgr = self.make_manager()
        mgr.subscribe(make_name([b"alice"]), 1)
        faces = mgr.get_subscriber_faces(make_name([b"bob"]))
        assert len(faces) == 0

    def test_get_subscriber_faces_reverse_prefix(self):
        """Subscribed to a child, published data at parent — should still match."""
        mgr = self.make_manager()
        child_name = make_name([b"alice", b"sensor", b"temp"])
        parent_name = make_name([b"alice", b"sensor"])
        mgr.subscribe(child_name, 1)
        # published data at /alice/sensor — subscriber has /alice/sensor/temp
        # This matches because stream_name.starts_with(name) is True
        faces = mgr.get_subscriber_faces(parent_name)
        assert 1 in faces

    def test_multiple_streams_isolation(self):
        mgr = self.make_manager()
        a = make_name([b"a"])
        b = make_name([b"b"])
        mgr.subscribe(a, 1)
        mgr.subscribe(b, 2)
        assert mgr.is_subscribed(a, 1)
        assert not mgr.is_subscribed(a, 2)
        assert mgr.is_subscribed(b, 2)
        assert not mgr.is_subscribed(b, 1)

    def test_stream_count(self):
        mgr = self.make_manager()
        assert mgr.stream_count() == 0
        mgr.subscribe(make_name([b"a"]), 1)
        mgr.subscribe(make_name([b"b"]), 1)
        assert mgr.stream_count() == 2
        mgr.unsubscribe(make_name([b"a"]), 1)
        assert mgr.stream_count() == 1


# ═════════════════════════════════════════════════
# APSManager publish delivery
# ═════════════════════════════════════════════════


class TestAPSManagerPublish:
    @pytest.mark.asyncio
    async def test_publish_no_server_is_noop(self):
        mgr = APSManager(server=None)
        name = make_name([b"stream"])
        mgr.subscribe(name, 1)
        data = Data.new(name=name, content=b"hello")
        # Should not raise
        await mgr.publish(data)

    @pytest.mark.asyncio
    async def test_push_to_subscriber(self):
        """Consumer subscribes, producer pushes — consumer receives Data."""
        server = ICNServer(rns_addr(0xAA))
        consumer_face = server._new_face()
        consumer_fid = consumer_face.id()

        # Subscribe
        sub = APSubscribe(name=make_name([b"stream"]))
        await server.handle_subscribe(sub, consumer_fid)

        # Producer pushes content
        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"hello")
        await server.publish_pushed(data)

        # Consumer face should have received the Data
        sent_raw = await consumer_face._send_q.get()
        assert sent_raw is not None
        assert sent_raw[0] == PacketType.DATA
        parsed = Data.from_bytes(sent_raw)
        assert parsed.content == b"hello"

    @pytest.mark.asyncio
    async def test_push_to_multiple_subscribers(self):
        server = ICNServer(rns_addr(0xAA))
        face1 = server._new_face()
        face2 = server._new_face()

        sub = APSubscribe(name=make_name([b"stream"]))
        await server.handle_subscribe(sub, face1.id())
        await server.handle_subscribe(sub, face2.id())

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"hello")
        await server.publish_pushed(data)

        # Both faces should receive
        for f in [face1, face2]:
            raw = await f._send_q.get()
            assert raw is not None
            assert raw[0] == PacketType.DATA

    @pytest.mark.asyncio
    async def test_push_only_matching_subscribers(self):
        server = ICNServer(rns_addr(0xAA))
        face1 = server._new_face()
        face2 = server._new_face()

        await server.handle_subscribe(
            APSubscribe(name=make_name([b"alice"])), face1.id()
        )
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"bob"])), face2.id()
        )

        # Push alice content
        data = Data.new(name=make_name([b"alice", b"msg1"]), content=b"alice_msg")
        await server.publish_pushed(data)

        raw1 = await face1._send_q.get()
        assert raw1 is not None

        # Face2 should NOT receive anything
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(face2._send_q.get(), timeout=0.05)

    @pytest.mark.asyncio
    async def test_aps_works_without_server_ref(self):
        """APSManager without server reference still tracks subscriptions."""
        mgr = APSManager(server=None)
        mgr.subscribe(make_name([b"stream"]), 1)
        assert mgr.subscription_count() == 1

    @pytest.mark.asyncio
    async def test_push_after_unsubscribe(self):
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        sub = APSubscribe(name=make_name([b"stream"]))
        await server.handle_subscribe(sub, face.id())

        # Unsubscribe
        server.aps.unsubscribe(make_name([b"stream"]), face.id())

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"hello")
        await server.publish_pushed(data)

        # Should NOT receive pushed data after unsubscribe
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(face._send_q.get(), timeout=0.05)

    @pytest.mark.asyncio
    async def test_handle_data_does_not_push_to_subscribers(self):
        """Incoming Data (receive_data) should NOT trigger push — only
        publish_pushed() triggers push delivery.
        """
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"])), face.id()
        )

        # Send Data via handle_data (simulating incoming Data from a peer)
        # This should not push to subscribers — just cache and satisfy PIT
        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"data")
        await server.handle_data(data, face.id() + 100)

        # Should NOT be pushed to subscriber face
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(face._send_q.get(), timeout=0.05)


# ═════════════════════════════════════════════════
# ICNServer handle_subscribe integration tests
# ═════════════════════════════════════════════════


class TestICNServerHandleSubscribe:
    @pytest.mark.asyncio
    async def test_handle_subscribe_registers_subscription(self):
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        sub = APSubscribe(name=make_name([b"stream"]))
        await server.handle_subscribe(sub, face.id())
        assert server.aps.is_subscribed(make_name([b"stream"]), face.id())
        assert server.aps.subscription_count() == 1

    @pytest.mark.asyncio
    async def test_handle_subscribe_with_existing_content_no_start_from_now(self):
        """When start_from_now is False, existing CS content is sent."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()

        # Pre-populate CS with stream content
        data1 = Data.new(
            name=make_name([b"stream", b"seg1"]), content=b"existing1"
        ).with_sequence(1)
        data2 = Data.new(
            name=make_name([b"stream", b"seg2"]), content=b"existing2"
        ).with_sequence(2)
        server.forwarder.cs.insert(data1.name, data1)
        server.forwarder.cs.insert(data2.name, data2)

        # Subscribe with default (start_from_now=False)
        sub = APSubscribe(name=make_name([b"stream"]))
        await server.handle_subscribe(sub, face.id())

        # Should receive existing content
        raw1 = await face._send_q.get()
        assert raw1 is not None
        parsed1 = Data.from_bytes(raw1)
        assert parsed1.content == b"existing1"

        raw2 = await face._send_q.get()
        assert raw2 is not None
        parsed2 = Data.from_bytes(raw2)
        assert parsed2.content == b"existing2"

    @pytest.mark.asyncio
    async def test_handle_subscribe_with_start_from_now_skips_existing(self):
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()

        data1 = Data.new(
            name=make_name([b"stream", b"seg1"]), content=b"existing1"
        )
        server.forwarder.cs.insert(data1.name, data1)

        # Subscribe with start_from_now=True
        sub = APSubscribe(name=make_name([b"stream"]), start_from_now=True)
        await server.handle_subscribe(sub, face.id())

        # Should NOT receive existing content
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(face._send_q.get(), timeout=0.05)

    @pytest.mark.asyncio
    async def test_handle_subscribe_non_matching_content_not_sent(self):
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()

        # Insert content that doesn't match our stream
        other = Data.new(
            name=make_name([b"other"]), content=b"should_not_send"
        )
        server.forwarder.cs.insert(other.name, other)

        sub = APSubscribe(name=make_name([b"stream"]))
        await server.handle_subscribe(sub, face.id())

        # Should not receive the non-matching content
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(face._send_q.get(), timeout=0.05)

    @pytest.mark.asyncio
    async def test_publish_pushed_caches_and_pushes(self):
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"])), face.id()
        )

        data = Data.new(
            name=make_name([b"stream", b"seg1"]), content=b"pushed"
        ).with_sequence(1)
        await server.publish_pushed(data)

        # Verify cached in CS
        cached = server.forwarder.cs.get(data.name)
        assert cached is not None
        assert cached.content == b"pushed"

        # Verify pushed to subscriber
        raw = await face._send_q.get()
        parsed = Data.from_bytes(raw)
        assert parsed.content == b"pushed"
        assert parsed.metadata.sequence == 1

    @pytest.mark.asyncio
    async def test_publish_pushed_no_subscribers(self):
        """publish_pushed still caches even with no subscribers."""
        server = ICNServer(rns_addr(0xAA))
        data = Data.new(name=make_name([b"orphan"]), content=b"lonely")
        await server.publish_pushed(data)
        cached = server.forwarder.cs.get(data.name)
        assert cached is not None


# ═════════════════════════════════════════════════
# Integration tests: full subscribe-then-push flow
# ═════════════════════════════════════════════════


class TestAPSPushIntegration:
    @pytest.mark.asyncio
    async def test_subscribe_then_push_receive(self):
        """Full flow: consumer subscribes, producer pushes,
        consumer receives without sending Interest."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()

        # Step 1: Subscribe
        subscribe_raw = APSubscribe(name=make_name([b"stream"])).to_bytes()
        await server.handle_incoming(face.id(), subscribe_raw)
        assert server.aps.subscription_count() == 1

        # Step 2: Producer pushes 3 Data packets
        for i in range(1, 4):
            data = Data.new(
                name=make_name([b"stream", f"seg{i}".encode()]),
                content=f"msg{i}".encode(),
            ).with_sequence(i)
            await server.publish_pushed(data)

        # Step 3: Consumer receives all 3 without sending Interests
        received = []
        for i in range(3):
            raw = await asyncio.wait_for(face._send_q.get(), timeout=0.5)
            parsed = Data.from_bytes(raw)
            received.append(parsed.content)

        assert len(received) == 3
        assert received[0] == b"msg1"
        assert received[1] == b"msg2"
        assert received[2] == b"msg3"

    @pytest.mark.asyncio
    async def test_handle_incoming_dispatch(self):
        """handle_incoming correctly dispatches APS_SUBSCRIBE."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()

        sub = APSubscribe(name=make_name([b"stream"]), start_from_now=True)
        await server.handle_incoming(face.id(), sub.to_bytes())

        assert server.aps.is_subscribed(make_name([b"stream"]), face.id())

    @pytest.mark.asyncio
    async def test_multiple_subscribers_same_stream(self):
        server = ICNServer(rns_addr(0xAA))
        face1 = server._new_face()
        face2 = server._new_face()

        sub = APSubscribe(name=make_name([b"stream"]), start_from_now=True)
        await server.handle_incoming(face1.id(), sub.to_bytes())
        await server.handle_incoming(face2.id(), sub.to_bytes())

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"hello")
        await server.publish_pushed(data)

        raw1 = await asyncio.wait_for(face1._send_q.get(), timeout=0.5)
        raw2 = await asyncio.wait_for(face2._send_q.get(), timeout=0.5)
        assert Data.from_bytes(raw1).content == b"hello"
        assert Data.from_bytes(raw2).content == b"hello"

    @pytest.mark.asyncio
    async def test_push_ordering(self):
        """Data is pushed in the order publish_pushed is called."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"])), face.id()
        )

        for i in range(5):
            data = Data.new(
                name=make_name([b"stream", f"seg{i}".encode()]),
                content=f"data{i}".encode(),
            ).with_sequence(i)
            await server.publish_pushed(data)

        for i in range(5):
            raw = await asyncio.wait_for(face._send_q.get(), timeout=0.5)
            parsed = Data.from_bytes(raw)
            assert parsed.content == f"data{i}".encode()
            assert parsed.metadata.sequence == i

    @pytest.mark.asyncio
    async def test_face_not_found_silent(self):
        """If the subscribed face no longer exists, publish skips silently."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        fid = face.id()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"])), fid
        )

        # Remove the face from server's tracking
        server._faces.pop(fid, None)

        data = Data.new(name=make_name([b"stream", b"seg1"]), content=b"test")
        await server.publish_pushed(data)
        # Should not raise — APSManager tries to find face, skips if missing

    @pytest.mark.asyncio
    async def test_subscribe_empty_name(self):
        """Subscribe with root-level name works."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        root_name = Name(rns_addr(0xAA))  # just the RNS address

        sub = APSubscribe(name=root_name, start_from_now=True)
        await server.handle_subscribe(sub, face.id())

        assert server.aps.is_subscribed(root_name, face.id())

    @pytest.mark.asyncio
    async def test_publish_prefix_matching(self):
        """Subscribe to /alice matches published /alice/sensor/temp."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()

        # Subscribe to a broad prefix
        sub = APSubscribe(name=make_name([b"alice"]), start_from_now=True)
        await server.handle_subscribe(sub, face.id())

        # Publish under a child name
        data = Data.new(
            name=make_name([b"alice", b"sensor", b"temp"]), content=b"23.5"
        )
        await server.publish_pushed(data)

        raw = await asyncio.wait_for(face._send_q.get(), timeout=0.5)
        assert Data.from_bytes(raw).content == b"23.5"
