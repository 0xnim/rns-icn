"""Tests for OfflineQueue — store-and-forward for offline subscribers.

Tests cover:
- OfflineQueue unit tests: put, drain, pending_count, prune, cleanup, clear, peek
- ICNServer integration: offline queue fills when face is gone, drains on re-subscribe
- Edge cases: empty queue, stale item pruning, re-drain after offline-while-offline
"""

from __future__ import annotations

import asyncio
import time

import pytest

from rns_icn.aps import APSManager
from rns_icn.face import FaceId
from rns_icn.name import Name, RNS_ADDR_BYTES
from rns_icn.offline_queue import OfflineQueue
from rns_icn.packet import APSubscribe, Data
from rns_icn.server import ICNServer


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


def make_name(parts: list[bytes]) -> Name:
    return Name(rns_addr(0xAA), parts)


def make_data(stream: str, seq: int) -> Data:
    name = make_name([stream.encode(), f"seg{seq}".encode()])
    return Data.new(name=name, content=f"msg{seq}".encode()).with_sequence(seq)


# ════════════════════════════════════════════════════
# OfflineQueue unit tests
# ════════════════════════════════════════════════════


class TestOfflineQueueUnit:
    def make_queue(self, max_age: int = 86400) -> OfflineQueue:
        return OfflineQueue(server=None, max_age_seconds=max_age)

    def test_empty_initial_state(self):
        q = self.make_queue()
        assert q.total_pending() == 0
        assert q.stream_count() == 0
        assert q.pending_count(make_name([b"test"])) == 0
        assert q.clear() == 0

    def test_put_and_pending_count(self):
        q = self.make_queue()
        stream = make_name([b"alice", b"sensor"])
        q.put(stream, make_data("sensor", 1))
        q.put(stream, make_data("sensor", 2))

        assert q.stream_count() == 1
        assert q.pending_count(stream) == 2
        assert q.total_pending() == 2

    def test_put_multiple_streams(self):
        q = self.make_queue()
        stream_a = make_name([b"alice", b"temp"])
        stream_b = make_name([b"bob", b"pressure"])

        q.put(stream_a, make_data("temp", 1))
        q.put(stream_b, make_data("pressure", 1))
        q.put(stream_b, make_data("pressure", 2))

        assert q.stream_count() == 2
        assert q.pending_count(stream_a) == 1
        assert q.pending_count(stream_b) == 2
        assert q.total_pending() == 3

    def test_put_same_data_multiple_calls(self):
        q = self.make_queue()
        stream = make_name([b"stream"])
        q.put(stream, make_data("stream", 1))
        q.put(stream, make_data("stream", 1))  # duplicate considered separate

        assert q.pending_count(stream) == 2

    def test_drain_empty(self):
        q = self.make_queue()
        stream = make_name([b"empty"])

        async def _run():
            return await q.drain(stream, 42)

        count = asyncio.run(_run())
        assert count == 0  # should not raise

    def test_cleanup_removes_stream(self):
        q = self.make_queue()
        stream = make_name([b"stream"])
        q.put(stream, make_data("stream", 1))
        assert q.pending_count(stream) == 1

        removed = q.cleanup(stream)
        assert removed == 1
        assert q.pending_count(stream) == 0
        assert q.stream_count() == 0

    def test_clear_all(self):
        q = self.make_queue()
        q.put(make_name([b"a"]), make_data("a", 1))
        q.put(make_name([b"b"]), make_data("b", 1))
        q.put(make_name([b"b"]), make_data("b", 2))
        assert q.total_pending() == 3

        cleared = q.clear()
        assert cleared == 3
        assert q.total_pending() == 0
        assert q.stream_count() == 0

    def test_peek_shape(self):
        q = self.make_queue()
        stream = make_name([b"debug"])
        q.put(stream, make_data("debug", 1))
        q.put(stream, make_data("debug", 2))

        preview = q.peek(stream, limit=2)
        assert len(preview) == 2
        for item in preview:
            assert len(item) == 3  # (hash_prefix, size, age)
            assert isinstance(item[0], str)  # hash prefix
            assert isinstance(item[1], int)   # size
            assert isinstance(item[2], float) # age

    def test_peek_empty(self):
        q = self.make_queue()
        assert q.peek(make_name([b"nothing"])) == []

    def test_prune_removes_expired(self):
        """Items older than max_age_seconds should be removed by prune()."""
        q = self.make_queue(max_age=0.1)  # 100ms TTL
        stream = make_name([b"ephemeral"])
        q.put(stream, make_data("ephemeral", 1))

        assert q.pending_count(stream) == 1

        # Wait past TTL
        time.sleep(0.15)
        removed = q.prune()
        assert removed == 1
        assert q.pending_count(stream) == 0
        assert q.stream_count() == 0

    def test_prune_mixed_age(self):
        """Only expired items are removed; fresh items survive."""
        q = self.make_queue(max_age=0.1)
        stream = make_name([b"mixed"])

        # Older item (will expire)
        q._queue[stream] = [(time.time() - 0.2, make_data("mixed", 1))]
        # Newer item (stays fresh)
        q.put(stream, make_data("mixed", 2))

        removed = q.prune()
        assert removed == 1
        assert q.pending_count(stream) == 1
        assert q.stream_count() == 1

    def test_prune_no_expired_noop(self):
        q = self.make_queue(max_age=3600)
        stream = make_name([b"fresh"])
        q.put(stream, make_data("fresh", 1))
        q.put(stream, make_data("fresh", 2))

        removed = q.prune()
        assert removed == 0
        assert q.pending_count(stream) == 2


# ════════════════════════════════════════════════════
# OfflineQueue + ICNServer integration tests
# ════════════════════════════════════════════════════


class TestOfflineQueueServerIntegration:
    """Tests the full loop: subscribe → disconnect → publish → reconnect → drain."""

    @pytest.mark.asyncio
    async def test_queue_on_dead_face(self):
        """Data published to a dead subscriber is queued in OfflineQueue."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()

        # Subscribe the face to a stream
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"])), face.id()
        )

        # Remove the face to simulate disconnect
        fid = face.id()
        server._faces.pop(fid, None)

        # Publish data — should be queued, not dropped
        data = Data.new(
            name=make_name([b"stream", b"seg1"]), content=b"offline-msg"
        ).with_sequence(1)
        await server.publish_pushed(data)

        # Verify data was queued
        stream_name = make_name([b"stream"])
        assert server.offline_queue.pending_count(stream_name) >= 1

    @pytest.mark.asyncio
    async def test_drain_on_reconnect(self):
        """Queued data is delivered to a new face on re-subscribe."""
        server = ICNServer(rns_addr(0xAA))
        face1 = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            face1.id(),
        )

        # Disconnect face1
        fid1 = face1.id()
        server._faces.pop(fid1, None)

        # Publish data while offline
        data = Data.new(
            name=make_name([b"stream", b"seg1"]), content=b"cached"
        ).with_sequence(1)
        await server.publish_pushed(data)

        assert server.offline_queue.pending_count(make_name([b"stream"])) == 1

        # Reconnect with a new face and re-subscribe
        face2 = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            face2.id(),
        )

        # The queued data should have been drained to face2
        assert server.offline_queue.pending_count(make_name([b"stream"])) == 0
        raw = await asyncio.wait_for(face2._send_q.get(), timeout=0.5)
        parsed = Data.from_bytes(raw)
        assert parsed.content == b"cached"

    @pytest.mark.asyncio
    async def test_multiple_queued_items_drained(self):
        """Multiple offline items are drained in order on reconnect."""
        server = ICNServer(rns_addr(0xAA))
        face1 = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            face1.id(),
        )

        # Disconnect
        fid1 = face1.id()
        server._faces.pop(fid1, None)

        # Publish 3 messages while offline
        for i in range(1, 4):
            data = Data.new(
                name=make_name([b"stream", f"seg{i}".encode()]),
                content=f"msg{i}".encode(),
            ).with_sequence(i)
            await server.publish_pushed(data)

        assert server.offline_queue.total_pending() == 3

        # Reconnect
        face2 = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            face2.id(),
        )

        # Drain order should be FIFO
        received = []
        for _ in range(3):
            raw = await asyncio.wait_for(face2._send_q.get(), timeout=0.5)
            parsed = Data.from_bytes(raw)
            received.append(parsed.content)

        assert received == [b"msg1", b"msg2", b"msg3"]

    @pytest.mark.asyncio
    async def test_no_queue_when_face_alive(self):
        """Data published to an active face is NOT queued."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            face.id(),
        )

        data = Data.new(
            name=make_name([b"stream", b"seg1"]), content=b"live"
        ).with_sequence(1)
        await server.publish_pushed(data)

        # Queue should be empty — data was delivered directly
        assert server.offline_queue.total_pending() == 0

    @pytest.mark.asyncio
    async def test_drain_respects_different_streams(self):
        """Data queued for one stream doesn't leak to another stream."""
        server = ICNServer(rns_addr(0xAA))
        face1 = server._new_face()
        # Subscribe to both temp AND pressure
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"temp"]), start_from_now=True),
            face1.id(),
        )
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"pressure"]), start_from_now=True),
            face1.id(),
        )

        fid1 = face1.id()
        server._faces.pop(fid1, None)

        # Publish to temp and pressure while offline
        data_temp = Data.new(
            name=make_name([b"temp", b"seg1"]), content=b"hot"
        ).with_sequence(1)
        data_pressure = Data.new(
            name=make_name([b"pressure", b"seg1"]), content=b"high"
        ).with_sequence(1)
        await server.publish_pushed(data_temp)
        await server.publish_pushed(data_pressure)

        # Both should be queued (face was subscribed to both)
        assert server.offline_queue.pending_count(make_name([b"temp"])) == 1
        assert server.offline_queue.pending_count(make_name([b"pressure"])) == 1

        # Reconnect with subscription to temp only
        face2 = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"temp"]), start_from_now=True),
            face2.id(),
        )

        # Only temp data should be drained, pressure remains queued
        assert server.offline_queue.pending_count(make_name([b"temp"])) == 0
        assert server.offline_queue.pending_count(make_name([b"pressure"])) == 1

        raw = await asyncio.wait_for(face2._send_q.get(), timeout=0.5)
        assert Data.from_bytes(raw).content == b"hot"

    @pytest.mark.asyncio
    async def test_start_from_now_true_skip_cs_but_still_drains_offline(self):
        """start_from_now=True skips CS content but still drains offline queue."""
        server = ICNServer(rns_addr(0xAA))
        face1 = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            face1.id(),
        )
        fid1 = face1.id()
        server._faces.pop(fid1, None)

        # Publish while offline
        data = Data.new(
            name=make_name([b"stream", b"seg1"]), content=b"offline-data"
        ).with_sequence(1)
        await server.publish_pushed(data)

        # Reconnect with start_from_now=True
        face2 = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            face2.id(),
        )

        # Offline queue should still drain even with start_from_now=True
        raw = await asyncio.wait_for(face2._send_q.get(), timeout=0.5)
        assert Data.from_bytes(raw).content == b"offline-data"

    @pytest.mark.asyncio
    async def test_face_remains_gone_during_drain(self):
        """If the new face also goes away before drain, items stay queued."""
        server = ICNServer(rns_addr(0xAA))
        face1 = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            face1.id(),
        )
        fid1 = face1.id()
        server._faces.pop(fid1, None)

        data = Data.new(
            name=make_name([b"stream", b"seg1"]), content=b"orphan"
        ).with_sequence(1)
        await server.publish_pushed(data)

        # Create a new face, subscribe *but* remove it before drain runs
        # (drain happens inside handle_subscribe which looks up _faces)
        face2 = server._new_face()
        fid2 = face2.id()
        server._faces.pop(fid2, None)  # Remove before subscribe

        # When handle_subscribe tries to drain, face2 won't exist
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"stream"]), start_from_now=True),
            fid2,
        )

        # Items should still be queued (drain re-queued them)
        assert server.offline_queue.pending_count(make_name([b"stream"])) == 1

    @pytest.mark.asyncio
    async def test_aps_manager_publish_offline_queue_parameter(self):
        """APSManager.publish accepts offline_queue and queues on failed delivery."""
        server = ICNServer(rns_addr(0xAA))
        face = server._new_face()
        await server.handle_subscribe(
            APSubscribe(name=make_name([b"aps-test"]), start_from_now=True),
            face.id(),
        )

        # Remove face
        server._faces.pop(face.id(), None)

        data = Data.new(
            name=make_name([b"aps-test", b"seg1"]), content=b"aps-offline"
        ).with_sequence(1)

        # Manually call aps.publish with offline queue
        await server.aps.publish(data, offline_queue=server.offline_queue)

        assert server.offline_queue.pending_count(make_name([b"aps-test"])) == 1
