"""Tests for 2.3 multi-path forwarding (primary/backup failover).

The FIB stores multiple cost-ordered faces per prefix. On a forward timeout the
Forwarder falls through to the next usable face — choosing between distinct
content-equivalent peers (a producer/cache that holds the same name), which is
an ICN-layer decision RNS can't make for us (RNS re-paths to a fixed
destination; here the next-hops are different sources).
"""

import asyncio

import pytest

from rns_icn.face import Face, FaceCapabilities, FaceId
from rns_icn.forwarder import Forwarder
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import Data, Interest, parse_packet
from rns_icn.strategy import BestRoute


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


class _ScriptedFace(Face):
    """Face that optionally answers a forwarded Interest with preset Data.

    With ``response`` set it schedules ``forwarder.receive_data`` to simulate an
    upstream peer answering; with ``response=None`` it stays silent so the
    forward times out and the Forwarder falls through to the next face. Every
    raw send is recorded in ``sent`` for assertions.
    """

    __test__ = False

    def __init__(self, face_id: FaceId, fw: Forwarder,
                 response: Data | None = None, delay: float = 0.01):
        self._id = face_id
        self._fw = fw
        self._response = response
        self._delay = delay
        self.sent: list[bytes] = []

    async def express_interest(self, interest: Interest) -> Data | None:
        return None

    async def send_data(self, data: Data) -> None:
        pass

    async def send_raw(self, raw: bytes) -> None:
        self.sent.append(raw)
        if self._response is not None:
            async def _answer() -> None:
                await asyncio.sleep(self._delay)
                await self._fw.receive_data(self._response, self._id)
            asyncio.ensure_future(_answer())

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(is_local=True)

    def id(self) -> FaceId:
        return self._id


def _make_fw() -> Forwarder:
    # Short backoff so cross-Interest behaviour is testable without long waits.
    return Forwarder(strategy=BestRoute(backoff_base=0.05, max_backoff=0.2))


class TestFailover:
    @pytest.mark.asyncio
    async def test_primary_timeout_backup_serves(self):
        fw = _make_fw()
        name = Name(rns_addr(0x01), [b"file"])
        response = Data.new(name=name, content=b"from-backup")

        primary = _ScriptedFace(11, fw, response=None)            # silent
        backup = _ScriptedFace(22, fw, response=response)         # answers
        fw.register_face(primary)
        fw.register_face(backup)
        fw.add_route(name, primary.id(), cost=10)
        fw.add_route(name, backup.id(), cost=20)

        result = await fw.express(Interest(name=name, lifetime_ms=100), 0)

        assert result is not None
        assert result.content == b"from-backup"
        assert primary.sent, "primary should have been tried first"
        assert backup.sent, "backup should have been tried after primary timeout"

    @pytest.mark.asyncio
    async def test_both_fail_returns_none(self):
        fw = _make_fw()
        name = Name(rns_addr(0x02), [b"file"])
        primary = _ScriptedFace(11, fw, response=None)
        backup = _ScriptedFace(22, fw, response=None)
        fw.register_face(primary)
        fw.register_face(backup)
        fw.add_route(name, primary.id(), cost=10)
        fw.add_route(name, backup.id(), cost=20)

        result = await fw.express(Interest(name=name, lifetime_ms=60), 0)

        assert result is None
        assert primary.sent and backup.sent

    @pytest.mark.asyncio
    async def test_primary_success_skips_backup(self):
        fw = _make_fw()
        name = Name(rns_addr(0x03), [b"file"])
        response = Data.new(name=name, content=b"from-primary")
        primary = _ScriptedFace(11, fw, response=response)
        backup = _ScriptedFace(22, fw, response=None)
        fw.register_face(primary)
        fw.register_face(backup)
        fw.add_route(name, primary.id(), cost=10)
        fw.add_route(name, backup.id(), cost=20)

        result = await fw.express(Interest(name=name, lifetime_ms=2000), 0)

        assert result is not None and result.content == b"from-primary"
        assert primary.sent
        assert not backup.sent, "backup must not be tried when primary answers"

    @pytest.mark.asyncio
    async def test_lowest_cost_tried_first(self):
        # Backup has the *lower* cost here, so it must be the one tried first.
        fw = _make_fw()
        name = Name(rns_addr(0x04), [b"file"])
        response = Data.new(name=name, content=b"cheap")
        cheap = _ScriptedFace(22, fw, response=response)
        pricey = _ScriptedFace(11, fw, response=None)
        fw.register_face(cheap)
        fw.register_face(pricey)
        fw.add_route(name, pricey.id(), cost=50)
        fw.add_route(name, cheap.id(), cost=5)

        result = await fw.express(Interest(name=name, lifetime_ms=2000), 0)

        assert result is not None and result.content == b"cheap"
        assert cheap.sent
        assert not pricey.sent, "higher-cost face must not be tried when cheaper one answers"

    @pytest.mark.asyncio
    async def test_hop_limit_decremented_once_across_failover(self):
        # Trying a backup is the same logical hop: hop_limit drops by exactly 1
        # even though two next-hops are contacted.
        fw = _make_fw()
        name = Name(rns_addr(0x05), [b"file"])
        response = Data.new(name=name, content=b"x")
        primary = _ScriptedFace(11, fw, response=None)
        backup = _ScriptedFace(22, fw, response=response)
        fw.register_face(primary)
        fw.register_face(backup)
        fw.add_route(name, primary.id(), cost=10)
        fw.add_route(name, backup.id(), cost=20)

        await fw.express(Interest(name=name, hop_limit=5, lifetime_ms=100), 0)

        sent_primary = parse_packet(primary.sent[0]).interest
        sent_backup = parse_packet(backup.sent[0]).interest
        assert sent_primary is not None and sent_backup is not None
        assert sent_primary.hop_limit == 4
        assert sent_backup.hop_limit == 4  # not 3 — decremented once, not per-hop


class TestUsableFaces:
    def test_orders_and_filters_backoff(self):
        s = BestRoute()
        faces = [(11, 10), (22, 20), (33, 30)]
        assert s.usable_faces(faces) == [11, 22, 33]

        s.record_failure(22)
        assert s.usable_faces(faces) == [11, 33]

    def test_empty_when_all_backed_off(self):
        s = BestRoute()
        s.record_failure(11)
        assert s.usable_faces([(11, 10)]) == []
