"""Tests for Interest NACK — fast multi-path failover (Part B2).

A NACK lets an upstream say "I can't satisfy this" immediately, so a downstream
forward fails over to a backup face in ~one RTT instead of waiting out the
Interest lifetime. It is capability-gated (FEATURE_NACK) and carries no content.
"""

import asyncio

import pytest

from rns_icn.face import Face, FaceCapabilities, FaceId
from rns_icn.forwarder import _NACK, Forwarder
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import (
    FEATURE_NACK,
    Data,
    Interest,
    Nack,
    NackReason,
    PacketType,
    parse_packet,
)
from rns_icn.server import ICNServer
from rns_icn.strategy import BestRoute


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


# ── Wire ──


def test_nack_round_trip():
    name = Name(rns_addr(), [b"file"])
    nack = Nack(name=name, reason=NackReason.CONGESTION)
    decoded = Nack.from_bytes(nack.to_bytes())
    assert decoded.name == name
    assert decoded.reason is NackReason.CONGESTION


def test_parse_packet_routes_nack():
    nack = Nack(name=Name(rns_addr(), [b"x"]), reason=NackReason.NO_ROUTE)
    pkt = parse_packet(nack.to_bytes())
    assert pkt.type is PacketType.NACK
    assert pkt.nack is not None
    assert pkt.nack.reason is NackReason.NO_ROUTE


# ── Forwarder receive role ──


@pytest.mark.asyncio
async def test_receive_nack_resolves_pending_notifier():
    fw = Forwarder()
    name = Name(rns_addr(), [b"file"])
    fut = asyncio.get_event_loop().create_future()
    fw._pit_notifiers.setdefault(name, []).append(fut)

    fw.receive_nack(name, in_face=5)

    assert fut.done()
    assert fut.result() is _NACK


class _ScriptedFace(Face):
    """Answers a forwarded Interest with preset Data after a small delay."""

    __test__ = False

    def __init__(self, face_id: FaceId, fw: Forwarder, response: Data, delay: float = 0.01):
        self._id, self._fw, self._response, self._delay = face_id, fw, response, delay
        self.sent: list[bytes] = []

    async def express_interest(self, interest: Interest) -> Data | None:
        return None

    async def send_data(self, data: Data) -> None:
        pass

    async def send_raw(self, raw: bytes) -> None:
        self.sent.append(raw)

        async def _answer() -> None:
            await asyncio.sleep(self._delay)
            await self._fw.receive_data(self._response, self._id)

        asyncio.ensure_future(_answer())

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(is_local=True)

    def id(self) -> FaceId:
        return self._id


class _NackFace(Face):
    """Responds to any forwarded Interest with a NACK after a small delay."""

    __test__ = False

    def __init__(self, face_id: FaceId, fw: Forwarder, delay: float = 0.01):
        self._id, self._fw, self._delay = face_id, fw, delay
        self.sent: list[bytes] = []

    async def express_interest(self, interest: Interest) -> Data | None:
        return None

    async def send_data(self, data: Data) -> None:
        pass

    async def send_raw(self, raw: bytes) -> None:
        self.sent.append(raw)
        interest = parse_packet(raw).interest

        async def _nack() -> None:
            await asyncio.sleep(self._delay)
            self._fw.receive_nack(interest.name, self._id)

        asyncio.ensure_future(_nack())

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(is_local=True)

    def id(self) -> FaceId:
        return self._id


@pytest.mark.asyncio
async def test_nack_triggers_immediate_failover():
    fw = Forwarder(strategy=BestRoute(backoff_base=0.05, max_backoff=0.2))
    name = Name(rns_addr(), [b"file"])
    response = Data.new(name=name, content=b"from-backup")
    primary = _NackFace(11, fw)                       # NACKs fast
    backup = _ScriptedFace(22, fw, response=response)  # answers
    fw.register_face(primary)
    fw.register_face(backup)
    fw.add_route(name, primary.id(), cost=10)
    fw.add_route(name, backup.id(), cost=20)

    loop = asyncio.get_event_loop()
    start = loop.time()
    # Lifetime is 2s; if failover waited for a timeout this would take ~2s.
    result = await fw.express(Interest(name=name, lifetime_ms=2000), 0)
    elapsed = loop.time() - start

    assert result is not None and result.content == b"from-backup"
    assert primary.sent and backup.sent
    assert elapsed < 0.5, f"failover should be ~RTT, took {elapsed:.3f}s"
    # The NACKed primary was recorded as a failure (backoff), not the backup.
    assert 11 in fw.strategy._failures
    assert 22 not in fw.strategy._failures


# ── Server send role + gating ──


class _CaptureFace(Face):
    __test__ = False

    def __init__(self, face_id: FaceId):
        self._id = face_id
        self.raw_sent: list[bytes] = []
        self.data_sent: list[Data] = []

    async def express_interest(self, interest: Interest) -> Data | None:
        return None

    async def send_data(self, data: Data) -> None:
        self.data_sent.append(data)

    async def send_raw(self, raw: bytes) -> None:
        self.raw_sent.append(raw)

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(is_local=True)

    def id(self) -> FaceId:
        return self._id


class _NackCapableServer(ICNServer):
    def _peer_supports_nack(self, face_id: FaceId) -> bool:
        return True


@pytest.mark.asyncio
async def test_handle_interest_nacks_capable_peer_on_no_route():
    server = _NackCapableServer(rns_identity=rns_addr(0xAA))
    face = _CaptureFace(7)
    server._faces[7] = face

    await server.handle_interest(Interest(name=Name(rns_addr(0xBB), [b"x"])), 7)

    assert len(face.raw_sent) == 1
    pkt = parse_packet(face.raw_sent[0])
    assert pkt.nack is not None and pkt.nack.reason is NackReason.NO_ROUTE


@pytest.mark.asyncio
async def test_handle_interest_silent_to_legacy_peer():
    # Base server's _peer_supports_nack is False → no NACK for an unroutable name.
    server = ICNServer(rns_identity=rns_addr(0xAA))
    face = _CaptureFace(7)
    server._faces[7] = face

    await server.handle_interest(Interest(name=Name(rns_addr(0xBB), [b"x"])), 7)

    assert face.raw_sent == []


@pytest.mark.asyncio
async def test_handle_incoming_routes_nack_to_forwarder():
    server = ICNServer(rns_identity=rns_addr(0xAA))
    name = Name(rns_addr(0xBB), [b"file"])
    fut = asyncio.get_event_loop().create_future()
    server.forwarder._pit_notifiers.setdefault(name, []).append(fut)

    await server.handle_incoming(7, Nack(name=name).to_bytes())

    assert fut.done() and fut.result() is _NACK


def test_server_advertises_nack_feature():
    # Sanity: the feature bit is defined and distinct.
    assert FEATURE_NACK == 0x00000020
