"""Tests for cache coherency (ICN Roadmap Phase 2.4).

Covers the three pieces that make caching coherent:
  - freshness period: Data declares a lifetime, caches age it to stale
  - stale-while-revalidate: serve stale immediately + refresh in background
  - signed invalidation: a producer-signed purge applied + forwarded one hop
"""

import asyncio
import sqlite3

import pytest
import RNS

from rns_icn.content_store import ContentStore
from rns_icn.face import Face, FaceCapabilities
from rns_icn.forwarder import Forwarder
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import (
    Data,
    DataMetadata,
    Freshness,
    Interest,
    Invalidate,
    InvalidateError,
    PacketType,
    parse_packet,
)
from rns_icn.server import ICNServer
from rns_icn.strategy import BestRoute, StrategyDecision


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


@pytest.fixture
def identity() -> RNS.Identity:
    return RNS.Identity()


# ── Wire format: freshness period ──


def test_freshness_period_round_trip():
    meta = DataMetadata(content_hash=b"\x00" * 32, freshness_period=300)
    parsed = DataMetadata.from_bytes(meta.to_bytes())
    assert parsed.freshness_period == 300


def test_data_with_freshness_period_survives_serialization():
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"hello").with_freshness_period(120)
    parsed = Data.from_bytes(data.to_bytes())
    assert parsed.metadata.freshness_period == 120


def test_data_without_period_parses_as_none():
    """Legacy Data (no period flag) must still parse, with period None."""
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"hello")
    assert data.metadata.freshness_period is None
    parsed = Data.from_bytes(data.to_bytes())
    assert parsed.metadata.freshness_period is None


# ── Wire format: Invalidate ──


def test_invalidate_round_trip_unsigned():
    inv = Invalidate(name=Name(rns_addr(), [b"doc"]), epoch=42, is_prefix=True)
    parsed = Invalidate.from_bytes(inv.to_bytes())
    assert parsed.name == inv.name
    assert parsed.epoch == 42
    assert parsed.is_prefix is True
    assert parsed.signature is None


def test_invalidate_sign_verify_round_trip(identity):
    inv = Invalidate(name=Name(rns_addr(), [b"doc"]), epoch=7)
    inv.sign(identity.sign)
    parsed = Invalidate.from_bytes(inv.to_bytes())
    assert parsed.signature == inv.signature
    assert parsed.verify_signature(identity.validate)


def test_invalidate_tamper_fails_verification(identity):
    inv = Invalidate(name=Name(rns_addr(), [b"doc"]), epoch=7).sign(identity.sign)
    inv.epoch = 8  # mutate after signing
    assert not inv.verify_signature(identity.validate)


def test_invalidate_bad_signature_length_rejected():
    inv = Invalidate(name=Name(rns_addr(), [b"doc"]))
    with pytest.raises(InvalidateError):
        inv.sign(lambda _h: b"too-short")


def test_parse_packet_dispatches_invalidate(identity):
    inv = Invalidate(name=Name(rns_addr(), [b"doc"]), epoch=1).sign(identity.sign)
    pkt = parse_packet(inv.to_bytes())
    assert pkt.type == PacketType.INVALIDATE
    assert pkt.invalidate is not None
    assert pkt.invalidate.epoch == 1


# ── ContentStore: dynamic freshness ──


def _backdate(cs: ContentStore, seconds: int) -> None:
    cs._conn.execute(
        "UPDATE content SET inserted_at = inserted_at - ?", (seconds,)
    )


def test_entry_goes_stale_after_freshness_period():
    cs = ContentStore()
    name = Name(rns_addr(0x01), [b"doc"])
    cs.insert(name, Data.new(name=name, content=b"x").with_freshness_period(10))

    fresh = cs.get(name)
    assert fresh is not None
    assert fresh.metadata.freshness.fresh is True
    assert fresh.metadata.freshness_period == 10

    _backdate(cs, 100)
    stale = cs.get(name)
    assert stale is not None
    assert stale.metadata.freshness.fresh is False
    assert stale.metadata.freshness.age_seconds >= 100


def test_entry_without_period_stays_fresh():
    cs = ContentStore()
    name = Name(rns_addr(0x02), [b"doc"])
    cs.insert(name, Data.new(name=name, content=b"x"))  # no declared period
    _backdate(cs, 10_000)
    got = cs.get(name)
    assert got is not None
    assert got.metadata.freshness.fresh is True


# ── ContentStore: invalidate ──


def test_invalidate_exact_removes_only_that_name():
    cs = ContentStore()
    a = Name(rns_addr(0x01), [b"a"])
    b = Name(rns_addr(0x01), [b"b"])
    cs.insert(a, Data.new(name=a, content=b"1"))
    cs.insert(b, Data.new(name=b, content=b"2"))

    removed = cs.invalidate(a)
    assert removed == 1
    assert not cs.contains(a)
    assert cs.contains(b)


def test_invalidate_prefix_removes_all_under_it():
    cs = ContentStore()
    prefix = Name(rns_addr(0x01), [b"app"])
    child1 = Name(rns_addr(0x01), [b"app", b"x"])
    child2 = Name(rns_addr(0x01), [b"app", b"y"])
    other = Name(rns_addr(0x01), [b"other"])
    for n in (child1, child2, other):
        cs.insert(n, Data.new(name=n, content=b"c"))

    removed = cs.invalidate(prefix, prefix=True)
    assert removed == 2
    assert not cs.contains(child1)
    assert not cs.contains(child2)
    assert cs.contains(other)


def test_migration_adds_freshness_period_column(tmp_path):
    """An on-disk DB predating the column upgrades cleanly on open."""
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE content (
            name_hash BLOB PRIMARY KEY, name_bytes BLOB NOT NULL,
            content_bytes BLOB NOT NULL, content_hash BLOB NOT NULL,
            sequence INTEGER, freshness INTEGER DEFAULT 1,
            age_seconds INTEGER DEFAULT 0, metadata_json TEXT,
            inserted_at INTEGER NOT NULL, expires_at INTEGER,
            size_bytes INTEGER NOT NULL);
        CREATE TABLE name_prefixes (
            prefix_hash BLOB NOT NULL, name_hash BLOB NOT NULL,
            PRIMARY KEY (prefix_hash, name_hash));
    """)
    conn.commit()
    conn.close()

    cs = ContentStore(path=str(path))
    cols = {r[1] for r in cs._conn.execute("PRAGMA table_info(content)")}
    assert "freshness_period" in cols

    name = Name(rns_addr(0x01), [b"doc"])
    cs.insert(name, Data.new(name=name, content=b"x").with_freshness_period(60))
    got = cs.get(name)
    assert got is not None
    assert got.metadata.freshness_period == 60


# ── Strategy: stale-while-revalidate decisions ──


def _stale_data(name: Name, age: int, period: int) -> Data:
    data = Data.new(name=name, content=b"stale")
    data.metadata.freshness = Freshness(fresh=False, age_seconds=age)
    data.metadata.freshness_period = period
    return data


@pytest.mark.asyncio
async def test_stale_within_window_serves_stale_revalidate():
    s = BestRoute(stale_while_revalidate=300)
    name = Name(rns_addr(0x01), [b"doc"])
    cs_hit = _stale_data(name, age=120, period=60)  # 120 < 60 + 300
    decision, face = await s.decide(Interest(name=name), [(9, 10)], None, cs_hit)
    assert decision == StrategyDecision.SERVE_STALE_REVALIDATE
    assert face == 9


@pytest.mark.asyncio
async def test_stale_beyond_window_serves_from_cache():
    s = BestRoute(stale_while_revalidate=10)
    name = Name(rns_addr(0x01), [b"doc"])
    cs_hit = _stale_data(name, age=500, period=60)  # 500 > 60 + 10
    decision, _ = await s.decide(Interest(name=name), [(9, 10)], None, cs_hit)
    assert decision == StrategyDecision.SERVE_FROM_CACHE


@pytest.mark.asyncio
async def test_swr_disabled_by_default_serves_from_cache():
    s = BestRoute()  # stale_while_revalidate=0
    name = Name(rns_addr(0x01), [b"doc"])
    cs_hit = _stale_data(name, age=120, period=60)
    decision, _ = await s.decide(Interest(name=name), [(9, 10)], None, cs_hit)
    assert decision == StrategyDecision.SERVE_FROM_CACHE


@pytest.mark.asyncio
async def test_must_be_fresh_skips_stale_cache():
    s = BestRoute(stale_while_revalidate=300)
    name = Name(rns_addr(0x01), [b"doc"])
    cs_hit = _stale_data(name, age=120, period=60)
    interest = Interest(name=name, must_be_fresh=True)
    decision, face = await s.decide(interest, [(9, 10)], None, cs_hit)
    assert decision == StrategyDecision.FORWARD_TO
    assert face == 9


@pytest.mark.asyncio
async def test_stale_with_no_route_serves_from_cache():
    s = BestRoute(stale_while_revalidate=300)
    name = Name(rns_addr(0x01), [b"doc"])
    cs_hit = _stale_data(name, age=120, period=60)
    decision, _ = await s.decide(Interest(name=name), [], None, cs_hit)
    assert decision == StrategyDecision.SERVE_FROM_CACHE


# ── Forwarder: serve stale + background revalidation ──


class _RevalFace(Face):
    """Face that answers each forwarded Interest with fresh Data."""

    __test__ = False

    def __init__(self, face_id, forwarder, fresh_content: bytes):
        self._id = face_id
        self._fwd = forwarder
        self._fresh = fresh_content
        self.sent: list[bytes] = []

    async def send_raw(self, raw: bytes) -> None:
        self.sent.append(raw)
        interest = Interest.from_bytes(raw)
        reply = Data.new(name=interest.name, content=self._fresh).with_freshness_period(1000)
        await self._fwd.receive_data(reply, self._id)

    async def send_data(self, data) -> None:
        pass

    async def express_interest(self, interest):
        return None

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(is_local=True)

    def id(self):
        return self._id


@pytest.mark.asyncio
async def test_forwarder_serves_stale_and_revalidates_once():
    fwd = Forwarder(strategy=BestRoute(stale_while_revalidate=3600))
    name = Name(rns_addr(0x01), [b"doc"])
    fwd.cs.insert(name, Data.new(name=name, content=b"stale").with_freshness_period(1))
    _backdate(fwd.cs, 100)  # age past period, within SWR window

    face = _RevalFace(7, fwd, fresh_content=b"fresh")
    fwd.register_face(face)
    fwd.add_route(name, face.id())

    # Two stale hits (distinct nonces) should serve stale and fire ONE
    # revalidation between them.
    r1 = await fwd.express(Interest(name=name, lifetime_ms=100), in_face=0)
    r2 = await fwd.express(Interest(name=name, lifetime_ms=100), in_face=0)
    assert r1 is not None and r1.content == b"stale"
    assert r2 is not None and r2.content == b"stale"

    await asyncio.sleep(0.3)  # let the (single, deduped) background refresh land
    assert len(face.sent) == 1  # deduped: two stale hits → one revalidation
    refreshed = fwd.cs.get(name)
    assert refreshed is not None
    assert refreshed.content == b"fresh"
    assert refreshed.metadata.freshness.fresh is True


# ── Server: signed invalidation apply + 1-hop forward ──


def _server_with_verifier(identity: RNS.Identity) -> ICNServer:
    return ICNServer(
        rns_identity=rns_addr(0x01),
        signer=identity.sign,
        invalidation_verifier=lambda inv: inv.verify_signature(identity.validate),
    )


async def _drain(server: ICNServer, face_id) -> list[bytes]:
    q = server.get_face_send_queue(face_id)
    out = []
    while q is not None and not q.empty():
        out.append(q.get_nowait())
    return out


@pytest.mark.asyncio
async def test_signed_invalidate_purges_and_forwards_one_hop(identity):
    server = _server_with_verifier(identity)
    face_in = server._new_face()
    face_out = server._new_face()

    name = Name(rns_addr(0x01), [b"doc"])
    server.forwarder.cs.insert(name, Data.new(name=name, content=b"x"))

    inv = Invalidate(name=name, epoch=100).sign(identity.sign)
    await server.handle_invalidate(inv, face_in.id())

    assert not server.forwarder.cs.contains(name)
    # Forwarded to the other face, not back to the incoming one.
    assert await _drain(server, face_out.id()) == [inv.to_bytes()]
    assert await _drain(server, face_in.id()) == []


@pytest.mark.asyncio
async def test_unsigned_invalidate_is_dropped(identity):
    server = _server_with_verifier(identity)
    face = server._new_face()
    name = Name(rns_addr(0x01), [b"doc"])
    server.forwarder.cs.insert(name, Data.new(name=name, content=b"x"))

    inv = Invalidate(name=name, epoch=100)  # no signature
    await server.handle_invalidate(inv, face.id())

    assert server.forwarder.cs.contains(name)  # untouched


@pytest.mark.asyncio
async def test_replayed_invalidate_is_ignored(identity):
    server = _server_with_verifier(identity)
    server._new_face()
    name = Name(rns_addr(0x01), [b"doc"])

    server.forwarder.cs.insert(name, Data.new(name=name, content=b"x"))
    await server.handle_invalidate(
        Invalidate(name=name, epoch=100).sign(identity.sign), 999
    )
    assert not server.forwarder.cs.contains(name)

    # Re-publish, then replay the same epoch — must be ignored.
    server.forwarder.cs.insert(name, Data.new(name=name, content=b"x2"))
    await server.handle_invalidate(
        Invalidate(name=name, epoch=100).sign(identity.sign), 999
    )
    assert server.forwarder.cs.contains(name)  # replay did not purge

    # A newer epoch goes through.
    await server.handle_invalidate(
        Invalidate(name=name, epoch=101).sign(identity.sign), 999
    )
    assert not server.forwarder.cs.contains(name)


@pytest.mark.asyncio
async def test_originate_invalidation_purges_locally_and_pushes(identity):
    server = _server_with_verifier(identity)
    face = server._new_face()
    name = Name(rns_addr(0x01), [b"doc"])
    server.forwarder.cs.insert(name, Data.new(name=name, content=b"x"))

    inv = await server.invalidate(name)
    assert inv.signature is not None
    assert not server.forwarder.cs.contains(name)
    assert await _drain(server, face.id()) == [inv.to_bytes()]


@pytest.mark.asyncio
async def test_originate_invalidation_requires_signer():
    server = ICNServer(rns_identity=rns_addr(0x01))  # no signer
    with pytest.raises(RuntimeError):
        await server.invalidate(Name(rns_addr(0x01), [b"doc"]))
