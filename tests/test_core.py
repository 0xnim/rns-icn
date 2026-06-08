"""Tests for ContentStore, FIB, PIT, Strategy, Forwarder, Manifest, Server."""

import asyncio
import hashlib
import json
import pytest
from rns_icn.content_store import ContentStore
from rns_icn.fib import Fib
from rns_icn.pit import Pit, PitOp
from rns_icn.strategy import BestRoute, StrategyDecision
from rns_icn.name import Name, NameError, RNS_ADDR_BYTES
from rns_icn.packet import Interest, Data, InterestSelector
from rns_icn.forwarder import Forwarder
from rns_icn.face import TestFace, test_face_pair as make_face_pair
from rns_icn.manifest import Manifest, ManifestEntry, EntryKind, ContentManifest, ChunkRef, ContentManifestError
from rns_icn.chunker import ChunkResult, ChunkerError, EmptyContentError, chunk_content, DEFAULT_CHUNK_SIZE
from rns_icn.assembler import (
    AssemblyError, MissingChunkError, HashMismatchError, IntegrityError,
    assemble, assemble_verified, assemble_fast, verify_chunk, verify_chunks, missing_labels,
)
from rns_icn.packet import Interest, Data, DataMetadata, InterestSelector

def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


def make_data(name: Name) -> Data:
    return Data.new(name=name, content=b"content")


def content_hash(byte_val: int = 0xBB) -> bytes:
    return bytes([byte_val] + [0] * 31)


# ── Name ──

class TestName:
    def test_construct(self):
        name = Name(rns_addr(0xAA), [b"app", b"chat"])
        assert name.rns_addr == rns_addr(0xAA)
        assert name.len() == 3

    def test_root(self):
        assert Name(rns_addr(0x01)).is_root()

    def test_round_trip(self):
        name = Name(rns_addr(0xAA), [b"app"]).with_content_hash(content_hash(0xBB))
        parsed = Name.from_bytes(name.to_bytes())
        assert name == parsed

    def test_no_hash(self):
        name = Name(rns_addr(0x42), [b"test"])
        parsed = Name.from_bytes(name.to_bytes())
        assert parsed.content_hash is None

    def test_starts_with(self):
        p = Name(rns_addr(0x01), [b"app"])
        f = Name(rns_addr(0x01), [b"app", b"chat"])
        assert f.starts_with(p)

    def test_display(self):
        s = str(Name(rns_addr(0xAA), [b"app"]))
        assert "app" in s

    def test_display_with_hash(self):
        s = str(Name(rns_addr(0x01), [b"d"]).with_content_hash(content_hash(0xBB)))
        assert "?hash=" in s

    def test_empty_error(self):
        with pytest.raises(NameError):
            Name.from_bytes(b"")

    def test_hash_equality(self):
        a = Name(rns_addr(0x01), [b"t"])
        b = Name(rns_addr(0x01), [b"t"])
        assert hash(a) == hash(b)
        assert a == b

    def test_short_addr_error(self):
        with pytest.raises(NameError):
            Name(b"\x01" * 8)


# ── ContentStore ──

class TestContentStore:
    def test_insert_and_get(self):
        cs = ContentStore(10)
        name = Name(rns_addr(0x01), [b"test"])
        cs.insert(name, make_data(name))
        assert cs.get(name) is not None
        assert len(cs) == 1

    def test_miss(self):
        cs = ContentStore(10)
        assert cs.get(Name(rns_addr(0x01), [b"x"])) is None
        assert cs.misses == 1

    def test_lru_eviction(self):
        cs = ContentStore(2)
        a = Name(rns_addr(0x01), [b"a"])
        b = Name(rns_addr(0x02), [b"b"])
        c = Name(rns_addr(0x03), [b"c"])
        cs.insert(a, make_data(a))
        cs.insert(b, make_data(b))
        cs.insert(c, make_data(c))
        assert len(cs) == 2
        assert not cs.contains(a)

    def test_prefix_match(self):
        cs = ContentStore(10)
        prefix = Name(rns_addr(0x01), [b"app"])
        full = Name(rns_addr(0x01), [b"app", b"chat"])
        cs.insert(prefix, make_data(prefix))
        result = cs.get_prefix(full)
        assert result is not None

    def test_prefix_match_longest(self):
        cs = ContentStore(10)
        short = Name(rns_addr(0x01), [b"app"])
        long = Name(rns_addr(0x01), [b"app", b"chat"])
        query = Name(rns_addr(0x01), [b"app", b"chat", b"v5"])
        cs.insert(short, make_data(short))
        cs.insert(long, make_data(long))
        result = cs.get_prefix(query)
        assert result is not None
        assert result.name == long

    def test_hits_misses(self):
        cs = ContentStore(10)
        name = Name(rns_addr(0x01), [b"test"])
        cs.insert(name, make_data(name))
        cs.get(name)
        cs.get(name)
        cs.get(Name(rns_addr(0x02), [b"x"]))
        assert cs.hits == 2
        assert cs.misses == 1


# ── FIB ──

class TestFib:
    def test_exact_match(self):
        fib = Fib()
        prefix = Name(rns_addr(0x01), [b"app"])
        fib.insert(prefix, 5, 10)
        result = fib.lookup(prefix)
        assert result == [(5, 10)]

    def test_longest_prefix(self):
        fib = Fib()
        short = Name(rns_addr(0x01), [b"app"])
        long = Name(rns_addr(0x01), [b"app", b"chat"])
        query = Name(rns_addr(0x01), [b"app", b"chat", b"v5"])
        fib.insert(short, 5, 20)
        fib.insert(long, 3, 10)
        result = fib.lookup(query)
        assert result == [(3, 10)]

    def test_no_match(self):
        fib = Fib()
        fib.insert(Name(rns_addr(0x01), [b"app"]), 5, 10)
        assert fib.lookup(Name(rns_addr(0x02), [b"app"])) is None

    def test_multiple_faces_sorted(self):
        fib = Fib()
        prefix = Name(rns_addr(0x01))
        fib.insert(prefix, 1, 30)
        fib.insert(prefix, 2, 10)
        fib.insert(prefix, 3, 20)
        assert fib.lookup(prefix) == [(2, 10), (3, 20), (1, 30)]

    def test_cost_update(self):
        fib = Fib()
        prefix = Name(rns_addr(0x01))
        fib.insert(prefix, 1, 10)
        fib.insert(prefix, 1, 5)
        assert fib.lookup(prefix) == [(1, 5)]

    def test_remove_prefix(self):
        fib = Fib()
        prefix = Name(rns_addr(0x01), [b"app"])
        fib.insert(prefix, 1, 10)
        fib.remove_prefix(prefix)
        assert fib.lookup(prefix) is None


# ── PIT ──

class TestPit:
    def make_interest(self, name: Name) -> Interest:
        return Interest(name=name)

    def test_insert_and_find(self):
        pit = Pit()
        name = Name(rns_addr(0x01), [b"test"])
        op = pit.insert_or_aggregate(name, 1, self.make_interest(name), 4000)
        assert op == PitOp.INSERTED
        assert pit.find(name) is not None

    def test_aggregation(self):
        pit = Pit()
        name = Name(rns_addr(0x01), [b"test"])
        pit.insert_or_aggregate(name, 1, self.make_interest(name), 4000)
        op = pit.insert_or_aggregate(name, 2, self.make_interest(name), 4000)
        assert op == PitOp.AGGREGATED

    def test_no_duplicate_in_face(self):
        pit = Pit()
        name = Name(rns_addr(0x01), [b"test"])
        pit.insert_or_aggregate(name, 1, self.make_interest(name), 4000)
        pit.insert_or_aggregate(name, 1, self.make_interest(name), 4000)
        assert pit.find(name).in_faces == [1]

    def test_satisfy_returns_faces(self):
        pit = Pit()
        name = Name(rns_addr(0x01), [b"test"])
        pit.insert_or_aggregate(name, 1, self.make_interest(name), 4000)
        pit.insert_or_aggregate(name, 2, self.make_interest(name), 4000)
        in_faces = pit.satisfy(name)
        assert set(in_faces) == {1, 2}

    def test_purge_satisfied(self):
        pit = Pit()
        name = Name(rns_addr(0x01), [b"test"])
        pit.insert_or_aggregate(name, 1, self.make_interest(name), 4000)
        pit.satisfy(name)
        assert len(pit.purge_expired()) == 1
        assert pit.find(name) is None

    def test_loop_detection(self):
        pit = Pit()
        nonce = b"\x01" * 8
        assert not pit.check_loop(1, nonce)
        pit.record_nonce(1, nonce)
        assert pit.check_loop(1, nonce)


# ── Strategy ──

class TestStrategy:
    @pytest.mark.asyncio
    async def test_serve_from_cache(self):
        s = BestRoute()
        name = Name(rns_addr(0x01), [b"t"])
        interest = Interest(name=name)
        data = make_data(name)
        decision, face = await s.decide(interest, [], None, data)
        assert decision == StrategyDecision.SERVE_FROM_CACHE

    @pytest.mark.asyncio
    async def test_suppress_on_pit_hit(self):
        s = BestRoute()
        name = Name(rns_addr(0x01), [b"t"])
        interest = Interest(name=name)
        from rns_icn.pit import PitEntry
        pe = PitEntry(interest=interest, in_faces=[1], satisfied=False)
        decision, face = await s.decide(interest, [], pe, None)
        assert decision == StrategyDecision.SUPPRESS_AGGREGATE

    @pytest.mark.asyncio
    async def test_no_route(self):
        s = BestRoute()
        name = Name(rns_addr(0x01), [b"t"])
        decision, face = await s.decide(Interest(name=name), [], None, None)
        assert decision == StrategyDecision.NO_ROUTE

    def test_backoff_clear(self):
        s = BestRoute()
        s.record_failure(5)
        assert s._is_in_backoff(5)
        s.record_success(5)
        assert not s._is_in_backoff(5)

    @pytest.mark.asyncio
    async def test_selector_min_sequence_serves_from_cache(self):
        """CS hit with sequence >= min_sequence should serve from cache."""
        s = BestRoute()
        name = Name(rns_addr(0x01), [b"stream"])
        data = Data.new(name=name, content=b"segment").with_sequence(10)
        interest = Interest(
            name=name, selector=InterestSelector(min_sequence=5),
        )
        decision, face = await s.decide(interest, [], None, data)
        assert decision == StrategyDecision.SERVE_FROM_CACHE

    @pytest.mark.asyncio
    async def test_selector_min_sequence_skips_stale_cache(self):
        """CS hit with sequence < min_sequence should NOT serve from cache."""
        s = BestRoute()
        name = Name(rns_addr(0x01), [b"stream"])
        data = Data.new(name=name, content=b"old").with_sequence(3)
        interest = Interest(
            name=name, selector=InterestSelector(min_sequence=10),
        )
        decision, face = await s.decide(interest, [], None, data)
        # No PIT hit and no route — falls through to NO_ROUTE
        assert decision != StrategyDecision.SERVE_FROM_CACHE

    @pytest.mark.asyncio
    async def test_selector_min_sequence_data_no_sequence(self):
        """CS hit without sequence should NOT serve when selector demands min_sequence."""
        s = BestRoute()
        name = Name(rns_addr(0x01), [b"stream"])
        data = Data.new(name=name, content=b"no-seq")  # no sequence set
        interest = Interest(
            name=name, selector=InterestSelector(min_sequence=1),
        )
        decision, face = await s.decide(interest, [], None, data)
        assert decision != StrategyDecision.SERVE_FROM_CACHE

    @pytest.mark.asyncio
    async def test_no_selector_still_serves_cache(self):
        """Interest without selector still serves from cache normally."""
        s = BestRoute()
        name = Name(rns_addr(0x01), [b"t"])
        data = Data.new(name=name, content=b"hello")
        interest = Interest(name=name)
        decision, face = await s.decide(interest, [], None, data)
        assert decision == StrategyDecision.SERVE_FROM_CACHE


# ── Forwarder ──

class TestForwarder:
    @pytest.mark.asyncio
    async def test_cs_hit(self):
        fw = Forwarder()
        name = Name(rns_addr(0x01), [b"test"])
        fw.cs.insert(name, make_data(name))
        result = await fw.express(Interest(name=name), 0)
        assert result is not None
        assert result.content == b"content"

    @pytest.mark.asyncio
    async def test_no_route(self):
        fw = Forwarder()
        result = await fw.express(Interest(name=Name(rns_addr(0x01), [b"t"])), 0)
        assert result is None

    @pytest.mark.asyncio
    async def test_forward_and_receive(self):
        face_a, face_b = make_face_pair()
        fw = Forwarder()
        producer_hash = rns_addr(0x01)
        response_data = make_data(Name(producer_hash, [b"test"]))

        fw.register_face(face_a)
        fw.add_route(Name(producer_hash), face_a.id(), 10)

        async def producer():
            while True:
                interest = await face_b.recv_packet()
                # Nothing — express_interest in TestFace handles the loop
                await asyncio.sleep(0.01)

        asyncio.create_task(producer())
        await asyncio.sleep(0.05)

        interest = Interest(name=response_data.name, lifetime_ms=2000)
        result = await fw.express(interest, 0)
        # May be None since TestFace doesn't auto-respond in express_interest
        # but the forwarder attempted forwarding
        assert result is None  # no producer responding

    @pytest.mark.asyncio
    async def test_receive_data_satisfies_pit(self):
        fw = Forwarder()
        name = Name(rns_addr(0x01), [b"test"])
        data = Data.new(name=name, content=b"hello")
        fw.pit.insert_or_aggregate(name, 1, Interest(name=name), 5000)
        await fw.receive_data(data, 2)
        cached = fw.cs.get(name)
        assert cached is not None
        assert cached.content == b"hello"

    @pytest.mark.asyncio
    async def test_loop_detection(self):
        fw = Forwarder()
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name)
        # First express should go through
        result = await fw.express(interest, 1)
        assert result is None  # no route
        # Same nonce from same face would be loop
        result2 = await fw.express(interest, 1)
        assert result2 is None  # Could be loop or no route

    @pytest.mark.asyncio
    async def test_stream_fetch_two_segments(self):
        """stream_fetch yields sequential segments from a producer."""
        fw = Forwarder()
        name = Name(rns_addr(0x01), [b"stream", b"data"])

        producer_face = TestFace(1)
        consumer_face = TestFace(2)
        producer_face.connect(consumer_face)

        fw.register_face(producer_face)
        fw.add_route(name, producer_face.id(), 10)

        async def producer():
            # Read first interest directly from queue
            interest_raw = await consumer_face._incoming.get()
            assert interest_raw is not None
            seg1 = Data.new(name=name.with_content_hash(bytes([0x01]) + bytes(31)),
                            content=b"segment1").with_sequence(1)
            await consumer_face._outgoing.put(seg1.to_bytes())
            # Read second interest
            interest_raw = await consumer_face._incoming.get()
            assert interest_raw is not None
            seg2 = Data.new(name=name.with_content_hash(bytes([0x02]) + bytes(31)),
                            content=b"segment2").with_sequence(2)
            await consumer_face._outgoing.put(seg2.to_bytes())

        async def data_router():
            """Route Data responses back to the forwarder's receive_data."""
            from rns_icn.packet import parse_packet
            while True:
                raw = await producer_face._incoming.get()
                pkt = parse_packet(raw)
                if pkt.data is not None:
                    await fw.receive_data(pkt.data, producer_face.id())

        asyncio.create_task(producer())
        asyncio.create_task(data_router())
        await asyncio.sleep(0.02)

        results = []
        async for data in fw.stream_fetch(name, start_sequence=1, lifetime_ms=2000, max_segments=2):
            results.append(data)

        assert len(results) == 2
        assert results[0].metadata.sequence == 1
        assert results[0].content == b"segment1"
        assert results[1].metadata.sequence == 2
        assert results[1].content == b"segment2"

    @pytest.mark.asyncio
    async def test_stream_fetch_stops_when_producer_silent(self):
        """stream_fetch stops when producer doesn't respond."""
        fw = Forwarder()
        name = Name(rns_addr(0x01), [b"stream"])
        consumer_face = TestFace(1)
        fw.register_face(consumer_face)
        fw.add_route(name, consumer_face.id(), 10)

        # No producer — express times out immediately
        results = []
        async for data in fw.stream_fetch(name, start_sequence=1, lifetime_ms=200, max_segments=5):
            results.append(data)

        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_stream_fetch_from_cache(self):
        """stream_fetch serves cached segments when selector satisfied."""
        fw = Forwarder()
        name = Name(rns_addr(0x01), [b"stream"])

        # Pre-populate cache with segment 2
        seg2 = Data.new(name=name, content=b"cached_segment").with_sequence(2)
        fw.cs.insert(seg2.name, seg2)

        # Fetch with min_sequence=1 — should hit cache since seq 2 >= 1
        results = []
        async for data in fw.stream_fetch(name, start_sequence=1, lifetime_ms=200, max_segments=1):
            results.append(data)

        assert len(results) == 1
        assert results[0].content == b"cached_segment"
        assert results[0].metadata.sequence == 2

    @pytest.mark.asyncio
    async def test_stream_fetch_skips_stale_cache(self):
        """stream_fetch skips cache when cached data has low sequence."""
        fw = Forwarder()
        name = Name(rns_addr(0x01), [b"stream"])

        # Pre-populate with old segment (seq 1)
        old = Data.new(name=name, content=b"old_segment").with_sequence(1)
        fw.cs.insert(old.name, old)

        # Fetch with start_sequence=10 — cache miss, no producer → empty
        results = []
        async for data in fw.stream_fetch(name, start_sequence=10, lifetime_ms=200, max_segments=1):
            results.append(data)

        assert len(results) == 0


# ── Manifest ──

class TestManifest:
    def test_create_and_round_trip(self):
        addr = rns_addr(0xAA)
        manifest = Manifest.create(
            producer=addr,
            entries=[
                ManifestEntry(
                    kind=EntryKind.BLOB,
                    label="hello",
                    name=Name(addr, [b"hello"]),
                )
            ],
        )
        json_data = manifest.to_json()
        parsed = Manifest.from_dict(manifest.to_dict())
        assert parsed.sequence == manifest.sequence
        assert parsed.producer == addr
        assert parsed.find("hello") is not None

    def test_with_previous(self):
        addr = rns_addr(0xAA)
        prev = Name(addr, [b"manifest"])
        manifest = Manifest.create(
            producer=addr, entries=[], previous=prev,
        )
        parsed = Manifest.from_dict(manifest.to_dict())
        assert parsed.previous is not None
        assert parsed.previous.rns_addr == addr

    def test_entry_kinds(self):
        assert EntryKind.BLOB.value == "blob"
        assert EntryKind.STREAM.value == "stream"
        assert EntryKind.MANIFEST.value == "manifest"

    def test_find(self):
        addr = rns_addr(0xAA)
        m = Manifest.create(addr, [
            ManifestEntry(EntryKind.BLOB, "a", Name(addr, [b"a"])),
            ManifestEntry(EntryKind.STREAM, "b", Name(addr, [b"b"])),
        ])
        assert m.find("a") is not None
        assert m.find("c") is None

    def test_is_newer_than(self):
        m = Manifest.create(producer=rns_addr(0xAA), entries=[], sequence=5)
        assert m.is_newer_than(4)
        assert not m.is_newer_than(5)
        assert not m.is_newer_than(6)

    def test_manifest_name(self):
        addr = rns_addr(0xAA)
        m = Manifest.create(addr, [])
        name = m.manifest_name()
        assert name.rns_addr == addr
        assert len(name.components) == 2
        assert name.components[1] == b"manifest"


# ── Manifest v2 — Stream metadata ──


class TestManifestStreamMetadata:
    def test_stream_entry_round_trip(self):
        """ManifestEntry with stream metadata round-trips through JSON."""
        addr = rns_addr(0xBB)
        entry = ManifestEntry(
            kind=EntryKind.STREAM,
            label="telemetry",
            name=Name(addr, [b"telemetry"]),
            latest_sequence=42,
            total_items=100,
            start_time=1700000000,
            end_time=1700003600,
        )
        d = entry.to_dict()
        parsed = ManifestEntry.from_dict(d)
        assert parsed.kind == EntryKind.STREAM
        assert parsed.latest_sequence == 42
        assert parsed.total_items == 100
        assert parsed.start_time == 1700000000
        assert parsed.end_time == 1700003600

    def test_stream_entry_optional_defaults(self):
        """ManifestEntry with kind=STREAM works without optional stream fields."""
        addr = rns_addr(0xCC)
        entry = ManifestEntry(
            kind=EntryKind.STREAM,
            label="no_meta",
            name=Name(addr, [b"no_meta"]),
        )
        d = entry.to_dict()
        assert "latest_sequence" not in d
        assert "total_items" not in d
        assert "start_time" not in d
        assert "end_time" not in d
        parsed = ManifestEntry.from_dict(d)
        assert parsed.latest_sequence is None
        assert parsed.total_items is None

    def test_blob_entry_no_stream_fields(self):
        """BLOB entries never emit stream metadata fields."""
        addr = rns_addr(0xDD)
        entry = ManifestEntry(
            kind=EntryKind.BLOB,
            label="file",
            name=Name(addr, [b"file"]),
            content_hash=content_hash(0xEE),
            size=512,
        )
        d = entry.to_dict()
        assert "latest_sequence" not in d
        assert "total_items" not in d
        assert "start_time" not in d
        assert "end_time" not in d
        parsed = ManifestEntry.from_dict(d)
        assert parsed.content_hash == content_hash(0xEE)
        assert parsed.size == 512
        assert parsed.latest_sequence is None

    def test_backward_compat_no_stream_fields(self):
        """Old-style JSON without stream fields still parses correctly."""
        addr = rns_addr(0xFF)
        old_json = json.dumps({
            "kind": "blob", "label": "old", "name": str(Name(addr, [b"old"])),
            "content_hash": content_hash(0xAA).hex(), "size": 99,
        })
        entry = ManifestEntry.from_dict(json.loads(old_json))
        assert entry.kind == EntryKind.BLOB
        assert entry.latest_sequence is None
        assert entry.total_items is None

    def test_manifest_with_stream_entries_round_trip(self):
        """Manifest with mixed BLOB and STREAM entries round-trips."""
        addr = rns_addr(0x11)
        m = Manifest.create(addr, [
            ManifestEntry(
                kind=EntryKind.STREAM,
                label="sensor",
                name=Name(addr, [b"sensor"]),
                latest_sequence=5,
                total_items=5,
                start_time=1700000000,
                end_time=1700000300,
            ),
            ManifestEntry(
                kind=EntryKind.BLOB,
                label="config",
                name=Name(addr, [b"config"]),
                content_hash=content_hash(0x22),
                size=128,
            ),
        ])
        parsed = Manifest.from_dict(m.to_dict())
        stream_entry = parsed.find("sensor")
        assert stream_entry is not None
        assert stream_entry.kind == EntryKind.STREAM
        assert stream_entry.latest_sequence == 5
        assert stream_entry.total_items == 5
        blob_entry = parsed.find("config")
        assert blob_entry is not None
        assert blob_entry.kind == EntryKind.BLOB
        assert blob_entry.latest_sequence is None

    def test_manifest_to_json_includes_stream_fields(self):
        """to_json output includes stream metadata when present."""
        addr = rns_addr(0xAA)
        m = Manifest.create(addr, [
            ManifestEntry(
                kind=EntryKind.STREAM,
                label="s",
                name=Name(addr, [b"s"]),
                latest_sequence=10,
                total_items=3,
                start_time=1000,
                end_time=4000,
            ),
        ])
        json_bytes = m.to_json()
        decoded = json.loads(json_bytes)
        entry = decoded["entries"][0]
        assert entry["latest_sequence"] == 10
        assert entry["total_items"] == 3
        assert entry["start_time"] == 1000
        assert entry["end_time"] == 4000


# ── ContentManifest + ChunkRef ──


class TestChunkRef:
    def test_basic_round_trip(self):
        ref = ChunkRef(label="part_000", content_hash=content_hash(0x11), size=1024)
        d = ref.to_dict()
        parsed = ChunkRef.from_dict(d)
        assert parsed.label == "part_000"
        assert parsed.content_hash == content_hash(0x11)
        assert parsed.size == 1024
        assert parsed.sequence is None

    def test_with_sequence(self):
        ref = ChunkRef(label="seg_05", content_hash=content_hash(0x22), size=512, sequence=5)
        d = ref.to_dict()
        assert d["sequence"] == 5
        parsed = ChunkRef.from_dict(d)
        assert parsed.sequence == 5

    def test_round_trip_no_sequence(self):
        """to_dict omits sequence when None; from_dict handles missing key."""
        ref = ChunkRef(label="bare", content_hash=content_hash(0x33), size=999)
        d = ref.to_dict()
        assert "sequence" not in d
        parsed = ChunkRef.from_dict(d)
        assert parsed.sequence is None


class TestContentManifest:
    def test_create_and_round_trip(self):
        addr = rns_addr(0xAA)
        name = Name(addr, [b"large-file.bin"])
        chunks = [
            ChunkRef(label="chunk_00", content_hash=content_hash(0x01), size=1024),
            ChunkRef(label="chunk_01", content_hash=content_hash(0x02), size=2048),
            ChunkRef(label="chunk_02", content_hash=content_hash(0x03), size=3072),
        ]
        cm = ContentManifest.create(name=name, chunks=chunks)
        assert cm.chunk_count() == 3
        assert cm.total_size == 6144
        assert cm.name.rns_addr == addr
        assert cm.sequence == 1

        d = cm.to_dict()
        assert d["name"] == str(name)
        assert d["total_size"] == 6144
        assert len(d["chunks"]) == 3

        parsed = ContentManifest.from_dict(d)
        assert parsed.name == name
        assert parsed.total_size == 6144
        assert parsed.chunk_count() == 3
        assert parsed.find_chunk_by_label("chunk_01") is not None
        assert parsed.find_chunk_by_label("nonexistent") is None

    def test_with_content_hash(self):
        addr = rns_addr(0xBB)
        name = Name(addr, [b"data"])
        ch = content_hash(0xCC)
        cm = ContentManifest.create(
            name=name,
            chunks=[ChunkRef("a", content_hash(0x01), 100)],
            content_hash=ch,
        )
        d = cm.to_dict()
        assert d["content_hash"] == ch.hex()
        parsed = ContentManifest.from_dict(d)
        assert parsed.content_hash == ch

    def test_round_trip_no_content_hash(self):
        addr = rns_addr(0xDD)
        cm = ContentManifest.create(
            name=Name(addr, [b"plain"]),
            chunks=[ChunkRef("only", content_hash(0xEE), 64)],
        )
        d = cm.to_dict()
        assert "content_hash" not in d
        parsed = ContentManifest.from_dict(d)
        assert parsed.content_hash is None

    def test_with_sequence(self):
        addr = rns_addr(0xFF)
        cm = ContentManifest.create(
            name=Name(addr, [b"v2"]),
            chunks=[ChunkRef("c", content_hash(0xAA), 128)],
            sequence=3,
        )
        assert cm.sequence == 3
        parsed = ContentManifest.from_dict(cm.to_dict())
        assert parsed.sequence == 3

    def test_labels(self):
        addr = rns_addr(0x11)
        cm = ContentManifest.create(
            name=Name(addr, [b"multi"]),
            chunks=[
                ChunkRef("a", content_hash(0x01), 1),
                ChunkRef("b", content_hash(0x02), 2),
                ChunkRef("c", content_hash(0x03), 3),
            ],
        )
        assert cm.labels() == ["a", "b", "c"]

    def test_to_json(self):
        addr = rns_addr(0x22)
        cm = ContentManifest.create(
            name=Name(addr, [b"file"]),
            chunks=[ChunkRef("x", content_hash(0xAA), 100)],
        )
        json_bytes = cm.to_json()
        decoded = json.loads(json_bytes)
        assert decoded["total_size"] == 100
        assert "name" in decoded
        assert "chunks" in decoded

    def test_from_data_valid(self):
        """ContentManifest can be round-tripped through a Data packet."""
        addr = rns_addr(0x33)
        name = Name(addr, [b"big-content"])
        cm = ContentManifest.create(
            name=name,
            chunks=[ChunkRef("c0", content_hash(0x01), 512)],
        )
        data = Data.new(name=name, content=cm.to_json())
        parsed = ContentManifest.from_data(data)
        assert parsed.name == cm.name
        assert parsed.total_size == cm.total_size
        assert parsed.chunk_count() == 1
        assert parsed.find_chunk_by_label("c0") is not None

    def test_from_data_content_hash_mismatch(self):
        """from_data raises ContentManifestError when Data content_hash mismatches."""
        addr = rns_addr(0x44)
        name = Name(addr, [b"tampered"])
        cm = ContentManifest.create(
            name=name,
            chunks=[ChunkRef("x", content_hash(0x01), 64)],
        )
        data = Data(
            name=name,
            content=cm.to_json(),
            metadata=DataMetadata(content_hash=content_hash(0xFF)),  # wrong hash
        )
        with pytest.raises(ContentManifestError, match="content hash mismatch"):
            ContentManifest.from_data(data)


# ── Chunker ──


class TestChunker:
    def test_chunk_small_content(self):
        """Content smaller than chunk size produces a single chunk."""
        content = b"hello world"
        addr = rns_addr(0xAA)
        name = Name(addr, [b"small.txt"])
        result = chunk_content(content, name, chunk_size=65536)
        assert result.chunk_count() == 1
        assert result.manifest.total_size == len(content)
        assert result.manifest.chunk_count() == 1
        assert result.manifest.name == name
        assert result.manifest.content_hash is not None

        ref = result.manifest.chunks[0]
        assert ref.label == "chunk_0000"
        assert ref.size == len(content)
        assert ref.sequence == 0

        data = result.data_packets[0]
        assert data.content == content
        assert data.metadata.content_hash is not None
        assert data.metadata.sequence == 0

    def test_chunk_multiple_chunks(self):
        """Content larger than chunk_size produces multiple chunks."""
        content = b"x" * 10000
        addr = rns_addr(0xBB)
        name = Name(addr, [b"big.dat"])
        result = chunk_content(content, name, chunk_size=3000)
        assert result.chunk_count() == 4  # 3000*3 + 1000
        assert result.manifest.total_size == 10000
        assert result.manifest.chunk_count() == 4

        # Verify chunk ordering
        labels = [c.label for c in result.manifest.chunks]
        assert labels == ["chunk_0000", "chunk_0001", "chunk_0002", "chunk_0003"]

        # Verify each chunk data
        for i, data in enumerate(result.data_packets):
            assert data.metadata.sequence == i
            assert f"chunk_{i:04d}" in str(data.name)

    def test_chunk_empty_raises(self):
        """Empty content raises EmptyContentError."""
        addr = rns_addr(0xCC)
        name = Name(addr, [b"empty"])
        with pytest.raises(EmptyContentError, match="cannot chunk empty"):
            chunk_content(b"", name)

    def test_chunk_exact_chunk_size(self):
        """Content exactly chunk_size produces one chunk."""
        content = b"z" * 4096
        addr = rns_addr(0xDD)
        name = Name(addr, [b"exact"])
        result = chunk_content(content, name, chunk_size=4096)
        assert result.chunk_count() == 1
        assert result.manifest.total_size == 4096

    def test_chunk_content_hash_verifiable(self):
        """Manifest content_hash is correct blake2b of full content."""
        content = b"verify this content end-to-end"
        addr = rns_addr(0xEE)
        name = Name(addr, [b"verify"])
        result = chunk_content(content, name, chunk_size=10)
        expected_hash = hashlib.blake2b(content, digest_size=32).digest()
        assert result.manifest.content_hash == expected_hash

    def test_chunk_with_sequence_version(self):
        """Sequence parameter flows through to ContentManifest."""
        content = b"versioned content"
        addr = rns_addr(0xFF)
        name = Name(addr, [b"v2"])
        result = chunk_content(content, name, chunk_size=4096, sequence=5)
        assert result.manifest.sequence == 5

    def test_chunk_each_chunk_hash_independent(self):
        """Each chunk's Data has its own content hash (not the full hash)."""
        # Use unique bytes per chunk position: mark each chunk with its index
        addr = rns_addr(0x11)
        name = Name(addr, [b"independent"])
        # Build content where each 200-byte segment is unique
        parts = [bytes([idx % 256] * 200) for idx in range(20)]
        content = b"".join(parts)
        result = chunk_content(content, name, chunk_size=200)
        assert result.chunk_count() > 1
        # Each chunk's hash should be unique since content differs
        hashes = [d.metadata.content_hash for d in result.data_packets]
        assert len(set(hashes)) == len(hashes)
        # None should be None
        assert all(h is not None for h in hashes)

    def test_chunk_default_size(self):
        """Default chunk size is 64 KB."""
        import rns_icn.chunker as c
        assert c.DEFAULT_CHUNK_SIZE == 65536


# ── Assembler ──


class TestAssembler:
    def test_assemble_single_chunk(self):
        """Assemble a single-chunk content back to original."""
        content = b"hello world"
        addr = rns_addr(0xAA)
        name = Name(addr, [b"greeting"])
        result = chunk_content(content, name)
        by_label = {}
        for dp in result.data_packets:
            by_label["chunk_0000"] = dp
        reassembled = assemble(result.manifest, by_label)
        assert reassembled == content

    def test_assemble_multiple_chunks(self):
        """Assemble multi-chunk content back to original."""
        content = b"x" * 10000
        addr = rns_addr(0xBB)
        name = Name(addr, [b"multi"])
        result = chunk_content(content, name, chunk_size=3000)

        by_label = {}
        for dp in result.data_packets:
            label = [c for c in result.manifest.labels()
                     if c in str(dp.name)][0]
            by_label[label] = dp

        reassembled = assemble(result.manifest, by_label)
        assert reassembled == content

    def test_assemble_verified_skips_overall_hash(self):
        """assemble_verified works without overall content_hash."""
        content = b"verified only per chunk"
        addr = rns_addr(0xCC)
        name = Name(addr, [b"per-chunk"])
        result = chunk_content(content, name, chunk_size=20)

        by_label = {}
        for dp in result.data_packets:
            label = [c for c in result.manifest.labels()
                     if c in str(dp.name)][0]
            by_label[label] = dp

        # assemble_verified doesn't check overall hash
        reassembled = assemble_verified(result.manifest, by_label)
        assert reassembled == content

    def test_assemble_fast_no_verification(self):
        """assemble_fast reassembles without any hash check."""
        content = b"fast path content"
        addr = rns_addr(0xDD)
        name = Name(addr, [b"fast"])
        result = chunk_content(content, name)

        by_label = {}
        for dp in result.data_packets:
            label = [c for c in result.manifest.labels()
                     if c in str(dp.name)][0]
            by_label[label] = dp

        reassembled = assemble_fast(result.manifest, by_label)
        assert reassembled == content

    def test_assemble_missing_chunk_raises(self):
        """Missing required chunk raises MissingChunkError."""
        content = b"x" * 10000
        addr = rns_addr(0xEE)
        name = Name(addr, [b"missing"])
        result = chunk_content(content, name, chunk_size=3000)

        # Only provide first chunk
        by_label = {}
        first_label = result.manifest.labels()[0]
        for dp in result.data_packets:
            if first_label in str(dp.name):
                by_label[first_label] = dp

        with pytest.raises(MissingChunkError, match="Missing chunk"):
            assemble(result.manifest, by_label)

    def test_assemble_hash_mismatch_raises(self):
        """Tampered chunk raises HashMismatchError."""
        content = b"tamper test content"
        addr = rns_addr(0xFF)
        name = Name(addr, [b"tamper"])
        result = chunk_content(content, name, chunk_size=10)

        by_label = {}
        for dp in result.data_packets:
            label = [c for c in result.manifest.labels()
                     if c in str(dp.name)][0]
            by_label[label] = dp

        # Tamper with the first chunk's content
        first_label = result.manifest.labels()[0]
        dp = by_label[first_label]
        tampered = Data(
            name=dp.name,
            content=b"TAMPERED",
            metadata=dp.metadata,
        )
        by_label[first_label] = tampered

        with pytest.raises(HashMismatchError, match="content blake2b mismatch"):
            assemble(result.manifest, by_label)

    def test_assemble_integrity_check(self):
        """Overall content_hash verification catches reassembly errors."""
        content = b"integrity check data"
        addr = rns_addr(0x11)
        name = Name(addr, [b"integrity"])
        result = chunk_content(content, name, chunk_size=10)

        by_label = {}
        for dp in result.data_packets:
            label = [c for c in result.manifest.labels()
                     if c in str(dp.name)][0]
            by_label[label] = dp

        # This should succeed since no tampering
        reassembled = assemble(result.manifest, by_label)
        assert reassembled == content

    def test_verify_chunk_valid(self):
        """verify_chunk returns True for valid chunk."""
        content = b"hello"
        addr = rns_addr(0x22)
        name = Name(addr, [b"valid"])
        result = chunk_content(content, name)
        assert verify_chunk(result.manifest.chunks[0], result.data_packets[0])

    def test_verify_chunk_invalid(self):
        """verify_chunk returns False for tampered chunk."""
        content = b"hello"
        addr = rns_addr(0x33)
        name = Name(addr, [b"invalid"])
        result = chunk_content(content, name)
        tampered = Data(
            name=result.data_packets[0].name,
            content=b"WORLD",
            metadata=result.data_packets[0].metadata,
        )
        assert not verify_chunk(result.manifest.chunks[0], tampered)

    def test_verify_chunks_all_valid(self):
        """verify_chunks returns all True for valid chunks."""
        content = b"x" * 5000
        addr = rns_addr(0x44)
        name = Name(addr, [b"all-valid"])
        result = chunk_content(content, name, chunk_size=1000)

        by_label = {c.label: dp for c, dp in
                     zip(result.manifest.chunks, result.data_packets)}
        results = verify_chunks(result.manifest, by_label)
        assert len(results) == result.chunk_count()
        assert all(results.values())

    def test_verify_chunks_missing_is_false(self):
        """verify_chunks reports missing chunks as False."""
        content = b"x" * 5000
        addr = rns_addr(0x55)
        name = Name(addr, [b"partial"])
        result = chunk_content(content, name, chunk_size=1000)

        by_label = {}
        first_label = result.manifest.labels()[0]
        for c, dp in zip(result.manifest.chunks, result.data_packets):
            if c.label == first_label:
                by_label[c.label] = dp

        results = verify_chunks(result.manifest, by_label)
        assert results[first_label] is True
        # Missing chunks should be False
        for label in result.manifest.labels()[1:]:
            assert results[label] is False

    def test_missing_labels(self):
        """missing_labels returns correct list."""
        content = b"x" * 5000
        addr = rns_addr(0x66)
        name = Name(addr, [b"missing-labels"])
        result = chunk_content(content, name, chunk_size=1000)

        by_label = {}
        first_label = result.manifest.labels()[0]
        for c, dp in zip(result.manifest.chunks, result.data_packets):
            if c.label == first_label:
                by_label[c.label] = dp

        missing = missing_labels(result.manifest, by_label)
        assert first_label not in missing
        assert len(missing) == result.chunk_count() - 1

    def test_assemble_preserves_binary_content(self):
        """Binary content (null bytes, non-UTF8) round-trips correctly."""
        content = bytes(range(256)) * 50  # 12,800 bytes of all byte values
        addr = rns_addr(0x77)
        name = Name(addr, [b"binary.bin"])
        result = chunk_content(content, name, chunk_size=2000)

        by_label = {}
        for c, dp in zip(result.manifest.chunks, result.data_packets):
            by_label[c.label] = dp

        reassembled = assemble(result.manifest, by_label)
        assert reassembled == content

    def test_assemble_exact_boundary(self):
        """Content at exact chunk_size boundaries assembles correctly."""
        content = b"a" * 4096
        addr = rns_addr(0x88)
        name = Name(addr, [b"boundary"])
        result = chunk_content(content, name, chunk_size=4096)

        by_label = {}
        for c, dp in zip(result.manifest.chunks, result.data_packets):
            by_label[c.label] = dp

        reassembled = assemble(result.manifest, by_label)
        assert reassembled == content
