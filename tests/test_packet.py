"""Tests for Interest, Data, and packet wire format."""

import pytest

from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import (
    Data,
    DataMetadata,
    Freshness,
    Interest,
    InterestError,
    InterestSelector,
    PacketType,
    parse_packet,
)


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


class TestInterest:
    def test_round_trip(self):
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name, lifetime_ms=4000, can_be_prefix=True)
        data = interest.to_bytes()
        parsed = Interest.from_bytes(data)
        assert parsed.name == interest.name
        assert parsed.lifetime_ms == 4000
        assert parsed.can_be_prefix is True
        assert parsed.nonce == interest.nonce

    def test_must_be_fresh(self):
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name, must_be_fresh=True)
        data = interest.to_bytes()
        parsed = Interest.from_bytes(data)
        assert parsed.must_be_fresh is True

    def test_default_flags(self):
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name)
        data = interest.to_bytes()
        parsed = Interest.from_bytes(data)
        assert parsed.can_be_prefix is False
        assert parsed.must_be_fresh is False

    def test_random_nonce(self):
        name = Name(rns_addr(0x01), [b"test"])
        a = Interest(name=name)
        b = Interest(name=name)
        assert a.nonce != b.nonce

    def test_invalid_nonce(self):
        name = Name(rns_addr(0x01), [b"test"])
        with pytest.raises(InterestError):
            Interest(name=name, nonce=b"short")

    def test_clone(self):
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name, can_be_prefix=True, must_be_fresh=True, lifetime_ms=5000)
        cloned = interest.clone()
        assert cloned.name == interest.name
        assert cloned.nonce == interest.nonce
        assert cloned.can_be_prefix is True
        assert cloned.must_be_fresh is True
        assert cloned.lifetime_ms == 5000

    def test_hop_limit_default(self):
        from rns_icn.packet import DEFAULT_HOP_LIMIT
        interest = Interest(name=Name(rns_addr(0x01), [b"t"]))
        assert interest.hop_limit == DEFAULT_HOP_LIMIT

    def test_hop_limit_round_trip(self):
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name, hop_limit=3)
        parsed = Interest.from_bytes(interest.to_bytes())
        assert parsed.hop_limit == 3

    def test_hop_limit_round_trip_with_selector(self):
        # hop_limit is serialized after the selector — both must survive together.
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(
            name=name, hop_limit=7, selector=InterestSelector(min_sequence=9)
        )
        parsed = Interest.from_bytes(interest.to_bytes())
        assert parsed.hop_limit == 7
        assert parsed.selector is not None
        assert parsed.selector.min_sequence == 9

    def test_hop_limit_zero_round_trip(self):
        name = Name(rns_addr(0x01), [b"test"])
        parsed = Interest.from_bytes(Interest(name=name, hop_limit=0).to_bytes())
        assert parsed.hop_limit == 0

    def test_hop_limit_clone(self):
        interest = Interest(name=Name(rns_addr(0x01), [b"t"]), hop_limit=5)
        assert interest.clone().hop_limit == 5

    def test_hop_limit_with_method(self):
        interest = Interest(name=Name(rns_addr(0x01), [b"t"])).with_hop_limit(4)
        assert interest.hop_limit == 4

    def test_hop_limit_out_of_range(self):
        with pytest.raises(InterestError):
            Interest(name=Name(rns_addr(0x01), [b"t"]), hop_limit=256)

    def test_hop_limit_absent_defaults(self):
        # Wire bytes from an older peer that predates the hop_limit flag:
        # build a normal Interest, then clear flag bit 3 and drop the trailing
        # hop_limit byte. The parser must fall back to DEFAULT_HOP_LIMIT.
        from rns_icn.packet import DEFAULT_HOP_LIMIT
        name = Name(rns_addr(0x01), [b"test"])
        raw = bytearray(Interest(name=name).to_bytes())
        assert raw[-2] & 0x08  # flags byte has hop_limit bit set
        raw[-2] &= ~0x08       # clear has_hop_limit
        legacy = bytes(raw[:-1])  # strip trailing hop_limit byte
        parsed = Interest.from_bytes(legacy)
        assert parsed.hop_limit == DEFAULT_HOP_LIMIT

    def test_packet_type_byte(self):
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name)
        data = interest.to_bytes()
        assert data[0] == PacketType.INTEREST

    def test_parse_bad_type(self):
        with pytest.raises(InterestError):
            Interest.from_bytes(b"\x02\x00")


class TestInterestSelector:
    def test_default_min_sequence(self):
        sel = InterestSelector()
        assert sel.min_sequence is None

    def test_round_trip(self):
        sel = InterestSelector(min_sequence=42)
        data = sel.to_bytes()
        parsed = InterestSelector.from_bytes(data)
        assert parsed.min_sequence == 42

    def test_round_trip_zero(self):
        sel = InterestSelector(min_sequence=0)
        data = sel.to_bytes()
        parsed = InterestSelector.from_bytes(data)
        assert parsed.min_sequence == 0

    def test_large_sequence(self):
        sel = InterestSelector(min_sequence=2**48 + 1)
        data = sel.to_bytes()
        parsed = InterestSelector.from_bytes(data)
        assert parsed.min_sequence == 2**48 + 1

    def test_short_buffer(self):
        with pytest.raises(InterestError):
            InterestSelector.from_bytes(b"\x00\x00")


class TestInterestWithSelector:
    def test_round_trip_with_selector(self):
        name = Name(rns_addr(0x01), [b"stream", b"chat"])
        sel = InterestSelector(min_sequence=7)
        interest = Interest(name=name, can_be_prefix=True, selector=sel)
        data = interest.to_bytes()
        parsed = Interest.from_bytes(data)
        assert parsed.selector is not None
        assert parsed.selector.min_sequence == 7
        assert parsed.can_be_prefix is True
        assert parsed.name == name

    def test_no_selector_default(self):
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name)
        data = interest.to_bytes()
        parsed = Interest.from_bytes(data)
        assert parsed.selector is None

    def test_clone_with_selector(self):
        name = Name(rns_addr(0x01), [b"stream"])
        interest = Interest(
            name=name, can_be_prefix=True, must_be_fresh=True,
            selector=InterestSelector(min_sequence=5),
        )
        cloned = interest.clone()
        assert cloned.selector is not None
        assert cloned.selector.min_sequence == 5
        assert cloned.can_be_prefix is True

    def test_clone_no_selector(self):
        interest = Interest(name=Name(rns_addr(0x01), [b"t"]))
        cloned = interest.clone()
        assert cloned.selector is None

    def test_with_selector_method(self):
        name = Name(rns_addr(0x01), [b"stream"])
        interest = Interest(name=name).with_selector(InterestSelector(min_sequence=10))
        assert interest.selector is not None
        assert interest.selector.min_sequence == 10
        parsed = Interest.from_bytes(interest.to_bytes())
        assert parsed.selector.min_sequence == 10


class TestData:
    def test_round_trip_minimal(self):
        name = Name(rns_addr(0x01), [b"test"])
        data = Data.new(name=name, content=b"hello world")
        serialized = data.to_bytes()
        parsed = Data.from_bytes(serialized)
        assert parsed.name == data.name
        assert parsed.content == b"hello world"
        assert parsed.metadata.content_hash is not None

    def test_with_sequence(self):
        name = Name(rns_addr(0x01), [b"test"])
        data = Data.new(name=name, content=b"hello").with_sequence(42)
        assert data.metadata.sequence == 42
        parsed = Data.from_bytes(data.to_bytes())
        assert parsed.metadata.sequence == 42

    def test_with_staleness(self):
        name = Name(rns_addr(0x01), [b"test"])
        data = Data.new(name=name, content=b"hello").with_staleness(3600)
        assert not data.metadata.freshness.fresh
        assert data.metadata.freshness.age_seconds == 3600
        parsed = Data.from_bytes(data.to_bytes())
        assert not parsed.metadata.freshness.fresh
        assert parsed.metadata.freshness.age_seconds == 3600

    def test_with_signature(self):
        name = Name(rns_addr(0x01), [b"test"])
        sig = b"\xAA" * 64  # RNS Ed25519 signatures are 64 bytes
        data = Data.new(name=name, content=b"hello")
        data.signature = sig
        serialized = data.to_bytes()
        parsed = Data.from_bytes(serialized)
        assert parsed.signature == sig

    def test_content_hash_auto(self):
        data = Data.new(
            name=Name(rns_addr(0x01), [b"test"]),
            content=b"hello",
        )
        assert data.metadata.content_hash is not None
        assert len(data.metadata.content_hash) == 32

    def test_signed_hash_stable(self):
        name = Name(rns_addr(0x01), [b"test"])
        data = Data.new(name=name, content=b"hello")
        assert data.signed_hash() == data.signed_hash()

    def test_packet_type_byte(self):
        data = Data.new(name=Name(rns_addr(0x01), [b"t"]), content=b"x")
        assert data.to_bytes()[0] == PacketType.DATA


class TestDataMetadata:
    def test_empty(self):
        meta = DataMetadata()
        data = meta.to_bytes()
        parsed = DataMetadata.from_bytes(data)
        assert parsed.content_hash is None
        assert parsed.sequence is None
        assert parsed.freshness.fresh

    def test_all_fields(self):
        meta = DataMetadata(
            content_hash=b"\x01" + b"\x00" * 31,
            sequence=99,
            freshness=Freshness(fresh=True),
        )
        data = meta.to_bytes()
        parsed = DataMetadata.from_bytes(data)
        assert parsed.content_hash == meta.content_hash
        assert parsed.sequence == 99

    def test_stale(self):
        meta = DataMetadata(freshness=Freshness(fresh=False, age_seconds=5000))
        data = meta.to_bytes()
        parsed = DataMetadata.from_bytes(data)
        assert not parsed.freshness.fresh
        assert parsed.freshness.age_seconds == 5000


class TestPacket:
    def test_parse_interest(self):
        name = Name(rns_addr(0x01), [b"test"])
        interest = Interest(name=name)
        raw = interest.to_bytes()
        pkt = parse_packet(raw)
        assert pkt.type == PacketType.INTEREST
        assert pkt.interest is not None
        assert pkt.data is None

    def test_parse_data(self):
        data = Data.new(name=Name(rns_addr(0x01), [b"t"]), content=b"hello")
        raw = data.to_bytes()
        pkt = parse_packet(raw)
        assert pkt.type == PacketType.DATA
        assert pkt.data is not None
        assert pkt.interest is None

    def test_parse_empty_error(self):
        with pytest.raises(ValueError):
            parse_packet(b"")

    def test_parse_bad_type(self):
        with pytest.raises(ValueError):
            parse_packet(b"\xFF\x00")


class TestEncryptedFlag:
    """Phase 3.3: the authenticated `encrypted` metadata flag."""

    def test_metadata_encrypted_round_trip(self):
        meta = DataMetadata(content_hash=b"\x00" * 32, encrypted=True)
        restored = DataMetadata.from_bytes(meta.to_bytes())
        assert restored.encrypted is True

    def test_metadata_defaults_unencrypted(self):
        meta = DataMetadata(content_hash=b"\x00" * 32)
        assert DataMetadata.from_bytes(meta.to_bytes()).encrypted is False

    def test_data_encrypted_flag_survives_serialization(self):
        data = Data.new(name=Name(rns_addr(0x01), [b"sec"]), content=b"ciphertext")
        data.metadata.encrypted = True
        restored = Data.from_bytes(data.to_bytes())
        assert restored.metadata.encrypted is True

    def test_signed_hash_binds_encrypted_flag(self):
        # signed_hash differs by the encrypted flag, so a relay flipping it
        # invalidates the producer signature.
        name = Name(rns_addr(0x01), [b"sec"])
        plain = Data.new(name=name, content=b"x")
        enc = Data.new(name=name, content=b"x")
        enc.metadata.encrypted = True
        assert plain.signed_hash() != enc.signed_hash()

    def test_pre_33_signature_unaffected(self):
        # Unencrypted Data hashes exactly as before (flag only appended when set).
        name = Name(rns_addr(0x01), [b"doc"])
        data = Data.new(name=name, content=b"hello")
        h = data.signed_hash()
        # Re-deriving with encrypted explicitly False must be identical.
        data.metadata.encrypted = False
        assert data.signed_hash() == h


class TestProtocolVersion:
    """Every packet is framed [type:1][version:1]; unknown versions are rejected."""

    def test_version_byte_present_on_all_packet_types(self):
        from rns_icn.packet import (
            PROTOCOL_VERSION,
            APSubscribe,
            CapPeer,
            Invalidate,
            PropPeer,
        )
        name = Name(rns_addr(0x01), [b"item"])
        packets = [
            Interest(name=name),
            Data.new(name=name, content=b"x"),
            APSubscribe(name=name),
            Invalidate(name=name, epoch=1),
            PropPeer(rns_addr=rns_addr(0x02)),
            CapPeer(role=1, features=0x3),
        ]
        for p in packets:
            raw = p.to_bytes()
            assert raw[1] == PROTOCOL_VERSION, type(p).__name__

    def test_unknown_version_rejected(self):
        from rns_icn.packet import (
            APSubscribe,
            CapPeer,
            Invalidate,
            PropPeer,
            UnsupportedVersionError,
        )
        name = Name(rns_addr(0x01), [b"item"])
        builders = [
            Interest(name=name),
            Data.new(name=name, content=b"x"),
            APSubscribe(name=name),
            Invalidate(name=name, epoch=1),
            PropPeer(rns_addr=rns_addr(0x02)),
            CapPeer(role=1),
        ]
        for p in builders:
            raw = bytearray(p.to_bytes())
            raw[1] = 0xFF  # a generation this build does not speak
            with pytest.raises(UnsupportedVersionError):
                parse_packet(bytes(raw))

    def test_version_skew_distinguishable_from_corruption(self):
        # UnsupportedVersionError is its own type, not the generic per-packet
        # parse error, so a caller can tell skew apart from a malformed packet.
        from rns_icn.packet import UnsupportedVersionError
        name = Name(rns_addr(0x01), [b"item"])
        raw = bytearray(Data.new(name=name, content=b"x").to_bytes())
        raw[1] = 0x02
        with pytest.raises(UnsupportedVersionError):
            Data.from_bytes(bytes(raw))
