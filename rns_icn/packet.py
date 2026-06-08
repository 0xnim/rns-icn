"""Packet types — Interest and Data wire formats.

Interests and Data flow over RNS Links. A 1-byte type discriminator
precedes each packet so the receiver knows what's arriving.

Type bytes:
  0x01 = Interest
  0x02 = Data
"""

from __future__ import annotations

import hashlib
import os
import struct
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional

from .name import Name


# ── Varint (same encoding as rsticulum-icn for interop) ──


def _write_varint(value: int) -> bytes:
    if value < 0xFD:
        return struct.pack("B", value)
    elif value <= 0xFFFF:
        return b"\xFD" + struct.pack(">H", value)
    elif value <= 0xFFFFFFFF:
        return b"\xFE" + struct.pack(">I", value)
    else:
        return b"\xFF" + struct.pack(">Q", value)


def _read_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    if offset >= len(data):
        raise InterestError("buffer too short for varint")
    b = data[offset]
    if b < 0xFD:
        return b, 1
    elif b == 0xFD:
        return struct.unpack(">H", data[offset + 1: offset + 3])[0], 3
    elif b == 0xFE:
        return struct.unpack(">I", data[offset + 1: offset + 5])[0], 5
    else:
        return struct.unpack(">Q", data[offset + 1: offset + 9])[0], 9


# ── Packet type ──


class PacketType(IntEnum):
    INTEREST = 0x01
    DATA = 0x02
    APS_SUBSCRIBE = 0x03
    PROP_PEER = 0x04
    CAP_PEER = 0x05  # Peer capability exchange on link


# ── InterestSelector ──


@dataclass
class InterestSelector:
    """Selector for Interests — constraints on which Data satisfies the Interest.

    Wire (when present, after flags byte):
      [min_sequence:8]

    min_sequence: minimum sequence number accepted. Used for stream fetch:
      'I already have segments 1-5, give me segment 6 or later.'
    """
    min_sequence: Optional[int] = None

    def to_bytes(self) -> bytes:
        return struct.pack(">Q", self.min_sequence or 0)

    @classmethod
    def from_bytes(cls, data: bytes) -> InterestSelector:
        if len(data) < 8:
            raise InterestError("buffer too short for selector")
        seq = struct.unpack(">Q", data[:8])[0]
        return cls(min_sequence=seq)


# ── Interest ──


@dataclass
class Interest:
    """An Interest packet — 'I want this named data.'

    Wire: [type:1=0x01][name_len:varint][name...][nonce:8][lifetime_ms:4][flags:1]
          [selector:8 if flags bit 2]
      flags bit 0: can_be_prefix
      flags bit 1: must_be_fresh
      flags bit 2: has_selector  → 8-byte min_sequence follows
    """
    name: Name
    nonce: bytes = field(default_factory=lambda: os.urandom(8))
    lifetime_ms: int = 4000
    can_be_prefix: bool = False
    must_be_fresh: bool = False
    selector: Optional[InterestSelector] = None

    def __post_init__(self):
        if len(self.nonce) != 8:
            raise InterestError("nonce must be 8 bytes")

    def with_lifetime(self, ms: int) -> Interest:
        self.lifetime_ms = ms
        return self

    def with_can_be_prefix(self) -> Interest:
        self.can_be_prefix = True
        return self

    def with_must_be_fresh(self) -> Interest:
        self.must_be_fresh = True
        return self

    def with_selector(self, selector: InterestSelector) -> Interest:
        self.selector = selector
        return self

    def clone(self) -> Interest:
        return Interest(
            name=self.name,
            nonce=self.nonce,
            lifetime_ms=self.lifetime_ms,
            can_be_prefix=self.can_be_prefix,
            must_be_fresh=self.must_be_fresh,
            selector=InterestSelector(min_sequence=self.selector.min_sequence) if self.selector else None,
        )

    def to_bytes(self) -> bytes:
        name_bytes = self.name.to_bytes()
        buf = bytearray()
        buf.append(PacketType.INTEREST)
        buf.extend(_write_varint(len(name_bytes)))
        buf.extend(name_bytes)
        buf.extend(self.nonce)
        buf.extend(struct.pack(">I", self.lifetime_ms))
        flags = 0
        if self.can_be_prefix:
            flags |= 0x01
        if self.must_be_fresh:
            flags |= 0x02
        if self.selector is not None:
            flags |= 0x04
        buf.append(flags)
        if self.selector is not None:
            buf.extend(self.selector.to_bytes())
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> Interest:
        pos = 0
        if data[pos] != PacketType.INTEREST:
            raise InterestError(f"expected INTEREST type byte, got {data[pos]:#x}")
        pos += 1
        name_len, consumed = _read_varint(data, pos)
        pos += consumed
        if pos + name_len > len(data):
            raise InterestError("buffer too short for name")
        name = Name.from_bytes(data[pos:pos + name_len])
        pos += name_len
        if pos + 8 > len(data):
            raise InterestError("buffer too short for nonce")
        nonce = data[pos:pos + 8]
        pos += 8
        if pos + 4 > len(data):
            raise InterestError("buffer too short for lifetime")
        lifetime_ms = struct.unpack(">I", data[pos:pos + 4])[0]
        pos += 4
        if pos >= len(data):
            raise InterestError("buffer too short for flags")
        flags = data[pos]
        pos += 1
        selector = None
        if flags & 0x04:
            if pos + 8 > len(data):
                raise InterestError("buffer too short for selector")
            selector = InterestSelector.from_bytes(data[pos:pos + 8])
        return cls(
            name=name,
            nonce=nonce,
            lifetime_ms=lifetime_ms,
            can_be_prefix=bool(flags & 0x01),
            must_be_fresh=bool(flags & 0x02),
            selector=selector,
        )


# ── Errors ──


class InterestError(Exception):
    ...


class DataError(Exception):
    ...


# ── Freshness ──


@dataclass
class Freshness:
    fresh: bool = True
    age_seconds: int = 0


# ── DataMetadata ──


@dataclass
class DataMetadata:
    content_hash: Optional[bytes] = None
    sequence: Optional[int] = None
    freshness: Freshness = field(default_factory=Freshness)

    def to_bytes(self) -> bytes:
        flags = 0
        if self.content_hash is not None:
            flags |= 0x01
        if self.sequence is not None:
            flags |= 0x02
        if not self.freshness.fresh:
            flags |= 0x04
        buf = bytearray([flags])
        if self.content_hash is not None:
            buf.extend(self.content_hash)
        if self.sequence is not None:
            buf.extend(struct.pack(">Q", self.sequence))
        if not self.freshness.fresh:
            buf.extend(struct.pack(">Q", self.freshness.age_seconds))
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> DataMetadata:
        if not data:
            raise DataError("buffer too short for metadata")
        flags = data[0]
        pos = 1
        content_hash = None
        if flags & 0x01:
            if pos + 32 > len(data):
                raise DataError("buffer too short for content_hash")
            content_hash = data[pos:pos + 32]
            pos += 32
        sequence = None
        if flags & 0x02:
            if pos + 8 > len(data):
                raise DataError("buffer too short for sequence")
            sequence = struct.unpack(">Q", data[pos:pos + 8])[0]
            pos += 8
        freshness = Freshness(fresh=True)
        if flags & 0x04:
            if pos + 8 > len(data):
                raise DataError("buffer too short for staleness")
            age = struct.unpack(">Q", data[pos:pos + 8])[0]
            freshness = Freshness(fresh=False, age_seconds=age)
        return cls(content_hash=content_hash, sequence=sequence, freshness=freshness)


# ── Data ──


@dataclass
class Data:
    """A Data packet — 'Here is the content you asked for.'

    Wire: [type:1=0x02][name_len:varint][name...][content_len:4][content...]
          [flags:1][metadata_len:varint if h'01][metadata...][sig:96 if h'02]
      flags bit 0: has_metadata
      flags bit 1: has_signature
    """
    name: Name
    content: bytes
    signature: Optional[bytes] = None
    metadata: DataMetadata = field(default_factory=DataMetadata)

    @classmethod
    def new(cls, name: Name, content: bytes) -> Data:
        content_hash = hashlib.blake2b(content, digest_size=32).digest()
        return cls(
            name=name,
            content=content,
            metadata=DataMetadata(content_hash=content_hash),
        )

    def with_sequence(self, seq: int) -> Data:
        self.metadata.sequence = seq
        return self

    def with_staleness(self, age_seconds: int) -> Data:
        self.metadata.freshness = Freshness(fresh=False, age_seconds=age_seconds)
        return self

    def signed_hash(self) -> bytes:
        h = hashlib.blake2b(digest_size=32)
        h.update(self.name.to_bytes())
        h.update(self.content)
        if self.metadata.content_hash is not None:
            h.update(self.metadata.content_hash)
        return h.digest()

    def to_bytes(self) -> bytes:
        name_bytes = self.name.to_bytes()
        has_meta = self.metadata.content_hash is not None or self.metadata.sequence is not None or not self.metadata.freshness.fresh
        has_sig = self.signature is not None

        metadata_bytes = self.metadata.to_bytes() if has_meta else b""

        buf = bytearray()
        buf.append(PacketType.DATA)
        buf.extend(_write_varint(len(name_bytes)))
        buf.extend(name_bytes)
        buf.extend(struct.pack(">I", len(self.content)))
        buf.extend(self.content)

        flags = 0
        if has_meta:
            flags |= 0x01
        if has_sig:
            flags |= 0x02
        buf.append(flags)

        if has_meta:
            buf.extend(_write_varint(len(metadata_bytes)))
            buf.extend(metadata_bytes)
        if has_sig:
            buf.extend(self.signature)

        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> Data:
        pos = 0
        if data[pos] != PacketType.DATA:
            raise DataError(f"expected DATA type byte, got {data[pos]:#x}")
        pos += 1
        name_len, consumed = _read_varint(data, pos)
        pos += consumed
        if pos + name_len > len(data):
            raise DataError("buffer too short for name")
        name = Name.from_bytes(data[pos:pos + name_len])
        pos += name_len
        if pos + 4 > len(data):
            raise DataError("buffer too short for content length")
        content_len = struct.unpack(">I", data[pos:pos + 4])[0]
        pos += 4
        if pos + content_len > len(data):
            raise DataError("buffer too short for content")
        content = data[pos:pos + content_len]
        pos += content_len
        if pos >= len(data):
            raise DataError("buffer too short for flags")
        flags = data[pos]
        pos += 1

        metadata = DataMetadata()
        if flags & 0x01:
            meta_len, consumed = _read_varint(data, pos)
            pos += consumed
            if pos + meta_len > len(data):
                raise DataError("buffer too short for metadata")
            metadata = DataMetadata.from_bytes(data[pos:pos + meta_len])
            pos += meta_len

        signature = None
        if flags & 0x02:
            if pos + 96 > len(data):
                raise DataError("buffer too short for signature")
            signature = data[pos:pos + 96]

        return cls(name=name, content=content, signature=signature, metadata=metadata)


# ── APS Subscribe ──


class SubscribeError(Exception):
    ...


@dataclass
class APSubscribe:
    """APS Subscribe — 'Push this stream to me.'

    Consumer sends this to upgrade a link to push mode. The producer
    registers the subscription and pushes Data as it's produced,
    without requiring per-packet Interests.

    Wire: [type:1=0x03][name_len:varint][name...][flags:1]
      flags bit 0: start_from_now (don't push existing content)
    """
    name: Name
    start_from_now: bool = False

    def to_bytes(self) -> bytes:
        name_bytes = self.name.to_bytes()
        buf = bytearray()
        buf.append(PacketType.APS_SUBSCRIBE)
        buf.extend(_write_varint(len(name_bytes)))
        buf.extend(name_bytes)
        flags = 0
        if self.start_from_now:
            flags |= 0x01
        buf.append(flags)
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> APSubscribe:
        pos = 0
        if data[pos] != PacketType.APS_SUBSCRIBE:
            raise SubscribeError(f"expected APS_SUBSCRIBE type byte, got {data[pos]:#x}")
        pos += 1
        try:
            name_len, consumed = _read_varint(data, pos)
        except InterestError as e:
            raise SubscribeError(str(e)) from e
        pos += consumed
        if pos + name_len > len(data):
            raise SubscribeError("buffer too short for name")
        try:
            name = Name.from_bytes(data[pos:pos + name_len])
        except Exception as e:
            raise SubscribeError(f"invalid name: {e}") from e
        pos += name_len
        if pos >= len(data):
            raise SubscribeError("buffer too short for flags")
        flags = data[pos]
        return cls(
            name=name,
            start_from_now=bool(flags & 0x01),
        )


# ── Packet envelope ──


# ── PropPeer (Propagation Peering) ──


@dataclass
class PropPeer:
    """Propagation peer handshake — 'Let's be propagation peers.'

    Sent when two ICN servers establish a link and want to propagate
    content between them. After this handshake, each server subscribes
    to the other's streams and forwards pushed content.

    Wire: [type:1=0x04][version:1][rns_addr:16][flags:1]
      flags bit 0: wants_sync (peer wants to sync existing content now)
    """
    version: int = 1
    rns_addr: bytes = b""
    wants_sync: bool = False

    def to_bytes(self) -> bytes:
        buf = bytearray()
        buf.append(PacketType.PROP_PEER)
        buf.append(self.version)
        addr = self.rns_addr
        if len(addr) < 16:
            addr = addr + b"\x00" * (16 - len(addr))
        buf.extend(addr[:16])
        flags = 0
        if self.wants_sync:
            flags |= 0x01
        buf.append(flags)
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> PropPeer:
        if not data or data[0] != PacketType.PROP_PEER:
            label = hex(data[0]) if data else "empty"
            raise ValueError(f"expected PROP_PEER type byte, got {label}")
        if len(data) < 19:
            raise ValueError("buffer too short for PropPeer")
        version = data[1]
        rns_addr = data[2:18]
        flags = data[18]
        return cls(
            version=version,
            rns_addr=rns_addr,
            wants_sync=bool(flags & 0x01),
        )


@dataclass
class CapPeer:
    """Capability exchange — 'Here's what I can do.'

    Exchanged immediately after link establishment so both sides know
    each other's role, version, and supported features.

    Wire: [type:1=0x05][version:1][role:1][features:4]
      version: protocol version (currently 1)
      role: ServerRole enum value (0=ORIGIN, 1=CACHE, 2=PROPAGATION)
      features: 32-bit bitmask of supported features
    """
    version: int = 1
    role: int = 0
    features: int = 0

    def to_bytes(self) -> bytes:
        buf = bytearray()
        buf.append(PacketType.CAP_PEER)
        buf.append(self.version)
        buf.append(self.role)
        buf.extend(struct.pack(">I", self.features))
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> CapPeer:
        if not data or data[0] != PacketType.CAP_PEER:
            label = hex(data[0]) if data else "empty"
            raise ValueError(f"expected CAP_PEER type byte, got {label}")
        if len(data) < 7:
            raise ValueError("buffer too short for CapPeer")
        return cls(
            version=data[1],
            role=data[2],
            features=struct.unpack(">I", data[3:7])[0],
        )


# Feature bitmask constants
FEATURE_APS = 0x00000001
FEATURE_PROPAGATION = 0x00000002
FEATURE_OFFLINE_QUEUE = 0x00000004
FEATURE_MANIFEST = 0x00000008
FEATURE_CHUNKED = 0x00000010


@dataclass
class Packet:
    """A parsed ICN packet with known type."""
    type: PacketType
    interest: Optional[Interest] = None
    data: Optional[Data] = None
    subscribe: Optional[APSubscribe] = None
    peer: Optional[PropPeer] = None
    cap_peer: Optional[CapPeer] = None


def parse_packet(data: bytes) -> Packet:
    """Parse a raw byte stream into an Interest, Data, or APS Subscribe packet."""
    if not data:
        raise ValueError("empty packet data")
    ptype = data[0]
    if ptype == PacketType.INTEREST:
        interest = Interest.from_bytes(data)
        return Packet(type=PacketType.INTEREST, interest=interest)
    elif ptype == PacketType.DATA:
        d = Data.from_bytes(data)
        return Packet(type=PacketType.DATA, data=d)
    elif ptype == PacketType.APS_SUBSCRIBE:
        sub = APSubscribe.from_bytes(data)
        return Packet(type=PacketType.APS_SUBSCRIBE, subscribe=sub)
    elif ptype == PacketType.PROP_PEER:
        peer = PropPeer.from_bytes(data)
        return Packet(type=PacketType.PROP_PEER, peer=peer)
    elif ptype == PacketType.CAP_PEER:
        cap = CapPeer.from_bytes(data)
        return Packet(type=PacketType.CAP_PEER, cap_peer=cap)
    else:
        raise ValueError(f"unknown packet type: {ptype:#x}")
