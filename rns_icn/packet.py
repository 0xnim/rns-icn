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
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Callable, Optional

from .name import Name

# RNS Identity.sign() produces a 64-byte Ed25519 signature.
SIGNATURE_BYTES = 64

# Default Interest hop limit. Each forwarding hop decrements it; an Interest
# is dropped (not forwarded further) once it reaches 0. Defence-in-depth
# against forwarding loops beyond the per-face nonce check.
DEFAULT_HOP_LIMIT = 16

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
    INVALIDATE = 0x06  # Producer-signed cache purge for a name/prefix


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
          [selector:8 if flags bit 2][hop_limit:1 if flags bit 3]
      flags bit 0: can_be_prefix
      flags bit 1: must_be_fresh
      flags bit 2: has_selector   → 8-byte min_sequence follows
      flags bit 3: has_hop_limit  → 1-byte hop_limit follows

    hop_limit: remaining forwarding hops. Decremented at each hop; the
      Interest is dropped once it reaches 0 (it may still be satisfied
      from a local cache). Absent on the wire (older peers) → DEFAULT_HOP_LIMIT.
    """
    name: Name
    nonce: bytes = field(default_factory=lambda: os.urandom(8))
    lifetime_ms: int = 4000
    can_be_prefix: bool = False
    must_be_fresh: bool = False
    selector: Optional[InterestSelector] = None
    hop_limit: int = DEFAULT_HOP_LIMIT

    def __post_init__(self):
        if len(self.nonce) != 8:
            raise InterestError("nonce must be 8 bytes")
        if not 0 <= self.hop_limit <= 0xFF:
            raise InterestError("hop_limit must be in 0..255")

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

    def with_hop_limit(self, hops: int) -> Interest:
        self.hop_limit = hops
        return self

    def clone(self) -> Interest:
        return Interest(
            name=self.name,
            nonce=self.nonce,
            lifetime_ms=self.lifetime_ms,
            can_be_prefix=self.can_be_prefix,
            must_be_fresh=self.must_be_fresh,
            selector=InterestSelector(min_sequence=self.selector.min_sequence) if self.selector else None,
            hop_limit=self.hop_limit,
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
        flags |= 0x08  # always carry hop_limit
        buf.append(flags)
        if self.selector is not None:
            buf.extend(self.selector.to_bytes())
        buf.append(self.hop_limit & 0xFF)
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
            pos += 8
        hop_limit = DEFAULT_HOP_LIMIT
        if flags & 0x08:
            if pos + 1 > len(data):
                raise InterestError("buffer too short for hop_limit")
            hop_limit = data[pos]
            pos += 1
        return cls(
            name=name,
            nonce=nonce,
            lifetime_ms=lifetime_ms,
            can_be_prefix=bool(flags & 0x01),
            must_be_fresh=bool(flags & 0x02),
            selector=selector,
            hop_limit=hop_limit,
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
    # Declared freshness lifetime in seconds. A cache treats the Data as fresh
    # until it has been held longer than this, then stale. None = no declared
    # lifetime (caches consider it fresh until TTL eviction — legacy behaviour).
    freshness_period: Optional[int] = None
    # Unix timestamp (seconds) the producer signed this Data at. Folded into the
    # signed envelope (see Data.signed_hash) so it is authenticated, letting a
    # consumer detect a cache/relay replaying a stale-but-validly-signed version
    # (rollback). None on unsigned Data and on pre-3.1 signed Data.
    signed_at: Optional[int] = None
    # True when ``content`` is ciphertext for a restricted prefix (Phase 3.3).
    # Bound into the signed envelope so a relay cannot flip it; a consumer with a
    # capability for the name decrypts it (see rns_icn.access). Caches store and
    # relay the opaque ciphertext untouched.
    encrypted: bool = False

    def to_bytes(self) -> bytes:
        flags = 0
        if self.content_hash is not None:
            flags |= 0x01
        if self.sequence is not None:
            flags |= 0x02
        if not self.freshness.fresh:
            flags |= 0x04
        if self.freshness_period is not None:
            flags |= 0x08
        if self.signed_at is not None:
            flags |= 0x10
        if self.encrypted:
            flags |= 0x20
        buf = bytearray([flags])
        if self.content_hash is not None:
            buf.extend(self.content_hash)
        if self.sequence is not None:
            buf.extend(struct.pack(">Q", self.sequence))
        if not self.freshness.fresh:
            buf.extend(struct.pack(">Q", self.freshness.age_seconds))
        if self.freshness_period is not None:
            buf.extend(struct.pack(">Q", self.freshness_period))
        if self.signed_at is not None:
            buf.extend(struct.pack(">Q", self.signed_at))
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
        freshness_period = None
        if flags & 0x08:
            if pos + 8 > len(data):
                raise DataError("buffer too short for freshness_period")
            freshness_period = struct.unpack(">Q", data[pos:pos + 8])[0]
            pos += 8
        signed_at = None
        if flags & 0x10:
            if pos + 8 > len(data):
                raise DataError("buffer too short for signed_at")
            signed_at = struct.unpack(">Q", data[pos:pos + 8])[0]
            pos += 8
        encrypted = bool(flags & 0x20)
        return cls(content_hash=content_hash, sequence=sequence,
                   freshness=freshness, freshness_period=freshness_period,
                   signed_at=signed_at, encrypted=encrypted)


# ── Data ──


@dataclass
class Data:
    """A Data packet — 'Here is the content you asked for.'

    Wire: [type:1=0x02][name_len:varint][name...][content_len:4][content...]
          [flags:1][metadata_len:varint if h'01][metadata...][sig:64 if h'02]
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

    def with_freshness_period(self, seconds: int) -> Data:
        """Declare how long this Data stays fresh in a cache (seconds)."""
        self.metadata.freshness_period = seconds
        return self

    def verify_content_hash(self) -> bool:
        """Verify content matches metadata.content_hash."""
        if self.metadata.content_hash is None:
            return True  # No hash to verify
        computed = hashlib.blake2b(self.content, digest_size=32).digest()
        return computed == self.metadata.content_hash

    def signed_hash(self) -> bytes:
        h = hashlib.blake2b(digest_size=32)
        h.update(self.name.to_bytes())
        h.update(self.content)
        if self.metadata.content_hash is not None:
            h.update(self.metadata.content_hash)
        # Sequence and signed-at are domain-tagged so they bind into the
        # envelope unambiguously (and distinguish "value 0" from "absent").
        # A relay that tampers with either field, or strips it, breaks the
        # signature; this is what lets a consumer trust them for rollback
        # detection. Appended after the legacy fields so pre-3.1 signatures
        # over name+content+hash still verify.
        if self.metadata.sequence is not None:
            h.update(b"\x01")
            h.update(struct.pack(">Q", self.metadata.sequence))
        if self.metadata.signed_at is not None:
            h.update(b"\x02")
            h.update(struct.pack(">Q", self.metadata.signed_at))
        # Bind the encrypted flag so a relay can't strip it (making a consumer
        # treat ciphertext as plaintext) or set it. Domain-tagged and only added
        # when true, so unencrypted pre-3.3 signatures still verify.
        if self.metadata.encrypted:
            h.update(b"\x03")
            h.update(b"\x01")
        return h.digest()

    def sign(
        self,
        signer: Callable[[bytes], bytes],
        signed_at: Optional[int] = None,
    ) -> Data:
        """Attach a producer signature over signed_hash().

        ``signer`` is typically ``RNS.Identity.sign``; it must return a
        SIGNATURE_BYTES-long Ed25519 signature.

        Stamps ``metadata.signed_at`` (unix seconds; the current time unless
        an explicit ``signed_at`` is given) before signing, so the timestamp is
        part of the signed envelope. An already-set ``signed_at`` is preserved.
        """
        if self.metadata.signed_at is None:
            self.metadata.signed_at = (
                signed_at if signed_at is not None else int(time.time())
            )
        sig = signer(self.signed_hash())
        if len(sig) != SIGNATURE_BYTES:
            raise DataError(
                f"signature must be {SIGNATURE_BYTES} bytes, got {len(sig)}"
            )
        self.signature = sig
        return self

    def freshness_key(self) -> Optional[tuple[int, int]]:
        """Authenticated ``(signed_at, sequence)`` ordering key, or None.

        Only meaningful for signed Data: an unsigned timestamp/sequence is
        attacker-controlled, so this returns None unless a signature is present
        and at least one of the fields is set. Consumers compare keys for the
        same name to reject a rollback to an older signed version.
        """
        if self.signature is None:
            return None
        if self.metadata.signed_at is None and self.metadata.sequence is None:
            return None
        return (self.metadata.signed_at or 0, self.metadata.sequence or 0)

    def verify_signature(self, validator: Callable[[bytes, bytes], bool]) -> bool:
        """Verify the attached signature against signed_hash().

        ``validator`` is typically ``RNS.Identity.validate`` for the
        producer recalled from ``name.rns_addr``. Returns False if no
        signature is present.
        """
        if self.signature is None:
            return False
        return validator(self.signature, self.signed_hash())

    def to_bytes(self) -> bytes:
        name_bytes = self.name.to_bytes()
        has_meta = (self.metadata.content_hash is not None
                    or self.metadata.sequence is not None
                    or not self.metadata.freshness.fresh
                    or self.metadata.freshness_period is not None
                    or self.metadata.signed_at is not None
                    or self.metadata.encrypted)
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
        if self.signature is not None:
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
            if pos + SIGNATURE_BYTES > len(data):
                raise DataError("buffer too short for signature")
            signature = data[pos:pos + SIGNATURE_BYTES]

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


# ── Invalidate ──


class InvalidateError(Exception):
    ...


@dataclass
class Invalidate:
    """A producer-signed cache-purge control packet.

    Wire: [type:1=0x06][name_len:varint][name...][epoch:8][flags:1]
          [sig:64 if flags bit1]
      flags bit 0: is_prefix  → purge every name under ``name``
      flags bit 1: has_signature

    The signature is over ``signed_hash()`` and is verified against the
    producer's RNS identity recalled from ``name.rns_addr`` — self-certifying,
    so only the producer can invalidate names beneath its own address. ``epoch``
    is a producer-chosen monotonic value (e.g. unix time) used by relays to
    drop replayed/stale invalidations.
    """
    name: Name
    epoch: int = 0
    is_prefix: bool = False
    signature: Optional[bytes] = None

    def signed_hash(self) -> bytes:
        h = hashlib.blake2b(digest_size=32)
        h.update(self.name.to_bytes())
        h.update(struct.pack(">Q", self.epoch))
        h.update(b"\x01" if self.is_prefix else b"\x00")
        return h.digest()

    def sign(self, signer: Callable[[bytes], bytes]) -> Invalidate:
        sig = signer(self.signed_hash())
        if len(sig) != SIGNATURE_BYTES:
            raise InvalidateError(
                f"signature must be {SIGNATURE_BYTES} bytes, got {len(sig)}"
            )
        self.signature = sig
        return self

    def verify_signature(self, validator: Callable[[bytes, bytes], bool]) -> bool:
        if self.signature is None:
            return False
        return validator(self.signature, self.signed_hash())

    def to_bytes(self) -> bytes:
        name_bytes = self.name.to_bytes()
        buf = bytearray()
        buf.append(PacketType.INVALIDATE)
        buf.extend(_write_varint(len(name_bytes)))
        buf.extend(name_bytes)
        buf.extend(struct.pack(">Q", self.epoch))
        flags = 0
        if self.is_prefix:
            flags |= 0x01
        if self.signature is not None:
            flags |= 0x02
        buf.append(flags)
        if self.signature is not None:
            buf.extend(self.signature)
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> Invalidate:
        pos = 0
        if data[pos] != PacketType.INVALIDATE:
            raise InvalidateError(
                f"expected INVALIDATE type byte, got {data[pos]:#x}"
            )
        pos += 1
        name_len, consumed = _read_varint(data, pos)
        pos += consumed
        if pos + name_len > len(data):
            raise InvalidateError("buffer too short for name")
        name = Name.from_bytes(data[pos:pos + name_len])
        pos += name_len
        if pos + 8 > len(data):
            raise InvalidateError("buffer too short for epoch")
        epoch = struct.unpack(">Q", data[pos:pos + 8])[0]
        pos += 8
        if pos >= len(data):
            raise InvalidateError("buffer too short for flags")
        flags = data[pos]
        pos += 1
        signature = None
        if flags & 0x02:
            if pos + SIGNATURE_BYTES > len(data):
                raise InvalidateError("buffer too short for signature")
            signature = data[pos:pos + SIGNATURE_BYTES]
        return cls(name=name, epoch=epoch,
                   is_prefix=bool(flags & 0x01), signature=signature)


@dataclass
class Packet:
    """A parsed ICN packet with known type."""
    type: PacketType
    interest: Optional[Interest] = None
    data: Optional[Data] = None
    subscribe: Optional[APSubscribe] = None
    peer: Optional[PropPeer] = None
    cap_peer: Optional[CapPeer] = None
    invalidate: Optional[Invalidate] = None


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
    elif ptype == PacketType.INVALIDATE:
        inv = Invalidate.from_bytes(data)
        return Packet(type=PacketType.INVALIDATE, invalidate=inv)
    else:
        raise ValueError(f"unknown packet type: {ptype:#x}")
