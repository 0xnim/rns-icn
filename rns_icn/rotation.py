"""Producer key rotation (Phase 3.1).

Names are self-certifying: ``name.rns_addr`` is the truncated hash of the
producer's RNS public key, so the name *is* the key. That makes rotation
non-trivial — you cannot swap keys and keep the name. Instead a producer issues
a chain of signed *rotation certificates*: the anchor key (the one whose hash
equals ``name.rns_addr``) signs a delegation to a new key, that key signs the
next, and so on. Each link binds the producer namespace, a monotonic epoch, the
signing (``prev``) key and the delegated (``new``) key.

Verification is fully self-contained and offline: an RNS identity hash is the
truncated hash of its public key, so the chain is anchored simply by checking
that the first certificate's ``prev`` key hashes to ``name.rns_addr`` — no mesh
recall of the (possibly retired) anchor identity is required. Each subsequent
link must chain (``prev == previous.new``) and carry a valid signature.

This delivers rotation as key *continuity*: a valid chain widens the set of
public keys authorized to sign for a namespace (anchor + every delegated key),
so content signed by an older generation still verifies and caches are not
invalidated by a rotation. *Revocation* — shrinking that set when a key is
compromised — is deliberately separate (roadmap §3.4).
"""

from __future__ import annotations

import hashlib
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional

import RNS

from .name import RNS_ADDR_BYTES, Name
from .packet import SIGNATURE_BYTES

# RNS public keys are 64 bytes (32-byte X25519 + 32-byte Ed25519).
PUBLIC_KEY_BYTES = 64

# Domain-separation tag so a rotation signature can never be confused with a
# Data/Invalidate signature over a colliding byte string.
_ROTATION_DOMAIN = b"icn-key-rotation\x01"

# Separate tag for revocations so a revocation signature can never be replayed
# as a rotation (or vice versa) even over a colliding byte string.
_REVOCATION_DOMAIN = b"icn-key-revocation\x01"

# Reserved label under which a producer publishes its rotation bundle so peers
# can fetch it over the mesh: ``/<producer>/_rotation``. The leading underscore
# marks it reserved, distinct from ordinary content labels.
ROTATION_LABEL = b"_rotation"


def rotation_name(producer_addr: bytes) -> Name:
    """The well-known name a producer serves its rotation bundle under."""
    return Name(producer_addr, [ROTATION_LABEL])


class RotationError(Exception):
    ...


def _identity_from_public_key(public_key: bytes) -> RNS.Identity:
    """Reconstruct a verify-only RNS identity from its raw public key."""
    if len(public_key) != PUBLIC_KEY_BYTES:
        raise RotationError(
            f"public key must be {PUBLIC_KEY_BYTES} bytes, got {len(public_key)}"
        )
    ident = RNS.Identity(create_keys=False)
    # load_public_key returns None on success and False on failure (depending on
    # RNS version); treat only an explicit False as a failure.
    if ident.load_public_key(public_key) is False:
        raise RotationError("invalid public key")
    if ident.get_public_key() != public_key:
        raise RotationError("invalid public key")
    return ident


def addr_of_public_key(public_key: bytes) -> bytes:
    """The 16-byte producer address (identity hash) for a public key."""
    return _identity_from_public_key(public_key).hash


@dataclass
class KeyRotation:
    """One link in a producer's key-rotation chain.

    ``prev_public_key`` signs the delegation; ``new_public_key`` is the key
    being authorized. The first link's ``prev_public_key`` is the anchor key
    (its hash equals the producer address); ``epoch`` starts at 1 and increments
    by one per link.
    """

    producer: bytes          # 16-byte anchor address (the namespace)
    epoch: int               # generation, starts at 1
    prev_public_key: bytes   # signer's public key
    new_public_key: bytes    # delegated public key
    signature: Optional[bytes] = None

    def __post_init__(self) -> None:
        if len(self.producer) != RNS_ADDR_BYTES:
            raise RotationError(f"producer must be {RNS_ADDR_BYTES} bytes")
        if len(self.prev_public_key) != PUBLIC_KEY_BYTES:
            raise RotationError("prev_public_key must be a 64-byte RNS public key")
        if len(self.new_public_key) != PUBLIC_KEY_BYTES:
            raise RotationError("new_public_key must be a 64-byte RNS public key")
        if self.epoch < 1:
            raise RotationError("epoch must be >= 1")

    def signed_hash(self) -> bytes:
        h = hashlib.blake2b(digest_size=32)
        h.update(_ROTATION_DOMAIN)
        h.update(self.producer)
        h.update(struct.pack(">Q", self.epoch))
        h.update(self.prev_public_key)
        h.update(self.new_public_key)
        return h.digest()

    def sign(self, signer: Callable[[bytes], bytes]) -> "KeyRotation":
        """Sign with ``prev``'s signer (typically ``prev_identity.sign``)."""
        sig = signer(self.signed_hash())
        if len(sig) != SIGNATURE_BYTES:
            raise RotationError(
                f"signature must be {SIGNATURE_BYTES} bytes, got {len(sig)}"
            )
        self.signature = sig
        return self

    def verify_signature(self) -> bool:
        """Verify the signature against the embedded ``prev_public_key``."""
        if self.signature is None:
            return False
        try:
            validator = _identity_from_public_key(self.prev_public_key).validate
        except RotationError:
            return False
        return validator(self.signature, self.signed_hash())

    @classmethod
    def create(
        cls,
        producer: bytes,
        epoch: int,
        prev_identity: RNS.Identity,
        new_identity: RNS.Identity,
    ) -> "KeyRotation":
        """Mint and sign a rotation certificate.

        ``prev_identity`` must hold private keys (it signs the delegation);
        ``new_identity`` need only expose a public key.
        """
        cert = cls(
            producer=producer,
            epoch=epoch,
            prev_public_key=prev_identity.get_public_key(),
            new_public_key=new_identity.get_public_key(),
        )
        return cert.sign(prev_identity.sign)

    def to_bytes(self) -> bytes:
        buf = bytearray()
        buf.extend(self.producer)
        buf.extend(struct.pack(">Q", self.epoch))
        buf.extend(self.prev_public_key)
        buf.extend(self.new_public_key)
        buf.append(0x01 if self.signature is not None else 0x00)
        if self.signature is not None:
            buf.extend(self.signature)
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> "KeyRotation":
        fixed = RNS_ADDR_BYTES + 8 + 2 * PUBLIC_KEY_BYTES + 1
        if len(data) < fixed:
            raise RotationError("buffer too short for KeyRotation")
        pos = 0
        producer = data[pos:pos + RNS_ADDR_BYTES]
        pos += RNS_ADDR_BYTES
        epoch = struct.unpack(">Q", data[pos:pos + 8])[0]
        pos += 8
        prev_key = data[pos:pos + PUBLIC_KEY_BYTES]
        pos += PUBLIC_KEY_BYTES
        new_key = data[pos:pos + PUBLIC_KEY_BYTES]
        pos += PUBLIC_KEY_BYTES
        has_sig = data[pos]
        pos += 1
        signature = None
        if has_sig:
            if pos + SIGNATURE_BYTES > len(data):
                raise RotationError("buffer too short for signature")
            signature = data[pos:pos + SIGNATURE_BYTES]
        return cls(producer=producer, epoch=epoch, prev_public_key=prev_key,
                   new_public_key=new_key, signature=signature)


@dataclass
class Revocation:
    """A signed statement that a public key is no longer authorized.

    Revocation is the *shrinking* counterpart to rotation: where a chain widens
    the authorized-key set, a revocation removes a key (and the keys it
    delegated) — the compromise response. Only the namespace **anchor** (the key
    whose hash equals the producer address) may revoke; it is the root of trust
    for the name, so revocation is a deliberate cold-key operation even when
    day-to-day signing is delegated to a hot key.
    """

    producer: bytes            # 16-byte anchor address (the namespace)
    revoked_public_key: bytes  # 64-byte key being revoked
    anchor_public_key: bytes   # 64-byte anchor key (the signer)
    revoked_at: int            # unix seconds, for audit/ordering
    signature: Optional[bytes] = None

    def __post_init__(self) -> None:
        if len(self.producer) != RNS_ADDR_BYTES:
            raise RotationError(f"producer must be {RNS_ADDR_BYTES} bytes")
        if len(self.revoked_public_key) != PUBLIC_KEY_BYTES:
            raise RotationError("revoked_public_key must be a 64-byte RNS public key")
        if len(self.anchor_public_key) != PUBLIC_KEY_BYTES:
            raise RotationError("anchor_public_key must be a 64-byte RNS public key")
        if self.revoked_at < 0:
            raise RotationError("revoked_at must be >= 0")

    def signed_hash(self) -> bytes:
        h = hashlib.blake2b(digest_size=32)
        h.update(_REVOCATION_DOMAIN)
        h.update(self.producer)
        h.update(struct.pack(">Q", self.revoked_at))
        h.update(self.anchor_public_key)
        h.update(self.revoked_public_key)
        return h.digest()

    def sign(self, signer: Callable[[bytes], bytes]) -> "Revocation":
        """Sign with the anchor's signer (typically ``anchor_identity.sign``)."""
        sig = signer(self.signed_hash())
        if len(sig) != SIGNATURE_BYTES:
            raise RotationError(
                f"signature must be {SIGNATURE_BYTES} bytes, got {len(sig)}"
            )
        self.signature = sig
        return self

    def verify_signature(self) -> bool:
        """Verify the signature against the embedded ``anchor_public_key``."""
        if self.signature is None:
            return False
        try:
            validator = _identity_from_public_key(self.anchor_public_key).validate
        except RotationError:
            return False
        return validator(self.signature, self.signed_hash())

    @classmethod
    def create(
        cls,
        producer: bytes,
        revoked_public_key: bytes,
        anchor_identity: RNS.Identity,
        revoked_at: Optional[int] = None,
    ) -> "Revocation":
        """Mint and sign a revocation. ``anchor_identity`` must hold private keys."""
        rev = cls(
            producer=producer,
            revoked_public_key=revoked_public_key,
            anchor_public_key=anchor_identity.get_public_key(),
            revoked_at=revoked_at if revoked_at is not None else int(time.time()),
        )
        return rev.sign(anchor_identity.sign)

    def to_bytes(self) -> bytes:
        buf = bytearray()
        buf.extend(self.producer)
        buf.extend(struct.pack(">Q", self.revoked_at))
        buf.extend(self.anchor_public_key)
        buf.extend(self.revoked_public_key)
        buf.append(0x01 if self.signature is not None else 0x00)
        if self.signature is not None:
            buf.extend(self.signature)
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Revocation":
        fixed = RNS_ADDR_BYTES + 8 + 2 * PUBLIC_KEY_BYTES + 1
        if len(data) < fixed:
            raise RotationError("buffer too short for Revocation")
        pos = 0
        producer = data[pos:pos + RNS_ADDR_BYTES]
        pos += RNS_ADDR_BYTES
        revoked_at = struct.unpack(">Q", data[pos:pos + 8])[0]
        pos += 8
        anchor_key = data[pos:pos + PUBLIC_KEY_BYTES]
        pos += PUBLIC_KEY_BYTES
        revoked_key = data[pos:pos + PUBLIC_KEY_BYTES]
        pos += PUBLIC_KEY_BYTES
        has_sig = data[pos]
        pos += 1
        signature = None
        if has_sig:
            if pos + SIGNATURE_BYTES > len(data):
                raise RotationError("buffer too short for signature")
            signature = data[pos:pos + SIGNATURE_BYTES]
        return cls(producer=producer, revoked_public_key=revoked_key,
                   anchor_public_key=anchor_key, revoked_at=revoked_at,
                   signature=signature)


def verify_rotation_chain(
    producer_addr: bytes,
    certs: List[KeyRotation],
    revocations: Optional[List["Revocation"]] = None,
) -> List[bytes]:
    """Validate an ordered rotation chain and return the authorized public keys.

    Returns ``[anchor_pubkey, gen1_pubkey, ...]`` — the anchor plus every
    delegated key, i.e. the full set authorized to sign for ``producer_addr``.
    An empty chain returns ``[]`` (caller falls back to recalling the anchor).
    Raises :class:`RotationError` on any malformed or broken link.

    When ``revocations`` are supplied, every revoked key — and any key it
    transitively delegated down the chain (a compromised key could have minted
    rogue delegations) — is dropped from the authorized set. Each revocation
    must be signed by the namespace anchor; an invalid or foreign one raises.
    """
    if not certs:
        return []
    anchor_pub = certs[0].prev_public_key
    if addr_of_public_key(anchor_pub) != producer_addr:
        raise RotationError("chain anchor does not match producer address")
    authorized = [anchor_pub]
    expected_prev = anchor_pub
    for i, cert in enumerate(certs):
        if cert.producer != producer_addr:
            raise RotationError(f"cert epoch {cert.epoch} producer mismatch")
        if cert.epoch != i + 1:
            raise RotationError(f"non-contiguous epoch at index {i}: {cert.epoch}")
        if cert.prev_public_key != expected_prev:
            raise RotationError(f"chain break before epoch {cert.epoch}")
        if not cert.verify_signature():
            raise RotationError(f"invalid signature on epoch {cert.epoch}")
        authorized.append(cert.new_public_key)
        expected_prev = cert.new_public_key
    if revocations:
        revoked = _revoked_key_set(producer_addr, anchor_pub, certs, revocations)
        authorized = [k for k in authorized if k not in revoked]
    return authorized


def _revoked_key_set(
    producer_addr: bytes,
    anchor_pub: bytes,
    certs: List[KeyRotation],
    revocations: List["Revocation"],
) -> set[bytes]:
    """Resolve the full set of revoked keys, including delegated descendants.

    Each revocation must name ``producer_addr``, be signed by the namespace
    anchor (``anchor_pub``), and carry a valid signature. The cascade then walks
    the chain in epoch order: any cert delegated *by* an already-revoked key has
    its delegated key revoked too.
    """
    revoked: set[bytes] = set()
    for rev in revocations:
        if rev.producer != producer_addr:
            raise RotationError("revocation producer mismatch")
        if rev.anchor_public_key != anchor_pub:
            raise RotationError("revocation not signed by the namespace anchor")
        if not rev.verify_signature():
            raise RotationError("invalid revocation signature")
        revoked.add(rev.revoked_public_key)
    # certs are epoch-ordered and each link's prev precedes its new, so a single
    # forward pass propagates revocation down every delegated branch.
    for cert in certs:
        if cert.prev_public_key in revoked:
            revoked.add(cert.new_public_key)
    return revoked


def authorized_validators(
    producer_addr: bytes,
    certs: List[KeyRotation],
    revocations: Optional[List["Revocation"]] = None,
) -> List[Callable[[bytes, bytes], bool]]:
    """Validators (``RNS.Identity.validate``-shaped) for every authorized key."""
    return [
        _identity_from_public_key(pub).validate
        for pub in verify_rotation_chain(producer_addr, certs, revocations)
    ]


# ── Chain serialization (one producer's chain per file) ──


def chain_to_bytes(certs: List[KeyRotation]) -> bytes:
    buf = bytearray(struct.pack(">H", len(certs)))
    for cert in certs:
        cb = cert.to_bytes()
        buf.extend(struct.pack(">H", len(cb)))
        buf.extend(cb)
    return bytes(buf)


def chain_from_bytes(data: bytes) -> List[KeyRotation]:
    if len(data) < 2:
        raise RotationError("buffer too short for chain")
    count = struct.unpack(">H", data[:2])[0]
    pos = 2
    certs: List[KeyRotation] = []
    for _ in range(count):
        if pos + 2 > len(data):
            raise RotationError("buffer too short for cert length")
        clen = struct.unpack(">H", data[pos:pos + 2])[0]
        pos += 2
        if pos + clen > len(data):
            raise RotationError("buffer too short for cert body")
        certs.append(KeyRotation.from_bytes(data[pos:pos + clen]))
        pos += clen
    return certs


def save_rotation_chain(path: str, certs: List[KeyRotation]) -> None:
    Path(path).expanduser().write_bytes(chain_to_bytes(certs))


def load_rotation_chain(path: str) -> List[KeyRotation]:
    return chain_from_bytes(Path(path).expanduser().read_bytes())


# ── Rotation bundle (chain + revocations, one producer per blob) ──


@dataclass
class RotationBundle:
    """A producer's full rotation state: its chain plus any revocations.

    This is the unit distributed over the mesh (served as self-verifying Data at
    :func:`rotation_name`) and persisted to disk. The wire format is a superset
    of the bare chain — a legacy chain blob decodes as a bundle with no
    revocations, and a bundle decodes under the legacy chain reader (which stops
    after the certs) — so old and new files interoperate.
    """

    certs: List[KeyRotation] = field(default_factory=list)
    revocations: List[Revocation] = field(default_factory=list)

    def to_bytes(self) -> bytes:
        buf = bytearray(chain_to_bytes(self.certs))
        buf.extend(struct.pack(">H", len(self.revocations)))
        for rev in self.revocations:
            rb = rev.to_bytes()
            buf.extend(struct.pack(">H", len(rb)))
            buf.extend(rb)
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> "RotationBundle":
        if len(data) < 2:
            raise RotationError("buffer too short for bundle")
        count = struct.unpack(">H", data[:2])[0]
        pos = 2
        certs: List[KeyRotation] = []
        for _ in range(count):
            if pos + 2 > len(data):
                raise RotationError("buffer too short for cert length")
            clen = struct.unpack(">H", data[pos:pos + 2])[0]
            pos += 2
            if pos + clen > len(data):
                raise RotationError("buffer too short for cert body")
            certs.append(KeyRotation.from_bytes(data[pos:pos + clen]))
            pos += clen
        revocations: List[Revocation] = []
        # A legacy chain blob ends here; a bundle carries a revocation section.
        if pos < len(data):
            if pos + 2 > len(data):
                raise RotationError("buffer too short for revocation count")
            rcount = struct.unpack(">H", data[pos:pos + 2])[0]
            pos += 2
            for _ in range(rcount):
                if pos + 2 > len(data):
                    raise RotationError("buffer too short for revocation length")
                rlen = struct.unpack(">H", data[pos:pos + 2])[0]
                pos += 2
                if pos + rlen > len(data):
                    raise RotationError("buffer too short for revocation body")
                revocations.append(Revocation.from_bytes(data[pos:pos + rlen]))
                pos += rlen
        return cls(certs=certs, revocations=revocations)

    def verify(self, producer_addr: bytes) -> List[bytes]:
        """Validate the whole bundle and return the authorized public keys."""
        return verify_rotation_chain(producer_addr, self.certs, self.revocations)

    def validators(
        self, producer_addr: bytes
    ) -> List[Callable[[bytes, bytes], bool]]:
        return authorized_validators(producer_addr, self.certs, self.revocations)


def save_rotation_bundle(path: str, bundle: RotationBundle) -> None:
    Path(path).expanduser().write_bytes(bundle.to_bytes())


def load_rotation_bundle(path: str) -> RotationBundle:
    return RotationBundle.from_bytes(Path(path).expanduser().read_bytes())
