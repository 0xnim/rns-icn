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
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional

import RNS

from .name import RNS_ADDR_BYTES
from .packet import SIGNATURE_BYTES

# RNS public keys are 64 bytes (32-byte X25519 + 32-byte Ed25519).
PUBLIC_KEY_BYTES = 64

# Domain-separation tag so a rotation signature can never be confused with a
# Data/Invalidate signature over a colliding byte string.
_ROTATION_DOMAIN = b"icn-key-rotation\x01"


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


def verify_rotation_chain(producer_addr: bytes, certs: List[KeyRotation]) -> List[bytes]:
    """Validate an ordered rotation chain and return the authorized public keys.

    Returns ``[anchor_pubkey, gen1_pubkey, ...]`` — the anchor plus every
    delegated key, i.e. the full set authorized to sign for ``producer_addr``.
    An empty chain returns ``[]`` (caller falls back to recalling the anchor).
    Raises :class:`RotationError` on any malformed or broken link.
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
    return authorized


def authorized_validators(
    producer_addr: bytes, certs: List[KeyRotation]
) -> List[Callable[[bytes, bytes], bool]]:
    """Validators (``RNS.Identity.validate``-shaped) for every authorized key."""
    return [
        _identity_from_public_key(pub).validate
        for pub in verify_rotation_chain(producer_addr, certs)
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
