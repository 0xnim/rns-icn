"""Access control (Phase 3.3) — encrypted content + capability tokens + ACLs.

ICN content is cached by intermediaries the producer doesn't control, so access
control can't be "don't serve it" — anything served can be cached and replayed.
The only durable boundary is encryption: a restricted name's content is
encrypted so only authorized consumers can read it, while caches still store,
verify, and relay the opaque ciphertext exactly as before.

The model mirrors NDN Name-based Access Control, adapted to RNS identities:

* **ACL per prefix** — the producer declares, per name prefix, the set of
  consumer identities allowed to read it (:class:`AccessRule`, from config).
* **Encrypted content** — content under a restricted prefix is encrypted with a
  symmetric *content-encryption key* (CEK) derived deterministically from the
  producer's private key and the prefix (:func:`derive_cek`). The CEK is stable
  across restarts (so cached ciphertext stays decryptable) and never stored.
  Encryption happens once at publish; the content hash and producer signature
  cover the ciphertext, so caching/dedup/verification are untouched.
* **Capability tokens** — to read, a consumer needs the CEK. The producer issues
  a :class:`Capability`: a signed grant binding (consumer, prefix, validity)
  that carries the CEK *wrapped to the consumer's RNS identity* (ECIES, so only
  that consumer can unwrap it). The consumer verifies the producer's signature
  (self-certifying — the producer key is recalled from the name), unwraps the
  CEK, and decrypts.

A capability is safe to distribute over any channel — the wrapped CEK is opaque
to everyone but its named consumer — but distribution itself (files vs. mesh) is
left to the caller; the client loads capability files from config.
"""

from __future__ import annotations

import hashlib
import struct
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import RNS
from RNS.Cryptography import Token

from .name import RNS_ADDR_BYTES, Name
from .packet import SIGNATURE_BYTES

# 32-byte symmetric content-encryption key (AES via RNS Token).
CEK_BYTES = 32

# Domain-separation tags so a derived key / capability signature can never be
# confused with another use over a colliding byte string.
_CEK_DOMAIN = b"icn-content-key\x01"
_CAP_DOMAIN = b"icn-capability\x01"


class AccessError(Exception):
    ...


# ── Symmetric content encryption ──


def derive_cek(producer_identity: RNS.Identity, prefix: Name) -> bytes:
    """Derive the stable content-encryption key for a restricted prefix.

    Keyed off the producer's private key so only the producer can compute it,
    and off the prefix so each restricted prefix gets its own key. Deterministic
    — no key storage, and content published before/after a restart shares one CEK
    (and so one capability decrypts it all).
    """
    private = producer_identity.get_private_key()
    if not private:
        raise AccessError("producer identity has no private key for CEK derivation")
    # blake2b's keyed mode caps the key at 64 bytes; fold the private key first.
    secret = hashlib.blake2b(private, digest_size=64).digest()
    return hashlib.blake2b(
        _CEK_DOMAIN + prefix.to_bytes(), key=secret, digest_size=CEK_BYTES
    ).digest()


def encrypt_content(plaintext: bytes, cek: bytes) -> bytes:
    """Encrypt content under a CEK (AES, authenticated via RNS Token)."""
    if len(cek) != CEK_BYTES:
        raise AccessError(f"CEK must be {CEK_BYTES} bytes, got {len(cek)}")
    return Token(cek).encrypt(plaintext)


def decrypt_content(ciphertext: bytes, cek: bytes) -> bytes:
    """Decrypt content under a CEK. Raises :class:`AccessError` on failure."""
    if len(cek) != CEK_BYTES:
        raise AccessError(f"CEK must be {CEK_BYTES} bytes, got {len(cek)}")
    try:
        return Token(cek).decrypt(ciphertext)
    except Exception as e:
        raise AccessError(f"content decryption failed: {e}") from e


# ── CEK wrapping (per-consumer, ECIES via RNS identity) ──


def wrap_cek(cek: bytes, recipient: RNS.Identity) -> bytes:
    """Wrap a CEK so only ``recipient`` (by its X25519 key) can unwrap it."""
    if len(cek) != CEK_BYTES:
        raise AccessError(f"CEK must be {CEK_BYTES} bytes, got {len(cek)}")
    return recipient.encrypt(cek)


def unwrap_cek(wrapped: bytes, identity: RNS.Identity) -> bytes:
    """Unwrap a CEK with our own (private-key-holding) identity."""
    cek = identity.decrypt(wrapped)
    if cek is None or len(cek) != CEK_BYTES:
        raise AccessError("could not unwrap CEK (not the intended recipient?)")
    return cek


# ── Capability token ──


@dataclass
class Capability:
    """A producer-signed grant letting one consumer read a restricted prefix.

    Binds ``consumer`` (identity hash) to ``prefix`` for a validity window and
    carries ``wrapped_cek`` — the prefix CEK encrypted to the consumer. Signed by
    the producer; the consumer verifies the signature against the producer's
    authorized key (recalled from the self-certifying name), then unwraps and
    decrypts. ``expires_at`` of 0 means no expiry.
    """

    producer: bytes        # 16-byte producer (namespace) address
    consumer: bytes        # 16-byte authorized consumer identity hash
    prefix: Name           # the restricted name prefix this grants
    wrapped_cek: bytes     # CEK encrypted to the consumer's identity
    issued_at: int = 0
    expires_at: int = 0    # 0 = never expires
    signature: bytes | None = None

    def __post_init__(self) -> None:
        if len(self.producer) != RNS_ADDR_BYTES:
            raise AccessError(f"producer must be {RNS_ADDR_BYTES} bytes")
        if len(self.consumer) != RNS_ADDR_BYTES:
            raise AccessError(f"consumer must be {RNS_ADDR_BYTES} bytes")
        if self.prefix.rns_addr != self.producer:
            raise AccessError("prefix must live under the producer's namespace")

    def signed_hash(self) -> bytes:
        prefix_bytes = self.prefix.to_bytes()
        h = hashlib.blake2b(digest_size=32)
        h.update(_CAP_DOMAIN)
        h.update(self.producer)
        h.update(self.consumer)
        h.update(struct.pack(">QQ", self.issued_at, self.expires_at))
        h.update(struct.pack(">H", len(prefix_bytes)))
        h.update(prefix_bytes)
        h.update(struct.pack(">H", len(self.wrapped_cek)))
        h.update(self.wrapped_cek)
        return h.digest()

    def sign(self, signer: Callable[[bytes], bytes]) -> Capability:
        """Sign with the producer's signer (typically ``identity.sign``)."""
        sig = signer(self.signed_hash())
        if len(sig) != SIGNATURE_BYTES:
            raise AccessError(
                f"signature must be {SIGNATURE_BYTES} bytes, got {len(sig)}"
            )
        self.signature = sig
        return self

    def verify_signature(self, validator: Callable[[bytes, bytes], bool]) -> bool:
        """Verify against a producer-authorized validator (``Identity.validate``)."""
        if self.signature is None:
            return False
        return validator(self.signature, self.signed_hash())

    def is_expired(self, now: int | None = None) -> bool:
        if self.expires_at == 0:
            return False
        return (now if now is not None else int(time.time())) > self.expires_at

    def covers(self, name: Name, now: int | None = None) -> bool:
        """True if this capability is valid and grants ``name``."""
        return name.starts_with(self.prefix) and not self.is_expired(now)

    def unwrap(self, identity: RNS.Identity) -> bytes:
        """Unwrap the CEK with the consumer's own identity."""
        return unwrap_cek(self.wrapped_cek, identity)

    @classmethod
    def create(
        cls,
        prefix: Name,
        consumer: RNS.Identity,
        producer_identity: RNS.Identity,
        signer: Callable[[bytes], bytes],
        producer_addr: bytes | None = None,
        ttl_seconds: int = 0,
        now: int | None = None,
    ) -> Capability:
        """Mint and sign a capability granting ``consumer`` access to ``prefix``.

        Derives the prefix CEK from ``producer_identity`` (the namespace owner),
        wraps it to ``consumer``, and signs with ``signer`` (the producer's
        signing key). ``producer_addr`` defaults to ``prefix.rns_addr``.
        """
        addr = producer_addr if producer_addr is not None else prefix.rns_addr
        cek = derive_cek(producer_identity, prefix)
        issued = now if now is not None else int(time.time())
        expires = issued + ttl_seconds if ttl_seconds > 0 else 0
        cap = cls(
            producer=addr,
            consumer=consumer.hash,
            prefix=prefix,
            wrapped_cek=wrap_cek(cek, consumer),
            issued_at=issued,
            expires_at=expires,
        )
        return cap.sign(signer)

    def to_bytes(self) -> bytes:
        prefix_bytes = self.prefix.to_bytes()
        buf = bytearray()
        buf.extend(self.producer)
        buf.extend(self.consumer)
        buf.extend(struct.pack(">QQ", self.issued_at, self.expires_at))
        buf.extend(struct.pack(">H", len(prefix_bytes)))
        buf.extend(prefix_bytes)
        buf.extend(struct.pack(">H", len(self.wrapped_cek)))
        buf.extend(self.wrapped_cek)
        buf.append(0x01 if self.signature is not None else 0x00)
        if self.signature is not None:
            buf.extend(self.signature)
        return bytes(buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> Capability:
        fixed = 2 * RNS_ADDR_BYTES + 16
        if len(data) < fixed + 2:
            raise AccessError("buffer too short for Capability")
        pos = 0
        producer = data[pos:pos + RNS_ADDR_BYTES]
        pos += RNS_ADDR_BYTES
        consumer = data[pos:pos + RNS_ADDR_BYTES]
        pos += RNS_ADDR_BYTES
        issued_at, expires_at = struct.unpack(">QQ", data[pos:pos + 16])
        pos += 16
        plen = struct.unpack(">H", data[pos:pos + 2])[0]
        pos += 2
        if pos + plen > len(data):
            raise AccessError("buffer too short for prefix")
        prefix = Name.from_bytes(data[pos:pos + plen])
        pos += plen
        if pos + 2 > len(data):
            raise AccessError("buffer too short for wrapped_cek length")
        clen = struct.unpack(">H", data[pos:pos + 2])[0]
        pos += 2
        if pos + clen > len(data):
            raise AccessError("buffer too short for wrapped_cek")
        wrapped_cek = data[pos:pos + clen]
        pos += clen
        if pos >= len(data):
            raise AccessError("buffer too short for flag")
        has_sig = data[pos]
        pos += 1
        signature = None
        if has_sig:
            if pos + SIGNATURE_BYTES > len(data):
                raise AccessError("buffer too short for signature")
            signature = data[pos:pos + SIGNATURE_BYTES]
        return cls(producer=producer, consumer=consumer, prefix=prefix,
                   wrapped_cek=wrapped_cek, issued_at=issued_at,
                   expires_at=expires_at, signature=signature)


def save_capability(path: str, cap: Capability) -> None:
    Path(path).expanduser().write_bytes(cap.to_bytes())


def load_capability(path: str) -> Capability:
    return Capability.from_bytes(Path(path).expanduser().read_bytes())


# ── Producer-side access controller (ACL + encryption + issuance) ──


@dataclass
class AccessRule:
    """One ACL entry: a prefix and the consumers allowed to read it."""

    prefix: Name
    consumers: set[bytes] = field(default_factory=set)

    def allows(self, consumer_hash: bytes) -> bool:
        return consumer_hash in self.consumers


class AccessController:
    """Producer-side enforcement of per-prefix ACLs and content encryption.

    Holds the producer's restricted prefixes and authorized consumers, derives
    the per-prefix CEK from the producer identity, encrypts content at publish,
    and mints capabilities for authorized consumers.
    """

    def __init__(
        self,
        producer_identity: RNS.Identity,
        producer_addr: bytes,
        rules: list[AccessRule] | None = None,
    ):
        self.identity = producer_identity
        self.producer_addr = producer_addr
        self.rules: list[AccessRule] = list(rules or [])

    def matching_rule(self, name: Name) -> AccessRule | None:
        """Longest-prefix matching rule for ``name``, or None if public."""
        best: AccessRule | None = None
        for rule in self.rules:
            if name.starts_with(rule.prefix) and (
                best is None or rule.prefix.len() > best.prefix.len()
            ):
                best = rule
        return best

    def is_restricted(self, name: Name) -> bool:
        return self.matching_rule(name) is not None

    def encrypt_content(self, name: Name, plaintext: bytes) -> tuple[bytes, bool]:
        """Return ``(content, encrypted)`` for publishing ``name``.

        Encrypts under the matching restricted prefix's CEK, or returns the
        plaintext unchanged when ``name`` is public.
        """
        rule = self.matching_rule(name)
        if rule is None:
            return plaintext, False
        cek = derive_cek(self.identity, rule.prefix)
        return encrypt_content(plaintext, cek), True

    def issue_capability(
        self,
        prefix: Name,
        consumer: RNS.Identity,
        signer: Callable[[bytes], bytes],
        ttl_seconds: int = 0,
        now: int | None = None,
    ) -> Capability:
        """Mint a capability for ``consumer`` over ``prefix``.

        Refuses unless an ACL rule for ``prefix`` lists the consumer — the ACL is
        the authorization boundary; the capability is the bearer of that grant.
        """
        rule = next((r for r in self.rules if r.prefix == prefix), None)
        if rule is None:
            raise AccessError(f"no access rule for prefix {prefix}")
        if not rule.allows(consumer.hash):
            raise AccessError("consumer not authorized for prefix")
        return Capability.create(
            prefix=prefix,
            consumer=consumer,
            producer_identity=self.identity,
            signer=signer,
            producer_addr=self.producer_addr,
            ttl_seconds=ttl_seconds,
            now=now,
        )
