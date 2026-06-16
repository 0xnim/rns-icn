"""Tests for producer signing / verification (Phase 3.1/3.2).

Covers the packet-level sign/verify round trip, that the content store
persists and re-attaches signatures (so caches re-serve verifiable Data),
and the client's verify-if-present / require_signature policy.
"""

import pytest
import RNS

from rns_icn.config import ClientConfig
from rns_icn.content_store import ContentStore
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import SIGNATURE_BYTES, Data


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


@pytest.fixture
def identity() -> RNS.Identity:
    # RNS.Identity() generates a keypair without needing a running stack.
    return RNS.Identity()


# ── Packet-level ──


def test_rns_signature_is_64_bytes(identity):
    assert len(identity.sign(b"x")) == SIGNATURE_BYTES


def test_sign_verify_round_trip(identity):
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"hello world")
    data.sign(identity.sign)

    assert data.signature is not None
    assert len(data.signature) == SIGNATURE_BYTES
    assert data.verify_signature(identity.validate)


def test_signature_survives_serialization(identity):
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"payload").sign(identity.sign)

    parsed = Data.from_bytes(data.to_bytes())
    assert parsed.signature == data.signature
    assert parsed.verify_signature(identity.validate)


def test_tampered_content_fails_verification(identity):
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"original").sign(identity.sign)

    # A cache swaps the bytes (and recomputes the content hash) but cannot
    # forge the producer signature.
    data.content = b"tampered!"
    data.metadata.content_hash = None
    assert not data.verify_signature(identity.validate)


def test_signed_envelope_covers_sequence(identity):
    """The sequence number is authenticated — a relay can't tamper with it."""
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"v").with_sequence(7).sign(identity.sign)
    assert data.verify_signature(identity.validate)

    # Relay bumps the sequence to make stale content look newer.
    data.metadata.sequence = 8
    assert not data.verify_signature(identity.validate)


def test_signed_envelope_covers_signed_at(identity):
    """The signing timestamp is authenticated — a relay can't forge freshness."""
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"v").sign(identity.sign, signed_at=1000)
    assert data.metadata.signed_at == 1000
    assert data.verify_signature(identity.validate)

    data.metadata.signed_at = 9999
    assert not data.verify_signature(identity.validate)


def test_sign_auto_stamps_signed_at(identity):
    """sign() stamps a timestamp when none is set, and preserves an explicit one."""
    auto = Data.new(name=Name(rns_addr(), [b"a"]), content=b"x").sign(identity.sign)
    assert auto.metadata.signed_at is not None

    explicit = Data.new(name=Name(rns_addr(), [b"b"]), content=b"x")
    explicit.metadata.signed_at = 42
    explicit.sign(identity.sign)
    assert explicit.metadata.signed_at == 42


def test_signed_at_and_sequence_survive_serialization(identity):
    """The envelope fields round-trip on the wire so caches re-serve verifiable Data."""
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"p").with_sequence(3).sign(identity.sign, signed_at=555)
    parsed = Data.from_bytes(data.to_bytes())
    assert parsed.metadata.sequence == 3
    assert parsed.metadata.signed_at == 555
    assert parsed.verify_signature(identity.validate)


def test_pre_31_signature_without_envelope_still_verifies(identity):
    """A signature over only name+content+hash (no sequence/signed_at) verifies —
    the new fields are appended, so legacy signed Data is unaffected."""
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"legacy")
    # Sign without stamping signed_at, mimicking pre-3.1 producers.
    data.signature = identity.sign(data.signed_hash())
    assert data.metadata.signed_at is None
    assert data.verify_signature(identity.validate)


def test_freshness_key_requires_signature(identity):
    """An unsigned timestamp/sequence is attacker-controlled → no ordering key."""
    unsigned = Data.new(name=Name(rns_addr(), [b"d"]), content=b"x").with_sequence(5)
    unsigned.metadata.signed_at = 100
    assert unsigned.freshness_key() is None

    signed = Data.new(name=Name(rns_addr(), [b"d"]), content=b"x").with_sequence(5)
    signed.sign(identity.sign, signed_at=100)
    assert signed.freshness_key() == (100, 5)


def test_wrong_key_fails_verification(identity):
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"hello").sign(identity.sign)

    attacker = RNS.Identity()
    assert not data.verify_signature(attacker.validate)


def test_unsigned_verify_returns_false(identity):
    data = Data.new(name=Name(rns_addr(), [b"doc"]), content=b"hi")
    assert data.signature is None
    assert not data.verify_signature(identity.validate)


def test_sign_rejects_wrong_length():
    data = Data.new(name=Name(rns_addr(), [b"doc"]), content=b"hi")
    with pytest.raises(Exception):
        data.sign(lambda _digest: b"\x00" * 32)


# ── Content store ──


def test_content_store_persists_signature(tmp_path, identity):
    cs = ContentStore(str(tmp_path / "cs.db"), max_entries=10, default_ttl=86400)
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"cached payload").sign(identity.sign)

    cs.insert(name, data)
    got = cs.get(name)

    assert got is not None
    assert got.signature == data.signature
    assert got.verify_signature(identity.validate)


def test_content_store_persists_signed_envelope(tmp_path, identity):
    """signed_at survives the store round-trip, so cached signed Data re-verifies
    (the signature covers signed_at — dropping it would break verification)."""
    cs = ContentStore(str(tmp_path / "cs.db"), max_entries=10, default_ttl=86400)
    name = Name(rns_addr(), [b"doc"])
    data = Data.new(name=name, content=b"cached").with_sequence(4).sign(
        identity.sign, signed_at=777
    )

    cs.insert(name, data)
    got = cs.get(name)

    assert got is not None
    assert got.metadata.signed_at == 777
    assert got.metadata.sequence == 4
    assert got.verify_signature(identity.validate)


def test_content_store_unsigned_stays_unsigned(tmp_path):
    cs = ContentStore(str(tmp_path / "cs.db"), max_entries=10, default_ttl=86400)
    name = Name(rns_addr(), [b"doc"])
    cs.insert(name, Data.new(name=name, content=b"plain"))

    got = cs.get(name)
    assert got is not None
    assert got.signature is None


# ── Client policy ──


def _client(require_signature: bool, rotation_store=None):
    client = object.__new__(_ClientForPolicy)
    client.config = ClientConfig(require_signature=require_signature)
    client._rotation_store = rotation_store or {}
    client._revocation_store = {}
    return client


class _ClientForPolicy:
    # Borrow the real method under test without constructing a full ICNClient.
    from rns_icn.client import ICNClient as _C
    _check_signature = _C._check_signature
    _check_rollback = _C._check_rollback


def _rollback_client(reject_rollback: bool):
    client = object.__new__(_ClientForPolicy)
    client.config = ClientConfig(reject_rollback=reject_rollback)
    client._seen_signed_key = {}
    client._rotation_store = {}
    client._revocation_store = {}
    return client


@pytest.fixture
def recallable(monkeypatch, identity):
    monkeypatch.setattr(
        RNS.Identity, "recall",
        staticmethod(lambda _addr, from_identity_hash=False: identity),
    )
    return identity


@pytest.fixture
def not_recallable(monkeypatch):
    monkeypatch.setattr(
        RNS.Identity, "recall",
        staticmethod(lambda _addr, from_identity_hash=False: None),
    )


def _signed(identity) -> Data:
    name = Name(rns_addr(), [b"doc"])
    return Data.new(name=name, content=b"hello").sign(identity.sign)


def test_policy_valid_signature_accepted(recallable):
    ok, err = _client(require_signature=False)._check_signature(_signed(recallable))
    assert ok and err is None


def test_policy_invalid_signature_always_rejected(recallable):
    data = _signed(recallable)
    data.content = b"tampered"
    data.metadata.content_hash = None
    # Even non-strict must reject a present-but-bad signature.
    ok, err = _client(require_signature=False)._check_signature(data)
    assert not ok and err is not None


def test_policy_unsigned_accepted_when_not_strict(not_recallable):
    data = Data.new(name=Name(rns_addr(), [b"doc"]), content=b"hi")
    ok, err = _client(require_signature=False)._check_signature(data)
    assert ok and err is None


def test_policy_unsigned_rejected_when_strict(not_recallable):
    data = Data.new(name=Name(rns_addr(), [b"doc"]), content=b"hi")
    ok, err = _client(require_signature=True)._check_signature(data)
    assert not ok and err is not None


def test_policy_unknown_key_rejected_when_strict(not_recallable, identity):
    ok, err = _client(require_signature=True)._check_signature(_signed(identity))
    assert not ok and err is not None


def test_policy_unknown_key_accepted_when_not_strict(not_recallable, identity):
    ok, err = _client(require_signature=False)._check_signature(_signed(identity))
    assert ok and err is None


# ── Rollback protection (Phase 3.1: authenticated signed_at/sequence) ──


def _signed_seq(identity, seq: int, signed_at: int) -> Data:
    name = Name(rns_addr(), [b"doc"])
    return Data.new(name=name, content=f"v{seq}".encode()).with_sequence(seq).sign(
        identity.sign, signed_at=signed_at
    )


def test_rollback_rejected_when_enabled(identity):
    client = _rollback_client(reject_rollback=True)
    newer = _signed_seq(identity, seq=5, signed_at=2000)
    older = _signed_seq(identity, seq=4, signed_at=1000)

    ok, _ = client._check_rollback(newer)
    assert ok
    ok, err = client._check_rollback(older)
    assert not ok and err is not None


def test_rollback_allows_same_and_newer(identity):
    client = _rollback_client(reject_rollback=True)
    first = _signed_seq(identity, seq=5, signed_at=2000)
    assert client._check_rollback(first)[0]
    # Re-delivery of the same version is fine (not a rollback).
    assert client._check_rollback(first)[0]
    # A genuinely newer version advances the watermark.
    assert client._check_rollback(_signed_seq(identity, seq=6, signed_at=3000))[0]


def test_rollback_ignored_when_disabled(identity):
    client = _rollback_client(reject_rollback=False)
    assert client._check_rollback(_signed_seq(identity, seq=5, signed_at=2000))[0]
    # Older version still accepted because the guard is off.
    assert client._check_rollback(_signed_seq(identity, seq=4, signed_at=1000))[0]


def test_rollback_ignores_unsigned_data(identity):
    """Unsigned Data has no trustworthy key, so it bypasses the guard entirely."""
    client = _rollback_client(reject_rollback=True)
    name = Name(rns_addr(), [b"doc"])
    unsigned_new = Data.new(name=name, content=b"x").with_sequence(5)
    unsigned_new.metadata.signed_at = 2000
    unsigned_old = Data.new(name=name, content=b"y").with_sequence(1)
    unsigned_old.metadata.signed_at = 10
    assert client._check_rollback(unsigned_new)[0]
    assert client._check_rollback(unsigned_old)[0]


def test_rollback_is_per_name(identity):
    client = _rollback_client(reject_rollback=True)
    a_new = Data.new(name=Name(rns_addr(), [b"a"]), content=b"x").with_sequence(5).sign(
        identity.sign, signed_at=2000
    )
    b_old = Data.new(name=Name(rns_addr(), [b"b"]), content=b"y").with_sequence(1).sign(
        identity.sign, signed_at=1
    )
    assert client._check_rollback(a_new)[0]
    # A different name with a low key is not a rollback of /a.
    assert client._check_rollback(b_old)[0]


# ── Key rotation (Phase 3.1: _check_signature consults the rotation chain) ──

from rns_icn.rotation import KeyRotation  # noqa: E402


def _rotation_client(anchor, *gens):
    """A policy client holding a valid rotation chain anchor → gens..."""
    certs, prev = [], anchor
    for i, nxt in enumerate(gens, start=1):
        certs.append(KeyRotation.create(anchor.hash, i, prev, nxt))
        prev = nxt
    client = object.__new__(_ClientForPolicy)
    client.config = ClientConfig(require_signature=True)
    client._rotation_store = {anchor.hash: certs}
    client._revocation_store = {}
    return client


def test_rotated_key_signature_accepted():
    anchor, new = RNS.Identity(), RNS.Identity()
    client = _rotation_client(anchor, new)
    data = Data.new(name=Name(anchor.hash, [b"doc"]), content=b"x").sign(new.sign)
    ok, err = client._check_signature(data)
    assert ok and err is None


def test_anchor_signature_still_accepted_with_chain():
    anchor, new = RNS.Identity(), RNS.Identity()
    client = _rotation_client(anchor, new)
    data = Data.new(name=Name(anchor.hash, [b"doc"]), content=b"x").sign(anchor.sign)
    ok, err = client._check_signature(data)
    assert ok and err is None


def test_unauthorized_key_rejected_under_chain():
    anchor, new, attacker = RNS.Identity(), RNS.Identity(), RNS.Identity()
    client = _rotation_client(anchor, new)
    data = Data.new(name=Name(anchor.hash, [b"doc"]), content=b"x").sign(attacker.sign)
    ok, err = client._check_signature(data)
    assert not ok and err is not None


def test_chain_consulted_without_recall(monkeypatch):
    """A known chain is self-certifying — verification must not require recall."""
    monkeypatch.setattr(
        RNS.Identity, "recall",
        staticmethod(lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("recall should not be called when a chain is present")
        )),
    )
    anchor, new = RNS.Identity(), RNS.Identity()
    client = _rotation_client(anchor, new)
    data = Data.new(name=Name(anchor.hash, [b"doc"]), content=b"x").sign(new.sign)
    assert client._check_signature(data)[0]


# ── Key revocation (Phase 3.4: _check_signature honours revocations) ──

from rns_icn.rotation import Revocation  # noqa: E402


def test_revoked_key_signature_rejected():
    """A revoked delegated key no longer verifies even though it's in the chain."""
    anchor, compromised = RNS.Identity(), RNS.Identity()
    client = _rotation_client(anchor, compromised)
    rev = Revocation.create(anchor.hash, compromised.get_public_key(), anchor)
    client._revocation_store = {anchor.hash: [rev]}
    data = Data.new(name=Name(anchor.hash, [b"doc"]), content=b"x").sign(compromised.sign)
    ok, err = client._check_signature(data)
    assert not ok and err is not None


def test_anchor_still_valid_after_revoking_delegate():
    anchor, compromised = RNS.Identity(), RNS.Identity()
    client = _rotation_client(anchor, compromised)
    rev = Revocation.create(anchor.hash, compromised.get_public_key(), anchor)
    client._revocation_store = {anchor.hash: [rev]}
    data = Data.new(name=Name(anchor.hash, [b"doc"]), content=b"x").sign(anchor.sign)
    assert client._check_signature(data)[0]


# ── Per-chunk signatures (Phase 3.2: resource_transport / streamed files) ──

from rns_icn.assembler import (  # noqa: E402
    HashMismatchError,
    SignatureError,
    assemble,
    assemble_verified,
    verify_chunks,
)
from rns_icn.chunker import chunk_content  # noqa: E402


def _big_name() -> Name:
    return Name(rns_addr(0x42), [b"large.bin"])


def test_chunk_content_unsigned_by_default():
    """Without a signer, chunk Data packets carry no signature (back-compat)."""
    result = chunk_content(b"x" * 5000, _big_name(), chunk_size=1000)
    assert result.chunk_count() == 5
    assert all(dp.signature is None for dp in result.data_packets)


def test_chunk_content_signs_every_chunk(identity):
    """A signer signs each chunk with a verifiable producer signature."""
    result = chunk_content(b"y" * 5000, _big_name(), chunk_size=1000, signer=identity.sign)
    assert result.chunk_count() == 5
    for dp in result.data_packets:
        assert dp.signature is not None
        assert len(dp.signature) == SIGNATURE_BYTES
        assert dp.verify_signature(identity.validate)


def test_signed_chunks_survive_serialization(identity):
    """Chunk signatures round-trip through the wire format, so caches re-serve
    verifiable chunks."""
    result = chunk_content(b"z" * 3000, _big_name(), chunk_size=1000, signer=identity.sign)
    for dp in result.data_packets:
        parsed = Data.from_bytes(dp.to_bytes())
        assert parsed.signature == dp.signature
        assert parsed.verify_signature(identity.validate)


def test_assemble_with_validator_accepts_signed_chunks(identity):
    """assemble() with a validator reassembles signed content end-to-end."""
    content = b"streamed large file payload" * 100
    result = chunk_content(content, _big_name(), chunk_size=256, signer=identity.sign)
    chunks = {ref.label: dp for ref, dp in zip(result.manifest.chunks, result.data_packets)}
    assert assemble(result.manifest, chunks, validator=identity.validate) == content


def test_assemble_rejects_unsigned_chunks_when_validator_given(identity):
    """A validator requires every chunk to be signed — unsigned streams fail."""
    result = chunk_content(b"a" * 3000, _big_name(), chunk_size=1000)  # no signer
    chunks = {ref.label: dp for ref, dp in zip(result.manifest.chunks, result.data_packets)}
    with pytest.raises(SignatureError, match="unsigned"):
        assemble(result.manifest, chunks, validator=identity.validate)


def test_assemble_rejects_chunk_signed_by_wrong_key(identity):
    """A chunk signed by a different identity (relay substitution) is rejected."""
    result = chunk_content(b"b" * 3000, _big_name(), chunk_size=1000, signer=identity.sign)
    chunks = {ref.label: dp for ref, dp in zip(result.manifest.chunks, result.data_packets)}
    attacker = RNS.Identity()
    with pytest.raises(SignatureError, match="failed verification"):
        assemble(result.manifest, chunks, validator=attacker.validate)


def test_assemble_detects_substituted_chunk_payload(identity):
    """Swapping a chunk's content for a same-length forgery is caught — by the
    content hash first, and by the signature if hashes were also forged."""
    content = b"c" * 3000
    result = chunk_content(content, _big_name(), chunk_size=1000, signer=identity.sign)
    chunks = {ref.label: dp for ref, dp in zip(result.manifest.chunks, result.data_packets)}
    # Forge a middle chunk's payload but keep its (now-stale) signature.
    victim = chunks["chunk_0001"]
    victim.content = b"X" * 1000
    with pytest.raises((HashMismatchError, SignatureError)):
        assemble(result.manifest, chunks, validator=identity.validate)


def test_assemble_without_validator_ignores_signatures(identity):
    """Back-compat: omitting the validator skips signature checks entirely."""
    content = b"d" * 2000
    result = chunk_content(content, _big_name(), chunk_size=1000)  # unsigned
    chunks = {ref.label: dp for ref, dp in zip(result.manifest.chunks, result.data_packets)}
    assert assemble(result.manifest, chunks) == content


def test_assemble_verified_honours_validator(identity):
    """assemble_verified() (no overall-hash check) also enforces signatures."""
    result = chunk_content(b"e" * 2500, _big_name(), chunk_size=1000, signer=identity.sign)
    chunks = {ref.label: dp for ref, dp in zip(result.manifest.chunks, result.data_packets)}
    out = assemble_verified(result.manifest, chunks, validator=identity.validate)
    assert out == b"e" * 2500


def test_verify_chunks_reports_signature_state(identity):
    """verify_chunks() with a validator flags unsigned/forged chunks per label."""
    result = chunk_content(b"f" * 3000, _big_name(), chunk_size=1000, signer=identity.sign)
    chunks = {ref.label: dp for ref, dp in zip(result.manifest.chunks, result.data_packets)}
    assert all(verify_chunks(result.manifest, chunks, validator=identity.validate).values())

    attacker = RNS.Identity()
    results = verify_chunks(result.manifest, chunks, validator=attacker.validate)
    assert not any(results.values())
