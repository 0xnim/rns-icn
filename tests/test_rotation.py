"""Tests for producer key rotation (Phase 3.1).

Covers the rotation certificate sign/verify round trip, chain validation
(anchor binding, ordering, broken links, forgery), serialization, and the
authorized-key set a valid chain produces.
"""

import pytest
import RNS

from rns_icn.name import Name
from rns_icn.packet import Data
from rns_icn.rotation import (
    KeyRotation,
    Revocation,
    RotationBundle,
    RotationError,
    addr_of_public_key,
    authorized_validators,
    chain_from_bytes,
    chain_to_bytes,
    load_rotation_bundle,
    load_rotation_chain,
    rotation_name,
    save_rotation_bundle,
    save_rotation_chain,
    verify_rotation_chain,
)


@pytest.fixture
def anchor() -> RNS.Identity:
    return RNS.Identity()


def _name_for(identity: RNS.Identity, label=b"doc") -> Name:
    return Name(identity.hash, [label])


def _chain(anchor: RNS.Identity, *gens: RNS.Identity) -> list[KeyRotation]:
    """Build a valid chain anchor → gens[0] → gens[1] → ..."""
    certs = []
    prev = anchor
    for i, nxt in enumerate(gens, start=1):
        certs.append(KeyRotation.create(anchor.hash, i, prev, nxt))
        prev = nxt
    return certs


# ── Certificate level ──


def test_addr_of_public_key_matches_identity_hash(anchor):
    assert addr_of_public_key(anchor.get_public_key()) == anchor.hash


def test_cert_sign_verify_round_trip(anchor):
    new = RNS.Identity()
    cert = KeyRotation.create(anchor.hash, 1, anchor, new)
    assert cert.signature is not None
    assert cert.verify_signature()


def test_cert_tampered_new_key_fails(anchor):
    new, attacker = RNS.Identity(), RNS.Identity()
    cert = KeyRotation.create(anchor.hash, 1, anchor, new)
    cert.new_public_key = attacker.get_public_key()
    assert not cert.verify_signature()


def test_cert_serialization_round_trip(anchor):
    cert = KeyRotation.create(anchor.hash, 1, anchor, RNS.Identity())
    parsed = KeyRotation.from_bytes(cert.to_bytes())
    assert parsed.producer == cert.producer
    assert parsed.epoch == cert.epoch
    assert parsed.prev_public_key == cert.prev_public_key
    assert parsed.new_public_key == cert.new_public_key
    assert parsed.signature == cert.signature
    assert parsed.verify_signature()


def test_cert_rejects_bad_lengths(anchor):
    with pytest.raises(RotationError):
        KeyRotation(producer=b"short", epoch=1,
                    prev_public_key=anchor.get_public_key(),
                    new_public_key=anchor.get_public_key())


# ── Chain validation ──


def test_empty_chain_returns_no_keys(anchor):
    assert verify_rotation_chain(anchor.hash, []) == []


def test_single_link_chain_authorizes_anchor_and_new(anchor):
    new = RNS.Identity()
    keys = verify_rotation_chain(anchor.hash, _chain(anchor, new))
    assert keys == [anchor.get_public_key(), new.get_public_key()]


def test_multi_link_chain_authorizes_every_generation(anchor):
    g1, g2 = RNS.Identity(), RNS.Identity()
    keys = verify_rotation_chain(anchor.hash, _chain(anchor, g1, g2))
    assert keys == [anchor.get_public_key(), g1.get_public_key(), g2.get_public_key()]


def test_chain_anchor_must_match_producer(anchor):
    # A chain whose anchor key hashes to a different address is rejected for
    # this producer (prevents grafting another producer's chain onto a name).
    other = RNS.Identity()
    certs = _chain(anchor, RNS.Identity())
    with pytest.raises(RotationError, match="anchor"):
        verify_rotation_chain(other.hash, certs)


def test_chain_break_detected(anchor):
    # Replace the second link's prev key so it no longer chains.
    g1, g2 = RNS.Identity(), RNS.Identity()
    certs = _chain(anchor, g1, g2)
    stray = RNS.Identity()
    certs[1] = KeyRotation.create(anchor.hash, 2, stray, g2)
    with pytest.raises(RotationError, match="chain break"):
        verify_rotation_chain(anchor.hash, certs)


def test_chain_non_contiguous_epoch_detected(anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    certs[0].epoch = 2  # invalidates signature too, but epoch check fires first
    with pytest.raises(RotationError, match="epoch"):
        verify_rotation_chain(anchor.hash, certs)


def test_chain_forged_signature_detected(anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    certs[0].signature = b"\x00" * 64
    with pytest.raises(RotationError, match="signature"):
        verify_rotation_chain(anchor.hash, certs)


# ── Authorized validators / end-to-end against Data ──


def test_rotated_key_signs_verifiable_data(anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    # Producer rotates and signs content under its (unchanged) anchor name with
    # the new key.
    data = Data.new(name=_name_for(anchor), content=b"after rotation").sign(g1.sign)
    validators = authorized_validators(anchor.hash, certs)
    assert any(data.verify_signature(v) for v in validators)


def test_pre_rotation_data_still_verifies(anchor):
    # Content signed by the anchor before rotation stays valid (continuity).
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    data = Data.new(name=_name_for(anchor), content=b"before").sign(anchor.sign)
    validators = authorized_validators(anchor.hash, certs)
    assert any(data.verify_signature(v) for v in validators)


def test_unauthorized_key_rejected(anchor):
    g1, attacker = RNS.Identity(), RNS.Identity()
    certs = _chain(anchor, g1)
    data = Data.new(name=_name_for(anchor), content=b"forged").sign(attacker.sign)
    validators = authorized_validators(anchor.hash, certs)
    assert not any(data.verify_signature(v) for v in validators)


# ── Chain file serialization ──


def test_chain_bytes_round_trip(anchor):
    certs = _chain(anchor, RNS.Identity(), RNS.Identity())
    restored = chain_from_bytes(chain_to_bytes(certs))
    assert [c.to_bytes() for c in restored] == [c.to_bytes() for c in certs]


def test_save_and_load_chain(tmp_path, anchor):
    certs = _chain(anchor, RNS.Identity())
    path = str(tmp_path / "producer.chain")
    save_rotation_chain(path, certs)
    loaded = load_rotation_chain(path)
    assert verify_rotation_chain(anchor.hash, loaded) == [
        anchor.get_public_key(), certs[0].new_public_key
    ]


# ── Revocation (Phase 3.4) ──


def test_revocation_sign_verify_round_trip(anchor):
    victim = RNS.Identity()
    rev = Revocation.create(anchor.hash, victim.get_public_key(), anchor)
    assert rev.signature is not None
    assert rev.verify_signature()


def test_revocation_tampered_key_fails(anchor):
    victim, other = RNS.Identity(), RNS.Identity()
    rev = Revocation.create(anchor.hash, victim.get_public_key(), anchor)
    rev.revoked_public_key = other.get_public_key()
    assert not rev.verify_signature()


def test_revocation_serialization_round_trip(anchor):
    victim = RNS.Identity()
    rev = Revocation.create(anchor.hash, victim.get_public_key(), anchor, revoked_at=42)
    parsed = Revocation.from_bytes(rev.to_bytes())
    assert parsed.producer == rev.producer
    assert parsed.revoked_at == 42
    assert parsed.anchor_public_key == rev.anchor_public_key
    assert parsed.revoked_public_key == rev.revoked_public_key
    assert parsed.signature == rev.signature
    assert parsed.verify_signature()


def test_revocation_rejects_bad_length(anchor):
    with pytest.raises(RotationError):
        Revocation(producer=b"short", revoked_public_key=anchor.get_public_key(),
                   anchor_public_key=anchor.get_public_key(), revoked_at=0)


def test_revoked_delegate_dropped_from_authorized(anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    rev = Revocation.create(anchor.hash, g1.get_public_key(), anchor)
    keys = verify_rotation_chain(anchor.hash, certs, [rev])
    assert keys == [anchor.get_public_key()]  # g1 removed


def test_revocation_cascades_to_descendants(anchor):
    # Revoking g1 must also drop g2 (delegated by the compromised g1).
    g1, g2 = RNS.Identity(), RNS.Identity()
    certs = _chain(anchor, g1, g2)
    rev = Revocation.create(anchor.hash, g1.get_public_key(), anchor)
    keys = verify_rotation_chain(anchor.hash, certs, [rev])
    assert keys == [anchor.get_public_key()]


def test_revocation_by_non_anchor_rejected(anchor):
    # A revocation signed by a key that is not the namespace anchor is rejected.
    g1, impostor = RNS.Identity(), RNS.Identity()
    certs = _chain(anchor, g1)
    rev = Revocation.create(anchor.hash, g1.get_public_key(), impostor)
    with pytest.raises(RotationError, match="anchor"):
        verify_rotation_chain(anchor.hash, certs, [rev])


def test_forged_revocation_signature_rejected(anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    rev = Revocation.create(anchor.hash, g1.get_public_key(), anchor)
    rev.signature = b"\x00" * 64
    with pytest.raises(RotationError, match="revocation"):
        verify_rotation_chain(anchor.hash, certs, [rev])


# ── Rotation bundle (chain + revocations) ──


def test_bundle_round_trip(anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    rev = Revocation.create(anchor.hash, g1.get_public_key(), anchor)
    bundle = RotationBundle(certs=certs, revocations=[rev])
    restored = RotationBundle.from_bytes(bundle.to_bytes())
    assert [c.to_bytes() for c in restored.certs] == [c.to_bytes() for c in certs]
    assert [r.to_bytes() for r in restored.revocations] == [rev.to_bytes()]


def test_bundle_reads_legacy_chain_bytes(anchor):
    # A bare chain blob (no revocation section) loads as a bundle with none.
    certs = _chain(anchor, RNS.Identity())
    bundle = RotationBundle.from_bytes(chain_to_bytes(certs))
    assert [c.to_bytes() for c in bundle.certs] == [c.to_bytes() for c in certs]
    assert bundle.revocations == []


def test_bundle_verify_applies_revocations(anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    rev = Revocation.create(anchor.hash, g1.get_public_key(), anchor)
    bundle = RotationBundle(certs=certs, revocations=[rev])
    assert bundle.verify(anchor.hash) == [anchor.get_public_key()]


def test_save_and_load_bundle(tmp_path, anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    rev = Revocation.create(anchor.hash, g1.get_public_key(), anchor)
    path = str(tmp_path / "producer.bundle")
    save_rotation_bundle(path, RotationBundle(certs=certs, revocations=[rev]))
    loaded = load_rotation_bundle(path)
    assert loaded.verify(anchor.hash) == [anchor.get_public_key()]


def test_rotation_name_is_well_known(anchor):
    name = rotation_name(anchor.hash)
    assert name.rns_addr == anchor.hash
    assert name.components[1] == b"_rotation"


# ── Mesh distribution: client fetch_rotation_bundle ──


def _bundle_client():
    """A bare ICNClient with just the rotation stores, no RNS/forwarder."""
    from rns_icn.client import ICNClient
    client = object.__new__(ICNClient)
    client._rotation_store = {}
    client._revocation_store = {}
    return client


@pytest.mark.asyncio
async def test_fetch_rotation_bundle_loads_stores(anchor):
    g1 = RNS.Identity()
    certs = _chain(anchor, g1)
    rev = Revocation.create(anchor.hash, g1.get_public_key(), anchor)
    bundle = RotationBundle(certs=certs, revocations=[rev])

    client = _bundle_client()

    async def fake_fetch(name, peer_hash, timeout=None, apply_policy=True):
        assert name == rotation_name(anchor.hash)
        assert apply_policy is False  # bundle is self-verifying, not signed
        return Data.new(name=name, content=bundle.to_bytes())

    client.fetch = fake_fetch  # type: ignore[method-assign]
    keys = await client.fetch_rotation_bundle(anchor.hash, b"\x00" * 16)
    assert keys == [anchor.get_public_key()]  # g1 revoked
    assert client._rotation_store[anchor.hash] == certs
    assert client._revocation_store[anchor.hash] == [rev]


@pytest.mark.asyncio
async def test_fetch_rotation_bundle_rejects_foreign_chain(anchor):
    # A relay returns a valid chain that anchors to a *different* producer.
    other = RNS.Identity()
    bundle = RotationBundle(certs=_chain(other, RNS.Identity()))

    client = _bundle_client()

    async def fake_fetch(name, peer_hash, timeout=None, apply_policy=True):
        return Data.new(name=name, content=bundle.to_bytes())

    client.fetch = fake_fetch  # type: ignore[method-assign]
    with pytest.raises(RotationError, match="anchor"):
        await client.fetch_rotation_bundle(anchor.hash, b"\x00" * 16)
    assert anchor.hash not in client._rotation_store


@pytest.mark.asyncio
async def test_fetch_rotation_bundle_returns_none_when_absent(anchor):
    client = _bundle_client()

    async def fake_fetch(name, peer_hash, timeout=None, apply_policy=True):
        return None

    client.fetch = fake_fetch  # type: ignore[method-assign]
    assert await client.fetch_rotation_bundle(anchor.hash, b"\x00" * 16) is None
