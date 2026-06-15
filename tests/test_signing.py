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


def test_content_store_unsigned_stays_unsigned(tmp_path):
    cs = ContentStore(str(tmp_path / "cs.db"), max_entries=10, default_ttl=86400)
    name = Name(rns_addr(), [b"doc"])
    cs.insert(name, Data.new(name=name, content=b"plain"))

    got = cs.get(name)
    assert got is not None
    assert got.signature is None


# ── Client policy ──


def _client(require_signature: bool):
    client = object.__new__(_ClientForPolicy)
    client.config = ClientConfig(require_signature=require_signature)
    return client


class _ClientForPolicy:
    # Borrow the real method under test without constructing a full ICNClient.
    from rns_icn.client import ICNClient as _C
    _check_signature = _C._check_signature


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
