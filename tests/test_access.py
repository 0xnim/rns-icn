"""Tests for access control (Phase 3.3).

Covers CEK derivation, symmetric content encryption, per-consumer key wrapping,
the capability token (sign/verify/expiry/cover/serialize), and the producer-side
AccessController (per-prefix ACL enforcement, publish-time encryption, capability
issuance).
"""

import pytest
import RNS

from rns_icn.access import (
    AccessController,
    AccessError,
    AccessRule,
    Capability,
    decrypt_content,
    derive_cek,
    encrypt_content,
    load_capability,
    save_capability,
    unwrap_cek,
    wrap_cek,
)
from rns_icn.name import Name


@pytest.fixture
def producer() -> RNS.Identity:
    return RNS.Identity()


@pytest.fixture
def consumer() -> RNS.Identity:
    return RNS.Identity()


def _prefix(producer: RNS.Identity, *labels: bytes) -> Name:
    return Name(producer.hash, list(labels) or [b"private"])


# ── CEK derivation ──


def test_derive_cek_is_deterministic(producer):
    p = _prefix(producer)
    assert derive_cek(producer, p) == derive_cek(producer, p)


def test_derive_cek_differs_per_prefix(producer):
    assert derive_cek(producer, _prefix(producer, b"a")) != derive_cek(
        producer, _prefix(producer, b"b")
    )


def test_derive_cek_differs_per_producer(producer):
    other = RNS.Identity()
    # Same label path, different producer namespace → different key.
    assert derive_cek(producer, Name(producer.hash, [b"x"])) != derive_cek(
        other, Name(other.hash, [b"x"])
    )


def test_derive_cek_requires_private_key(producer):
    pub_only = RNS.Identity(create_keys=False)
    pub_only.load_public_key(producer.get_public_key())
    with pytest.raises(AccessError):
        derive_cek(pub_only, _prefix(producer))


# ── Symmetric encryption ──


def test_encrypt_decrypt_round_trip(producer):
    cek = derive_cek(producer, _prefix(producer))
    ct = encrypt_content(b"hello world", cek)
    assert ct != b"hello world"
    assert decrypt_content(ct, cek) == b"hello world"


def test_decrypt_wrong_key_fails(producer):
    cek = derive_cek(producer, _prefix(producer, b"a"))
    other = derive_cek(producer, _prefix(producer, b"b"))
    ct = encrypt_content(b"secret", cek)
    with pytest.raises(AccessError):
        decrypt_content(ct, other)


def test_encrypt_rejects_bad_key_length():
    with pytest.raises(AccessError):
        encrypt_content(b"x", b"tooshort")


# ── CEK wrapping (ECIES via RNS identity) ──


def test_wrap_unwrap_round_trip(producer, consumer):
    cek = derive_cek(producer, _prefix(producer))
    wrapped = wrap_cek(cek, consumer)
    assert unwrap_cek(wrapped, consumer) == cek


def test_unwrap_wrong_identity_fails(producer, consumer):
    cek = derive_cek(producer, _prefix(producer))
    attacker = RNS.Identity()
    wrapped = wrap_cek(cek, consumer)
    with pytest.raises(AccessError):
        unwrap_cek(wrapped, attacker)


# ── Capability token ──


def test_capability_sign_verify(producer, consumer):
    cap = Capability.create(_prefix(producer), consumer, producer, producer.sign)
    assert cap.signature is not None
    assert cap.verify_signature(producer.validate)


def test_capability_tampered_prefix_fails(producer, consumer):
    cap = Capability.create(_prefix(producer, b"a"), consumer, producer, producer.sign)
    cap.prefix = Name(producer.hash, [b"b"])
    assert not cap.verify_signature(producer.validate)


def test_capability_covers_only_its_prefix(producer, consumer):
    cap = Capability.create(_prefix(producer, b"private"), consumer, producer, producer.sign)
    assert cap.covers(Name(producer.hash, [b"private", b"doc"]))
    assert not cap.covers(Name(producer.hash, [b"public", b"doc"]))


def test_capability_expiry(producer, consumer):
    cap = Capability.create(
        _prefix(producer), consumer, producer, producer.sign,
        ttl_seconds=100, now=1000,
    )
    name = Name(producer.hash, [b"private", b"doc"])
    assert cap.covers(name, now=1050)
    assert not cap.covers(name, now=2000)  # expired
    assert cap.is_expired(now=2000)


def test_capability_no_expiry_when_ttl_zero(producer, consumer):
    cap = Capability.create(_prefix(producer), consumer, producer, producer.sign, ttl_seconds=0)
    assert not cap.is_expired(now=10**12)


def test_capability_serialization_round_trip(producer, consumer):
    cap = Capability.create(
        _prefix(producer, b"private"), consumer, producer, producer.sign,
        ttl_seconds=3600, now=5000,
    )
    parsed = Capability.from_bytes(cap.to_bytes())
    assert parsed.producer == cap.producer
    assert parsed.consumer == cap.consumer
    assert parsed.prefix == cap.prefix
    assert parsed.wrapped_cek == cap.wrapped_cek
    assert parsed.issued_at == cap.issued_at
    assert parsed.expires_at == cap.expires_at
    assert parsed.verify_signature(producer.validate)


def test_capability_unwrap_decrypts(producer, consumer):
    prefix = _prefix(producer, b"private")
    cap = Capability.create(prefix, consumer, producer, producer.sign)
    cek = derive_cek(producer, prefix)
    ct = encrypt_content(b"payload", cek)
    assert decrypt_content(ct, cap.unwrap(consumer)) == b"payload"


def test_capability_rejects_prefix_outside_producer(producer, consumer):
    with pytest.raises(AccessError):
        Capability(
            producer=producer.hash,
            consumer=consumer.hash,
            prefix=Name(RNS.Identity().hash, [b"x"]),  # different namespace
            wrapped_cek=wrap_cek(derive_cek(producer, _prefix(producer)), consumer),
        )


def test_save_and_load_capability(tmp_path, producer, consumer):
    cap = Capability.create(_prefix(producer), consumer, producer, producer.sign)
    path = str(tmp_path / "consumer.cap")
    save_capability(path, cap)
    loaded = load_capability(path)
    assert loaded.verify_signature(producer.validate)
    assert loaded.unwrap(consumer) == derive_cek(producer, _prefix(producer))


# ── AccessController (ACL + publish encryption + issuance) ──


def _controller(producer: RNS.Identity, consumer: RNS.Identity, *labels: bytes):
    prefix = _prefix(producer, *(labels or (b"private",)))
    rule = AccessRule(prefix=prefix, consumers={consumer.hash})
    return AccessController(producer, producer.hash, [rule]), prefix


def test_controller_encrypts_restricted_prefix(producer, consumer):
    ac, _ = _controller(producer, consumer)
    name = Name(producer.hash, [b"private", b"doc"])
    ct, enc = ac.encrypt_content(name, b"secret")
    assert enc and ct != b"secret"


def test_controller_leaves_public_content_plaintext(producer, consumer):
    ac, _ = _controller(producer, consumer)
    name = Name(producer.hash, [b"public", b"doc"])
    content, enc = ac.encrypt_content(name, b"hi")
    assert not enc and content == b"hi"


def test_controller_longest_prefix_wins(producer, consumer):
    inner = AccessRule(prefix=Name(producer.hash, [b"a", b"b"]), consumers={consumer.hash})
    outer = AccessRule(prefix=Name(producer.hash, [b"a"]), consumers={consumer.hash})
    ac = AccessController(producer, producer.hash, [outer, inner])
    matched = ac.matching_rule(Name(producer.hash, [b"a", b"b", b"c"]))
    assert matched is inner


def test_controller_issues_capability_for_authorized(producer, consumer):
    ac, prefix = _controller(producer, consumer)
    cap = ac.issue_capability(prefix, consumer, producer.sign, ttl_seconds=60)
    assert cap.verify_signature(producer.validate)
    assert cap.unwrap(consumer) == derive_cek(producer, prefix)


def test_controller_refuses_unauthorized_consumer(producer, consumer):
    ac, prefix = _controller(producer, consumer)
    attacker = RNS.Identity()
    with pytest.raises(AccessError, match="not authorized"):
        ac.issue_capability(prefix, attacker, producer.sign)


def test_controller_refuses_unknown_prefix(producer, consumer):
    ac, _ = _controller(producer, consumer)
    with pytest.raises(AccessError, match="no access rule"):
        ac.issue_capability(Name(producer.hash, [b"other"]), consumer, producer.sign)
