"""Conformance test: the live implementation must reproduce the committed KAT
vectors in ``tests/vectors/wire_vectors.json`` byte-for-byte.

This is the check that makes rns-icn a *reference* implementation: any other
implementation can verify itself against the same fixture, and a silent change
to our own wire format fails here (regenerate deliberately via
``python scripts/gen_test_vectors.py`` and review the diff).

The fixture holds only deterministic (byte-exact) vectors. Non-deterministic
crypto (ECIES wrap, AES content encryption) is covered by the round-trip tests
at the bottom of this file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from _vectors_common import build, build_name, producer_identity

from rns_icn.access import (
    Capability,
    decrypt_content,
    derive_cek,
    encrypt_content,
    unwrap_cek,
    wrap_cek,
)
from rns_icn.name import Name
from rns_icn.packet import (
    APSubscribe,
    CapPeer,
    Data,
    DataMetadata,
    Interest,
    Invalidate,
    PropPeer,
    UnsupportedVersionError,
    _read_varint,
    _write_varint,
    parse_packet,
)

FIXTURE = Path(__file__).parent / "vectors" / "wire_vectors.json"
IDENT = producer_identity()

with FIXTURE.open() as f:
    _FIX = json.load(f)

VECTORS: list[dict[str, Any]] = _FIX["vectors"]

# from_bytes dispatch for the wire-serializable kinds.
_PARSERS = {
    "name": Name.from_bytes,
    "interest": Interest.from_bytes,
    "metadata": DataMetadata.from_bytes,
    "data": Data.from_bytes,
    "invalidate": Invalidate.from_bytes,
    "apsubscribe": APSubscribe.from_bytes,
    "proppeer": PropPeer.from_bytes,
    "cappeer": CapPeer.from_bytes,
    "capability": Capability.from_bytes,
}


def _ids(v: dict[str, Any]) -> str:
    return v["name"]


def test_fixture_metadata_matches_identity():
    """The committed seed/producer/pubkey describe the live fixed identity."""
    assert _FIX["producer_hash_hex"] == IDENT.hash.hex()
    assert _FIX["public_key_hex"] == IDENT.get_public_key().hex()


@pytest.mark.parametrize("vec", VECTORS, ids=_ids)
def test_vector_conformance(vec: dict[str, Any]):
    kind = vec["kind"]
    fields = vec["fields"]

    if kind == "varint":
        value = fields["value"]
        assert _write_varint(value).hex() == vec["wire_hex"]
        decoded, consumed = _read_varint(bytes.fromhex(vec["wire_hex"]))
        assert decoded == value
        assert consumed == len(bytes.fromhex(vec["wire_hex"]))
        return

    if kind == "derive_cek":
        prefix = build_name(fields["prefix"])
        assert derive_cek(IDENT, prefix).hex() == vec["cek_hex"]
        return

    # Wire-serializable object: rebuild, attach signature if any, serialize.
    obj = build(kind, fields)

    if "signed_hash_hex" in vec:
        assert obj.signed_hash().hex() == vec["signed_hash_hex"]

    if "signature_hex" in vec:
        obj.signature = bytes.fromhex(vec["signature_hex"])
        assert obj.verify_signature(IDENT.validate)

    wire = bytes.fromhex(vec["wire_hex"])
    assert obj.to_bytes() == wire, f"{vec['name']}: to_bytes mismatch"

    # Parse the committed bytes back and confirm both directions agree.
    parsed = _PARSERS[kind](wire)
    assert parsed == obj, f"{vec['name']}: parsed object differs from source"
    assert parsed.to_bytes() == wire, f"{vec['name']}: re-serialization differs"


def test_parse_packet_dispatches_framed_vectors():
    """Every framed (type-byte-prefixed) vector dispatches via parse_packet."""
    framed = {"interest", "data", "apsubscribe", "proppeer", "cappeer", "invalidate"}
    seen = set()
    for vec in VECTORS:
        if vec["kind"] not in framed:
            continue
        pkt = parse_packet(bytes.fromhex(vec["wire_hex"]))
        assert pkt.type == bytes.fromhex(vec["wire_hex"])[0]
        seen.add(vec["kind"])
    assert seen == framed, f"missing framed kinds: {framed - seen}"


# ── Negative / robustness vectors ──


def test_unsupported_version_is_rejected():
    """A packet declaring an unknown protocol version must be rejected cleanly."""
    data_vec = next(v for v in VECTORS if v["name"] == "data/unsigned-no-metadata")
    wire = bytearray(bytes.fromhex(data_vec["wire_hex"]))
    wire[1] = 0x02  # bump the version byte past SUPPORTED_VERSIONS
    with pytest.raises(UnsupportedVersionError):
        Data.from_bytes(bytes(wire))
    with pytest.raises(UnsupportedVersionError):
        parse_packet(bytes(wire))


def test_tampered_signed_data_fails_verification():
    """Flipping one content byte must break the producer signature."""
    vec = next(v for v in VECTORS if v["name"] == "data/signed-full")
    parsed = Data.from_bytes(bytes.fromhex(vec["wire_hex"]))
    assert parsed.verify_signature(IDENT.validate)  # baseline
    tampered = Data(
        name=parsed.name,
        content=parsed.content + b"!",
        signature=parsed.signature,
        metadata=parsed.metadata,
    )
    assert not tampered.verify_signature(IDENT.validate)


# ── Round-trip vectors (non-deterministic crypto, not byte-exact) ──


def test_wrap_unwrap_cek_roundtrip():
    cek = bytes(range(32))
    wrapped = wrap_cek(cek, IDENT)
    assert unwrap_cek(wrapped, IDENT) == cek


def test_encrypt_decrypt_content_roundtrip():
    cek = bytes(range(32))
    plaintext = b"the quick brown fox" * 4
    ciphertext = encrypt_content(plaintext, cek)
    assert ciphertext != plaintext
    assert decrypt_content(ciphertext, cek) == plaintext


def test_capability_real_ecies_roundtrip():
    """A capability minted with a real ECIES-wrapped CEK round-trips and verifies."""
    prefix = Name(IDENT.hash, [b"secret"])
    cap = Capability.create(
        prefix=prefix,
        consumer=IDENT,            # self-grant for the round-trip
        producer_identity=IDENT,
        signer=IDENT.sign,
    )
    restored = Capability.from_bytes(cap.to_bytes())
    assert restored == cap
    assert restored.verify_signature(IDENT.validate)
    assert restored.unwrap(IDENT) == derive_cek(IDENT, prefix)
