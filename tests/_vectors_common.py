"""Shared helpers for the wire-format test vectors (KAT fixtures).

Both the generator (``scripts/gen_test_vectors.py``) and the conformance test
(``tests/test_vectors.py``) import this module so there is a single way to:

* derive the fixed producer identity (``producer_identity``), and
* reconstruct a live protocol object from a vector's ``fields`` dict
  (``build``).

The generator turns ``fields`` into the committed ``wire_hex`` / ``signed_hash``
/ ``signature``; the test rebuilds from the *committed* ``fields`` and asserts
the live implementation reproduces those exact bytes. One builder, no drift
between the two sides.

Field encoding in the fixture JSON is deliberately language-neutral: ``bytes``
are lowercase hex strings, integers are JSON numbers, and a nested name is a
``{"components": [hex, ...], "content_hash": hex|null}`` object.
"""

from __future__ import annotations

import hashlib
from typing import Any

import RNS

from rns_icn.access import Capability
from rns_icn.name import Name
from rns_icn.packet import (
    APSubscribe,
    CapPeer,
    ChildSelector,
    Data,
    DataMetadata,
    Freshness,
    Interest,
    InterestSelector,
    Invalidate,
    PropPeer,
)

# The fixed producer identity. Derived deterministically from a domain string so
# the seed is reproducible and self-documenting; RNS.Identity.from_bytes(seed)
# yields a stable identity hash and (Ed25519 being deterministic, RFC 8032)
# reproducible signatures.
SEED = hashlib.blake2b(b"rns-icn-test-vector-producer", digest_size=64).digest()


def producer_identity() -> RNS.Identity:
    """Return the fixed test-vector producer identity (holds the private key)."""
    ident = RNS.Identity.from_bytes(SEED)
    if ident is None:  # pragma: no cover - sanity guard
        raise RuntimeError("could not load fixed test-vector identity")
    return ident


# ── field <-> bytes helpers ──


def _b(hexstr: str | None) -> bytes | None:
    return None if hexstr is None else bytes.fromhex(hexstr)


def build_name(fields: dict[str, Any]) -> Name:
    components = [bytes.fromhex(c) for c in fields["components"]]
    content_hash = _b(fields.get("content_hash"))
    return Name(components[0], components[1:], content_hash)


def _build_metadata(fields: dict[str, Any]) -> DataMetadata:
    fresh = fields.get("fresh", True)
    age = fields.get("age_seconds", 0)
    return DataMetadata(
        content_hash=_b(fields.get("content_hash")),
        sequence=fields.get("sequence"),
        freshness=Freshness(fresh=fresh, age_seconds=age),
        freshness_period=fields.get("freshness_period"),
        signed_at=fields.get("signed_at"),
        encrypted=fields.get("encrypted", False),
    )


def build(kind: str, fields: dict[str, Any]) -> Any:
    """Reconstruct a live protocol object from a vector's ``fields`` dict.

    Returns the *unsigned* object; callers attach ``signature`` separately (the
    signature is a computed output, not an input field).
    """
    if kind == "name":
        return build_name(fields)

    if kind == "interest":
        sel = fields.get("selector_min_sequence")
        child = ChildSelector(fields.get("selector_child", 0))
        selector = None
        if sel is not None or child is not ChildSelector.NONE:
            selector = InterestSelector(min_sequence=sel, child=child)
        return Interest(
            name=build_name(fields["name"]),
            nonce=bytes.fromhex(fields["nonce"]),
            lifetime_ms=fields["lifetime_ms"],
            can_be_prefix=fields.get("can_be_prefix", False),
            must_be_fresh=fields.get("must_be_fresh", False),
            selector=selector,
            hop_limit=fields["hop_limit"],
        )

    if kind == "metadata":
        return _build_metadata(fields)

    if kind == "data":
        meta = _build_metadata(fields["metadata"]) if fields.get("metadata") else DataMetadata()
        return Data(
            name=build_name(fields["name"]),
            content=bytes.fromhex(fields["content"]),
            metadata=meta,
        )

    if kind == "invalidate":
        return Invalidate(
            name=build_name(fields["name"]),
            epoch=fields["epoch"],
            is_prefix=fields.get("is_prefix", False),
        )

    if kind == "apsubscribe":
        return APSubscribe(
            name=build_name(fields["name"]),
            start_from_now=fields.get("start_from_now", False),
        )

    if kind == "proppeer":
        return PropPeer(
            version=fields["version"],
            rns_addr=bytes.fromhex(fields["rns_addr"]),
            wants_sync=fields.get("wants_sync", False),
        )

    if kind == "cappeer":
        return CapPeer(
            version=fields["version"],
            role=fields["role"],
            features=fields["features"],
        )

    if kind == "capability":
        return Capability(
            producer=bytes.fromhex(fields["producer"]),
            consumer=bytes.fromhex(fields["consumer"]),
            prefix=build_name(fields["prefix"]),
            wrapped_cek=bytes.fromhex(fields["wrapped_cek"]),
            issued_at=fields.get("issued_at", 0),
            expires_at=fields.get("expires_at", 0),
        )

    raise ValueError(f"unknown vector kind: {kind}")
