#!/usr/bin/env python3
"""Generate (or --check) the canonical wire-format test vectors.

This script is the source of truth for the *input field values* of every
known-answer-test vector. It builds each object via the shared
``tests._vectors_common.build`` helper, serializes it, signs the ones that carry
a producer signature, and writes the result to ``tests/vectors/wire_vectors.json``.

Usage:
    python scripts/gen_test_vectors.py            # (re)write the fixture
    python scripts/gen_test_vectors.py --check      # exit 1 if the impl drifted

The committed JSON is the language-neutral artifact a re-implementer checks
against; ``tests/test_vectors.py`` verifies the live implementation still
reproduces it byte-for-byte.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

# Allow running as a plain script from the repo root.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from rns_icn.access import derive_cek  # noqa: E402
from rns_icn.packet import _write_varint  # noqa: E402
from tests._vectors_common import SEED, build, build_name, producer_identity  # noqa: E402

FIXTURE = _ROOT / "tests" / "vectors" / "wire_vectors.json"

IDENT = producer_identity()
PRODUCER = IDENT.hash  # 16-byte truncated identity hash == Name.rns_addr
PHEX = PRODUCER.hex()


def _blake(content: bytes) -> str:
    return hashlib.blake2b(content, digest_size=32).digest().hex()


def name_fields(*labels: bytes, content_hash: str | None = None) -> dict[str, Any]:
    return {
        "components": [PHEX] + [label.hex() for label in labels],
        "content_hash": content_hash,
    }


# Each spec: (vector-name, kind, fields, sign?). ``sign`` attaches a producer
# signature (and records signed_hash + signature) for objects that have one.
SPECS: list[dict[str, Any]] = [
    # ── varint (foundational; interop with rsticulum-icn) ──
    {"name": "varint/0", "kind": "varint", "fields": {"value": 0}},
    {"name": "varint/0xfc", "kind": "varint", "fields": {"value": 0xFC}},
    {"name": "varint/0xfd", "kind": "varint", "fields": {"value": 0xFD}},
    {"name": "varint/0xffff", "kind": "varint", "fields": {"value": 0xFFFF}},
    {"name": "varint/0x10000", "kind": "varint", "fields": {"value": 0x10000}},
    {"name": "varint/0xffffffff", "kind": "varint", "fields": {"value": 0xFFFFFFFF}},
    {"name": "varint/0x100000000", "kind": "varint", "fields": {"value": 0x100000000}},

    # ── Name ──
    {"name": "name/root", "kind": "name", "fields": name_fields()},
    {"name": "name/single-label", "kind": "name", "fields": name_fields(b"doc")},
    {"name": "name/multi-component", "kind": "name",
     "fields": name_fields(b"a", b"b", b"c")},
    {"name": "name/with-content-hash", "kind": "name",
     "fields": name_fields(b"doc", content_hash=_blake(b"payload"))},

    # ── Interest ──
    {"name": "interest/minimal", "kind": "interest",
     "fields": {"name": name_fields(b"doc"), "nonce": "0001020304050607",
                "lifetime_ms": 4000, "hop_limit": 16}},
    {"name": "interest/with-selector", "kind": "interest",
     "fields": {"name": name_fields(b"stream"), "nonce": "1112131415161718",
                "lifetime_ms": 8000, "selector_min_sequence": 6, "hop_limit": 16}},
    {"name": "interest/child-latest", "kind": "interest",
     "fields": {"name": name_fields(b"feed"), "nonce": "3132333435363738",
                "lifetime_ms": 4000, "can_be_prefix": True,
                "selector_child": 1, "hop_limit": 16}},
    {"name": "interest/prefix-and-fresh", "kind": "interest",
     "fields": {"name": name_fields(b"doc"), "nonce": "2122232425262728",
                "lifetime_ms": 4000, "can_be_prefix": True, "must_be_fresh": True,
                "hop_limit": 8}},

    # ── DataMetadata (standalone, flag combinations) ──
    {"name": "metadata/full", "kind": "metadata",
     "fields": {"content_hash": _blake(b"hello"), "sequence": 7, "fresh": False,
                "age_seconds": 42, "freshness_period": 3600, "signed_at": 1700000000,
                "encrypted": True}},

    # ── Data ──
    {"name": "data/unsigned-no-metadata", "kind": "data",
     "fields": {"name": name_fields(b"doc"), "content": b"hello world".hex(),
                "metadata": None}},
    {"name": "data/content-hash-only", "kind": "data",
     "fields": {"name": name_fields(b"doc"), "content": b"hello world".hex(),
                "metadata": {"content_hash": _blake(b"hello world")}}},
    {"name": "data/signed-full", "kind": "data", "sign": True,
     "fields": {"name": name_fields(b"doc"), "content": b"hello world".hex(),
                "metadata": {"content_hash": _blake(b"hello world"), "sequence": 3,
                             "signed_at": 1700000000}}},
    {"name": "data/signed-encrypted", "kind": "data", "sign": True,
     "fields": {"name": name_fields(b"secret"), "content": b"ciphertext-bytes".hex(),
                "metadata": {"content_hash": _blake(b"ciphertext-bytes"),
                             "sequence": 1, "signed_at": 1700000000,
                             "encrypted": True}}},

    # ── Invalidate ──
    {"name": "invalidate/unsigned", "kind": "invalidate",
     "fields": {"name": name_fields(b"doc"), "epoch": 1700000000}},
    {"name": "invalidate/prefix-signed", "kind": "invalidate", "sign": True,
     "fields": {"name": name_fields(b"feed"), "epoch": 1700000000, "is_prefix": True}},

    # ── Control packets ──
    {"name": "apsubscribe/basic", "kind": "apsubscribe",
     "fields": {"name": name_fields(b"stream"), "start_from_now": True}},
    {"name": "proppeer/wants-sync", "kind": "proppeer",
     "fields": {"version": 1, "rns_addr": PHEX, "wants_sync": True}},
    {"name": "cappeer/origin", "kind": "cappeer",
     "fields": {"version": 1, "role": 0, "features": 0x1F}},

    # ── Capability (fixed placeholder wrapped_cek -> byte-exact) ──
    {"name": "capability/signed", "kind": "capability", "sign": True,
     "fields": {"producer": PHEX, "consumer": ("aa" * 16),
                "prefix": name_fields(b"secret"),
                "wrapped_cek": ("bb" * 80), "issued_at": 1700000000,
                "expires_at": 1700003600}},

    # ── derive_cek (deterministic key derivation) ──
    {"name": "derive_cek/secret-prefix", "kind": "derive_cek",
     "fields": {"prefix": name_fields(b"secret")}},
]


def _signed_hash(kind: str, obj: Any) -> bytes:
    return obj.signed_hash()


def make_vector(spec: dict[str, Any]) -> dict[str, Any]:
    kind = spec["kind"]
    fields = spec["fields"]
    vec: dict[str, Any] = {"name": spec["name"], "kind": kind, "exact": True,
                           "fields": fields}

    if kind == "varint":
        value = fields["value"]
        wire = _write_varint(value)
        vec["wire_hex"] = wire.hex()
        return vec

    if kind == "derive_cek":
        prefix = build_name(fields["prefix"])
        vec["cek_hex"] = derive_cek(IDENT, prefix).hex()
        return vec

    obj = build(kind, fields)

    if spec.get("sign"):
        signed = _signed_hash(kind, obj)
        vec["signed_hash_hex"] = signed.hex()
        sig = IDENT.sign(signed)
        obj.signature = sig
        vec["signature_hex"] = sig.hex()

    vec["wire_hex"] = obj.to_bytes().hex()
    return vec


def build_fixture() -> dict[str, Any]:
    return {
        "_comment": "Canonical wire-format KAT vectors for rns-icn. "
                    "Regenerate with: python scripts/gen_test_vectors.py",
        "seed_hex": SEED.hex(),
        "producer_hash_hex": PHEX,
        "public_key_hex": IDENT.get_public_key().hex(),
        "vectors": [make_vector(s) for s in SPECS],
    }


def main() -> int:
    fixture = build_fixture()
    rendered = json.dumps(fixture, indent=2, sort_keys=False) + "\n"

    if "--check" in sys.argv:
        if not FIXTURE.exists():
            print(f"FAIL: {FIXTURE} does not exist; run without --check first.")
            return 1
        current = FIXTURE.read_text()
        if current != rendered:
            print("FAIL: committed vectors differ from the current implementation.")
            print("      Re-run `python scripts/gen_test_vectors.py` and review the diff.")
            return 1
        print(f"OK: {FIXTURE} is up to date ({len(fixture['vectors'])} vectors).")
        return 0

    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(rendered)
    print(f"Wrote {len(fixture['vectors'])} vectors to {FIXTURE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
