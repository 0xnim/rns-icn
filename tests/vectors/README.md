# rns-icn wire-format test vectors

`wire_vectors.json` is the canonical known-answer-test (KAT) fixture for the
ICN-over-RNS wire format. It exists so that **any** implementation — a rewrite,
a port to another language, or this one after a refactor — can prove it produces
and parses the protocol byte-for-byte identically to the reference.

If your implementation reproduces every byte-exact vector here and rejects
every `negative` vector, it is wire-conformant with rns-icn at this protocol
version.
See `PROTOCOL.md` Appendix A for how the vectors map onto the spec sections.

## Fixed identity

All vectors are produced by one deterministic producer identity:

```python
import hashlib, RNS
seed  = hashlib.blake2b(b"rns-icn-test-vector-producer", digest_size=64).digest()
ident = RNS.Identity.from_bytes(seed)   # stable hash; deterministic Ed25519
```

The fixture commits, at top level:

| Field | Meaning |
|-------|---------|
| `seed_hex` | the 64-byte identity seed (X25519 ‖ Ed25519 private material) |
| `producer_hash_hex` | the 16-byte truncated identity hash, used as `Name.rns_addr` |
| `public_key_hex` | the 64-byte public key (X25519 32 ‖ Ed25519 32) |

Ed25519 signing is deterministic (RFC 8032), so signed vectors carry exact
`signed_hash_hex` and `signature_hex`. You can verify a committed signature
against the Ed25519 half of `public_key_hex` **without** RNS — that alone proves
your `signed_hash` construction and signature verification are correct.

## Vector schema

```jsonc
{
  "name": "data/signed-full",   // human label
  "kind": "data",               // see kinds below
  "exact": true,                // byte-exact (always true in this file)
  "fields": { ... },            // inputs; bytes as lowercase hex, ints as numbers
  "signed_hash_hex": "...",     // present for producer-signed objects
  "signature_hex": "...",       // present for producer-signed objects
  "wire_hex": "...",            // expected serialization (for wire kinds)
  "cek_hex": "...",             // expected key (derive_cek only)
  "reject": "..."               // rejection class (negative vectors only)
}
```

A nested name is `{"components": [hex, ...], "content_hash": hex | null}` where
`components[0]` is the 16-byte producer address.

### Kinds

- `varint` — `_write_varint` boundary values; check both encode and decode.
- `name` — `Name` serialization, including the optional content-hash suffix.
- `interest` — `Interest` (minimal, with selector, with prefix/fresh flags).
- `metadata` — `DataMetadata` flag combinations (standalone).
- `data` — `Data` (unsigned, content-hash only, signed, signed+encrypted).
- `invalidate` — `Invalidate` (unsigned, prefix, signed).
- `apsubscribe` / `proppeer` / `cappeer` — control packets.
- `nack` — `Nack`, one vector per reason code (the codes are normative).
- `capability` — `Capability` with a **fixed placeholder** `wrapped_cek` so the
  envelope is byte-exact (a real wrapped CEK is non-deterministic — see below).
- `derive_cek` — deterministic content-encryption-key derivation; check `cek_hex`.
- `negative` — byte streams that must **not** be accepted. `reject` names the
  check: `unsupported-version` and `unknown-packet-type` must raise a parse
  error; `bad-signature` parses cleanly but must fail producer-signature
  verification against `public_key_hex`. These have no `fields` — only the
  committed `wire_hex`.

## What is *not* byte-exact

ECIES CEK wrapping (`Identity.encrypt`) and AES content encryption (`Token`) use
fresh randomness, so they cannot be fixed-byte vectors. The reference test
(`tests/test_vectors.py`) covers them by **round-trip** instead: wrap→unwrap,
encrypt→decrypt, and a full `Capability` minted with a real wrapped CEK that
serializes, parses, verifies, and unwraps back to `derive_cek(...)`.

## Regenerating

The fixture is generated from the reference implementation:

```bash
python scripts/gen_test_vectors.py            # rewrite the fixture
python scripts/gen_test_vectors.py --check      # fail if the impl has drifted
```

Regeneration is a deliberate act: the resulting git diff is the record of a wire
change. During the `0.x` series the wire is unstable and vectors may change
freely; from `1.0`, a change to any byte-exact vector is a breaking change.
