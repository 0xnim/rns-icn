# Changelog

All notable changes to this project are documented here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/).

> **Wire stability:** the project has not been published and the on-wire protocol
> is **unstable** for the entire `0.x` series — the packet format, signed-byte
> layout, and protocol-version value may change between `0.x` commits without a
> SemVer-breaking release. The normative format lives in
> [PROTOCOL.md](PROTOCOL.md). The backward-compatibility guarantees described
> there take effect at `1.0`.

## [0.1.0] — unreleased

First end-to-end ICN-over-RNS stack. Nothing here has shipped yet, so the wire
is still being shaped in place.

### Protocol & security

- **Reliable single-hop fetch** with retry/backoff, timeouts, and a persistent
  SQLite content store (TTL + LRU + crash recovery).
- **Multi-hop forwarding**: FIB/PIT/CS forwarder, `icn-router` binary,
  nonce-based loop detection, Interest hop-limit.
- **Cache coherency**: `freshness_period`, stale-while-revalidate, and
  producer-signed cache invalidation.
- **Ed25519 producer signatures** over an authenticated sequence/timestamp
  envelope, with consumer rollback protection and per-chunk signatures for
  streamed content.
- **Per-prefix access control**: producer-derived content encryption keys +
  ECIES-wrapped capability tokens (fails closed).
- **Domain-separated signatures**: the Data envelope and Invalidate hash commit
  to a leading object-kind tag (`icn-data\x01` / `icn-invalidate\x01`), matching
  the rotation/revocation/capability constructions, so a signature over one
  object kind can never be replayed as another.
- **Per-packet protocol version (Phase 4.1).** Every packet is now framed
  uniformly as `[type:1][version:1]…` (previously only `PropPeer`/`CapPeer`
  carried a version byte). A receiver rejects a packet — including a cached or
  relayed one — whose version it does not speak with a distinct
  `UnsupportedVersionError`, instead of silently mis-parsing it. Current wire
  generation is `1`. `CapPeer`'s 4-byte feature bitmask remains the
  capability-advertisement channel, separate from the wire version.
- Early Phase 4: capability negotiation, pub/sub with an offline queue, and
  chunked transfer for large content.
- **Canonical wire-format test vectors.** `tests/vectors/wire_vectors.json` is a
  committed KAT fixture (fixed identity → exact bytes, signed hashes, and
  signatures) covering every wire-serializable type plus `derive_cek` and a
  reject-unknown-version case; `tests/test_vectors.py` holds the reference to it
  and re-implementers can self-verify against it. Generated/checked via
  `scripts/gen_test_vectors.py`. See `PROTOCOL.md` Appendix A.
- **Fix `DataMetadata` parse misalignment.** The staleness field (`age_seconds`)
  did not advance the read cursor, so a Data carrying a stale age *and* a later
  field (`freshness_period`/`signed_at`) mis-parsed everything after it — most
  damagingly the authenticated `signed_at` of a signed Data served while stale.
  Caught by the new test vectors.

### Tooling / operations

- TOML config, structured JSON logging, HTTP + RNS health/metrics endpoints.
- `MetricsCollector.malformed_packets` counter: packets that fail to parse are
  counted (and dropped), not silently swallowed.
- Content Store no longer caches unsolicited Data by default — only Data that
  satisfies a pending Interest (the standard NDN rule). Trusted propagation push
  opts in via `Forwarder.receive_data(..., cache_unsolicited=True)`.
- `PROTOCOL.md` normative specification, `LICENSE` (MIT), `SECURITY.md`.
- Expanded ruff rule set (bugbear, async, comprehensions, pytest, pyupgrade,
  ruff-specific, simplify); CI matrix across Python 3.10–3.13.

### Design decisions

- **Key rotation and revocation removed.** An earlier iteration implemented a
  signed anchor→key delegation chain with anchor-signed revocation, distributed
  over the mesh as a self-verifying bundle. It was removed because it
  reintroduced a delegation-chain trust layer that fought the self-certifying
  "name *is* the key" model: its only real benefit was planned key hygiene (it
  cannot recover from an anchor-key compromise, since the anchor is the root),
  and revocation was not separable from it. A producer key is now
  single-generation; recovery from loss/compromise means republishing under a new
  name out of band. (Also removes `ServerConfig.signing_identity_path`,
  `rotation_chains`, and `rotation_chain_path`.)
