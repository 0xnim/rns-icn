# Changelog

All notable changes to this project are documented here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/). The on-wire protocol
version is tracked separately in [PROTOCOL.md](PROTOCOL.md); breaking wire
changes are called out below.

## [0.2.0] — unreleased

### ⚠ Breaking — protocol v2 (signature wire format)

- **Domain-separated producer signatures.** The Data signature envelope and the
  Invalidate signed hash now commit to a leading domain-separation tag
  (`icn-data\x01` / `icn-invalidate\x01`), matching the rotation/revocation/
  capability constructions. This **changes the signed bytes**: signatures
  produced by v1 (0.1.x) do **not** verify under v2 and vice versa. Producers
  and consumers must run matching versions. Cached v1-signed Data will fail
  verification after upgrade and should be re-published. See
  [PROTOCOL.md](PROTOCOL.md) §10.

### Changed

- **Content Store no longer caches unsolicited Data by default.** A forwarder
  caches incoming Data only when it satisfies a pending Interest (the standard
  NDN rule), narrowing the surface for a peer injecting content into the cache.
  Trusted propagation push between peered servers opts in explicitly
  (`Forwarder.receive_data(..., cache_unsolicited=True)`). Consumer-side
  signature verification remains the primary poisoning defence.

### Added

- `PROTOCOL.md` — normative wire-format and security specification.
- `LICENSE` (MIT), `SECURITY.md`, this changelog.
- `MetricsCollector.malformed_packets` counter: packets that fail to parse are
  now counted (and dropped) instead of being silently swallowed.

### Fixed

- Replaced the redundant `except (ValueError, Exception)` packet-parse handlers
  (which swallowed all errors including bugs) with `except Exception` plus a
  malformed-packet metric.

### Tooling / hygiene

- Expanded the ruff rule set (bugbear, async, comprehensions, pytest, pyupgrade,
  ruff-specific, simplify).
- CI now tests the full supported Python range (3.10–3.13), matching
  `requires-python`.
- Removed the archived `scripts/dev/` throwaway scripts from the tree.

## [0.1.0]

- Phases 1–2: reliable single-hop fetch, multi-hop forwarding (FIB/PIT/CS),
  SQLite content store, cache coherency (freshness, stale-while-revalidate,
  signed invalidation).
- Phase 3.1/3.2: Ed25519 producer signatures with an authenticated
  sequence/timestamp envelope, per-chunk signatures, key rotation via signed
  delegation chains.
- Phase 3.3: per-prefix access control — encrypted content + capability tokens.
- Phase 3.4 (partial): key revocation + mesh distribution of rotation bundles.
- Parts of Phase 4 landed early: capability negotiation, pub/sub, chunked
  transfer.
