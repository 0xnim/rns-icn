# Changelog

All notable changes to this project are documented here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/).

> **Wire stability:** the on-wire protocol is **unstable** for the entire `0.x`
> series — the packet format, signed-byte layout, and protocol-version value may
> change between `0.x` releases without a SemVer-breaking bump. The normative
> format lives in [PROTOCOL.md](PROTOCOL.md). Backward-compatibility guarantees
> described there take effect at `1.0`.

## [Unreleased]

### Added

- **Follow** (`rns_icn.follow`): a router can keep a known peer's content in its
  cache ahead of demand (`follow = true` on the peer). New editions arrive by
  push; what was missed is pulled by manifest diff on (re)connect.
- **NomadNet page front end** (`rns_icn.pages`, `pages_enabled`): serves what a
  node holds as pages on a `nomadnetwork.node` destination, so existing
  Reticulum page browsers can read it.
- **Request binding** (PROTOCOL.md §3.1): Interests as RNS link requests, used
  by `ICNClient` when the peer advertises it. Fewer packets per fetch.
- **Holder discovery** (`rns_icn.holders`, PROTOCOL.md §3.2): a node lists the
  producers it follows in its announce. `ICNClient.fetch_nearest(name)` asks
  the nearest node heard holding that producer; a router with
  `route_via_holders = true` routes producers it has no configured route for
  the same way.
- **Publish directory** (`rns_icn.publish_dir`, `publish_dir`): files under a
  directory are published as this node's content and kept in step with it.
- **Space weather feed** (`feeds/spaceweather.py`): writes NOAA SWPC conditions,
  forecasts and alerts into a publish directory, with a systemd timer.
- **Benchmark** (`bench/run.py`): ICN through a hub vs a NomadNet-style page
  fetch over rate-limited links between real Reticulum instances, with bytes
  per link and an optional per-packet trace.

### Changed

- A server uses the request binding on links it opens (upstream fetches), not
  only when serving. On a slow link the Channel's retransmission timer fired
  while a large Data packet was still arriving and sent it again; a cold 2 KB
  fetch over two 1200 bit/s links timed out because of it.
- Benchmark links use a 500-byte MTU (`BENCH_MTU`) instead of whatever
  Reticulum negotiates over TCP.

- An origin builds its manifest on every request and advances the manifest
  sequence when the listed content changes; the manifest lists every blob under
  a label and no longer lists itself or reserved discovery objects.
- The accepting side of a link sends its CapPeer with the peer's first Channel
  packet instead of at link establishment (PROTOCOL.md §9.3).
- Announce `app_data` gains a transport-feature byte and is also set as the
  destination default, so path responses carry it.

### Fixed

- `LinkPool` tore down links that had not been requested from the pool for
  120 s even while they carried traffic, killing a router's upstream link
  mid-transfer and withdrawing its routes. Idleness now looks at the link's own
  activity, and servers keep links until Reticulum reports them dead.
- `ICNClient` could not receive Data larger than the Channel MDU (sent as an RNS
  Resource); it now listens for Resources like the server does.
- Manifest names: labels that are valid hex (`feed`, `cafe`, `2026`) were parsed
  as hex bytes, so names read from a manifest did not match the published ones.

## [0.1.0] — 2026-07-07

First end-to-end ICN-over-RNS stack. The 0.x wire is still being shaped.

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
  signatures) covering every wire-serializable type — including `Nack`, one
  vector per reason code — plus `derive_cek`, and committed negative vectors
  (unsupported version, unknown packet type, bad signature) that a conformant
  implementation must reject; `tests/test_vectors.py` holds the reference to it
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
- **Declare `pytest-asyncio` as a dev dependency** (with explicit
  `asyncio_mode = "strict"`). The async test suite relied on it being present in
  the local environment; a clean `pip install -e ".[dev]"` lacked it, so all
  `async def` tests errored with "async def functions are not natively
  supported" — invisible locally, fatal in CI.
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
