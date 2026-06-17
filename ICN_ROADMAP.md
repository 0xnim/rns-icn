# ICN Protocol Roadmap — From Prototype to Production

## Vision

**ICN over RNS**: A production-grade, content-centric networking protocol enabling reliable, cached, multi-hop content retrieval over RNS mesh — with LXMF-level reliability, API stability, and operational maturity.

---

## Current State

Phases 1 and 2 are complete; Phase 4.1 (protocol versioning) is done and other parts of Phase 4 (pub/sub, chunked transfer) landed early. Phase 3.1 (signed manifests, authenticated sequence/timestamp), 3.2 (per-packet/per-chunk producer signatures), and 3.3 (access control: per-prefix ACLs, encrypted content, capability tokens) are implemented. Phase 3.4 is **skipped by design** in full: petnames/TOFU don't fit an offline-first mesh (a local petname map belongs to the application layer and needs no protocol change), and key rotation/revocation was **removed** — it added a delegation-chain layer that fought the self-certifying "name *is* the key" model to address planned key hygiene (not anchor-compromise recovery, which it can't provide); a producer key is single-generation and recovery means republishing under a new name out of band (see §3.4). Phase 3 is otherwise complete.

| Component | Status | Gaps |
|-----------|--------|------|
| Client fetch | `ICNClient` with retry + timeout config | — |
| Link establishment | `LinkPool` w/ reuse, health, announce injection; routes re-installed on peer re-announce after a drop | — |
| Content store | SQLite + TTL + LRU + crash recovery | — |
| Forwarding | Multi-hop (FIB/PIT/CS); `icn-router` binary; **cache coherency** (freshness period, stale-while-revalidate, signed invalidation); **multi-path primary/backup failover**; **dynamic FIB** (withdrawal on link close, re-install on re-announce) | — |
| Naming | /hash/label, content-hash verified, **Ed25519 producer signatures** (sequence + timestamp authenticated; client rollback protection); **per-prefix access control** (encrypted content + capability tokens) | petname/TOFU resolution and key rotation/revocation removed by design |
| API | Per-packet wire version (`[type][version]`) + capability exchange; unknown generation rejected cleanly | — |
| Operations | TOML config, JSON logs, health + metrics | — |

---

## Phase 1: Foundation (Weeks 1-4) — "Reliable Single-Hop"

### 1.1 ICN Transport Abstraction
- [x] `ICNClient` class with config-driven setup (`rns_icn/client.py`)
- [x] `ICNServer` class with lifecycle management (`rns_icn/rns_server.py`, `start()`/`shutdown()`)
- [x] Connection pooling / link reuse (`rns_icn/link_pool.py`)
- [x] Automatic announce table injection (configurable peer identities) (`_inject_known_peers`)
- [x] Graceful shutdown / cleanup

### 1.2 Reliability Layer
- [x] Interest retransmission (exponential backoff: `min(base * 2**attempt, max)`, `client.py`)
- [x] Interest timeout (configurable, default 30s) (`ClientConfig.fetch_timeout`)
- [x] Duplicate Interest suppression (by name + nonce) (`Pit.check_loop` / `record_nonce`)
- [x] Data validation (content hash verification) (`Data.verify_content_hash`)
- [x] Link health monitoring + reconnect-on-use (`LinkPool._monitor_links`, `get_link`)

### 1.3 Persistent Content Store
- [x] SQLite backend (files + metadata) (`rns_icn/content_store.py`)
- [x] TTL support (configurable per prefix) (`cs_prefix_ttls`, `_compute_ttl`)
- [x] LRU eviction (max size config) (`max_entries`)
- [x] Index by name (primary key + `name_prefixes` table; content hash stored per entry)
- [x] Atomic write + crash recovery (WAL + `PRAGMA integrity_check` + salvage recovery)

### 1.4 Configuration & Operations
- [x] TOML config file (client + server) (`rns_icn/config.py`)
- [x] Structured logging (JSON for aggregation) (`rns_icn/icn_logging.py`, `log_json`)
- [x] Health endpoint (HTTP + RNS) (`rns_icn/health.py`: `setup_http_api` + `is_health_interest`)
- [x] Metrics: fetch latency, hit/miss, link uptime (`rns_icn/metrics.py`)

**Deliverable:** ✅ `icn-client`, `icn-server` binaries with `icn.toml` config. Single-hop fetch works reliably with retries, persistence, observability — proven over real RNS by `tests/test_integration.py::TestRNSIntegration`.

---

## Phase 2: Multi-Hop Forwarding (Weeks 5-8) — "Router Mesh"

### 2.1 ICN Router
- [x] Router role: `Forwarder` forwards Interests and caches Data; driven by `icn-router` binary (`ServerRole.CACHE`)
- [x] FIB (Forwarding Information Base): prefix → next-hop(s) (`rns_icn/fib.py`)
- [x] PIT (Pending Interest Table): tracks in-flight Interests (`rns_icn/pit.py`)
- [x] CS (Content Store): local cache with TTL (`rns_icn/content_store.py`, SQLite)

### 2.2 Forwarding Logic
- [x] Longest-prefix match for Interest forwarding (`Fib.lookup`)
- [x] PIT aggregation (multiple Interests → single upstream)
- [x] Data return path via PIT (reverse path forwarding)
- [x] Loop detection (nonce-based; `Pit.check_loop`)
- [x] Hop-count limit on Interests (defence-in-depth beyond nonce; `Interest.hop_limit`, decremented per hop in `Forwarder._forward`, default `DEFAULT_HOP_LIMIT=16`)

### 2.3 Router Mesh Formation
- [x] Router discovery via RNS announces (`rns_icn/peer_discovery.py`)
- [x] Route installation from configured peers (`icn-router` derives FIB prefix from peer identity)
- [x] Dynamic FIB updates: a face's routes are **withdrawn** when its link
  closes (`Forwarder.withdraw_face` / `Fib.remove_all_for_face`, driven by the
  `LinkFace` close hook → `ICNServer._cleanup_closed_face`), so a dead next-hop
  stops black-holing Interests. Routes are **re-installed** when the peer
  re-announces while disconnected: `PeerDiscoveryManager` surfaces that as a
  reconnect signal and `icn-router` re-establishes the link + reinstalls the
  route (`_wire_route_reinstall`). Rides RNS keepalive (close) and announce
  cadence (recovery) rather than polling.
- [x] Multi-path support (**primary/backup failover**): the FIB holds multiple
  cost-ordered faces per prefix; on a forward timeout the `Forwarder` falls
  through to the next usable face (`BestRoute.usable_faces`), hop-limit
  decremented once across the attempt. FIB cost MAY be derived from the RNS
  transport hop count. (ECMP/parallel left as a non-goal.)

### 2.4 Cache Coherency
- [x] Cache validation: Data carries a `freshness_period` (`DataMetadata`); the
  ContentStore computes freshness from age on read, so cached entries age to
  stale and `must_be_fresh` Interests revalidate upstream instead of serving
  indefinitely (`content_store.py`, `strategy.py`)
- [x] Stale-while-revalidate: a stale-but-servable cache hit is returned
  immediately while a deduped background Interest refreshes it upstream
  (`StrategyDecision.SERVE_STALE_REVALIDATE`, `Forwarder._schedule_revalidation`;
  window configured by `cs_stale_while_revalidate`)
- [x] Cache purge/invalidation protocol: producer-signed `Invalidate` packet
  (`PacketType.INVALIDATE`), self-certifying via the producer's RNS identity,
  applied to the local store and forwarded one hop with epoch-based replay
  suppression (`ContentStore.invalidate`, `ICNServer.handle_invalidate`/
  `invalidate`). Mesh-wide flood hardening deferred.

**Deliverable:** ✅ `icn-router` binary. Client ↔ Router ↔ Server works over real RNS and content caches at the hop — proven end-to-end by `tests/test_integration.py::TestRNSMultiHop` (three processes, three Reticulum instances over localhost TCP). Cache coherency (§2.4), dynamic FIB updates, and multi-path failover (§2.3) have all landed. **Phase 2 complete.**

---

## Phase 3: Naming, Security & Auth (Weeks 9-12) — "Trust & Identity"

### 3.1 Signed Manifests
- [x] Producer keypair (Ed25519) — the producer's RNS identity (`name.rns_addr` is its address)
- [x] Manifest signing (`ICNServer._maybe_sign` signs origin-owned Data incl. manifests; signs over `name + content + content_hash`)
- [x] Client validation (`ICNClient._check_signature` recalls producer via `RNS.Identity.recall`, `verify-if-present` + `require_signature` strict mode)
- [ ] ~~Key rotation~~ — **removed by design** (see §3.4). A producer signs with
  its own self-certifying identity; the name *is* the key.
- [x] Sequence/timestamp inside the signed envelope: `signed_hash` now binds
  `name + content + content_hash + sequence + signed_at` (the latter two
  domain-tagged and appended, so pre-3.1 signatures still verify). `Data.sign`
  auto-stamps `metadata.signed_at`; the envelope round-trips on the wire and
  through the ContentStore. Consumed by `Data.freshness_key()` +
  `ICNClient._check_rollback` (config `reject_rollback`), which rejects a cache/
  relay replaying a stale-but-validly-signed version (rollback) by tracking the
  highest authenticated `(signed_at, sequence)` accepted per name.

### 3.2 Signed Data Packets
- [x] Per-packet signature (`Data.sign`/`verify_signature`, 64-byte Ed25519; persisted in CS so caches re-serve verifiable Data)
- [x] Manifest references signed content hashes (entries carry `content_hash`; Data binds name↔content↔hash)
- [x] Per-chunk signatures for `resource_transport` (selective verification of streamed large files): `chunk_content(..., signer=)` signs each chunk Data with the producer key; `assemble`/`assemble_verified`/`verify_chunk(s)` take an optional `validator` and raise `SignatureError` on missing/forged chunk signatures — defends streamed files against chunk substitution by a relay/cache

### 3.3 Access Control
Implemented as NDN-NAC-style name-based access control (`rns_icn/access.py`),
the only model that holds when content lives in caches the producer doesn't
control: the boundary is encryption, not "don't serve it."
- [x] ACL per prefix (producer config): `ServerConfig.access_rules` lists, per
  name prefix, the consumer identities allowed to read it; `AccessController`
  does longest-prefix matching and is the authorization boundary for issuance.
- [x] Encrypted content (optional per-packet): content under a restricted prefix
  is encrypted at publish with a CEK *derived from the producer's private key +
  prefix* (`derive_cek` — stable, never stored, so cached ciphertext stays
  decryptable). The hash and producer signature cover the ciphertext, so caching
  / dedup / verification are untouched; an authenticated `encrypted` flag is
  bound into the signed envelope (Phase 3.1 style, appended) so a relay can't
  flip it. Persisted through the ContentStore.
- [x] Access tokens (capability-based): a producer-signed `Capability` binds
  (consumer, prefix, validity) and carries the CEK *wrapped to the consumer's
  RNS identity* (ECIES). The consumer verifies the producer signature
  (self-certifying — the key is recalled from the name, via
  `ICNClient._producer_validators`), unwraps the CEK, and
  `ICNClient._maybe_decrypt` returns plaintext. AEAD +
  ECIES make decryption fail closed even if a forged capability's signature
  can't be checked offline. Capabilities load from `ClientConfig.capabilities`;
  `ICNServer.issue_capability` mints them. (Mesh distribution of capabilities is
  possible — the wrapped CEK is opaque to non-recipients — but left to the
  operator for now.)

### 3.4 Name Resolution
- [ ] ~~Human-readable names → producer hash (Petname / DNS-like)~~ — **skipped
  by design.** Self-certifying names are secure + decentralized; adding a global
  human-meaningful layer (Zooko's triangle) needs either a central authority
  (DNS) or consensus (blockchain), both at odds with an offline-first mesh, and
  Reticulum offers no naming standard to build on. A purely local petname map is
  the only fit and is deliberately left to the application/UI layer — it stays
  off the wire (Interests/Data always carry the hash), so it can be added later
  without any protocol change.
- [ ] ~~Trust-on-first-use (TOFU) for producers~~ — **skipped by design**
  alongside petnames (the two form one resolution story); revisit only if a
  concrete UX needs it.
- [ ] ~~Key rotation + revocation~~ — **removed by design.** Rotation was once
  implemented (a signed anchor→key delegation chain, with anchor-signed
  revocation cascading down it, distributed over the mesh as a self-verifying
  bundle) but removed: it reintroduced a delegation-chain trust layer that fought
  the self-certifying "name *is* the key" model. Its only real benefit was
  *planned key hygiene* (cold anchor / rotated hot key) — it cannot recover from
  an anchor-key compromise, because the anchor is the root. Revocation was not
  separable: in this model the only thing to revoke is the anchor, and revoking
  it destroys rather than recovers the namespace. So both were dropped. A
  producer key is single-generation; if it is lost or compromised, recovery means
  publishing under a new name (a new key) and re-establishing trust out of band.

**Deliverable:** Signed manifests + data, producer auth, per-prefix access control (encrypted content + capability tokens). (Human-readable name resolution and key rotation/revocation removed by design — see §3.4.) **Phase 3 complete.**

---

## Phase 4: Protocol Maturity (Weeks 13-16) — "LXMF Parity"

### 4.1 Protocol Versioning ✅
- [x] Protocol version in Interest/Data headers (every packet is framed uniformly as `[type:1][version:1]…`; an unknown generation is rejected with `UnsupportedVersionError` rather than silently mis-parsed — applies to cached/relayed packets too; `packet.PROTOCOL_VERSION`)
- [x] Capability negotiation (client ↔ router ↔ server) (`CapPeer` exchange on each link; wire `version` byte + 4-byte feature bitmask for optional behaviour)
- [x] Backward compatibility policy (`PROTOCOL.md` §19: append-only flags/tags/feature-bits for compatible growth, version bump for breaking parse/signed-bytes changes; effective from 1.0 — the 0.x wire is unstable)

### 4.2 Advanced Features
- [x] **Pub/Sub**: `Subscribe(prefix)` → proactive Data push (`rns_icn/aps.py`, `OfflineQueue` for disconnected subscribers)
- [x] **Selectors**: `min_sequence` (`>=version`), plus `latest`/`oldest`
  child selectors — a `can_be_prefix` Interest can ask for the highest- or
  lowest-`sequence` Data under a prefix (`InterestSelector.child` /
  `ChildSelector`; `packet.py`). The answering node (cache or producer) ranks
  matches by sequence (`ContentStore.get_prefix`), so selection is best-effort
  per node; pair `latest` with `must_be_fresh` to revalidate past a cache.
  Wired through `Forwarder.express`/`fetch_latest`/`fetch_oldest` and the
  origin serve path. Self-describing on the wire (`PROTOCOL.md` §7).
- [x] **Verifiable latest-version discovery (RDR-style)**: because a cache's
  `latest` ranking is *unverifiable* (it can withhold a newer version —
  exactly why NDN deprecated selector-based discovery), a producer publishes a
  signed **latest pointer** at a reserved name under each collection prefix
  (`rns_icn/discovery.py`, `META_LABEL`). It names the current latest,
  content-hash pinned. `ICNClient.fetch_latest` fetches the pointer with
  `must_be_fresh` (so a stale cache revalidates it to the origin), verifies the
  producer signature, then fetches the exact pinned target — making "latest" a
  producer assertion and engaging rollback protection on the pointer. Falls
  back to the best-effort `latest` selector only when no pointer exists. No
  protocol-version bump (a pointer is an ordinary signed Data at a reserved
  name; labels beginning `0x00` are reserved). Producer side:
  `RNSICNServer.publish_content` (`meta_freshness_period` config).
- **Non-goal — `Exclude`/enumeration selectors** (NDN's `Exclude`,
  `MinSuffixComponents`): deliberately not implemented. They are the
  selector-discovery footgun NDN walked back — cache-poisoning surface and
  namespace enumeration with no verifiable answer. Authenticated discovery is
  served by the signed latest pointer (above) and the signed manifest/catalog
  (§4.2 manifests), not by richer selectors.
- [x] **Chunked transfer**: Large files via segmented Data + reassembly (`chunker.py`, `assembler.py`, `resource_transport.py`)
- [ ] **Priority/QoS**: Interest priority field, router queueing

### 4.3 Developer Experience
- [ ] SDKs: Python, Rust, Go, TypeScript (Python only)
- [x] CLI: `icn-fetch`, `icn-publish`, `icn-subscribe` shipped. `icn-subscribe`
  upgrades the link to push mode via an APS Subscribe handshake and prints/saves
  each pushed Data until interrupted or `--count` reached (`cli_subscribe.py`;
  consumer-side push surfaced through `Forwarder.set_data_callback`)
- [ ] HTTP gateway (optional): `GET /icn/<name>` (only health/metrics HTTP API exists)
- [ ] Documentation: protocol spec, API ref, tutorials

### 4.4 Testing & Compliance
- [ ] Integration test suite (partial): real-RNS end-to-end tests (2-node + 3-node multi-hop over localhost TCP), not yet a multi-node testnet sim
- [ ] Chaos testing (link loss, router crash, partition)
- [ ] Interop test vectors
- [ ] Load testing (10K+ concurrent fetches)

**Deliverable:** Production-ready ICN protocol v1.0 with SDKs, docs, compliance.

---

## Phase 5: Ecosystem (Weeks 17-24) — "Platform"

| Feature | Description |
|---------|-------------|
| **ICN Gateway** | HTTP/REST ↔ ICN bridge for web apps |
| **ICN Sync** | Bi-directional folder sync over ICN |
| **ICN Package** | Software distribution (like apt/npm over ICN) |
| **ICN Search** | Distributed content index (optional) |
| **Monitoring** | Grafana dashboards, alerting rules |
| **Deployment** | Docker, systemd, k8s operators |

---

## Technical Architecture (Target)

```
┌─────────────────────────────────────────────────────────────────┐
│                        ICN APPLICATIONS                            │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                │
│  │  fetch  │ │ publish │ │ subscribe│ │  sync   │  ...           │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘                │
└───────┼────────────┼────────────┼────────────┼──────────────────┘
        │            │            │            │
        ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ICN SDK (Python/Rust/Go/TS)                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  ICNClient: fetch(name), subscribe(prefix), publish(name)  │  │
│  │  ICNRouter: start(config), stop(), metrics()               │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ICN PROTOCOL ENGINE                           │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌────────────────┐   │
│  │ Forwarder │ │   PIT     │ │    FIB    │ │  Content Store │   │
│  │ (Interest │ │ (pending  │ │ (prefix→  │ │ (SQLite + TTL  │   │
│  │  /Data)   │ │  Interests)│ │  next hop)│ │  + LRU evict)  │   │
│  └───────────┘ └───────────┘ └───────────┘ └────────────────┘   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                        RNS TRANSPORT                               │
│  RNS Link (AES-256-CBC) → RNS Mesh (TCPClientInterface...)       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Milestones & Success Criteria

| Milestone | Target | Success Criteria |
|-----------|--------|------------------|
| **M1: Reliable Client** | Week 4 | 1000 fetches, 0 failures, <100ms p99 latency |
| **M2: Router Mesh** | Week 8 | 5-hop fetch, cache hit >80%, router failover <5s |
| **M3: Signed Content** | Week 12 | Tampered content rejected, producer signatures verify |
| **M4: Protocol v1.0** | Week 16 | SDKs pass interop suite, docs complete |
| **M5: Ecosystem** | Week 24 | 3+ apps using ICN, production deployments |

---

## Resource Estimate

| Role | Phase 1-2 | Phase 3-4 | Phase 5 |
|------|-----------|-----------|---------|
| Core protocol (Rust/Python) | 1.5 FTE | 1 FTE | 0.5 FTE |
| Router/forwarding | 1 FTE | 0.5 FTE | - |
| Security/crypto | - | 1 FTE | 0.5 FTE |
| SDKs (multi-lang) | - | 1 FTE | 1 FTE |
| Testing/ops | 0.5 FTE | 0.5 FTE | 1 FTE |
| **Total** | **3 FTE** | **3 FTE** | **2 FTE** |

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| RNS mesh instability | High | High | Test on multiple mesh networks; fallback paths |
| PIT state explosion | Medium | High | PIT aging, max entries, priority eviction |
| Cache poisoning | Medium | High | Signed data, content hash verification |
| Protocol ossification | Low | High | Version negotiation, extensible TLV |
| Adoption chicken/egg | High | Medium | HTTP gateway, SDKs, killer app (sync?) |

---

## Immediate Next Steps (This Week)

1. **Extract `icn_client.py` → `icn/client.py`** with `ICNClient` class
2. **Add config** (`icn.toml`: mesh interfaces, timeouts, retries)
3. **Add SQLite content store** (replace in-memory dict)
4. **AddInterest retry logic** (exponential backoff)
5. **Run 100-iteration stress test** on current mesh

Then proceed to Phase 1 completion.