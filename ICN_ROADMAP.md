# ICN Protocol Roadmap — From Prototype to Production

## Vision

**ICN over RNS**: A production-grade, content-centric networking protocol enabling reliable, cached, multi-hop content retrieval over RNS mesh — with LXMF-level reliability, API stability, and operational maturity.

---

## Current State

Phases 1 and 2 are complete; parts of Phase 4 (capability negotiation, pub/sub, chunked transfer) landed early. Phase 3.1 (signed manifests, authenticated sequence/timestamp, key rotation) and 3.2 (per-packet/per-chunk producer signatures) are implemented, as is the key-management half of 3.4 (revocation + mesh distribution of rotation bundles); access control (3.3) and the rest of name resolution (3.4: petnames, TOFU) remain.

| Component | Status | Gaps |
|-----------|--------|------|
| Client fetch | `ICNClient` with retry + timeout config | — |
| Link establishment | `LinkPool` w/ reuse, health, announce injection | reconnect is on-use, not proactive |
| Content store | SQLite + TTL + LRU + crash recovery | — |
| Forwarding | Multi-hop (FIB/PIT/CS); `icn-router` binary; **cache coherency** (freshness period, stale-while-revalidate, signed invalidation) | no multi-path |
| Naming | /hash/label, content-hash verified, **Ed25519 producer signatures** (sequence + timestamp authenticated; client rollback protection; **key rotation + anchor-signed revocation** via signed delegation chains, distributed over the mesh as self-verifying bundles) | access control + petname/TOFU resolution (Phase 3.3/3.4) |
| API | Versioned via capability exchange | per-packet version not in Interest/Data |
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
- [ ] Dynamic FIB updates (prefix withdrawal/re-announce; routes re-installed on link reconnect)
- [ ] Multi-path support (ECMP or primary/backup)

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
  `invalidate`). Mesh-wide flood + revocation hardening deferred to Phase 3.4.

**Deliverable:** ✅ `icn-router` binary. Client ↔ Router ↔ Server works over real RNS and content caches at the hop — proven end-to-end by `tests/test_integration.py::TestRNSMultiHop` (three processes, three Reticulum instances over localhost TCP). Cache coherency (§2.4) has landed. **Residual for full Phase 2:** dynamic FIB updates / multi-path (§2.3).

---

## Phase 3: Naming, Security & Auth (Weeks 9-12) — "Trust & Identity"

### 3.1 Signed Manifests
- [x] Producer keypair (Ed25519) — the producer's RNS identity (`name.rns_addr` is its address)
- [x] Manifest signing (`ICNServer._maybe_sign` signs origin-owned Data incl. manifests; signs over `name + content + content_hash`)
- [x] Client validation (`ICNClient._check_signature` recalls producer via `RNS.Identity.recall`, `verify-if-present` + `require_signature` strict mode)
- [x] Key rotation support (`rns_icn/rotation.py`): a producer issues a signed
  chain of `KeyRotation` certificates (anchor key → new key → …), each binding
  the namespace, a monotonic epoch, and the prev/new public keys. Verification
  is self-certifying and offline — an RNS identity hash *is* the truncated hash
  of its public key, so the chain anchors by checking the root key hashes to
  `name.rns_addr` (no recall of the retired key needed). A valid chain widens
  the set of keys authorized to sign for the namespace; `ICNClient` loads chains
  from `config.rotation_chains` and `_check_signature` accepts any authorized
  key, and an origin can sign with a delegated key via
  `ServerConfig.signing_identity_path` while keeping its anchor namespace.
  Delivered as key *continuity* (old generations still verify, caches survive a
  rotation); chain distribution over the mesh and *revocation* landed in §3.4.
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
- [ ] Encrypted content (optional per-packet)
- [ ] Access tokens (capability-based)
- [ ] ACL per prefix (producer config)

### 3.4 Name Resolution
- [ ] Human-readable names → producer hash (Petname / DNS-like)
- [ ] Trust-on-first-use (TOFU) for producers
- [x] Revocation / key expiry: a `rotation.Revocation` (signed by the namespace
  anchor — the root of trust) removes a compromised key *and every key it
  transitively delegated* (cascade) from the authorized set, the shrinking
  counterpart to rotation's widening. Plus mesh distribution: chain + revocations
  travel together as a `RotationBundle` served as self-verifying Data at the
  reserved name `/<producer>/_rotation`; an origin publishes it
  (`ServerConfig.rotation_chain_path` → `ICNServer.publish_rotation_bundle`) and
  a client fetches + validates + installs it offline
  (`ICNClient.fetch_rotation_bundle`, anchoring the bundle to the requested
  producer so a relay can't graft a foreign chain). The bundle wire format is a
  backward-compatible superset of the bare chain.

**Deliverable:** Signed manifests + data, producer auth, encrypted content option, name resolution.

---

## Phase 4: Protocol Maturity (Weeks 13-16) — "LXMF Parity"

### 4.1 Protocol Versioning
- [ ] Protocol version in Interest/Data headers (Interest/Data carry a type byte but no version field; only `Subscribe`/`CapPeer` are versioned)
- [x] Capability negotiation (client ↔ router ↔ server) (`CapPeer` exchange on each link; `version` + 4-byte feature bitmask)
- [ ] Backward compatibility policy

### 4.2 Advanced Features
- [x] **Pub/Sub**: `Subscribe(prefix)` → proactive Data push (`rns_icn/aps.py`, `OfflineQueue` for disconnected subscribers)
- [ ] **Selectors** (partial): `min_sequence` (`>=version`) implemented (`InterestSelector`); `latest`/`oldest` not yet
- [x] **Chunked transfer**: Large files via segmented Data + reassembly (`chunker.py`, `assembler.py`, `resource_transport.py`)
- [ ] **Priority/QoS**: Interest priority field, router queueing

### 4.3 Developer Experience
- [ ] SDKs: Python, Rust, Go, TypeScript (Python only)
- [ ] CLI (partial): `icn-fetch`, `icn-publish` shipped; no `icn subscribe`
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
| **M3: Signed Content** | Week 12 | Tampered content rejected, key rotation works |
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