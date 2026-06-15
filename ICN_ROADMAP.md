# ICN Protocol Roadmap — From Prototype to Production

## Vision

**ICN over RNS**: A production-grade, content-centric networking protocol enabling reliable, cached, multi-hop content retrieval over RNS mesh — with LXMF-level reliability, API stability, and operational maturity.

---

## Current State

Phases 1 and 2 are complete; parts of Phase 4 (capability negotiation, pub/sub, chunked transfer) landed early. Phase 3 (signing/security) is the main unstarted work.

| Component | Status | Gaps |
|-----------|--------|------|
| Client fetch | `ICNClient` with retry + timeout config | — |
| Link establishment | `LinkPool` w/ reuse, health, announce injection | reconnect is on-use, not proactive |
| Content store | SQLite + TTL + LRU + crash recovery | — |
| Forwarding | Multi-hop (FIB/PIT/CS); `icn-router` binary | no cache coherency, no multi-path |
| Naming | /hash/label, content-hash verified | **no signing / auth (Phase 3)** |
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
- [ ] Hop-count limit on Interests (defence-in-depth beyond nonce)

### 2.3 Router Mesh Formation
- [x] Router discovery via RNS announces (`rns_icn/peer_discovery.py`)
- [x] Route installation from configured peers (`icn-router` derives FIB prefix from peer identity)
- [ ] Dynamic FIB updates (prefix withdrawal/re-announce; routes re-installed on link reconnect)
- [ ] Multi-path support (ECMP or primary/backup)

### 2.4 Cache Coherency
- [ ] Cache validation (CheckFreshness Interest)
- [ ] Stale-while-revalidate
- [ ] Cache purge/invalidation protocol

**Deliverable:** ✅ `icn-router` binary. Client ↔ Router ↔ Server works over real RNS and content caches at the hop — proven end-to-end by `tests/test_integration.py::TestRNSMultiHop` (three processes, three Reticulum instances over localhost TCP). **Residual for full Phase 2:** §2.4 cache coherency, plus dynamic FIB updates / multi-path (§2.3) and a hop-count limit (§2.2).

---

## Phase 3: Naming, Security & Auth (Weeks 9-12) — "Trust & Identity"

### 3.1 Signed Manifests
- [ ] Producer keypair (Ed25519)
- [ ] Manifest signing (manifest + sequence + timestamp)
- [ ] Client validation (trusted producer keys)
- [ ] Key rotation support

### 3.2 Signed Data Packets
- [ ] Per-packet signature (for large content)
- [ ] Manifest references signed content hashes
- [ ] Selective verification (streaming large files)

### 3.3 Access Control
- [ ] Encrypted content (optional per-packet)
- [ ] Access tokens (capability-based)
- [ ] ACL per prefix (producer config)

### 3.4 Name Resolution
- [ ] Human-readable names → producer hash (Petname / DNS-like)
- [ ] Trust-on-first-use (TOFU) for producers
- [ ] Revocation / key expiry

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