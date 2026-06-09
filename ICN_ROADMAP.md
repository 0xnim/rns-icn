# ICN Protocol Roadmap — From Prototype to Production

## Vision

**ICN over RNS**: A production-grade, content-centric networking protocol enabling reliable, cached, multi-hop content retrieval over RNS mesh — with LXMF-level reliability, API stability, and operational maturity.

---

## Current State (v0.1 - Prototype)

| Component | Status | Gaps |
|-----------|--------|------|
| Client fetch | Works (CLI) | No retry, no timeout config |
| Link establishment | Manual inject | Fragile, no reconnect |
| Content store | In-memory | No persistence, no TTL |
| Forwarding | None | Single-hop only |
| Naming | /hash/label | No signing, no auth |
| API | Ad-hoc Python | No versioning, nodocs |
| Operations | Manual | No config, no metrics |

---

## Phase 1: Foundation (Weeks 1-4) — "Reliable Single-Hop"

### 1.1 ICN Transport Abstraction
- [ ] `ICNClient` class with config-driven setup
- [ ] `ICNServer` class with lifecycle management
- [ ] Connection pooling / link reuse
- [ ] Automatic announce table injection (configurable peer identities)
- [ ] Graceful shutdown / cleanup

### 1.2 Reliability Layer
- [ ] Interest retransmission (exponential backoff: 1s, 2s, 4s, 8s, max 30s)
- [ ] Interest timeout (configurable, default 30s)
- [ ] Duplicate Interest suppression (by name + nonce)
- [ ] Data validation (content hash verification)
- [ ] Link health monitoring + auto-reconnect

### 1.3 Persistent Content Store
- [ ] SQLite backend (files + metadata)
- [ ] TTL support (configurable per prefix)
- [ ] LRU eviction (max size config)
- [ ] Index by name + content hash
- [ ] Atomic write + crash recovery

### 1.4 Configuration & Operations
- [ ] TOML config file (client + server)
- [ ] Structured logging (JSON for aggregation)
- [ ] Health endpoint (HTTP + RNS)
- [ ] Metrics: fetch latency, hit/miss, link uptime

**Deliverable:** `icn-client`, `icn-server` binaries with `icn.toml` config. Single-hop fetch works reliably with retries, persistence, observability.

---

## Phase 2: Multi-Hop Forwarding (Weeks 5-8) — "Router Mesh"

### 2.1 ICN Router
- [ ] `ICNRouter` class: forwards Interests, caches Data
- [ ] FIB (Forwarding Information Base): prefix → next-hop(s)
- [ ] PIT (Pending Interest Table): tracks in-flight Interests
- [ ] CS (Content Store): local cache with TTL

### 2.2 Forwarding Logic
- [ ] Longest-prefix match for Interest forwarding
- [ ] PIT aggregation (multiple Interests → single upstream)
- [ ] Data return path via PIT (reverse path forwarding)
- [ ] Loop detection (nonce + hop count)

### 2.3 Router Mesh Formation
- [ ] Router discovery via RNS announces
- [ ] Prefix announcement (ICN prefix + router identity)
- [ ] Dynamic FIB updates (prefix withdrawal/re-announce)
- [ ] Multi-path support (ECMP or primary/backup)

### 2.4 Cache Coherency
- [ ] Cache validation (CheckFreshness Interest)
- [ ] Stale-while-revalidate
- [ ] Cache purge/invalidation protocol

**Deliverable:** `icn-router` binary. Client ↔ Router ↔ ... ↔ Router ↔ Server works. Content caches at each hop.

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
- [ ] Protocol version in Interest/Data headers
- [ ] Capability negotiation (client ↔ router ↔ server)
- [ ] Backward compatibility policy

### 4.2 Advanced Features
- [ ] **Pub/Sub**: `Subscribe(prefix)` → proactive Data push
- [ ] **Selectors**: `Interest(name, selector: latest/oldest/>=version)`
- [ ] **Chunked transfer**: Large files via segmented Data + reassembly
- [ ] **Priority/QoS**: Interest priority field, router queueing

### 4.3 Developer Experience
- [ ] SDKs: Python, Rust, Go, TypeScript
- [ ] CLI: `icn fetch`, `icn publish`, `icn subscribe`
- [ ] HTTP gateway (optional): `GET /icn/<name>`
- [ ] Documentation: protocol spec, API ref, tutorials

### 4.4 Testing & Compliance
- [ ] Integration test suite (mesh sim via RNS testnet)
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