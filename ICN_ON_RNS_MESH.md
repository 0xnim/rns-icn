# ICN on RNS Mesh — Protocol Design & Production Notes

## Overview

ICN (Information-Centric Networking) over RNS (Reticulum Network Stack) operates as a content-addressable, name-based protocol on top of RNS's mesh transport. This document captures the production architecture, mesh constraints, and solutions developed for running ICN over the public RNS mesh (MichMesh, First Tula, UTN Oregon, ShadowLink).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ICN APPLICATION LAYER                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ icn_client   │  │ icn_browser  │  │ icn_server   │           │
│  │ (CLI fetch)  │  │ (Web UI)     │  │ (VPS daemon) │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
└─────────┼─────────────────┼─────────────────┼────────────────────┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ICN PROTOCOL LAYER                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Manifest     │  │ Interest/    │  │ Link Establishment   │  │
│  │ (name index) │  │ Data packets │  │ (AES-256-CBC, proof) │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      RNS TRANSPORT LAYER                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Destination  │  │ Announce     │  │ Path / Link          │  │
│  │ (SINGLE/IN)  │  │ Table        │  │ Management           │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      RNS MESH INTERFACES                          │
│  TCPClientInterface[UTN Oregon/rns.utn.lol:4965]  ◄── 2-3 hops   │
│  TCPClientInterface[ShadowLink/...:4242]                        │
│  TCPClientInterface[MichMesh/...:7822]                          │
│  TCPClientInterface[First Tula/...:4242]                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Mesh Constraints (Critical)

### 1. Announce Rate-Limiting

Public mesh nodes enforce strict announce rebroadcast limits:

```python
# RNS Transport.py constants (approximate)
LOCAL_REBROADCASTS_MAX = 10
ANNOUNCE_RATE_LIMIT = 10  # per node per second
```

**Impact:** VPS announces every 30s but announce propagation fails at 3-4 hops. Path table works (933 entries), announce table stays empty.

**Evidence from logs:**
```
[Debug] Blocking rebroadcast of announce from <...> due to excessive announce rate
[Debug] No interfaces could process the outbound packet
```

### 2. Path vs Announce Table

| Table | Populated | Source |
|-------|-----------|--------|
| `RNS.Transport.path_table` | ✅ Yes (933 entries) | Mesh routing + storage load |
| `RNS.Transport.announce_table` | ❌ No | Mesh rebroadcast blocked |

ICN link establishment **requires** peer's public key from announce table for link proof verification.

---

## Solution: Manual Announce Injection

### Server Identity Sharing

1. **VPS** runs `icn-server.service` with identity at `/etc/rns-icn/identity`
   - Identity hash: `b953bdc81db633de8d89e0207a220723`
   - ICN destination: `24cb54c7ec86294f0723e1d04015b8aa`

2. **Client** copies identity file once:
   ```bash
   scp root@172.81.133.81:/etc/rns-icn/identity /Users/niklaswoj/rns-icn/server_identity
   ```

3. **Client injects at runtime** (`rns_server.py:connect()`):
   ```python
   if peer_hash_bytes not in RNS.Transport.announce_table:
       server_identity = RNS.Identity.from_file("/Users/niklaswoj/rns-icn/server_identity")
       server_dest = RNS.Destination(server_identity, RNS.Destination.IN, 
                                      RNS.Destination.SINGLE, "icn", "default")
       RNS.Transport.announce_table[peer_hash_bytes] = server_dest
   ```

### Why This Works

- Path table already has route to VPS destination (2-3 hops via UTN Oregon)
- Manual inject provides peer's public key for link proof crypto
- Link establishment succeeds; content flows over mesh
- No mesh rate-limit bypass needed — only one manual inject per session

---

## Link Establishment Flow

```
Client                                    VPS (Server)
  │                                         │
  │── RNS.Transport.request_path(hash) ───►│  (triggers path lookup)
  │                                         │
  │◄── Path confirmed (2-3 hops) ──────────│
  │                                         │
  │── Manual inject server dest to ────────►│  (local only: announce_table)
  │     announce_table                      │
  │                                         │
  │── Link Request (encrypted w/ server     │
  │     pubkey from announce_table) ───────►│
  │                                         │
  │◄── Link Proof (signed by server) ──────│  (verified with server pubkey)
  │                                         │
  │── Link Established (AES-256-CBC) ──────►│  RTT ~200-600ms
  │                                         │
  │── ICN Interest /manifest ─────────────►│
  │◄── Data (manifest JSON) ───────────────│
  │── ICN Interest /blob/... ─────────────►│
  │◄── Data (file content) ────────────────│
```

---

## ICN Protocol Details

### Naming

```
/<producer_hash>/<label>
/<producer_hash>/manifest
```

Example:
```
/24cb54c7ec86294f0723e1d04015b8aa/proofoflife
/24cb54c7ec86294f0723e1d04015b8aa/manifest
```

### Manifest Structure

```json
{
  "version": 1,
  "sequence": 1,
  "entries": [
    {
      "kind": "blob",
      "label": "proofoflife",
      "name": "/24cb54c7ec86294f0723e1d04015b8aa/proofoflife",
      "content_hash": "sha256:...",
      "size": 28
    }
  ],
  "producer": "24cb54c7ec86294f0723e1d04015b8aa"
}
```

### Packet Types

| Type | Purpose |
|------|---------|
| `Interest` | Request content by name |
| `Data` | Content payload + metadata |
| `Manifest` | Index of available content |

---

## Components

### 1. VPS Server (`icn_server.py`)

```python
# Runs as systemd service: icn-server.service
# Config: /etc/rns-icn/config (mesh interfaces only)
# Identity: /etc/rns-icn/identity
# Announce interval: 30s (via announce_timer)
# Seed content: hello, quote, readme, proofoflife
```

**Deployment:**
```bash
# On VPS (172.81.133.81)
systemctl restart icn-server.service
# Binary: /root/sc-tools/target/release/forex (not used for ICN)
# Python: /opt/rns-icn-venv/bin/python3 /opt/rns-icn/icn_server.py
```

### 2. CLI Client (`icn_client.py`)

**Reliable fetch workflow:**
```bash
export RNS_DEST=24cb54c7ec86294f0723e1d04015b8aa
/Users/niklaswoj/rns-icn/.venv/bin/python3 icn_client.py
```

**Internals:**
1. Start local RNS with mesh interfaces
2. Stop local RNS (free port 49200)
3. Run fresh RNS + connect + fetch manifest + all files
4. Parse output for progress
5. Restart local RNS

**Output:**
```
[Client] Link established (face #100)
[Client] ✓ Got manifest v1
[Client]   Entries:
  [blob] proofoflife → "ICN mesh working: 1780948450"
  [blob] hello       → "Hello from ICN over RNS!"
  [blob] quote       → "The only way to learn..."
  [blob] readme      → "ICN standalone on VPS mesh."
```

### 3. Web Browser (`icn_browser.py`)

```bash
export RNS_DEST=24cb54c7ec86294f0723e1d04015b8aa
/Users/niklaswoj/rns-icn/.venv/bin/python3 icn_browser.py
# Open http://localhost:8080
```

**Endpoints:**
| Endpoint | Description |
|----------|-------------|
| `GET /` | UI with manifest list |
| `GET /api/manifest` | JSON manifest (cached) |
| `POST /api/refresh` | Trigger fresh fetch |
| `GET /download/<label>` | Fetch & return file content |

---

## Production Checklist

### Prerequisites

- [ ] VPS `icn-server.service` running with 30s announce
- [ ] Server identity copied to client: `/Users/niklaswoj/rns-icn/server_identity`
- [ ] Local RNS config (`~/.reticulum/config`) with mesh interfaces
- [ ] Direct TCP interface **disabled** in config
- [ ] Python deps: `aiohttp`, `aiohttp-jinja2`, `jinja2` (browser only)

### Adding Content

```bash
# 1. Edit seed content on VPS
vim /opt/rns-icn/icn_server.py
# Add to items list:
# ("newfile", b"New content here"),

# 2. Restart server
ssh root@172.81.133.81 "systemctl restart icn-server.service"

# 3. Wait ~30s for announce, then fetch
export RNS_DEST=24cb54c7ec86294f0723e1d04015b8aa
python3 icn_client.py
```

---

## Known Issues & Workarounds

| Issue | Severity | Workaround |
|-------|----------|------------|
| Mesh announce rate-limiting | High | Manual announce table injection from shared identity |
| RNS Transport TypeError (`Destination not subscriptable`) | Medium | CLI stops local RNS during fetch; browser gets killed periodically |
| No local state bookkeeping for commands | Design | API balance = source of truth |
| Browser process killed by manager | Env | Run directly in terminal |
| Announce doesn't auto-propagate | Mesh limit | Only affects first connection; path persists |

---

## Network Topology

```
Local (macOS)                          VPS (172.81.133.81)
┌─────────────────────┐                ┌─────────────────────┐
│ RNS Config          │                │ icn-server.service  │
│ - MichMesh:7822     │                │ - Identity: b953... │
│ - First Tula:4242   │                │ - Announce: 30s    │
│ - UTN Oregon:4965   │◄─── 2-3 hops ──│ - Destination:     │
│ - ShadowLink:4242   │                │   24cb54c7ec86...  │
│ - Direct TCP: OFF   │                │ - Seed: 4 files    │
└─────────────────────┘                └─────────────────────┘
         │
         ▼
   Path table: 933 entries
   Announce table: manual inject
```

---

## Files Reference

```
/Users/niklaswoj/rns-icn/
├── icn_server.py          # VPS daemon (server side)
├── icn_client.py          # CLI fetch (rock-solid)
├── icn_browser.py         # Web UI (http://localhost:8080)
├── rns_icn/
│   ├── rns_server.py      # Core connect logic + announce injection
│   ├── packet.py          # Interest/Data/Manifest packets
│   ├── manifest.py        # Manifest structure
│   └── forwarder.py       # Content store
├── server_identity        # Copied from VPS /etc/rns-icn/identity
├── templates/index.html   # Browser UI
└── .reticulum/config      # RNS mesh interfaces
```

---

## Summary

ICN over RNS mesh **works** but requires understanding the mesh constraint: **announce propagation is rate-limited, path is not**. The production pattern:

1. **Mesh provides path** (works reliably, 2-3 hops)
2. **Announce doesn't propagate** (mesh design)
3. **Manual inject server identity** (solves link proof crypto)
4. **Content flows** (dirty but effective)

The CLI client (`icn_client.py`) is the reference implementation — it handles RNS lifecycle cleanly and fetches 100% of content. The browser demonstrates the same protocol over HTTP but suffers from process management issues in this environment.