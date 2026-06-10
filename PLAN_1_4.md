# ICN Phase 1.4 — Configuration & Operations Implementation Plan

**Goal:** Production-ready observability and operations — structured logging, health endpoints, metrics.

---

## Current State (from 1.1-1.3)

| Component | Status |
|-----------|--------|
| TOML config | ✅ `ClientConfig`, `ServerConfig` in `rns_icn/config.py` |
| Client | ✅ `ICNClient` with retry, hash verification |
| Server | ✅ `ICNServer` with SQLite ContentStore |
| Link management | ✅ `LinkPool` with health monitoring |

---

## Implementation Tasks

### 1.4.1 Structured Logging (JSON)

**Location:** New `rns_icn/logging.py` + integrate into `config.py`, `client.py`, `rns_server.py`

```python
# rns_icn/logging.py
import json
import logging
import sys
from typing import Any, Dict

class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""
    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in {"name", "msg", "args", "levelname", "levelno",
                          "pathname", "filename", "module", "lineno",
                          "funcName", "created", "msecs", "relativeCreated",
                          "thread", "threadName", "processName", "process",
                          "exc_info", "exc_text", "stack_info", "getMessage"}:
                log_entry[key] = value
        return json.dumps(log_entry)

def setup_logging(config: ClientConfig | ServerConfig) -> None:
    """Configure logging based on config."""
    level = getattr(logging, config.log_level.upper(), logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    if config.log_json:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        ))
    
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]
```

**Config additions** (already in `ClientConfig`/`ServerConfig`):
```python
log_level: str = "INFO"
log_json: bool = False
```

**Integration points:**
- `ICNClient.start()` → `setup_logging(self.config)`
- `ICNServer.start()` → `setup_logging(self.config)`
- RNS internal logs → redirect to Python logging

---

### 1.4.2 Health Endpoint

**HTTP Health Endpoint** (optional, controlled by `http_enabled`)

**Location:** New `rns_icn/health.py` + integrate into `ICNServer`

```python
# rns_icn/health.py
from aiohttp import web
from rns_icn.rns_server import ICNServer

async def health_handler(request: web.Request) -> web.Response:
    server: ICNServer = request.app["server"]
    
    health = {
        "status": "healthy",
        "identity": server.identity.hexhash,
        "destination": server.hexhash,
        "uptime_seconds": time.time() - server._started_at,
        "content_store": {
            "entries": len(server.forwarder.cs),
            "capacity": server.forwarder.cs.capacity,
            "size_bytes": server.forwarder.cs.size_bytes,
            "hits": server.forwarder.cs.hits,
            "misses": server.forwarder.cs.misses,
            "hit_rate": server.forwarder.cs.hits / max(1, server.forwarder.cs.hits + server.forwarder.cs.misses),
        },
        "links": {
            "active": server.link_pool.active_link_count,
            "total": len(server.link_pool._links),
        },
        "announces": {
            "last_reannounce": getattr(server, "_last_announce_time", 0),
        },
    }
    return web.json_response(health)

async def metrics_handler(request: web.Request) -> web.Response:
    server: ICNServer = request.app["server"]
    # Prometheus-style text format
    lines = [
        f"icn_content_store_entries {len(server.forwarder.cs)}",
        f"icn_content_store_capacity {server.forwarder.cs.capacity}",
        f"icn_content_store_size_bytes {server.forwarder.cs.size_bytes}",
        f"icn_content_store_hits_total {server.forwarder.cs.hits}",
        f"icn_content_store_misses_total {server.forwarder.cs.misses}",
        f"icn_links_active {server.link_pool.active_link_count}",
        f"icn_links_total {len(server.link_pool._links)}",
        f"icn_uptime_seconds {time.time() - server._started_at}",
    ]
    return web.Response(text="\n".join(lines), content_type="text/plain")

def setup_http_api(server: ICNServer, host: str = "127.0.0.1", port: int = 8080) -> web.AppRunner:
    app = web.Application()
    app["server"] = server
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner
```

**Config additions** (already in `ServerConfig`):
```python
http_enabled: bool = False
http_host: str = "127.0.0.1"
http_port: int = 8080
```

**RNS Health Endpoint** (via ICN Interest):
- Name: `/<server_identity>/health`
- Returns Data with JSON health payload
- Integrated in `ICNServer.handle_interest()`

---

### 1.4.3 Metrics Collection

**Location:** New `rns_icn/metrics.py` — centralized metrics

```python
# rns_icn/metrics.py
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from threading import Lock

@dataclass
class MetricsCollector:
    """Thread-safe metrics collection."""
    
    # Fetch latency (seconds)
    fetch_latencies: list = field(default_factory=list)
    _fetch_lock: Lock = field(default_factory=Lock)
    
    # Link uptime
    link_up_times: Dict[str, float] = field(default_factory=dict)  # peer_hash -> start_time
    link_downtime_total: Dict[str, float] = field(default_factory=dict)
    _link_lock: Lock = field(default_factory=Lock)
    
    # Content store (delegated to ContentStore.hits/misses)
    
    def record_fetch(self, latency: float) -> None:
        with self._fetch_lock:
            self.fetch_latencies.append(latency)
            if len(self.fetch_latencies) > 10000:
                self.fetch_latencies = self.fetch_latencies[-5000:]
    
    def record_link_up(self, peer_hash: str) -> None:
        with self._link_lock:
            self.link_up_times[peer_hash] = time.time()
    
    def record_link_down(self, peer_hash: str) -> None:
        with self._link_lock:
            if peer_hash in self.link_up_times:
                uptime = time.time() - self.link_up_times[peer_hash]
                self.link_downtime_total[peer_hash] = self.link_downtime_total.get(peer_hash, 0) + uptime
                del self.link_up_times[peer_hash]
    
    def get_fetch_stats(self) -> dict:
        with self._fetch_lock:
            if not self.fetch_latencies:
                return {}
            sorted_lat = sorted(self.fetch_latencies)
            n = len(sorted_lat)
            return {
                "count": n,
                "mean": sum(sorted_lat) / n,
                "p50": sorted_lat[n // 2],
                "p95": sorted_lat[int(n * 0.95)],
                "p99": sorted_lat[int(n * 0.99)],
            }
    
    def get_link_uptime(self, peer_hash: str) -> Optional[float]:
        with self._link_lock:
            if peer_hash in self.link_up_times:
                return time.time() - self.link_up_times[peer_hash]
            return None
```

**Integration:**
- `ICNClient.fetch()` → record latency
- `LinkPool._monitor_links()` → record link up/down
- Expose via `/metrics` (Prometheus text) and RNS health Interest

---

### 1.4.4 Binaries / Entry Points

**`pyproject.toml` additions:**
```toml
[project.scripts]
icn-client = "rns_icn.cli:client_main"
icn-server = "rns_icn.cli:server_main"
```

**New `rns_icn/cli.py`:**
```python
# rns_icn/cli.py
import asyncio
import signal
from rns_icn.config import load_client_config, load_server_config
from rns_icn.client import ICNClient
from rns_icn.rns_server import ICNServer

async def client_main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="icn.toml")
    parser.add_argument("--fetch", help="Name to fetch (e.g., /producer/manifest)")
    parser.add_argument("--peer", help="Peer destination hash")
    args = parser.parse_args()
    
    config = load_client_config(args.config)
    async with ICNClient(config) as client:
        # ... fetch logic
        
async def server_main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="icn.toml")
    args = parser.parse_args()
    
    config = load_server_config(args.config)
    async with ICNServer(config) as server:
        # Wait for shutdown signal
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)
        await stop_event.wait()
```

---

## Files to Create/Modify

| File | Action |
|------|--------|
| `rns_icn/logging.py` | **NEW** — JSON formatter + setup |
| `rns_icn/health.py` | **NEW** — HTTP health + metrics endpoints |
| `rns_icn/metrics.py` | **NEW** — Metrics collector |
| `rns_icn/cli.py` | **NEW** — `icn-client`, `icn-server` binaries |
| `rns_icn/client.py` | Integrate logging + metrics |
| `rns_icn/rns_server.py` | Integrate logging + health + metrics + HTTP server |
| `rns_icn/link_pool.py` | Integrate link up/down metrics |
| `pyproject.toml` | Add entry points |
| `icn.toml.example` | Add logging + HTTP config |
| `tests/test_observability.py` | **NEW** — Tests for logging, health, metrics |

---

## Acceptance Criteria

```bash
# 1. JSON logging
LOG_JSON=true icn-server --config icn.toml 2>&1 | head -1 | jq .level
# > "INFO"

# 2. HTTP health endpoint
curl http://127.0.0.1:8080/health | jq .
# {
#   "status": "healthy",
#   "identity": "...",
#   "content_store": {"entries": 0, "hit_rate": 0.0, ...},
#   "links": {"active": 0, "total": 0}
# }

# 3. Metrics endpoint
curl http://127.0.0.1:8080/metrics
# icn_content_store_entries 0
# icn_content_store_hits_total 0
# ...

# 4. RNS health Interest
icn-client --config icn.toml --fetch /<server>/health --peer <server_hash>
# Returns Data with health JSON

# 5. Fetch latency recorded
# Run fetches, check /metrics for icn_fetch_latency_seconds

# 6. Binaries installed
pip install -e .
icn-server --help
icn-client --help
```

---

## Dependencies

- `aiohttp` for HTTP endpoints (add to `pyproject.toml`)
- `prometheus-client` optional (for native Prometheus format)

---

## Next Phase (2.0)

After 1.4: **Phase 2 — Multi-Hop Forwarding (Router Mesh)**
- `ICNRouter` class: FIB, PIT, CS
- Longest-prefix match forwarding
- PIT aggregation
- Router mesh formation via RNS announces