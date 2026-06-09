# ICN Phase 1.1 — Transport Abstraction Implementation Plan (Revised)

**Goal:** Replace ad-hoc scripts with reusable `ICNClient` / `ICNServer` classes + config-driven setup + shared link pool.

---

## Architecture (Clean)

```
rns_icn/
├── config.py          # ClientConfig, ServerConfig, KnownPeer, TOML loader
├── link_pool.py       # LinkPool: reuse, health, announce injection (shared)
├── client.py          # ICNClient: Forwarder + LinkPool, fetch(), context manager
├── server.py          # ICNServer (was RNSICNServer): ServerConfig + LinkPool, context manager
├── forwarder.py       # (existing)
├── face.py            # (existing)
└── ... (other modules)
```

**No `icn/` top-level package. No wrappers.** Everything in `rns_icn/` where it belongs.

---

## Implementation Tasks

### 1.1.1 `rns_icn/config.py` — Config Dataclasses + TOML Loader

```python
# rns_icn/config.py
from dataclasses import dataclass, field
from typing import Optional, List
import tomllib
from pathlib import Path

from .server import ServerRole  # re-export

@dataclass
class KnownPeer:
    """Pre-configured peer for announce table injection."""
    name: str
    destination_hash: str  # 32-char hex
    identity_path: Optional[str] = None
    aliases: List[str] = field(default_factory=list)

@dataclass
class ClientConfig:
    identity_path: Optional[str] = None
    mesh_interfaces: List[str] = field(default_factory=lambda: ["UTN Oregon"])
    known_peers: List[KnownPeer] = field(default_factory=list)
    connect_timeout: float = 60.0
    fetch_timeout: float = 30.0
    path_request_timeout: float = 30.0
    log_level: str = "INFO"
    log_json: bool = False

@dataclass
class ServerConfig:
    identity_path: str
    app_name: str = "icn"
    aspect: str = "default"
    mesh_interfaces: List[str] = field(default_factory=lambda: ["UTN Oregon"])
    role: ServerRole = ServerRole.ORIGIN
    announce_interval: float = 30.0
    reannounce_on_link: bool = True
    cs_max_entries: int = 10000
    cs_ttl_seconds: Optional[int] = None
    resource_threshold: int = 100_000
    known_peers: List[KnownPeer] = field(default_factory=list)
    log_level: str = "INFO"
    log_json: bool = False
    http_enabled: bool = False
    http_host: str = "127.0.0.1"
    http_port: int = 8080

def load_client_config(path: str = "icn.toml") -> ClientConfig:
    data = _load_toml(path).get("client", {})
    return _dict_to_client_config(data, path)

def load_server_config(path: str = "icn.toml") -> ServerConfig:
    data = _load_toml(path).get("server", {})
    return _dict_to_server_config(data, path)

def _load_toml(path: str) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)

def _dict_to_client_config(data: dict, base_path: str) -> ClientConfig:
    # Parse known_peers, expand paths, return ClientConfig
    ...

def _dict_to_server_config(data: dict, base_path: str) -> ServerConfig:
    # Parse known_peers, expand paths, return ServerConfig
    ...
```

**Example `icn.toml`:**
```toml
[client]
identity_path = "~/.icn/client_identity"
mesh_interfaces = ["UTN Oregon"]
fetch_timeout = 30.0

[[client.known_peers]]
name = "vps-prod"
destination_hash = "24cb54c7ec86294f0723e1d04015b8aa"
identity_path = "~/.icn/vps_identity"

[server]
identity_path = "/etc/rns-icn/identity"
app_name = "icn"
role = "ORIGIN"
announce_interval = 30.0

[[server.known_peers]]
name = "peer-1"
destination_hash = "..."
```

---

### 1.1.2 `rns_icn/link_pool.py` — Shared LinkPool

```python
# rns_icn/link_pool.py
import asyncio
import time
from typing import Optional, Dict, List
import RNS

from .config import KnownPeer

class LinkPool:
    """Manages outbound RNS Links: reuse, health monitoring, announce injection."""
    
    def __init__(
        self,
        identity: RNS.Identity,
        app_name: str,
        aspect: str,
        known_peers: List[KnownPeer],
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.identity = identity
        = identity
        self.app_name      = app_name
        self.aspect        = aspect
        self.known_peers   = {p.destination_hash: p for p in known_peers}
        self._loop         = loop or asyncio.get_event_loop()
        self._links: Dict[bytes, RNS.Link] = {}      # peer_hash -> link
        self._health: Dict[bytes, float] = {}        # peer_hash -> last_activity
        self._monitor_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self):
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_links())
    
    async def stop(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try: await self._monitor_task
            except asyncio.CancelledError: pass
        for link in self._links.values():
            link.teardown()
        await asyncio.sleep(0.2)
        self._links.clear()
        self._health.clear()
    
    async def get_link(self, peer_hash: bytes) -> Optional[RNS.Link]:
        """Get existing active link or create new one."""
        # Return existing active link
        if peer_hash in self._links:
            link = self._links[peer_hash]
            if link.status == RNS.Link.ACTIVE:
                self._health[peer_hash] = time.time()
                return link
            else:
                del self._links[peer_hash]
                self._health.pop(peer_hash, None)
        
        # Ensure announce in table
        await self._ensure_announce(peer_hash)
        
        # Create new link
        link = await self._create_link(peer_hash)
        if link:
            self._links[peer_hash] = link
            self._health[peer_hash] = time.time()
        return link
    
    async def _ensure_announce(self, peer_hash: bytes):
        if peer_hash in RNS.Transport.announce_table:
            return
        peer_config = self.known_peers.get(peer_hash.hex())
        if peer_config and peer_config.identity_path:
            identity = RNS.Identity.from_file(peer_config.identity_path)
            if identity:
                dest = RNS.Destination(identity, RNS.Destination.IN, RNS.Destination.SINGLE,
                                       self.app_name, self.aspect)
                RNS.Transport.announce_table[peer_hash] = dest
                return
        # Fallback: request path
        RNS.Transport.request_path(peer_hash, None, None, False)
        await self._wait_for_announce(peer_hash, timeout=30.0)
    
    async def _create_link(self, peer_hash: bytes) -> Optional[RNS.Link]:
        dest = RNS.Transport.announce_table.get(peer_hash)
        if not dest:
            return None
        link = RNS.Link(dest)
        timeout = 120.0
        start = time.time()
        while link.status != RNS.Link.ACTIVE:
            if link.status == RNS.Link.CLOSED:
                return None
            await asyncio.sleep(0.1)
            if time.time() - start > timeout:
                return None
        return link
    
    async def _wait_for_announce(self, peer_hash: bytes, timeout: float):
        start = time.time()
        while time.time() - start < timeout:
            if peer_hash in RNS.Transport.announce_table:
                return
            await asyncio.sleep(5.0)
            RNS.Transport.request_path(peer_hash, None, None, False)
    
    async def _monitor_links(self):
        while self._running:
            await asyncio.sleep(30)
            now = time.time()
            dead = [h for h, t in self._health.items() if now - t > 120]
            for h in dead:
                if h in self._links:
                    self._links[h].teardown()
                    del self._links[h]
                    del self._health[h]
```

---

### 1.1.3 `rns_icn/client.py` — ICNClient

```python
# rns_icn/client.py
import asyncio
from typing import Optional
from .config import ClientConfig
from .link_pool import LinkPool
from .forwarder import Forwarder
from .name import Name
from .packet import Interest, Data
import RNS

class ICNClient:
    """ICN Consumer client: expresses Interests, fetches Data over RNS mesh."""
    
    def __init__(self, config: ClientConfig):
        self.config = config
        self._started_rns = False
        self._identity: Optional[RNS.Identity] = None
        self._link_pool: Optional[LinkPool] = None
        self._forwarder: Optional[Forwarder] = None
    
    async def __aenter__(self) -> "ICNClient":
        return await self.start()
    
    async def __aexit__(self, *exc) -> None:
        await self.shutdown()
    
    async def start(self) -> "ICNClient":
        # Initialize RNS
        if not hasattr(RNS, "Reticulum") or RNS.Reticulum is None:
            RNS.Reticulum()
            self._started_rns = True
        
        # Load or create identity
        if self.config.identity_path:
            from .rns_utils import load_or_create_identity
            path = Path(self.config.identity_path).expanduser()
            self._identity = load_or_create_identity(str(path))
        else:
            self._identity = RNS.Identity()
        
        # Create forwarder (local, no destination)
        self._forwarder = Forwarder(cs_max=1000)
        
        # Create link pool
        self._link_pool = LinkPool(
            identity=self._identity,
            app_name="icn",
            aspect="default",
            known_peers=self.config.known_peers,
        )
        await self._link_pool.start()
        
        return self
    
    async def shutdown(self) -> None:
        if self._link_pool:
            await self._link_pool.stop()
        if self._started_rns and hasattr(RNS, "Reticulum") and RNS.Reticulum:
            RNS.Reticulum().exit()
    
    async def fetch(
        self,
        name: Name,
        peer_hash: bytes,
        timeout: Optional[float] = None,
    ) -> Optional[Data]:
        """Express Interest to peer, wait for Data."""
        timeout = timeout or self.config.fetch_timeout
        link = await self._link_pool.get_link(peer_hash)
        if not link:
            raise RuntimeError(f"Failed to establish link to {peer_hash.hex()}")
        
        # Register face for this link (if not already)
        face_id = self._register_link_face(link)
        
        # Express interest
        interest = Interest(name=name).with_lifetime(int(timeout * 1000))
        result = await self._forwarder.express(interest, face_id)
        return result
    
    async def fetch_manifest(
        self,
        producer_addr: bytes,
        timeout: Optional[float] = None,
    ) -> Optional["Manifest"]:
        from .manifest import Manifest
        name = Name(producer_addr, [b"manifest"])
        data = await self.fetch(name, producer_addr, timeout)
        if data:
            return Manifest.from_data(data)
        return None
    
    async def fetch_content(
        self,
        entry: "ManifestEntry",
        producer_addr: bytes,
        timeout: Optional[float] = None,
    ) -> Optional[bytes]:
        data = await self.fetch(entry.name, producer_addr, timeout)
        return data.content if data else None
    
    def _register_link_face(self, link: RNS.Link) -> int:
        # Reuse face if link already registered, else create new
        # This mirrors RNSICNServer._make_link_face logic
        from .face import LinkFace
        link_face = LinkFace(self._next_face_id(), link, loop=asyncio.get_event_loop())
        self._forwarder.register_face(link_face)
        return link_face.id()
    
    def _next_face_id(self) -> int:
        if not hasattr(self, "_face_counter"):
            self._face_counter = 1000
        self._face_counter += 1
        return self._face_counter
    
    @property
    def identity(self) -> RNS.Identity:
        return self._identity
    
    @property
    def forwarder(self) -> Forwarder:
        return self._forwarder
```

---

### 1.1.4 `rns_icn/server.py` — ICNServer (Refactored from RNSICNServer)

**Rename** `RNSICNServer` → `ICNServer`, **accept** `ServerConfig` + `LinkPool`, **add** context manager.

Key changes to `rns_server.py`:
- Remove hardcoded `/Users/niklaswoj/rns-icn/server_identity`
- Use `config.known_peers` in `inject_known_peers()`
- Add `__aenter__`/`__aexit__`
- Share `LinkPool` with client (same class)

```python
# rns_icn/server.py (refactored)
class ICNServer:
    def __init__(self, config: ServerConfig, link_pool: Optional[LinkPool] = None):
        self.config = config
        self.link_pool = link_pool or LinkPool(...)
        # ... existing init but load identity from config.identity_path
    
    async def __aenter__(self) -> "ICNServer":
        await self.start()
        return self
    
    async def __aexit__(self, *exc) -> None:
        await self.shutdown()
    
    async def start(self) -> None:
        # Start RNS if needed
        # Create destination
        # Start announce loop
        # Inject known peers
        await self.link_pool.start()
        await self._inject_known_peers()
    
    async def shutdown(self) -> None:
        await self.link_pool.stop()
        # teardown links, cancel announce loop, stop RNS if we started it
    
    async def _inject_known_peers(self):
        for peer_hash, peer_config in self.link_pool.known_peers.items():
            if peer_config.identity_path:
                identity = RNS.Identity.from_file(peer_config.identity_path)
                if identity:
                    dest = RNS.Destination(identity, RNS.Destination.IN, RNS.Destination.SINGLE,
                                           self.config.app_name, self.config.aspect)
                    RNS.Transport.announce_table[bytes.fromhex(peer_hash)] = dest
```

---

### 1.1.5 Example Config + Tests

**`icn.toml.example`** at repo root.

**`tests/test_client.py`:**
```python
async def test_client_lifecycle():
    cfg = load_client_config("icn.toml")
    async with ICNClient(cfg) as client:
        assert client.identity is not None
        assert client.forwarder is not None

async def test_server_lifecycle():
    cfg = load_server_config("icn.toml")
    async with ICNServer(cfg) as server:
        assert server.destination is not None
```

---

## File Changes Summary

| File | Action |
|------|--------|
| `rns_icn/config.py` | **NEW** — Config classes + TOML loader |
| `rns_icn/link_pool.py` | **NEW** — Shared LinkPool |
| `rns_icn/client.py` | **NEW** — ICNClient |
| `rns_icn/server.py` | **REFACTOR** — RNSICNServer → ICNServer (accept ServerConfig, LinkPool, context manager) |
| `icn.toml.example` | **NEW** — Example config |
| `tests/test_client.py` | **NEW** — Basic lifecycle tests |
| `rns_icn/rns_server.py` | **DELETE** — Replaced by `server.py` |

---

## Acceptance Criteria

```bash
# 1. Config loads
python3 -c "
from rns_icn.config import load_client_config, load_server_config
cfg = load_client_config('icn.toml')
print(cfg.known_peers[0].destination_hash)
"

# 2. Client context manager starts/stops cleanly
python3 -c "
import asyncio
from rns_icn.client import ICNClient
from rns_icn.config import load_client_config

async def test():
    cfg = load_client_config('icn.toml')
    async with ICNClient(cfg) as c:
        print('Client:', c.identity.hexhash)
asyncio.run(test()
"

# 3. Server context manager starts/stops cleanly
python3 -c "
import asyncio
from rns_icn.server import ICNServer
from rns_icn.config import load_server_config

async def test():
    cfg = load_server_config('icn.toml')
    async with ICNServer(cfg) as s:
        print('Server:', s.hexhash)
asyncio.run(test()
"

# 4. LinkPool reuses links (verify in logs: 2 fetches = 1 link)
# 5. Announce injection from config.known_peers works
# 6. No orphan links/tasks on context exit
```

---

## Dependencies

- `tomllib` (stdlib Python 3.11+)
- Existing `rns_icn` modules

---

## Next Phase (1.2)

Build on `ICNClient.fetch()` + `LinkPool`:
- Interest retransmission (exponential backoff)
- Interest timeout (configurable)
- Duplicate Interest suppression
- Data validation (content hash)
- Link health monitoring + auto-reconnect (already in LinkPool)