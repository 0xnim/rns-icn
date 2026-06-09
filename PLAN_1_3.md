# ICN Phase 1.3 — Persistent Content Store Implementation Plan

**Goal:** Replace in-memory `ContentStore` with SQLite backend supporting TTL, LRU eviction, atomic writes, and crash recovery.

---

## Current State

| Component | Location | Type |
|-----------|----------|------|
| `ContentStore` | `rns_icn/content_store.py` | In-memory `dict` |
| `Forwarder.cs` | `rns_icn/forwarder.py` | Uses `ContentStore` API |
| `ICNServer` | `rns_icn/rns_server.py` | Calls `forwarder.cs.insert()` |
| `ICNClient` | `rns_icn/client.py` | Uses `Forwarder.express()` |

---

## Implementation Tasks

### 1.3.1 SQLite Schema (`rns_icn/content_store.py`)

```sql
CREATE TABLE IF NOT EXISTS content (
    name_hash      BLOB PRIMARY KEY,      -- blake2b(name) for exact match
    name_bytes     BLOB NOT NULL,         -- serialized Name
    content_bytes  BLOB NOT NULL,         -- Data content
    content_hash   BLOB NOT NULL,         -- blake2b(content)
    sequence       INTEGER,               -- optional sequence number
    freshness      INTEGER DEFAULT 1,     -- 1=fresh, 0=stale
    age_seconds    INTEGER DEFAULT 0,     -- if stale
    metadata_json  TEXT,                  -- full metadata JSON
    inserted_at    INTEGER NOT NULL,      -- Unix timestamp
    expires_at     INTEGER,               -- inserted_at + TTL (NULL = no expiry)
    size_bytes     INTEGER NOT NULL       -- len(content_bytes)
);

-- Prefix index for prefix matching
CREATE TABLE IF NOT EXISTS name_prefixes (
    prefix_hash  BLOB NOT NULL,           -- blake2b(prefix)
    name_hash    BLOB NOT NULL,
    PRIMARY KEY (prefix_hash, name_hash)
);

-- TTL index for efficient expiry cleanup
CREATE INDEX IF NOT EXISTS idx_expires_at ON content(expires_at);
-- LRU index for eviction
CREATE INDEX IF NOT EXISTS idx_inserted_at ON content(inserted_at);
```

### 1.3.2 SQLiteContentStore Class

**Same API as current `ContentStore`:**
```python
class ContentStore:
    def __init__(self, path: str, max_entries: int = 10000, default_ttl: Optional[int] = None):
        
    def insert(self, name: Name, data: Data) -> None:
        # Atomic upsert with WAL mode
        
    def get(self, name: Name) -> Optional[Data]:
        # Exact match by name_hash
        
    def get_prefix(self, name: Name) -> Optional[Data]:
        # Longest prefix match in name_prefixes
        
    def remove(self, name: Name) -> bool:
        
    def purge_expired(self) -> int:
        # Delete expired entries, return count
        
    def evict_lru(self, target_count: int) -> int:
        # Evict oldest entries to reach max_entries
        
    def close(self) -> None:
    
    def __len__(self) -> int:
```

**Key behaviors:**
- **WAL mode** for concurrent read/write without locks
- **Atomic transactions** for each insert
- **TTL per prefix**: `config.prefix_ttls = {"manifest": 3600, "default": 86400}`
- **LRU eviction**: on `insert()` if over `max_entries`, evict oldest
- **Crash recovery**: WAL + `PRAGMA integrity_check` on startup

### 1.3.3 Config Updates

**`ServerConfig` additions** (`rns_icn/config.py`):
```python
cs_path: str = "~/.icn/content_store.db"  # SQLite file path
cs_max_entries: int = 10000
cs_ttl_seconds: Optional[int] = None  # default TTL, None = no expiry
cs_prefix_ttls: Dict[str, int] = field(default_factory=dict)  # per-prefix TTL
```

### 1.3.4 Integration

**`ICNServer.__init__`** — pass SQLite path to `ContentStore`:
```python
self.forwarder = Forwarder(cs_max=config.cs_max_entries)
# Replace forwarder.cs with SQLiteContentStore
from .content_store import SQLiteContentStore
self.forwarder.cs = SQLiteContentStore(
    path=config.cs_path,
    max_entries=config.cs_max_entries,
    default_ttl=config.cs_ttl_seconds,
)
# Load prefix TTLs
self.forwarder.cs.prefix_ttls = config.cs_prefix_ttls
```

**`ICNClient`** — similar for local cache if desired.

### 1.3.5 Tests

**`tests/test_content_store.py`:**
```python
async def test_insert_and_get():
    # Insert Data, get by exact name

async def test_prefix_match():
    # Insert /producer/foo/1, get_prefix(/producer/foo)

async def test_ttl_expiry():
    # Insert with TTL=1, wait, verify purge

async def test_lru_eviction():
    # Insert > max_entries, verify oldest evicted

async def test_atomic_write():
    # Crash simulation: kill process mid-write, verify integrity

async def test_crash_recovery():
    # Corrupt DB, restart, verify PRAGMA integrity_check
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `rns_icn/config.py` | Add `cs_path`, `cs_prefix_ttls` to `ServerConfig` |
| `rns_icn/content_store.py` | **REPLACE** with SQLite implementation |
| `rns_icn/rns_server.py` | Wire SQLiteContentStore in `ICNServer.__init__` |
| `icn.toml.example` | Add `[server]` content store config |
| `tests/test_content_store.py` | New test file |

---

## Acceptance Criteria

```python
# 1. Basic insert/get
store = SQLiteContentStore("test.db", max_entries=100)
store.insert(name, data)
retrieved = store.get(name)
assert retrieved.content == data.content

# 2. Prefix match
store.insert(Name(producer, [b"foo", b"1"]), data)
match = store.get_prefix(Name(producer, [b"foo"]))
assert match is not None

# 3. TTL expiry
store = SQLiteContentStore("test.db", default_ttl=1)  # 1 second
store.insert(name, data)
await asyncio.sleep(1.5)
store.purge_expired()
assert store.get(name) is None

# 4. LRU eviction
store = SQLiteContentStore("test.db", max_entries=2)
store.insert(name1, data1)
store.insert(name2, data2)
store.insert(name3, data3)  # Should evict name1
assert store.get(name1) is None
assert store.get(name2) is not None
assert store.get(name3) is not None

# 5. Crash recovery
# Write, kill -9, restart, verify data intact
```

---

## Dependencies

- `sqlite3` (stdlib)
- `aiosqlite` optional for async (use sync for simplicity + thread pool)

---

## Next Phase (1.4)

After content store: **Configuration & Operations**
- Full TOML config for client + server (already mostly done)
- Structured logging (JSON)
- Health endpoint (HTTP + RNS)
- Metrics: fetch latency, hit/miss, link uptime