# ICN Phase 1.2 — Reliability Layer Implementation Plan

**Goal:** Make single-hop fetch reliable with retries, timeouts, deduplication, and Data validation.

---

## Current State (from 1.1)

| Component | Location | Capability |
|-----------|----------|------------|
| `ICNClient.fetch()` | `rns_icn/client.py` | Single Interest → wait for Data |
| `LinkPool` | `rns_icn/link_pool.py` | Link reuse, health monitor, announce injection |
| `Forwarder.express()` | `rns_icn/forwarder.py` | PIT-based Interest/Data exchange |
| `ClientConfig` | `rns_icn/config.py` | `fetch_timeout`, `connect_timeout` |

---

## Implementation Tasks

### 1.2.1 Interest Retransmission (Exponential Backoff)

**Location:** `rns_icn/client.py` → enhance `fetch()`

```python
async def fetch(
    self,
    name: Name,
    peer_hash: bytes,
    timeout: Optional[float] = None,
    max_retries: Optional[int] = None,
) -> Optional[Data]:
    timeout = timeout or self.config.fetch_timeout
    max_retries = max_retries or self.config.max_retries  # NEW: default 5
    base_delay = self.config.base_retry_delay  # NEW: default 1.0
    max_delay = self.config.max_retry_delay    # NEW: default 30.0

    for attempt in range(max_retries + 1):
        try:
            link = await self._link_pool.get_link(peer_hash)
            if not link:
                raise RuntimeError(f"Failed to establish link to {peer_hash.hex()}")

            face_id = self._get_or_create_face_id(link)
            interest = Interest(name=name)
            interest.with_lifetime(int(timeout * 1000))
            # Add unique nonce for duplicate detection
            interest.nonce = os.urandom(8)

            result = await asyncio.wait_for(
                self._forwarder.express(interest, face_id),
                timeout=timeout,
            )
            if result:
                return result

        except asyncio.TimeoutError:
            pass  # Will retry

        # Exponential backoff
        if attempt < max_retries:
            delay = min(base_delay * (2 ** attempt), max_delay)
            await asyncio.sleep(delay)

    return None
```

**Config additions** (`rns_icn/config.py`):
```python
@dataclass
class ClientConfig:
    # ... existing ...
    max_retries: int = 5
    base_retry_delay: float = 1.0
    max_retry_delay: float = 30.0
```

---

### 1.2.2 Interest Timeout (Configurable)

Already supported via `fetch_timeout` config and `Interest.with_lifetime(ms)`. Verify the timeout propagates correctly through `Forwarder.express()`.

---

### 1.2.3 Duplicate Interest Suppression

**Location:** `rns_icn/pit.py` (PIT already exists) + `rns_icn/forwarder.py`

The PIT should already aggregate duplicate Interests (same name + nonce). Verify:

1. **Interest carries nonce** → already in `Interest` class
2. **PIT checks for existing entry** on `express()` → should aggregate
3. **Multiple callers get same Data** when Interest is aggregated

If not implemented, add to `Forwarder.express()`:
```python
async def express(self, interest: Interest, face_id: FaceId) -> Optional[Data]:
    # Check PIT for existing pending Interest with same name + nonce
    existing = self.pit.lookup(interest.name, interest.nonce)
    if existing:
        # Attach as downstream face, return existing future
        return await existing.add_downstream(face_id)

    # Create new PIT entry, express upstream
    ...
```

---

### 1.2.4 Data Validation (Content Hash Verification)

**Location:** `rns_icn/packet.py` (Data class) + `rns_icn/client.py`

1. **Data packet includes content hash** in metadata (already implemented via `DataMetadata.content_hash`)
2. **Client verifies on receipt**: compare `Data.content` hash vs `Data.metadata.content_hash`

```python
def verify_content_hash(self) -> bool:
    """Verify content matches metadata.content_hash."""
    if not self.metadata.content_hash:
        return True  # No hash to verify
    computed = hashlib.blake2b(self.content, digest_size=32).digest()
    return computed == self.metadata.content_hash
```

**In `ICNClient.fetch()`:**
```python
result = await ...
if result and not result.verify_content_hash():
    raise ValueError("Data content hash mismatch - possible corruption")
return result
```

---

### 1.2.5 Link Health Monitoring + Auto-Reconnect

**Already in `LinkPool`** (`rns_icn/link_pool.py`):
- `_monitor_links()` runs every 30s
- Removes links inactive > 120s
- `get_link()` recreates dead links automatically

**Verify:** `ICNClient.fetch()` calls `get_link()` which handles reconnect. ✅

---

## Config Updates

**`rns_icn/config.py`** — Add to `ClientConfig`:
```python
max_retries: int = 5
base_retry_delay: float = 1.0
max_retry_delay: float = 30.0
```

**`icn.toml.example`** — Add to `[client]`:
```toml
max_retries = 5
base_retry_delay = 1.0
max_retry_delay = 30.0
```

---

## Tests

**`tests/test_reliability.py`:**
```python
async def test_fetch_retry_on_timeout():
    # Mock server that doesn't respond
    # Verify fetch retries with exponential backoff

async def test_duplicate_interest_suppression():
    # Express same Interest twice simultaneously
    # Verify only one upstream Interest sent

async def test_data_hash_verification():
    # Tamper with Data.content
    # Verify fetch raises on hash mismatch

async def test_link_reconnect():
    # Kill link, verify get_link() recreates it
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `rns_icn/config.py` | Add retry config fields |
| `rns_icn/client.py` | Implement retry loop in `fetch()` |
| `rns_icn/packet.py` | Add `Data.verify_content_hash()` |
| `rns_icn/forwarder.py` | Verify PIT aggregation works |
| `icn.toml.example` | Add retry config |
| `tests/test_reliability.py` | New test file |

---

## Acceptance Criteria

```python
# 1. Retry with exponential backoff
async with ICNClient(cfg) as client:
    # Mock timeout → verify 5 retries with ~1s, 2s, 4s, 8s, 16s delays

# 2. Duplicate suppression
# Two concurrent fetch() for same name → 1 upstream Interest

# 3. Hash verification
# Corrupt Data.content → fetch() raises ValueError

# 4. Link reconnect
# Kill link, next fetch() re-establishes automatically

# 5. Configurable via icn.toml
cfg = load_client_config("icn.toml")
assert cfg.max_retries == 5
```

---

## Next Phase (1.3)

After 1.2: **Persistent Content Store** — SQLite backend with TTL, LRU eviction, atomic writes.