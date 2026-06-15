"""ContentStore — SQLite-backed persistent cache of Data packets."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict, List

from .name import Name
from .packet import Data, DataMetadata, Freshness


class ContentStore:
    """SQLite-backed LRU cache of Data packets, keyed by name.

    Features:
    - Persistent storage with WAL mode
    - TTL support (global default + per-prefix overrides)
    - LRU eviction when over capacity
    - Prefix matching via name_prefixes table
    - Crash recovery via WAL + integrity_check
    """

    def __init__(
        self,
        path: Optional[str] = None,
        max_entries: int = 10000,
        default_ttl: Optional[int] = None,
        prefix_ttls: Optional[Dict[str, int]] = None,
    ):
        if path is None:
            path = ":memory:"
        self._path = Path(path).expanduser()
        if path != ":memory:":
            self._path.parent.mkdir(parents=True, exist_ok=True)
        self._max = max(1, max_entries)
        self._default_ttl = default_ttl  # seconds, None = no expiry
        self._prefix_ttls = prefix_ttls or {}
        self._hits = 0
        self._misses = 0

        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._recover_if_needed()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS content (
                    name_hash      BLOB PRIMARY KEY,
                    name_bytes     BLOB NOT NULL,
                    content_bytes  BLOB NOT NULL,
                    content_hash   BLOB NOT NULL,
                    sequence       INTEGER,
                    freshness      INTEGER DEFAULT 1,
                    age_seconds    INTEGER DEFAULT 0,
                    metadata_json  TEXT,
                    inserted_at    INTEGER NOT NULL,
                    expires_at     INTEGER,
                    size_bytes     INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS name_prefixes (
                    prefix_hash  BLOB NOT NULL,
                    name_hash    BLOB NOT NULL,
                    PRIMARY KEY (prefix_hash, name_hash)
                );
                CREATE INDEX IF NOT EXISTS idx_expires_at ON content(expires_at);
                CREATE INDEX IF NOT EXISTS idx_inserted_at ON content(inserted_at);
            """)

    def _recover_if_needed(self) -> None:
        """Run integrity check on startup to detect/correct corruption."""
        try:
            result = self._conn.execute("PRAGMA integrity_check").fetchone()
            if result and result[0] != "ok":
                # Attempt recovery by salvaging valid rows
                self._salvage_recovery()
        except sqlite3.DatabaseError:
            self._salvage_recovery()

    def _salvage_recovery(self) -> None:
        """Salvage valid rows from potentially corrupted DB."""
        # Create new clean tables
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS content_new (
                name_hash      BLOB PRIMARY KEY,
                name_bytes     BLOB NOT NULL,
                content_bytes  BLOB NOT NULL,
                content_hash   BLOB NOT NULL,
                sequence       INTEGER,
                freshness      INTEGER DEFAULT 1,
                age_seconds    INTEGER DEFAULT 0,
                metadata_json  TEXT,
                inserted_at    INTEGER NOT NULL,
                expires_at     INTEGER,
                size_bytes     INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS name_prefixes_new (
                prefix_hash  BLOB NOT NULL,
                name_hash    BLOB NOT NULL,
                PRIMARY KEY (prefix_hash, name_hash)
            );
        """)
        # Copy valid rows
        try:
            self._conn.execute("""
                INSERT INTO content_new SELECT * FROM content
            """)
            self._conn.execute("""
                INSERT INTO name_prefixes_new SELECT * FROM name_prefixes
            """)
        except sqlite3.DatabaseError:
            # Row-by-row salvage
            for row in self._conn.execute("SELECT * FROM content"):
                try:
                    self._conn.execute(
                        "INSERT INTO content_new VALUES (?,?,?,?,?,?,?,?,?,?,?)", row
                    )
                except sqlite3.DatabaseError:
                    pass
        # Swap tables
        self._conn.execute("DROP TABLE IF EXISTS content")
        self._conn.execute("DROP TABLE IF EXISTS name_prefixes")
        self._conn.execute("ALTER TABLE content_new RENAME TO content")
        self._conn.execute("ALTER TABLE name_prefixes_new RENAME TO name_prefixes")

    def _name_hash(self, name: Name) -> bytes:
        return hashlib.blake2b(name.to_bytes(), digest_size=32).digest()

    def _prefix_hash(self, name: Name) -> bytes:
        return hashlib.blake2b(name.to_bytes(), digest_size=32).digest()

    def _compute_ttl(self, name: Name) -> Optional[int]:
        """Compute TTL for a name based on prefix config."""
        # Check per-prefix TTLs (longest match first)
        name_str = str(name)
        for prefix_str, ttl in sorted(
            self._prefix_ttls.items(), key=lambda x: -len(x[0])
        ):
            if name_str.startswith(prefix_str):
                return ttl
        return self._default_ttl

    def _calculate_expires_at(self, inserted_at: int, ttl: Optional[int]) -> Optional[int]:
        if ttl is None or ttl <= 0:
            return None
        return inserted_at + ttl

    def insert(self, name: Name, data: Data) -> None:
        """Insert or update Data packet. Evicts LRU if over capacity."""
        name_bytes = name.to_bytes()
        name_hash = self._name_hash(name)
        content_bytes = data.content
        content_hash = data.metadata.content_hash or hashlib.blake2b(content_bytes, digest_size=32).digest()
        sequence = data.metadata.sequence
        freshness = 1 if data.metadata.freshness.fresh else 0
        age_seconds = 0 if data.metadata.freshness.fresh else data.metadata.freshness.age_seconds
        metadata_json = json.dumps({
            "content_hash": content_hash.hex(),
            "sequence": sequence,
            "fresh": data.metadata.freshness.fresh,
            "age_seconds": age_seconds,
        })
        inserted_at = int(time.time())
        ttl = self._compute_ttl(name)
        expires_at = self._calculate_expires_at(inserted_at, ttl)
        size_bytes = len(content_bytes)

        with self._conn:
            # Upsert content
            self._conn.execute("""
                INSERT INTO content (name_hash, name_bytes, content_bytes, content_hash,
                                     sequence, freshness, age_seconds, metadata_json,
                                     inserted_at, expires_at, size_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name_hash) DO UPDATE SET
                    name_bytes=excluded.name_bytes,
                    content_bytes=excluded.content_bytes,
                    content_hash=excluded.content_hash,
                    sequence=excluded.sequence,
                    freshness=excluded.freshness,
                    age_seconds=excluded.age_seconds,
                    metadata_json=excluded.metadata_json,
                    inserted_at=excluded.inserted_at,
                    expires_at=excluded.expires_at,
                    size_bytes=excluded.size_bytes
            """, (name_hash, name_bytes, content_bytes, content_hash,
                  sequence, freshness, age_seconds, metadata_json,
                  inserted_at, expires_at, size_bytes))

            # Update prefix index
            self._conn.execute("DELETE FROM name_prefixes WHERE name_hash = ?", (name_hash,))
            # Add all prefixes of this name
            for i in range(1, name.len() + 1):
                prefix = Name(name.components[0], name.components[1:i])
                prefix_hash = self._prefix_hash(prefix)
                self._conn.execute("""
                    INSERT OR IGNORE INTO name_prefixes (prefix_hash, name_hash)
                    VALUES (?, ?)
                """, (prefix_hash, name_hash))

            # LRU eviction if over capacity
            count = self._conn.execute("SELECT COUNT(*) FROM content").fetchone()[0]
            if count > self._max:
                to_evict = count - self._max
                self._evict_lru_internal(to_evict)

    def _evict_lru_internal(self, count: int) -> int:
        """Evict oldest entries (by inserted_at). Returns number evicted."""
        if count <= 0:
            return 0
        rows = self._conn.execute(
            "SELECT name_hash FROM content ORDER BY inserted_at ASC LIMIT ?",
            (count,)
        ).fetchall()
        evicted = 0
        for (name_hash,) in rows:
            self._conn.execute("DELETE FROM content WHERE name_hash = ?", (name_hash,))
            self._conn.execute("DELETE FROM name_prefixes WHERE name_hash = ?", (name_hash,))
            evicted += 1
        return evicted

    def get(self, name: Name) -> Optional[Data]:
        """Get Data by exact name match."""
        self._purge_expired_internal()

        name_hash = self._name_hash(name)
        row = self._conn.execute(
            "SELECT name_bytes, content_bytes, metadata_json FROM content WHERE name_hash = ?",
            (name_hash,)
        ).fetchone()

        if row is None:
            self._misses += 1
            return None

        name_bytes, content_bytes, metadata_json = row
        stored_name = Name.from_bytes(name_bytes)
        if not self._verify_content(content_bytes, metadata_json):
            self._misses += 1
            return None

        self._hits += 1
        metadata = self._parse_metadata(metadata_json)
        return Data(name=stored_name, content=content_bytes, metadata=metadata)

    def get_prefix(self, prefix: Name) -> Optional[Data]:
        """Longest prefix match: find stored name that has this prefix."""
        self._purge_expired_internal()

        # Find all names that have this prefix
        prefix_hash = self._prefix_hash(prefix)
        rows = self._conn.execute("""
            SELECT c.name_bytes, c.content_bytes, c.metadata_json
            FROM content c
            JOIN name_prefixes np ON c.name_hash = np.name_hash
            WHERE np.prefix_hash = ?
            ORDER BY c.inserted_at DESC
        """, (prefix_hash,)).fetchall()

        if not rows:
            self._misses += 1
            return None

        # Return first valid match (newest first)
        for name_bytes, content_bytes, metadata_json in rows:
            if self._verify_content(content_bytes, metadata_json):
                stored_name = Name.from_bytes(name_bytes)
                metadata = self._parse_metadata(metadata_json)
                self._hits += 1
                return Data(name=stored_name, content=content_bytes, metadata=metadata)

        self._misses += 1
        return None

    def _verify_content(self, content_bytes: bytes, metadata_json: str) -> bool:
        try:
            meta = json.loads(metadata_json)
            expected_hash = bytes.fromhex(meta["content_hash"])
            actual_hash = hashlib.blake2b(content_bytes, digest_size=32).digest()
            return actual_hash == expected_hash
        except (json.JSONDecodeError, KeyError, ValueError):
            return False

    def _parse_metadata(self, metadata_json: str) -> DataMetadata:
        meta = json.loads(metadata_json)
        freshness_fresh = meta.get("fresh", True)
        freshness_age = meta.get("age_seconds", 0)
        freshness = Freshness(fresh=freshness_fresh, age_seconds=freshness_age)
        return DataMetadata(
            content_hash=bytes.fromhex(meta["content_hash"]),
            sequence=meta.get("sequence"),
            freshness=freshness,
        )

    def _purge_expired_internal(self) -> int:
        """Delete expired entries. Returns count."""
        now = int(time.time())
        rows = self._conn.execute(
            "SELECT name_hash FROM content WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (now,)
        ).fetchall()
        count = 0
        for (name_hash,) in rows:
            self._conn.execute("DELETE FROM content WHERE name_hash = ?", (name_hash,))
            self._conn.execute("DELETE FROM name_prefixes WHERE name_hash = ?", (name_hash,))
            count += 1
        return count

    def purge_expired(self) -> int:
        """Public method to purge expired entries."""
        with self._conn:
            return self._purge_expired_internal()

    def evict_lru(self, target_count: int) -> int:
        """Evict LRU entries to reach target. Returns evicted count."""
        with self._conn:
            current = self._conn.execute("SELECT COUNT(*) FROM content").fetchone()[0]
            to_evict = max(0, current - target_count)
            return self._evict_lru_internal(to_evict)

    def remove(self, name: Name) -> bool:
        """Remove a specific entry. Returns True if existed."""
        name_hash = self._name_hash(name)
        with self._conn:
            cur = self._conn.execute("DELETE FROM content WHERE name_hash = ?", (name_hash,))
            self._conn.execute("DELETE FROM name_prefixes WHERE name_hash = ?", (name_hash,))
            return cur.rowcount > 0

    def contains(self, name: Name) -> bool:
        name_hash = self._name_hash(name)
        return self._conn.execute(
            "SELECT 1 FROM content WHERE name_hash = ?", (name_hash,)
        ).fetchone() is not None

    def __len__(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM content").fetchone()[0]

    @property
    def hits(self) -> int:
        return self._hits

    @property
    def misses(self) -> int:
        return self._misses

    @property
    def capacity(self) -> int:
        return self._max

    def __len__(self) -> int:
        """Number of entries currently stored."""
        row = self._conn.execute("SELECT COUNT(*) FROM content").fetchone()
        return row[0] if row else 0

    @property
    def size_bytes(self) -> int:
        row = self._conn.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM content").fetchone()
        return row[0] if row else 0

    def close(self) -> None:
        self._conn.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    # For ICNServer compatibility
    @property
    def _entries(self) -> Dict[Name, Data]:
        """Compatibility: return all entries as dict (for manifest building)."""
        entries = {}
        for row in self._conn.execute("SELECT name_bytes, content_bytes, metadata_json FROM content"):
            name_bytes, content_bytes, metadata_json = row
            if self._verify_content(content_bytes, metadata_json):
                name = Name.from_bytes(name_bytes)
                metadata = self._parse_metadata(metadata_json)
                entries[name] = Data(name=name, content=content_bytes, metadata=metadata)
        return entries