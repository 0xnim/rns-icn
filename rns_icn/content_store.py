"""ContentStore — SQLite-backed persistent cache of Data packets."""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from .name import Name
from .packet import ChildSelector, Data, DataMetadata, Freshness


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
        path: str | None = None,
        max_entries: int = 10000,
        default_ttl: int | None = None,
        prefix_ttls: dict[str, int] | None = None,
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
        self._migrate_schema()
        self._recover_if_needed()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS content (
                    name_hash        BLOB PRIMARY KEY,
                    name_bytes       BLOB NOT NULL,
                    content_bytes    BLOB NOT NULL,
                    content_hash     BLOB NOT NULL,
                    sequence         INTEGER,
                    freshness        INTEGER DEFAULT 1,
                    age_seconds      INTEGER DEFAULT 0,
                    freshness_period INTEGER,
                    metadata_json    TEXT,
                    inserted_at      INTEGER NOT NULL,
                    expires_at       INTEGER,
                    size_bytes       INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS name_prefixes (
                    prefix_hash  BLOB NOT NULL,
                    name_hash    BLOB NOT NULL,
                    PRIMARY KEY (prefix_hash, name_hash)
                );
                CREATE INDEX IF NOT EXISTS idx_expires_at ON content(expires_at);
                CREATE INDEX IF NOT EXISTS idx_inserted_at ON content(inserted_at);
            """)

    def _migrate_schema(self) -> None:
        """Add columns introduced after the initial schema to existing DBs."""
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(content)")}
        with self._conn:
            if "freshness_period" not in cols:
                self._conn.execute(
                    "ALTER TABLE content ADD COLUMN freshness_period INTEGER"
                )

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
                name_hash        BLOB PRIMARY KEY,
                name_bytes       BLOB NOT NULL,
                content_bytes    BLOB NOT NULL,
                content_hash     BLOB NOT NULL,
                sequence         INTEGER,
                freshness        INTEGER DEFAULT 1,
                age_seconds      INTEGER DEFAULT 0,
                freshness_period INTEGER,
                metadata_json    TEXT,
                inserted_at      INTEGER NOT NULL,
                expires_at       INTEGER,
                size_bytes       INTEGER NOT NULL
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
                with contextlib.suppress(sqlite3.DatabaseError):
                    self._conn.execute(
                        "INSERT INTO content_new VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", row
                    )
        # Swap tables
        self._conn.execute("DROP TABLE IF EXISTS content")
        self._conn.execute("DROP TABLE IF EXISTS name_prefixes")
        self._conn.execute("ALTER TABLE content_new RENAME TO content")
        self._conn.execute("ALTER TABLE name_prefixes_new RENAME TO name_prefixes")

    def _name_hash(self, name: Name) -> bytes:
        return hashlib.blake2b(name.to_bytes(), digest_size=32).digest()

    def _prefix_hash(self, name: Name) -> bytes:
        return hashlib.blake2b(name.to_bytes(), digest_size=32).digest()

    def _compute_ttl(self, name: Name) -> int | None:
        """Compute TTL for a name based on prefix config."""
        # Check per-prefix TTLs (longest match first)
        name_str = str(name)
        for prefix_str, ttl in sorted(
            self._prefix_ttls.items(), key=lambda x: -len(x[0])
        ):
            if name_str.startswith(prefix_str):
                return ttl
        return self._default_ttl

    def _calculate_expires_at(self, inserted_at: int, ttl: int | None) -> int | None:
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
        freshness_period = data.metadata.freshness_period
        metadata_json = json.dumps({
            "content_hash": content_hash.hex(),
            "sequence": sequence,
            "fresh": data.metadata.freshness.fresh,
            "age_seconds": age_seconds,
            "freshness_period": freshness_period,
            "signed_at": data.metadata.signed_at,
            "encrypted": data.metadata.encrypted,
            "signature": data.signature.hex() if data.signature is not None else None,
        })
        inserted_at = int(time.time())
        ttl = self._compute_ttl(name)
        expires_at = self._calculate_expires_at(inserted_at, ttl)
        size_bytes = len(content_bytes)

        with self._conn:
            # Upsert content
            self._conn.execute("""
                INSERT INTO content (name_hash, name_bytes, content_bytes, content_hash,
                                     sequence, freshness, age_seconds, freshness_period,
                                     metadata_json, inserted_at, expires_at, size_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name_hash) DO UPDATE SET
                    name_bytes=excluded.name_bytes,
                    content_bytes=excluded.content_bytes,
                    content_hash=excluded.content_hash,
                    sequence=excluded.sequence,
                    freshness=excluded.freshness,
                    age_seconds=excluded.age_seconds,
                    freshness_period=excluded.freshness_period,
                    metadata_json=excluded.metadata_json,
                    inserted_at=excluded.inserted_at,
                    expires_at=excluded.expires_at,
                    size_bytes=excluded.size_bytes
            """, (name_hash, name_bytes, content_bytes, content_hash,
                  sequence, freshness, age_seconds, freshness_period,
                  metadata_json, inserted_at, expires_at, size_bytes))

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

    def get(self, name: Name) -> Data | None:
        """Get Data by exact name match."""
        self._purge_expired_internal()

        name_hash = self._name_hash(name)
        row = self._conn.execute(
            "SELECT name_bytes, content_bytes, metadata_json, inserted_at, "
            "freshness_period FROM content WHERE name_hash = ?",
            (name_hash,)
        ).fetchone()

        if row is None:
            self._misses += 1
            return None

        name_bytes, content_bytes, metadata_json, inserted_at, freshness_period = row
        stored_name = Name.from_bytes(name_bytes)
        if not self._verify_content(content_bytes, metadata_json):
            self._misses += 1
            return None

        self._hits += 1
        metadata = self._parse_metadata(metadata_json, inserted_at, freshness_period)
        signature = self._parse_signature(metadata_json)
        return Data(name=stored_name, content=content_bytes,
                    signature=signature, metadata=metadata)

    def get_prefix(
        self,
        prefix: Name,
        child: ChildSelector = ChildSelector.NONE,
        min_sequence: int | None = None,
    ) -> Data | None:
        """Prefix match: find a stored name under this prefix.

        With ``child`` NONE the newest-inserted match wins (legacy behaviour).
        With LATEST/OLDEST the match is chosen by Data ``sequence`` — the
        highest (latest) or lowest (oldest) sequenced entry under the prefix —
        which is what answers a ``latest``/``oldest`` selector. Only sequenced
        entries are candidates for child selection. ``min_sequence``, when set,
        restricts candidates to ``sequence >= min_sequence``.
        """
        self._purge_expired_internal()

        # Find all names that have this prefix
        prefix_hash = self._prefix_hash(prefix)
        params: list[Any] = [prefix_hash]
        where = "WHERE np.prefix_hash = ?"
        if child is not ChildSelector.NONE:
            # Child selection ranks by sequence, so only sequenced entries qualify.
            where += " AND c.sequence IS NOT NULL"
            if min_sequence is not None:
                where += " AND c.sequence >= ?"
                params.append(min_sequence)
            order = "c.sequence DESC" if child is ChildSelector.LATEST else "c.sequence ASC"
            order += ", c.inserted_at DESC"
        else:
            order = "c.inserted_at DESC"

        # `where`/`order` are built from constants and the enum, never user text;
        # all value bindings go through `params`.
        rows = self._conn.execute(f"""
            SELECT c.name_bytes, c.content_bytes, c.metadata_json,
                   c.inserted_at, c.freshness_period
            FROM content c
            JOIN name_prefixes np ON c.name_hash = np.name_hash
            {where}
            ORDER BY {order}
        """, params).fetchall()

        if not rows:
            self._misses += 1
            return None

        # Return first valid match in the chosen order (newest-inserted, or the
        # sequence extreme when a child selector is set).
        for name_bytes, content_bytes, metadata_json, inserted_at, freshness_period in rows:
            if self._verify_content(content_bytes, metadata_json):
                stored_name = Name.from_bytes(name_bytes)
                metadata = self._parse_metadata(metadata_json, inserted_at, freshness_period)
                signature = self._parse_signature(metadata_json)
                self._hits += 1
                return Data(name=stored_name, content=content_bytes,
                            signature=signature, metadata=metadata)

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

    def _parse_signature(self, metadata_json: str) -> bytes | None:
        sig_hex = json.loads(metadata_json).get("signature")
        return bytes.fromhex(sig_hex) if sig_hex else None

    def _parse_metadata(
        self,
        metadata_json: str,
        inserted_at: int | None = None,
        freshness_period: int | None = None,
    ) -> DataMetadata:
        """Reconstruct metadata, computing freshness dynamically from age.

        Freshness is the producer's stored ``fresh`` flag AND, when a
        ``freshness_period`` was declared, whether the entry has been held for
        less than that period. ``age_seconds`` always reflects the entry's real
        age in the cache so callers (e.g. stale-while-revalidate) can reason
        about how stale it is.
        """
        meta = json.loads(metadata_json)
        stored_fresh = meta.get("fresh", True)
        if freshness_period is None:
            freshness_period = meta.get("freshness_period")

        if inserted_at is not None:
            age = max(0, int(time.time()) - int(inserted_at))
        else:
            age = meta.get("age_seconds", 0)

        within_period = freshness_period is None or age < freshness_period
        fresh = stored_fresh and within_period
        freshness = Freshness(fresh=fresh, age_seconds=age)
        return DataMetadata(
            content_hash=bytes.fromhex(meta["content_hash"]),
            sequence=meta.get("sequence"),
            freshness=freshness,
            freshness_period=freshness_period,
            signed_at=meta.get("signed_at"),
            encrypted=meta.get("encrypted", False),
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

    def invalidate(self, name: Name, prefix: bool = False) -> int:
        """Purge an exact name (or, if ``prefix``, every name under it).

        Returns the number of entries removed. Used by the cache-invalidation
        protocol so a producer can actively evict stale content from caches.
        """
        with self._conn:
            if not prefix:
                cur = self._conn.execute(
                    "DELETE FROM content WHERE name_hash = ?", (self._name_hash(name),)
                )
                self._conn.execute(
                    "DELETE FROM name_prefixes WHERE name_hash = ?",
                    (self._name_hash(name),)
                )
                return cur.rowcount

            prefix_hash = self._prefix_hash(name)
            rows = self._conn.execute(
                "SELECT name_hash FROM name_prefixes WHERE prefix_hash = ?",
                (prefix_hash,)
            ).fetchall()
            removed = 0
            for (name_hash,) in rows:
                cur = self._conn.execute(
                    "DELETE FROM content WHERE name_hash = ?", (name_hash,)
                )
                self._conn.execute(
                    "DELETE FROM name_prefixes WHERE name_hash = ?", (name_hash,)
                )
                removed += cur.rowcount
            return removed

    def contains(self, name: Name) -> bool:
        name_hash = self._name_hash(name)
        return self._conn.execute(
            "SELECT 1 FROM content WHERE name_hash = ?", (name_hash,)
        ).fetchone() is not None

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
        with contextlib.suppress(Exception):
            self.close()

    # For ICNServer compatibility
    @property
    def _entries(self) -> dict[Name, Data]:
        """Compatibility: return all entries as dict (for manifest building)."""
        entries = {}
        for row in self._conn.execute("SELECT name_bytes, content_bytes, metadata_json FROM content"):
            name_bytes, content_bytes, metadata_json = row
            if self._verify_content(content_bytes, metadata_json):
                name = Name.from_bytes(name_bytes)
                metadata = self._parse_metadata(metadata_json)
                entries[name] = Data(name=name, content=content_bytes, metadata=metadata)
        return entries