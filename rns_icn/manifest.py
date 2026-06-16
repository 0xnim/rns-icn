"""Manifest — content discovery mechanism.

A Manifest is a signed index published by a producer listing available content.
Published at /<rns-addr>/manifest. Versioned with monotonic sequence numbers.
Uses JSON for debug-readability (CBOR later when it matters).
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum

from .name import RNS_ADDR_BYTES, Name
from .packet import Data


class ManifestError(Exception):
    ...


class ContentManifestError(Exception):
    ...


@dataclass
class ChunkRef:
    """A reference to a single chunk of larger content.

    Ordered list of chunks constitutes the full content. Each chunk
    is content-addressed by its blake2b hash for integrity verification.
    """
    label: str                                    # e.g. "part_000", "chunk_42"
    content_hash: bytes                           # 32-byte blake2b
    size: int                                     # chunk byte size
    sequence: int | None = None                # explicit ordering (ties into stream seq nums)

    def to_dict(self) -> dict:
        d = {
            "label": self.label,
            "content_hash": self.content_hash.hex(),
            "size": self.size,
        }
        if self.sequence is not None:
            d["sequence"] = self.sequence
        return d

    @classmethod
    def from_dict(cls, d: dict) -> ChunkRef:
        return cls(
            label=d["label"],
            content_hash=bytes.fromhex(d["content_hash"]),
            size=d["size"],
            sequence=d.get("sequence"),
        )


@dataclass
class ContentManifest:
    """Manifest for large content split into named, hashed chunks.

    A ContentManifest is itself published as Data (at /<rns-addr>/<name>)
    and tells a consumer how to reconstruct the full piece: fetch each
    chunk by label, verify each by its hash, reassemble in sequence.

    ``content_hash`` is the blake2b of the *complete* content (all chunks
    concatenated in sequence order), for end-to-end integrity checking.
    """
    name: Name                                    # canonical name of the overall content
    chunks: list[ChunkRef]                        # ordered chunk references
    total_size: int                               # sum of chunk sizes
    content_hash: bytes | None = None          # blake2b of full content (32 bytes)
    sequence: int = 1                             # monotonic version
    timestamp: int = 0                            # unix seconds (set by factory)

    def to_dict(self) -> dict:
        d = {
            "name": str(self.name),
            "chunks": [c.to_dict() for c in self.chunks],
            "total_size": self.total_size,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
        }
        if self.content_hash is not None:
            d["content_hash"] = self.content_hash.hex()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> ContentManifest:
        return cls(
            name=_parse_name(d["name"]),
            chunks=[ChunkRef.from_dict(c) for c in d["chunks"]],
            total_size=d["total_size"],
            content_hash=bytes.fromhex(d["content_hash"]) if d.get("content_hash") else None,
            sequence=d.get("sequence", 1),
            timestamp=d.get("timestamp", 0),
        )

    @classmethod
    def from_data(cls, data: Data) -> ContentManifest:
        cm = cls.from_dict(json.loads(data.content.decode("utf-8")))
        if data.metadata.content_hash is not None:
            actual = hashlib.blake2b(data.content, digest_size=32).digest()
            if data.metadata.content_hash != actual:
                raise ContentManifestError("content hash mismatch")
        return cm

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    def chunk_count(self) -> int:
        return len(self.chunks)

    def find_chunk_by_label(self, label: str) -> ChunkRef | None:
        for c in self.chunks:
            if c.label == label:
                return c
        return None

    def labels(self) -> list[str]:
        """Ordered list of chunk labels."""
        return [c.label for c in self.chunks]

    @classmethod
    def create(cls, name: Name, chunks: list[ChunkRef],
               content_hash: bytes | None = None,
               sequence: int = 1) -> ContentManifest:
        total_size = sum(c.size for c in chunks)
        return cls(
            name=name,
            chunks=chunks,
            total_size=total_size,
            content_hash=content_hash,
            sequence=sequence,
            timestamp=int(time.time()),
        )


class EntryKind(str, Enum):
    BLOB = "blob"
    STREAM = "stream"
    MANIFEST = "manifest"


@dataclass
class ManifestEntry:
    kind: EntryKind
    label: str
    name: Name
    content_hash: bytes | None = None
    size: int | None = None
    # Stream metadata (for STREAM entries)
    latest_sequence: int | None = None
    total_items: int | None = None
    start_time: int | None = None   # Unix timestamp of first item
    end_time: int | None = None     # Unix timestamp of last item

    def to_dict(self) -> dict:
        d: dict[str, object] = {"kind": self.kind.value, "label": self.label, "name": str(self.name)}
        if self.content_hash is not None:
            d["content_hash"] = self.content_hash.hex()
        if self.size is not None:
            d["size"] = self.size
        if self.latest_sequence is not None:
            d["latest_sequence"] = self.latest_sequence
        if self.total_items is not None:
            d["total_items"] = self.total_items
        if self.start_time is not None:
            d["start_time"] = self.start_time
        if self.end_time is not None:
            d["end_time"] = self.end_time
        return d

    @classmethod
    def from_dict(cls, d: dict) -> ManifestEntry:
        return cls(
            kind=EntryKind(d["kind"]),
            label=d["label"],
            name=_parse_name(d["name"]),
            content_hash=bytes.fromhex(d["content_hash"]) if d.get("content_hash") else None,
            size=d.get("size"),
            latest_sequence=d.get("latest_sequence"),
            total_items=d.get("total_items"),
            start_time=d.get("start_time"),
            end_time=d.get("end_time"),
        )


@dataclass
class Manifest:
    producer: bytes  # 16-byte RNS address
    sequence: int
    timestamp: int
    entries: list[ManifestEntry] = field(default_factory=list)
    previous: Name | None = None

    def manifest_name(self) -> Name:
        return Name(self.producer, [b"manifest"])

    def to_dict(self) -> dict:
        d = {"producer": self.producer.hex(), "sequence": self.sequence,
             "timestamp": self.timestamp, "entries": [e.to_dict() for e in self.entries]}
        if self.previous is not None:
            d["previous"] = str(self.previous)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> Manifest:
        return cls(
            producer=bytes.fromhex(d["producer"]),
            sequence=d["sequence"],
            timestamp=d["timestamp"],
            entries=[ManifestEntry.from_dict(e) for e in d["entries"]],
            previous=_parse_name(d["previous"]) if d.get("previous") else None,
        )

    @classmethod
    def from_data(cls, data: Data) -> Manifest:
        m = cls.from_dict(json.loads(data.content.decode("utf-8")))
        if data.metadata.content_hash is not None:
            actual = hashlib.blake2b(data.content, digest_size=32).digest()
            if data.metadata.content_hash != actual:
                raise ManifestError("content hash mismatch")
        return m

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    def find(self, label: str) -> ManifestEntry | None:
        for e in self.entries:
            if e.label == label:
                return e
        return None

    def is_newer_than(self, seq: int) -> bool:
        return self.sequence > seq

    @classmethod
    def create(cls, producer: bytes, entries: list[ManifestEntry],
               sequence: int = 1, previous: Name | None = None) -> Manifest:
        return cls(
            producer=producer,
            sequence=sequence,
            timestamp=int(time.time()),
            entries=entries,
            previous=previous,
        )


# ── Hierarchical manifest utilities ──


def flatten_content_directory(manifest: Manifest) -> list[ManifestEntry]:
    """Flatten a hierarchical manifest into a flat content directory.

    Takes a manifest that may contain MANIFEST-kind entries (references to
    downstream manifests) and returns only the BLOB and STREAM entries,
    filtering out MANIFEST references. This gives a root server a flat
    view of all available content in the subtree.

    Args:
        manifest: A potentially hierarchical manifest with MANIFEST entries.

    Returns:
        List of non-MANIFEST ManifestEntry objects (BLOB and STREAM only).
    """
    directory: list[ManifestEntry] = []
    seen_labels: set[str] = set()
    for entry in manifest.entries:
        # Skip MANIFEST references; deduplicate the rest by label (the prefixed
        # label like "peer:aa.../sensor") so content doesn't appear twice.
        if entry.kind != EntryKind.MANIFEST and entry.label not in seen_labels:
            seen_labels.add(entry.label)
            directory.append(entry)
    return directory


def _parse_name(s: str) -> Name:
    """Parse a Name from its display format."""
    s = s.lstrip("/")
    content_hash = None
    if "?hash=" in s:
        qpos = s.index("?hash=")
        hash_hex = s[qpos + 6:]
        content_hash = bytes.fromhex(hash_hex)
        s = s[:qpos]
    parts = [p for p in s.split("/") if p]
    if not parts:
        raise ManifestError("empty name")
    rns_addr = bytes.fromhex(parts[0])
    if len(rns_addr) != RNS_ADDR_BYTES:
        raise ManifestError(f"RNS address must be {RNS_ADDR_BYTES} bytes")
    path = []
    for p in parts[1:]:
        try:
            path.append(bytes.fromhex(p))
        except ValueError:
            path.append(p.encode("utf-8"))
    return Name(rns_addr, path, content_hash)
