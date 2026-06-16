"""Chunker — split content into named, hashed chunks for ICN transport.

Produces a ContentManifest and one Data packet per chunk, each with
its own blake2b content hash for independent integrity verification.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass

from .manifest import ChunkRef, ContentManifest
from .name import Name
from .packet import Data

DEFAULT_CHUNK_SIZE = 64 * 1024  # 64 KB
CHUNK_LABEL_PAD = 4  # zero-padded width for chunk indices


class ChunkerError(Exception):
    """Raised when content cannot be chunked."""
    ...


class EmptyContentError(ChunkerError):
    """Raised when attempting to chunk zero-length content."""
    ...


@dataclass
class ChunkResult:
    """Result of a chunking operation."""
    manifest: ContentManifest
    data_packets: list[Data]

    def chunk_count(self) -> int:
        return len(self.data_packets)

    def data_for_label(self, label: str) -> Data | None:
        for dp in self.data_packets:
            if label in str(dp.name):
                return dp
        return None


def _compute_blake2b(data: bytes) -> bytes:
    return hashlib.blake2b(data, digest_size=32).digest()


def _label(index: int) -> str:
    """Format a zero-padded chunk label."""
    return f"chunk_{index:0{CHUNK_LABEL_PAD}d}"


def chunk_content(
    content: bytes,
    name: Name,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    sequence: int = 1,
    signer: Callable[[bytes], bytes] | None = None,
) -> ChunkResult:
    """Split *content* into chunks and produce a ContentManifest + Data packets.

    Args:
        content: The raw bytes to chunk.
        name: The canonical Name of the overall content (used as the prefix
            for each chunk's Name).
        chunk_size: Maximum bytes per chunk (default 64 KB).
        sequence: Monotonic version for the ContentManifest.
        signer: Optional producer signer (typically ``RNS.Identity.sign``).
            When provided, each chunk Data packet is signed so that streamed
            large files are per-chunk verifiable end-to-end — a relay or cache
            cannot substitute chunks without breaking the producer signature.
            Uses the same per-packet Ed25519 scheme as single-fetch Data
            (`Data.sign`), so signatures persist in the content store and
            caches re-serve verifiable chunks.

    Returns:
        ChunkResult with manifest and one Data packet per chunk.

    Raises:
        EmptyContentError: If content is empty (zero bytes).
    """
    if not content:
        raise EmptyContentError("cannot chunk empty content")

    chunks: list[ChunkRef] = []
    data_packets: list[Data] = []
    offset = 0
    idx = 0

    while offset < len(content):
        chunk_bytes = content[offset : offset + chunk_size]
        chunk_hash = _compute_blake2b(chunk_bytes)
        label = _label(idx)

        ref = ChunkRef(
            label=label,
            content_hash=chunk_hash,
            size=len(chunk_bytes),
            sequence=idx,
        )
        chunks.append(ref)

        # Each chunk gets its own Name: /<rns>/<path...>/<label>?hash=<chunk_hash>
        chunk_name = Name(
            name.rns_addr,
            [*name.components[1:], label.encode("utf-8")],
            content_hash=chunk_hash,
        )
        data = Data.new(name=chunk_name, content=chunk_bytes)
        data.with_sequence(idx)
        if signer is not None:
            data.sign(signer)
        data_packets.append(data)

        offset += chunk_size
        idx += 1

    # Overall content hash (blake2b of complete content)
    content_hash = _compute_blake2b(content)

    manifest = ContentManifest.create(
        name=name,
        chunks=chunks,
        content_hash=content_hash,
        sequence=sequence,
    )

    return ChunkResult(manifest=manifest, data_packets=data_packets)
