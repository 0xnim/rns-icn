"""Assembler — reassemble chunked content from ICN Data packets with integrity verification.

Takes a ContentManifest and a dict of Data packets (keyed by chunk label),
reconstructs the original content, verifies each chunk's hash against the
manifest, and optionally verifies the overall content hash.
"""

from __future__ import annotations

import hashlib

from .manifest import ChunkRef, ContentManifest
from .packet import Data


class AssemblyError(Exception):
    """Base error for assembly failures."""
    ...


class MissingChunkError(AssemblyError):
    """A required chunk was not provided."""
    ...


class HashMismatchError(AssemblyError):
    """A chunk's content hash did not match the manifest's ChunkRef."""
    ...


class IntegrityError(AssemblyError):
    """The overall reassembled content hash did not match the manifest."""
    ...


def _compute_blake2b(data: bytes) -> bytes:
    return hashlib.blake2b(data, digest_size=32).digest()


def assemble(
    manifest: ContentManifest,
    chunks: dict[str, Data],
) -> bytes:
    """Reassemble original content from chunk Data packets.

    For each ChunkRef in the manifest's ordered chunk list:
      1. Requires a Data packet in *chunks* keyed by the chunk's label.
      2. Verifies the Data packet's content_hash against the ChunkRef's
         content_hash (strict verification — mismatch raises HashMismatchError).
      3. Appends the Data's content in order.

    After all chunks are reassembled, if the manifest carries an optional
    overall *content_hash*, verifies against the blake2b of the full
    reconstructed content.

    Args:
        manifest: The ContentManifest describing chunk layout.
        chunks: Dict mapping chunk label → Data packet.

    Returns:
        The fully reassembled original content as bytes.

    Raises:
        MissingChunkError: If a chunk from the manifest is missing.
        HashMismatchError: If a chunk's content hash doesn't match.
        IntegrityError: If the overall content hash doesn't match.
    """
    total = bytearray()

    for ref in manifest.chunks:
        data = _require_chunk(manifest, ref, chunks)

        # Strict hash verification: Data's content hash must match ChunkRef
        _verify_chunk_hash(ref, data)

        total.extend(data.content)

    # Verify end-to-end integrity
    _verify_content_hash(manifest, bytes(total))

    return bytes(total)


def assemble_verified(
    manifest: ContentManifest,
    chunks: dict[str, Data],
) -> bytes:
    """Reassemble with ONLY chunk-level hash verification (no overall check).

    Use this when the manifest doesn't carry an overall content_hash, or
    when you trust per-chunk hashes but don't have the full content hash.

    Same errors as assemble() except IntegrityError is never raised.
    """
    total = bytearray()

    for ref in manifest.chunks:
        data = _require_chunk(manifest, ref, chunks)
        _verify_chunk_hash(ref, data)
        total.extend(data.content)

    return bytes(total)


def assemble_fast(
    manifest: ContentManifest,
    chunks: dict[str, Data],
) -> bytes:
    """Reassemble WITHOUT any hash verification.

    Fast path for trusted environments (local cache rebuilds, testing).
    Skips all integrity checks. Only raises MissingChunkError.
    """
    total = bytearray()

    for ref in manifest.chunks:
        data = _require_chunk(manifest, ref, chunks)
        total.extend(data.content)

    return bytes(total)


def verify_chunk(
    ref: ChunkRef,
    data: Data,
) -> bool:
    """Verify a single chunk Data against its ChunkRef.

    Returns True if the Data's content_hash matches the ChunkRef's
    content_hash AND the raw content's blake2b matches.
    """
    if data.metadata.content_hash is None:
        return False
    if data.metadata.content_hash != ref.content_hash:
        return False
    actual = _compute_blake2b(data.content)
    return actual == ref.content_hash


def verify_chunks(
    manifest: ContentManifest,
    chunks: dict[str, Data],
) -> dict[str, bool]:
    """Verify all chunks in a manifest against their Data packets.

    Returns a dict mapping label → verification result (True/False).
    Missing chunks are reported as False.
    """
    results: dict[str, bool] = {}
    for ref in manifest.chunks:
        data = chunks.get(ref.label)
        if data is None:
            results[ref.label] = False
        else:
            results[ref.label] = verify_chunk(ref, data)
    return results


def missing_labels(
    manifest: ContentManifest,
    chunks: dict[str, Data],
) -> list[str]:
    """Return the labels of chunks required by manifest but missing from *chunks*."""
    return [ref.label for ref in manifest.chunks if ref.label not in chunks]


# ── Internal helpers ──


def _require_chunk(
    manifest: ContentManifest,
    ref: ChunkRef,
    chunks: dict[str, Data],
) -> Data:
    data = chunks.get(ref.label)
    if data is None:
        raise MissingChunkError(
            f"Missing chunk '{ref.label}' for content {manifest.name}"
        )
    return data


def _verify_chunk_hash(ref: ChunkRef, data: Data) -> None:
    """Raise HashMismatchError if the Data's content doesn't match the ChunkRef."""
    if data.metadata.content_hash is None:
        raise HashMismatchError(
            f"Chunk '{ref.label}' has no content_hash in Data metadata"
        )
    if data.metadata.content_hash != ref.content_hash:
        raise HashMismatchError(
            f"Chunk '{ref.label}': Data metadata content_hash mismatch "
            f"(expected {ref.content_hash.hex()[:16]}..., "
            f"got {data.metadata.content_hash.hex()[:16]}...)"
        )
    actual = _compute_blake2b(data.content)
    if actual != ref.content_hash:
        raise HashMismatchError(
            f"Chunk '{ref.label}': content blake2b mismatch "
            f"(expected {ref.content_hash.hex()[:16]}..., "
            f"got {actual.hex()[:16]}...)"
        )


def _verify_content_hash(manifest: ContentManifest, content: bytes) -> None:
    """Raise IntegrityError if the overall content hash doesn't match."""
    if manifest.content_hash is None:
        return
    actual = _compute_blake2b(content)
    if actual != manifest.content_hash:
        raise IntegrityError(
            f"Content hash mismatch for {manifest.name}: "
            f"expected {manifest.content_hash.hex()[:16]}..., "
            f"got {actual.hex()[:16]}..."
        )
