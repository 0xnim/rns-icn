"""Assembler — reassemble chunked content from ICN Data packets with integrity verification.

Takes a ContentManifest and a dict of Data packets (keyed by chunk label),
reconstructs the original content, verifies each chunk's hash against the
manifest, optionally verifies each chunk's producer signature, and optionally
verifies the overall content hash.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable

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


class SignatureError(AssemblyError):
    """A chunk's producer signature was missing or did not verify."""
    ...


# Validator signature mirrors ``Data.verify_signature`` / ``RNS.Identity.validate``:
# (signature_bytes, signed_message) -> bool.
Validator = Callable[[bytes, bytes], bool]


def _compute_blake2b(data: bytes) -> bytes:
    return hashlib.blake2b(data, digest_size=32).digest()


def assemble(
    manifest: ContentManifest,
    chunks: dict[str, Data],
    validator: Validator | None = None,
) -> bytes:
    """Reassemble original content from chunk Data packets.

    For each ChunkRef in the manifest's ordered chunk list:
      1. Requires a Data packet in *chunks* keyed by the chunk's label.
      2. Verifies the Data packet's content_hash against the ChunkRef's
         content_hash (strict verification — mismatch raises HashMismatchError).
      3. If *validator* is given, verifies the chunk's producer signature
         (mismatch or missing signature raises SignatureError).
      4. Appends the Data's content in order.

    After all chunks are reassembled, if the manifest carries an optional
    overall *content_hash*, verifies against the blake2b of the full
    reconstructed content.

    Args:
        manifest: The ContentManifest describing chunk layout.
        chunks: Dict mapping chunk label → Data packet.
        validator: Optional producer signature validator (typically
            ``RNS.Identity.validate`` for the producer recalled from the
            chunk's ``name.rns_addr``). When given, every chunk must carry a
            valid signature — defends streamed large files against chunk
            substitution by a relay or cache.

    Returns:
        The fully reassembled original content as bytes.

    Raises:
        MissingChunkError: If a chunk from the manifest is missing.
        HashMismatchError: If a chunk's content hash doesn't match.
        SignatureError: If *validator* is set and a chunk's signature is
            missing or invalid.
        IntegrityError: If the overall content hash doesn't match.
    """
    total = bytearray()

    for ref in manifest.chunks:
        data = _require_chunk(manifest, ref, chunks)

        # Strict hash verification: Data's content hash must match ChunkRef
        _verify_chunk_hash(ref, data)

        # Optional producer-signature verification (cache-substitution defence)
        if validator is not None:
            _verify_chunk_signature(ref, data, validator)

        total.extend(data.content)

    # Verify end-to-end integrity
    _verify_content_hash(manifest, bytes(total))

    return bytes(total)


def assemble_verified(
    manifest: ContentManifest,
    chunks: dict[str, Data],
    validator: Validator | None = None,
) -> bytes:
    """Reassemble with ONLY chunk-level verification (no overall hash check).

    Use this when the manifest doesn't carry an overall content_hash, or
    when you trust per-chunk hashes but don't have the full content hash.
    If *validator* is given, each chunk's producer signature is verified too.

    Same errors as assemble() except IntegrityError is never raised.
    """
    total = bytearray()

    for ref in manifest.chunks:
        data = _require_chunk(manifest, ref, chunks)
        _verify_chunk_hash(ref, data)
        if validator is not None:
            _verify_chunk_signature(ref, data, validator)
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
    validator: Validator | None = None,
) -> bool:
    """Verify a single chunk Data against its ChunkRef.

    Returns True if the Data's content_hash matches the ChunkRef's
    content_hash AND the raw content's blake2b matches. If *validator* is
    given, the chunk's producer signature must also verify.
    """
    if data.metadata.content_hash is None:
        return False
    if data.metadata.content_hash != ref.content_hash:
        return False
    actual = _compute_blake2b(data.content)
    if actual != ref.content_hash:
        return False
    return not (validator is not None and not data.verify_signature(validator))


def verify_chunks(
    manifest: ContentManifest,
    chunks: dict[str, Data],
    validator: Validator | None = None,
) -> dict[str, bool]:
    """Verify all chunks in a manifest against their Data packets.

    Returns a dict mapping label → verification result (True/False).
    Missing chunks are reported as False. When *validator* is given, a chunk
    must also carry a valid producer signature to be reported True.
    """
    results: dict[str, bool] = {}
    for ref in manifest.chunks:
        data = chunks.get(ref.label)
        if data is None:
            results[ref.label] = False
        else:
            results[ref.label] = verify_chunk(ref, data, validator)
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


def _verify_chunk_signature(ref: ChunkRef, data: Data, validator: Validator) -> None:
    """Raise SignatureError if the chunk has no valid producer signature."""
    if data.signature is None:
        raise SignatureError(f"Chunk '{ref.label}' is unsigned")
    if not data.verify_signature(validator):
        raise SignatureError(
            f"Chunk '{ref.label}': producer signature failed verification"
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
