"""Discovery conventions — verifiable "latest version" pointers (RDR-style).

A producer publishes a small Data object at a reserved name under a collection
prefix whose payload names the current latest version (content-hash pinned). A
consumer fetches it with ``must_be_fresh`` to revalidate past stale caches,
verifies the producer signature, then fetches the exact pinned name — so
"latest" becomes a producer-signed assertion rather than a cache's unverifiable
ranking of its own rows. This is the authenticated counterpart to the
best-effort ``latest``/``oldest`` child selectors (see PROTOCOL.md §7).

No wire-format change: a meta object is an ordinary signed Data at a reserved
name, riding existing signing (Phase 3.1) and freshness (Phase 2.4). Old nodes
treat it as opaque content.

Reserved names: labels beginning with byte ``0x00`` are protocol-reserved.
Application labels MUST NOT start with ``0x00``.
"""

from __future__ import annotations

from .name import Name

# Reserved name components (see module docstring).
META_LABEL = b"\x00m"      # latest-version pointer for a collection prefix
CATALOG_LABEL = b"\x00c"   # signed namespace catalog (the manifest)

# Meta payload framing: [fmt:1][target Name.to_bytes()].
_META_FMT = 0x01


class DiscoveryError(Exception):
    """Raised on a malformed discovery object payload."""


def meta_name(prefix: Name) -> Name:
    """The reserved name carrying ``prefix``'s latest-version pointer."""
    return Name(prefix.rns_addr, [*prefix.components[1:], META_LABEL])


def catalog_name(prefix: Name) -> Name:
    """The reserved name carrying ``prefix``'s signed namespace catalog."""
    return Name(prefix.rns_addr, [*prefix.components[1:], CATALOG_LABEL])


def is_reserved(name: Name) -> bool:
    """True if ``name``'s last component is protocol-reserved (0x00-prefixed)."""
    last = name.components[-1]
    return len(last) > 0 and last[0] == 0x00


def encode_meta(target: Name) -> bytes:
    """Serialize a latest-version pointer payload naming ``target``.

    ``target`` should be content-hash pinned (``Name.with_content_hash``) so the
    consumer's eventual fetch of it is self-certifying.
    """
    return bytes([_META_FMT]) + target.to_bytes()


def decode_meta(payload: bytes) -> Name:
    """Parse a meta payload back into the target ``Name``.

    Raises ``DiscoveryError`` on an unrecognized format byte or unparseable name.
    """
    if not payload or payload[0] != _META_FMT:
        raise DiscoveryError("unrecognized meta payload format")
    try:
        return Name.from_bytes(payload[1:])
    except Exception as e:  # normalize any Name parse failure
        raise DiscoveryError(f"invalid meta target name: {e}") from e
