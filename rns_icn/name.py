"""ICN Name — hierarchical name with RNS address as producer identity.

First component is always a 16-byte RNS address. Optional 32-byte content
hash suffix for self-certification.

Wire: [count:1][len1:1][comp1...][lenN:1][compN...][0xFF][hash:32]?
Display: /<rns-hex>/<path>?hash=<hex>
"""

from __future__ import annotations

HASH_DISCRIMINATOR = 0xFF
MAX_COMPONENTS = 32
RNS_ADDR_BYTES = 16
CONTENT_HASH_BYTES = 32


class NameError(Exception):
    ...


class Name:
    """A named-data name: routable prefix + optional content hash."""

    __slots__ = ("components", "content_hash")

    def __init__(self, rns_addr: bytes, path: list[bytes] | None = None,
                 content_hash: bytes | None = None):
        if len(rns_addr) != RNS_ADDR_BYTES:
            raise NameError(f"RNS address must be {RNS_ADDR_BYTES} bytes")
        self.components: list[bytes] = [rns_addr]
        if path:
            self.components.extend(path)
        if content_hash is not None and len(content_hash) != CONTENT_HASH_BYTES:
            raise NameError(f"content hash must be {CONTENT_HASH_BYTES} bytes")
        self.content_hash: bytes | None = content_hash

    @classmethod
    def from_bytes(cls, data: bytes) -> Name:
        if not data:
            raise NameError("empty buffer")
        count = data[0]
        if count == 0 or count > MAX_COMPONENTS:
            raise NameError(f"invalid component count: {count}")
        pos = 1
        components = []
        for _ in range(count):
            if pos >= len(data):
                raise NameError("buffer too short")
            clen = data[pos]
            pos += 1
            if pos + clen > len(data):
                raise NameError("buffer too short")
            components.append(data[pos:pos + clen])
            pos += clen
        content_hash = None
        if pos < len(data) and data[pos] == HASH_DISCRIMINATOR:
            pos += 1
            if pos + CONTENT_HASH_BYTES > len(data):
                raise NameError("buffer too short for content hash")
            content_hash = data[pos:pos + CONTENT_HASH_BYTES]
        name = cls.__new__(cls)
        name.components = components
        name.content_hash = content_hash
        return name

    def with_content_hash(self, hash_bytes: bytes) -> Name:
        if len(hash_bytes) != CONTENT_HASH_BYTES:
            raise NameError(f"content hash must be {CONTENT_HASH_BYTES} bytes")
        n = object.__new__(Name)
        n.components = self.components[:]
        n.content_hash = hash_bytes
        return n

    def without_content_hash(self) -> Name:
        n = object.__new__(Name)
        n.components = self.components[:]
        n.content_hash = None
        return n

    @property
    def rns_addr(self) -> bytes:
        return self.components[0]

    def len(self) -> int:
        return len(self.components)

    def is_root(self) -> bool:
        return len(self.components) == 1

    def starts_with(self, prefix: Name) -> bool:
        if prefix.len() > self.len():
            return False
        for a, b in zip(self.components, prefix.components):
            if a != b:
                return False
        return True

    def is_prefix_of(self, other: Name) -> bool:
        return other.starts_with(self)

    def to_bytes(self) -> bytes:
        buf = bytearray()
        buf.append(len(self.components))
        for comp in self.components:
            buf.append(len(comp))
            buf.extend(comp)
        if self.content_hash is not None:
            buf.append(HASH_DISCRIMINATOR)
            buf.extend(self.content_hash)
        return bytes(buf)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Name):
            return NotImplemented
        return self.components == other.components and self.content_hash == other.content_hash

    def __hash__(self) -> int:
        h = hash(tuple(self.components))
        if self.content_hash is not None:
            h ^= hash(self.content_hash)
        return h

    def __str__(self) -> str:
        parts = []
        for i, comp in enumerate(self.components):
            if i == 0 and len(comp) == RNS_ADDR_BYTES:
                parts.append(comp.hex())
            else:
                try:
                    parts.append(comp.decode("utf-8"))
                except UnicodeDecodeError:
                    parts.append(comp.hex())
        s = "/" + "/".join(parts)
        if self.content_hash is not None:
            s += f"?hash={self.content_hash.hex()}"
        return s

    def __repr__(self) -> str:
        return f"Name('{self}')"
