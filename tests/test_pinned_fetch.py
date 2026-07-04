"""Tests for content-hash-pinned (self-certifying) fetches.

A pinned name identifies exact bytes: the store must answer with the unpinned
row only when the pin matches its content, PIT state must match the unpinned
Data coming back, and rows are always keyed unpinned regardless of how the
insert named them.
"""

import hashlib

from rns_icn.content_store import ContentStore
from rns_icn.name import Name
from rns_icn.packet import Data

ADDR = bytes(range(16))


def _data(label: bytes, content: bytes) -> Data:
    return Data.new(Name(ADDR, [b"wire", b"main", label]), content)


def _pin(content: bytes) -> bytes:
    return hashlib.blake2b(content, digest_size=32).digest()


def test_pinned_get_matches_when_hash_agrees():
    cs = ContentStore(path=":memory:")
    data = _data(b"1", b"hello")
    cs.insert(data.name, data)
    pinned = data.name.with_content_hash(_pin(b"hello"))
    served = cs.get(pinned)
    assert served is not None
    assert served.content == b"hello"


def test_pinned_get_misses_on_wrong_pin():
    cs = ContentStore(path=":memory:")
    data = _data(b"1", b"hello")
    cs.insert(data.name, data)
    wrong = data.name.with_content_hash(_pin(b"evil"))
    assert cs.get(wrong) is None


def test_insert_keys_are_unpinned():
    cs = ContentStore(path=":memory:")
    data = _data(b"2", b"payload")
    cs.insert(data.name.with_content_hash(_pin(b"payload")), data)
    assert cs.get(data.name) is not None
