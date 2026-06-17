"""Tests for PIT bounds + aging (Part B1).

The Pending Interest Table is capped (nearest-expiry eviction past the cap) and
its loop-nonce set is bounded, so neither can grow without limit under a burst
of un-satisfiable Interests. A background aging loop (server-side) drives
purge_expired; here we exercise the table directly.
"""

import time

from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import Interest
from rns_icn.pit import Pit


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


def _name(label: str) -> Name:
    return Name(rns_addr(), [label.encode()])


def _interest(label: str) -> Interest:
    return Interest(name=_name(label))


def test_evicts_nearest_expiry_when_full():
    pit = Pit(max_entries=2)
    pit.insert_or_aggregate(_name("a"), 1, _interest("a"), timeout_ms=10_000)
    pit.insert_or_aggregate(_name("b"), 1, _interest("b"), timeout_ms=50)  # soonest
    # Third new entry at capacity evicts the nearest-expiry one (b).
    pit.insert_or_aggregate(_name("c"), 1, _interest("c"), timeout_ms=10_000)

    assert pit.find(_name("b")) is None
    assert pit.find(_name("a")) is not None
    assert pit.find(_name("c")) is not None
    assert pit.evictions == 1
    assert len(pit) == 2


def test_aggregate_does_not_evict():
    pit = Pit(max_entries=1)
    pit.insert_or_aggregate(_name("a"), 1, _interest("a"), timeout_ms=10_000)
    # Same name from another in-face aggregates — no new entry, no eviction.
    op = pit.insert_or_aggregate(_name("a"), 2, _interest("a"), timeout_ms=10_000)

    assert op.value == "aggregated"
    assert pit.evictions == 0
    assert len(pit) == 1
    assert pit.find(_name("a")).in_faces == [1, 2]


def test_is_full():
    pit = Pit(max_entries=1)
    assert not pit.is_full()
    pit.insert_or_aggregate(_name("a"), 1, _interest("a"), timeout_ms=10_000)
    assert pit.is_full()


def test_purge_removes_expired():
    pit = Pit(max_entries=10)
    pit.insert_or_aggregate(_name("a"), 1, _interest("a"), timeout_ms=0)
    time.sleep(0.01)
    removed = pit.purge_expired()

    assert len(removed) == 1
    assert pit.find(_name("a")) is None
    assert len(pit) == 0


def test_nonce_tracker_bounded():
    pit = Pit(max_nonces=3)
    for i in range(6):
        pit.record_nonce(in_face=1, nonce=bytes([i]))
    # Never exceeds the cap (oldest dropped to make room).
    assert len(pit._nonce_tracker) <= 3
