"""Pending Interest Table — Interest aggregation + loop detection."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum

from .face import FaceId
from .name import Name
from .packet import Interest


@dataclass
class PitEntry:
    interest: Interest
    in_faces: list[FaceId] = field(default_factory=list)
    out_face: FaceId | None = None
    expires_at: float = 0.0
    satisfied: bool = False


class PitOp(Enum):
    INSERTED = "inserted"
    AGGREGATED = "aggregated"


class Pit:
    def __init__(self, nonce_ttl: float = 60.0, max_entries: int = 10000,
                 max_nonces: int = 50000):
        self._entries: dict[Name, PitEntry] = {}
        self._nonce_tracker: dict[tuple[FaceId, bytes], float] = {}
        self._nonce_ttl = nonce_ttl
        self._max_entries = max_entries
        self._max_nonces = max_nonces
        # Count of entries dropped under capacity pressure (surfaced as a metric):
        # a steadily climbing value means the PIT is undersized for the load.
        self.evictions = 0

    def find(self, name: Name) -> PitEntry | None:
        return self._entries.get(name)

    def is_full(self) -> bool:
        """True if a new (non-aggregating) Interest would force an eviction."""
        return len(self._entries) >= self._max_entries

    def insert_or_aggregate(self, name: Name, in_face: FaceId,
                            interest: Interest, timeout_ms: int) -> PitOp:
        entry = self._entries.get(name)
        if entry is not None and not entry.satisfied:
            if in_face not in entry.in_faces:
                entry.in_faces.append(in_face)
            return PitOp.AGGREGATED
        # New entry: enforce the cap by evicting the nearest-expiry entry first
        # (graceful degradation under load — the soonest-to-die makes room).
        if name not in self._entries and len(self._entries) >= self._max_entries:
            self._evict_nearest_expiry()
        self._entries[name] = PitEntry(
            interest=interest,
            in_faces=[in_face],
            expires_at=time.monotonic() + timeout_ms / 1000.0,
        )
        return PitOp.INSERTED

    def _evict_nearest_expiry(self) -> None:
        if not self._entries:
            return
        victim = min(self._entries, key=lambda n: self._entries[n].expires_at)
        del self._entries[victim]
        self.evictions += 1

    def set_out_face(self, name: Name, out_face: FaceId) -> None:
        entry = self._entries.get(name)
        if entry is not None:
            entry.out_face = out_face

    def satisfy(self, name: Name) -> list[FaceId] | None:
        entry = self._entries.get(name)
        if entry is not None:
            entry.satisfied = True
            return list(entry.in_faces)
        return None

    def purge_expired(self) -> list[PitEntry]:
        now = time.monotonic()
        expired = [n for n, e in self._entries.items()
                   if e.satisfied or e.expires_at <= now]
        removed = []
        for name in expired:
            removed.append(self._entries.pop(name))
        self._nonce_tracker = {k: v for k, v in self._nonce_tracker.items() if v > now}
        return removed

    def check_loop(self, in_face: FaceId, nonce: bytes) -> bool:
        return (in_face, nonce) in self._nonce_tracker

    def record_nonce(self, in_face: FaceId, nonce: bytes) -> None:
        # Bound the loop-detection set: drop the soonest-to-expire nonce when
        # full so sustained traffic can't grow it without limit (the TTL sweep
        # in purge_expired handles the steady state).
        if len(self._nonce_tracker) >= self._max_nonces:
            oldest = min(self._nonce_tracker, key=lambda k: self._nonce_tracker[k])
            del self._nonce_tracker[oldest]
        self._nonce_tracker[(in_face, nonce)] = time.monotonic() + self._nonce_ttl

    def __len__(self) -> int:
        return len(self._entries)
