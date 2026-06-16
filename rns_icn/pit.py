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
    def __init__(self, nonce_ttl: float = 60.0):
        self._entries: dict[Name, PitEntry] = {}
        self._nonce_tracker: dict[tuple[FaceId, bytes], float] = {}
        self._nonce_ttl = nonce_ttl

    def find(self, name: Name) -> PitEntry | None:
        return self._entries.get(name)

    def insert_or_aggregate(self, name: Name, in_face: FaceId,
                            interest: Interest, timeout_ms: int) -> PitOp:
        entry = self._entries.get(name)
        if entry is not None and not entry.satisfied:
            if in_face not in entry.in_faces:
                entry.in_faces.append(in_face)
            return PitOp.AGGREGATED
        self._entries[name] = PitEntry(
            interest=interest,
            in_faces=[in_face],
            expires_at=time.monotonic() + timeout_ms / 1000.0,
        )
        return PitOp.INSERTED

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
        self._nonce_tracker[(in_face, nonce)] = time.monotonic() + self._nonce_ttl

    def __len__(self) -> int:
        return len(self._entries)
