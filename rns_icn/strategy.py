"""Strategy — pluggable forwarding decisions."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from enum import Enum

from .face import FaceId
from .packet import Data, Interest
from .pit import PitEntry


class StrategyDecision(Enum):
    SERVE_FROM_CACHE = "serve_from_cache"
    SERVE_STALE_REVALIDATE = "serve_stale_revalidate"
    SUPPRESS_AGGREGATE = "suppress_aggregate"
    FORWARD_TO = "forward_to"
    NO_ROUTE = "no_route"


class Strategy(ABC):
    @abstractmethod
    async def decide(
        self,
        interest: Interest,
        fib_faces: list[tuple[FaceId, int]],
        pit_hit: PitEntry | None,
        cs_hit: Data | None,
    ) -> tuple[StrategyDecision, FaceId | None]:
        """Returns (decision, face_id) where face_id is set for FORWARD_TO."""
        ...


class BestRoute(Strategy):
    def __init__(self, backoff_base: float = 1.0, max_backoff: float = 30.0,
                 stale_while_revalidate: int = 0):
        self._failures: dict[FaceId, _FailureRecord] = {}
        self._backoff_base = backoff_base
        self._max_backoff = max_backoff
        # Seconds past a Data's freshness_period during which a stale cache hit
        # is still served immediately while a background revalidation refreshes
        # it. 0 (default) disables stale-while-revalidate.
        self._swr = stale_while_revalidate

    def record_failure(self, face: FaceId) -> None:
        r = self._failures.setdefault(face, _FailureRecord())
        r.last_failure = time.monotonic()
        r.consecutive_failures += 1

    def record_success(self, face: FaceId) -> None:
        self._failures.pop(face, None)

    def _is_in_backoff(self, face: FaceId) -> bool:
        r = self._failures.get(face)
        if r is None:
            return False
        backoff = min(self._backoff_base * (2 ** min(r.consecutive_failures, 6)), self._max_backoff)
        return (time.monotonic() - r.last_failure) < backoff

    @staticmethod
    def _sequence_satisfied(interest: Interest, data: Data) -> bool:
        selector = interest.selector
        if selector is None or selector.min_sequence is None:
            return True
        return (
            data.metadata.sequence is not None
            and data.metadata.sequence >= selector.min_sequence
        )

    @classmethod
    def _selector_satisfied(cls, interest: Interest, data: Data) -> bool:
        if interest.must_be_fresh and not data.metadata.freshness.fresh:
            return False
        return cls._sequence_satisfied(interest, data)

    def _first_usable_face(self, fib_faces: list[tuple[FaceId, int]]) -> FaceId | None:
        for face_id, _ in fib_faces:
            if not self._is_in_backoff(face_id):
                return face_id
        return None

    def usable_faces(self, fib_faces: list[tuple[FaceId, int]]) -> list[FaceId]:
        """Cost-ordered next-hops not currently in backoff.

        ``fib_faces`` arrives cost-sorted from the FIB. The Forwarder forwards
        to the first and, on timeout, falls through to the next — primary/backup
        failover across content-equivalent peers (a decision RNS can't make: it
        re-paths to a fixed destination, while these are distinct producers/caches
        that each satisfy the name). Backed-off faces are skipped entirely.
        """
        return [fid for fid, _ in fib_faces if not self._is_in_backoff(fid)]

    def _within_swr_window(self, data: Data) -> bool:
        period = data.metadata.freshness_period
        if period is None or self._swr <= 0:
            return False
        return data.metadata.freshness.age_seconds < period + self._swr

    async def decide(
        self,
        interest: Interest,
        fib_faces: list[tuple[FaceId, int]],
        pit_hit: PitEntry | None,
        cs_hit: Data | None,
    ) -> tuple[StrategyDecision, FaceId | None]:

        if cs_hit is not None and self._selector_satisfied(interest, cs_hit):
            # Cache hit acceptable to the consumer. If it is stale-but-servable
            # and we're inside the stale-while-revalidate window with a usable
            # route, serve it now and refresh in the background.
            if not cs_hit.metadata.freshness.fresh and self._within_swr_window(cs_hit):
                face = self._first_usable_face(fib_faces)
                if face is not None:
                    return StrategyDecision.SERVE_STALE_REVALIDATE, face
            return StrategyDecision.SERVE_FROM_CACHE, None

        if pit_hit is not None and not pit_hit.satisfied:
            return StrategyDecision.SUPPRESS_AGGREGATE, None

        face = self._first_usable_face(fib_faces)
        if face is not None:
            return StrategyDecision.FORWARD_TO, face

        return StrategyDecision.NO_ROUTE, None


class _FailureRecord:
    __slots__ = ("consecutive_failures", "last_failure")
    def __init__(self):
        self.last_failure = 0.0
        self.consecutive_failures = 0
