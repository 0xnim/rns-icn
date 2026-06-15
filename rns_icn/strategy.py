"""Strategy — pluggable forwarding decisions."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from .face import FaceId
from .packet import Data, Interest
from .pit import PitEntry


class StrategyDecision(Enum):
    SERVE_FROM_CACHE = "serve_from_cache"
    SUPPRESS_AGGREGATE = "suppress_aggregate"
    FORWARD_TO = "forward_to"
    NO_ROUTE = "no_route"


class Strategy(ABC):
    @abstractmethod
    async def decide(
        self,
        interest: Interest,
        fib_faces: list[tuple[FaceId, int]],
        pit_hit: Optional[PitEntry],
        cs_hit: Optional[Data],
    ) -> tuple[StrategyDecision, Optional[FaceId]]:
        """Returns (decision, face_id) where face_id is set for FORWARD_TO."""
        ...


class BestRoute(Strategy):
    def __init__(self, backoff_base: float = 1.0, max_backoff: float = 30.0):
        self._failures: dict[FaceId, _FailureRecord] = {}
        self._backoff_base = backoff_base
        self._max_backoff = max_backoff

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
    def _selector_satisfied(interest: Interest, data: Data) -> bool:
        if interest.must_be_fresh and not data.metadata.freshness.fresh:
            return False
        if interest.selector is not None and interest.selector.min_sequence is not None:
            if data.metadata.sequence is None or data.metadata.sequence < interest.selector.min_sequence:
                return False
        return True

    async def decide(
        self,
        interest: Interest,
        fib_faces: list[tuple[FaceId, int]],
        pit_hit: Optional[PitEntry],
        cs_hit: Optional[Data],
    ) -> tuple[StrategyDecision, Optional[FaceId]]:

        if cs_hit is not None and self._selector_satisfied(interest, cs_hit):
            return StrategyDecision.SERVE_FROM_CACHE, None

        if pit_hit is not None and not pit_hit.satisfied:
            return StrategyDecision.SUPPRESS_AGGREGATE, None

        for face_id, _ in fib_faces:
            if not self._is_in_backoff(face_id):
                return StrategyDecision.FORWARD_TO, face_id

        return StrategyDecision.NO_ROUTE, None


class _FailureRecord:
    __slots__ = ("last_failure", "consecutive_failures")
    def __init__(self):
        self.last_failure = 0.0
        self.consecutive_failures = 0
