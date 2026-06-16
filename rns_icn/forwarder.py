"""Forwarder — the core ICN routing engine.

Ties FIB/PIT/CS/Strategy together. Processes Interests (consumer-facing)
and incoming Data (producer/relay-facing).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Optional

from .content_store import ContentStore
from .face import Face, FaceId
from .fib import Fib
from .name import Name
from .packet import Data, Interest, InterestSelector
from .pit import Pit
from .strategy import BestRoute, Strategy, StrategyDecision


class Forwarder:
    def __init__(self, strategy: Optional[Strategy] = None, cs_max: int = 1000):
        self.cs = ContentStore(max_entries=cs_max)
        self.fib = Fib()
        self.pit = Pit()
        self.strategy = strategy or BestRoute()
        self._faces: dict[FaceId, Face] = {}
        self._pit_notifiers: dict[Name, list[asyncio.Future]] = {}

    @property
    def faces(self) -> dict[FaceId, Face]:
        """Registered faces keyed by face id (read access for callers)."""
        return self._faces

    def register_face(self, face: Face) -> None:
        self._faces[face.id()] = face

    def unregister_face(self, face_id: FaceId) -> None:
        self._faces.pop(face_id, None)

    def add_route(self, prefix: Name, face_id: FaceId, cost: int = 10) -> None:
        self.fib.insert(prefix, face_id, cost)

    async def express(self, interest: Interest, in_face: FaceId) -> Optional[Data]:
        """Express an Interest — main consumer API. Returns Data or None."""
        # 1. Loop detection
        if self.pit.check_loop(in_face, interest.nonce):
            return None
        self.pit.record_nonce(in_face, interest.nonce)

        # 2. Check CS
        cs_hit = (self.cs.get_prefix(interest.name) if interest.can_be_prefix
                  else self.cs.get(interest.name))

        # 3. Check PIT
        pit_hit = self.pit.find(interest.name)

        # 4. FIB lookup
        fib_faces = self.fib.lookup(interest.name) or []

        # 5. Strategy
        decision, target_face = await self.strategy.decide(
            interest, fib_faces, pit_hit, cs_hit
        )

        if decision == StrategyDecision.SERVE_FROM_CACHE:
            return cs_hit

        if decision == StrategyDecision.SUPPRESS_AGGREGATE:
            fut = asyncio.get_event_loop().create_future()
            self._pit_notifiers.setdefault(interest.name, []).append(fut)
            try:
                return await asyncio.wait_for(fut, timeout=interest.lifetime_ms / 1000.0)
            except asyncio.TimeoutError:
                return None

        if decision == StrategyDecision.FORWARD_TO:
            if target_face is None:
                return None
            return await self._forward(interest, in_face, target_face)

        return None

    async def _forward(self, interest: Interest, in_face: FaceId,
                       out_face_id: FaceId) -> Optional[Data]:
        name = interest.name

        # Hop-limit enforcement (defence-in-depth beyond nonce loop detection).
        # An exhausted Interest is dropped rather than forwarded; CS/PIT
        # satisfaction already happened in express() before we got here.
        if interest.hop_limit <= 0:
            return None
        interest.hop_limit -= 1

        self.pit.insert_or_aggregate(name, in_face, interest, interest.lifetime_ms)
        self.pit.set_out_face(name, out_face_id)

        face = self._faces.get(out_face_id)
        if face is None:
            self.pit.satisfy(name)
            self.pit.purge_expired()
            return None

        # Send Interest bytes out via the face, then wait on PIT notifier.
        # This decouples Interest forwarding from express_interest() semantics
        # (which not all face implementations support for server-to-server use).
        # When Data arrives via receive_data(), the PIT entry matches and
        # _notify_waiters resolves this future.
        await face.send_raw(interest.to_bytes())

        fut = asyncio.get_event_loop().create_future()
        self._pit_notifiers.setdefault(name, []).append(fut)
        try:
            result = await asyncio.wait_for(
                fut, timeout=interest.lifetime_ms / 1000.0
            )
        except asyncio.TimeoutError:
            self.pit.satisfy(name)
            self.pit.purge_expired()
            if isinstance(self.strategy, BestRoute):
                self.strategy.record_failure(out_face_id)
            return None

        if result is not None:
            if isinstance(self.strategy, BestRoute):
                self.strategy.record_success(out_face_id)
            self.cs.insert(result.name, result)
            self.pit.satisfy(name)
            self.pit.purge_expired()
            return result

        self.pit.satisfy(name)
        self.pit.purge_expired()
        return None

    def _notify_waiters(self, name: Name, data: Optional[Data]) -> None:
        notifiers = self._pit_notifiers.pop(name, [])
        for fut in notifiers:
            if not fut.done():
                if data is not None:
                    fut.set_result(data)
                else:
                    fut.cancel()

    async def receive_data(self, data: Data, in_face: FaceId) -> None:
        """Receive incoming Data — satisfies PIT, caches in CS.

        PIT entries are keyed by the Interest name (which has no content hash).
        Strip any content hash from the Data name for PIT lookup so it
        matches the Interest's PIT entry regardless of content hash.
        """
        # Strip content hash for PIT lookup (PIT entries are keyed by
        # interest name, which never carries a content hash)
        pit_name = data.name.without_content_hash() if data.name.content_hash else data.name
        matched = self.pit.satisfy(pit_name)
        if matched is not None:
            self._notify_waiters(pit_name, data)
        self.cs.insert(data.name, data)
        self.pit.purge_expired()

    async def stream_fetch(
        self,
        name: Name,
        in_face: FaceId = 0,
        start_sequence: int = 0,
        lifetime_ms: int = 4000,
        max_segments: Optional[int] = None,
    ) -> AsyncIterator[Data]:
        """Fetch streaming data incrementally using InterestSelector.

        Expresses successive Interests with monotonic min_sequence,
        yielding each Data segment as it arrives. Stops when the
        producer stops responding (express returns None), or after
        max_segments if set.

        Each Interest uses can_be_prefix=True so CS prefix matching
        works against stream segment names stored as child names.

        Yields:
            Data packets in sequence order.
        """
        min_seq = start_sequence
        fetched = 0
        while max_segments is None or fetched < max_segments:
            interest = Interest(
                name=name,
                lifetime_ms=lifetime_ms,
                can_be_prefix=True,
                selector=InterestSelector(min_sequence=min_seq),
            )
            data = await self.express(interest, in_face)
            if data is None:
                break
            fetched += 1
            # Advance past whatever sequence we got, so next fetch
            # asks for strictly newer content
            if data.metadata.sequence is not None and data.metadata.sequence >= min_seq:
                min_seq = data.metadata.sequence + 1
            yield data
