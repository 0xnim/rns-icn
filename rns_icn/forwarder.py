"""Forwarder — the core ICN routing engine.

Ties FIB/PIT/CS/Strategy together. Processes Interests (consumer-facing)
and incoming Data (producer/relay-facing).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator

from .content_store import ContentStore
from .face import Face, FaceId
from .fib import Fib
from .name import Name
from .packet import ChildSelector, Data, Interest, InterestSelector
from .pit import Pit
from .strategy import BestRoute, Strategy, StrategyDecision

logger = logging.getLogger(__name__)


class Forwarder:
    def __init__(self, strategy: Strategy | None = None, cs_max: int = 1000):
        self.cs = ContentStore(max_entries=cs_max)
        self.fib = Fib()
        self.pit = Pit()
        self.strategy = strategy or BestRoute()
        self._faces: dict[FaceId, Face] = {}
        self._pit_notifiers: dict[Name, list[asyncio.Future]] = {}
        # Names with an in-flight stale-while-revalidate refresh, so we never
        # fire more than one background revalidation per name at a time.
        self._revalidating: set[Name] = set()
        # Strong references to fire-and-forget background tasks so the event loop
        # does not garbage-collect them mid-flight (see _schedule_revalidation).
        self._bg_tasks: set[asyncio.Future] = set()

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

    def withdraw_face(self, face_id: FaceId) -> None:
        """Tear down a face: withdraw all its FIB routes and unregister it.

        Called when a link closes so a dead next-hop stops being a black hole —
        Interests for its prefixes either fall through to a backup face
        (multi-path) or cleanly hit NO_ROUTE instead of timing out forever.
        """
        self.fib.remove_all_for_face(face_id)
        self.unregister_face(face_id)

    async def express(self, interest: Interest, in_face: FaceId) -> Data | None:
        """Express an Interest — main consumer API. Returns Data or None."""
        # 1. Loop detection
        if self.pit.check_loop(in_face, interest.nonce):
            return None
        self.pit.record_nonce(in_face, interest.nonce)

        # 2. Check CS. On a prefix Interest the selector decides which match
        #    answers (latest/oldest by sequence, and any min_sequence floor).
        if interest.can_be_prefix:
            sel = interest.selector
            cs_hit = self.cs.get_prefix(
                interest.name,
                child=sel.child if sel else ChildSelector.NONE,
                min_sequence=sel.min_sequence if sel else None,
            )
        else:
            cs_hit = self.cs.get(interest.name)

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

        if decision == StrategyDecision.SERVE_STALE_REVALIDATE:
            # Serve the stale copy immediately; refresh it upstream in the
            # background so the next request gets fresh content.
            if target_face is not None:
                self._schedule_revalidation(interest, in_face, target_face)
            return cs_hit

        if decision == StrategyDecision.SUPPRESS_AGGREGATE:
            fut = asyncio.get_event_loop().create_future()
            self._pit_notifiers.setdefault(interest.name, []).append(fut)
            try:
                return await asyncio.wait_for(fut, timeout=interest.lifetime_ms / 1000.0)
            except asyncio.TimeoutError:
                return None

        if decision == StrategyDecision.FORWARD_TO:
            out_faces = self._failover_candidates(fib_faces, target_face)
            if not out_faces:
                return None
            return await self._forward(interest, in_face, out_faces)

        return None

    def _failover_candidates(
        self, fib_faces: list[tuple[FaceId, int]], primary: FaceId | None
    ) -> list[FaceId]:
        """Ordered next-hops to try for one Interest (primary then backups).

        Asks the strategy for its cost-ordered, backoff-filtered face list so a
        timeout on the primary falls through to a backup. Strategies that don't
        expose ``usable_faces`` keep single-path behaviour via the primary.
        """
        usable = getattr(self.strategy, "usable_faces", None)
        if usable is not None:
            faces = usable(fib_faces)
            if faces:
                return faces
        return [primary] if primary is not None else []

    async def _forward(self, interest: Interest, in_face: FaceId,
                       out_faces: list[FaceId]) -> Data | None:
        """Forward an Interest, trying each next-hop in order until one answers.

        Hop-limit is enforced once per Interest here (not per next-hop): trying a
        backup is the same logical hop, so a single forward decision costs one hop
        regardless of how many content-equivalent peers we fall through.
        """
        # Hop-limit enforcement (defence-in-depth beyond nonce loop detection).
        # An exhausted Interest is dropped rather than forwarded; CS/PIT
        # satisfaction already happened in express() before we got here.
        if interest.hop_limit <= 0:
            return None
        interest.hop_limit -= 1

        for out_face_id in out_faces:
            result = await self._forward_one(interest, in_face, out_face_id)
            if result is not None:
                return result
        return None

    async def _forward_one(self, interest: Interest, in_face: FaceId,
                           out_face_id: FaceId) -> Data | None:
        name = interest.name

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

    def _schedule_revalidation(self, interest: Interest, in_face: FaceId,
                               out_face_id: FaceId) -> None:
        """Fire a one-shot background refresh for a stale-served name.

        Deduped per name so concurrent stale hits trigger at most one upstream
        revalidation. The refreshed Data is cached by ``_forward`` when it
        arrives; we ignore the return value here.
        """
        name = interest.name
        if name in self._revalidating:
            return
        self._revalidating.add(name)

        revalidate = interest.clone()
        revalidate.nonce = os.urandom(8)
        revalidate.must_be_fresh = True

        async def _run() -> None:
            try:
                await self._forward(revalidate, in_face, [out_face_id])
            except Exception:
                logger.warning(
                    "background revalidation failed for %s", revalidate.name, exc_info=True
                )
            finally:
                self._revalidating.discard(name)

        task = asyncio.ensure_future(_run())
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    def _notify_waiters(self, name: Name, data: Data | None) -> None:
        notifiers = self._pit_notifiers.pop(name, [])
        for fut in notifiers:
            if not fut.done():
                if data is not None:
                    fut.set_result(data)
                else:
                    fut.cancel()

    async def receive_data(
        self, data: Data, in_face: FaceId, cache_unsolicited: bool = False
    ) -> None:
        """Receive incoming Data — satisfies PIT, caches in CS.

        PIT entries are keyed by the Interest name (which has no content hash).
        Strip any content hash from the Data name for PIT lookup so it
        matches the Interest's PIT entry regardless of content hash.

        By default only Data that satisfies a pending Interest (a PIT match) is
        cached — the standard NDN rule, so an unauthenticated peer cannot inject
        unsolicited content into the Content Store. Trusted push flows that
        legitimately deliver unsolicited content (propagation replication between
        peered servers) set ``cache_unsolicited=True`` to opt in. Cache poisoning
        is in any case caught by consumer-side signature verification; this gate
        narrows the surface a forwarder exposes.
        """
        # Strip content hash for PIT lookup (PIT entries are keyed by
        # interest name, which never carries a content hash)
        pit_name = data.name.without_content_hash() if data.name.content_hash else data.name
        matched = self.pit.satisfy(pit_name)
        if matched is not None:
            self._notify_waiters(pit_name, data)
        if matched is not None or cache_unsolicited:
            self.cs.insert(data.name, data)
        self.pit.purge_expired()

    async def fetch_latest(
        self,
        name: Name,
        in_face: FaceId = 0,
        lifetime_ms: int = 4000,
        must_be_fresh: bool = False,
    ) -> Data | None:
        """Fetch the highest-sequence Data under ``name`` (the ``latest`` selector).

        Best-effort per node: a cache answers with the newest version it holds.
        Set ``must_be_fresh`` to revalidate past stale caches toward the producer.
        """
        return await self._fetch_child(
            name, ChildSelector.LATEST, in_face, lifetime_ms, must_be_fresh
        )

    async def fetch_oldest(
        self,
        name: Name,
        in_face: FaceId = 0,
        lifetime_ms: int = 4000,
        must_be_fresh: bool = False,
    ) -> Data | None:
        """Fetch the lowest-sequence Data under ``name`` (the ``oldest`` selector)."""
        return await self._fetch_child(
            name, ChildSelector.OLDEST, in_face, lifetime_ms, must_be_fresh
        )

    async def _fetch_child(
        self,
        name: Name,
        child: ChildSelector,
        in_face: FaceId,
        lifetime_ms: int,
        must_be_fresh: bool,
    ) -> Data | None:
        interest = Interest(
            name=name,
            lifetime_ms=lifetime_ms,
            can_be_prefix=True,
            must_be_fresh=must_be_fresh,
            selector=InterestSelector(child=child),
        )
        return await self.express(interest, in_face)

    async def stream_fetch(
        self,
        name: Name,
        in_face: FaceId = 0,
        start_sequence: int = 0,
        lifetime_ms: int = 4000,
        max_segments: int | None = None,
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
