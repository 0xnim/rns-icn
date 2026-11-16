"""Follower — fill the cache ahead of demand.

A cache that only stores what passes through it helps the *second* reader of an
item, and can only cover for an offline producer if somebody already asked for
the content. A hub that *follows* a producer holds that producer's content
before anyone asks:

  1. Subscribe (APS push, from now) to the producer's namespace on the upstream
     face, so new editions arrive as they are published.
  2. Fetch the producer's manifest from that face and pull whatever the local
     Content Store is missing: blobs whose content hash differs, and stream
     editions above the highest sequence already held.

Step 2 runs on every (re)connect and optionally on a timer, so a hub that was
partitioned catches up without re-transferring what it already has.

Trust: pushed Data is cached only when it arrives on the followed face *and* is
named under the followed producer. If it carries a signature and a validator
for the producer is known, the signature must verify. Everything else on that
face keeps the default solicited-only cache policy.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .face import FaceId
from .manifest import EntryKind, Manifest
from .name import Name
from .packet import APSubscribe, ChildSelector, Data, Interest, InterestSelector

if TYPE_CHECKING:
    from .server import ICNServer

logger = logging.getLogger(__name__)

# (signature, signed_hash) -> bool, e.g. RNS.Identity.validate
Validator = Callable[[bytes, bytes], bool]


@dataclass
class FollowStats:
    """Counters for one followed producer."""

    syncs: int = 0
    pulled: int = 0          # items fetched by manifest catch-up
    pushed: int = 0          # items accepted from live push
    rejected: int = 0        # pushes dropped (bad signature / content hash)
    last_error: str | None = None


@dataclass
class _Follow:
    producer: bytes
    labels: frozenset[str]
    face_id: FaceId
    stats: FollowStats = field(default_factory=FollowStats)
    task: asyncio.Task | None = None


class Follower:
    """Keeps the local Content Store filled with followed producers' content."""

    def __init__(
        self,
        server: ICNServer,
        validator_for: Callable[[bytes], Validator | None] | None = None,
        *,
        lifetime_ms: int = 60_000,
        max_items_per_stream: int = 1000,
    ):
        """Args:
            server: the owning server (forwarder, faces, APS).
            validator_for: producer address -> signature validator, or None if
                that producer's key is unknown. Without it, signed pushes are
                accepted on link trust alone.
            lifetime_ms: Interest lifetime for catch-up fetches. Generous on
                purpose: a multi-kilobyte item takes tens of seconds at radio
                bitrates, and giving up early only wastes the transfer.
            max_items_per_stream: cap on editions pulled per stream per sync.
        """
        self._server = server
        self._validator_for = validator_for
        self._lifetime_ms = lifetime_ms
        self._max_items = max_items_per_stream
        self._by_producer: dict[bytes, _Follow] = {}
        # Names of catch-up Interests in flight, so their answers take the
        # normal solicited-Data path instead of being treated as pushes.
        self._asking: list[Name] = []

    # ── Registration ──

    def follow(
        self,
        face_id: FaceId,
        producer: bytes,
        labels: list[str] | None = None,
        resync_interval: float = 0.0,
    ) -> asyncio.Task:
        """Follow ``producer`` over ``face_id``; returns the running sync task.

        ``labels`` restricts the follow to those top-level labels (the first
        path component under the producer); empty/None follows everything.
        Calling again for the same producer (e.g. after a reconnect) replaces
        the face and restarts the sync.
        """
        previous = self._by_producer.get(producer)
        if previous is not None and previous.task is not None:
            previous.task.cancel()
        entry = _Follow(
            producer=producer,
            labels=frozenset(labels or ()),
            face_id=face_id,
            stats=previous.stats if previous is not None else FollowStats(),
        )
        self._by_producer[producer] = entry
        entry.task = asyncio.ensure_future(self._run(entry, resync_interval))
        return entry.task

    def unfollow_face(self, face_id: FaceId) -> None:
        """Stop syncing over a face that closed. The follow is re-armed by the
        next ``follow()`` call for that producer."""
        for entry in self._by_producer.values():
            if entry.face_id == face_id and entry.task is not None:
                entry.task.cancel()
                entry.task = None

    def stop(self) -> None:
        for entry in self._by_producer.values():
            if entry.task is not None:
                entry.task.cancel()
                entry.task = None

    def stats(self) -> dict[str, FollowStats]:
        return {p.hex(): e.stats for p, e in self._by_producer.items()}

    def producers(self) -> list[bytes]:
        """Addresses of the producers being followed, in a stable order."""
        return sorted(self._by_producer)

    def is_following(self, producer: bytes) -> bool:
        return producer in self._by_producer

    # ── Live push ──

    def accepts_push(self, data: Data, face_id: FaceId) -> bool:
        """Whether unsolicited ``data`` from ``face_id`` may be cached."""
        entry = self._by_producer.get(data.name.rns_addr)
        if entry is None or entry.face_id != face_id:
            return False
        if not self._label_wanted(entry, data.name):
            return False
        if any(data.name.starts_with(asked) for asked in self._asking):
            return False  # an answer to our own catch-up Interest, not a push
        if not data.verify_content_hash():
            entry.stats.rejected += 1
            return False
        if data.signature is not None and self._validator_for is not None:
            validator = self._validator_for(entry.producer)
            if validator is not None and not data.verify_signature(validator):
                entry.stats.rejected += 1
                logger.warning("follow: bad signature on pushed %s", data.name)
                return False
        return True

    async def handle_push(self, data: Data, face_id: FaceId) -> None:
        """Cache an accepted push and relay it to our own subscribers."""
        accepted = await self._server.forwarder.receive_data(
            data, face_id, cache_unsolicited=True
        )
        if accepted is False:
            return
        entry = self._by_producer.get(data.name.rns_addr)
        if entry is not None:
            entry.stats.pushed += 1
        await self._server.aps.publish(data, offline_queue=self._server.offline_queue)

    # ── Sync ──

    async def _run(self, entry: _Follow, resync_interval: float) -> None:
        try:
            await self._subscribe(entry)
            while True:
                try:
                    await self.sync(entry.producer)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # a failed sync must not kill the follow
                    entry.stats.last_error = str(e)
                    logger.warning("follow: sync of %s failed: %s", entry.producer.hex(), e)
                if resync_interval <= 0:
                    return
                await asyncio.sleep(resync_interval)
        except asyncio.CancelledError:
            pass

    async def _subscribe(self, entry: _Follow) -> None:
        face = self._server._faces.get(entry.face_id)
        if face is None:
            return
        prefixes = (
            [Name(entry.producer, [label.encode()]) for label in sorted(entry.labels)]
            if entry.labels
            else [Name(entry.producer)]
        )
        for prefix in prefixes:
            # From now: history is pulled by manifest diff, not re-pushed whole.
            await face.send_raw(APSubscribe(name=prefix, start_from_now=True).to_bytes())

    async def sync(self, producer: bytes) -> int:
        """Pull what we are missing from ``producer``. Returns items fetched."""
        entry = self._by_producer.get(producer)
        if entry is None:
            return 0
        entry.stats.syncs += 1
        manifest_data = await self._ask(
            entry, Interest(name=Name(producer, [b"manifest"]), lifetime_ms=self._lifetime_ms)
        )
        if manifest_data is None:
            entry.stats.last_error = "no manifest"
            return 0
        manifest = Manifest.from_data(manifest_data)

        fetched = 0
        streams_done: set[str] = set()
        for item in manifest.entries:
            name = item.name
            if name.rns_addr != producer or not self._label_wanted(entry, name):
                continue  # downstream/aggregated entries are not ours to mirror
            if item.kind is EntryKind.STREAM:
                label = name.components[1] if name.len() > 1 else b""
                key = label.decode("utf-8", errors="replace")
                if key in streams_done:
                    continue
                streams_done.add(key)
                fetched += await self._sync_stream(entry, Name(producer, [label]), item)
            elif item.kind is EntryKind.BLOB:
                fetched += await self._sync_blob(entry, item)
        entry.stats.pulled += fetched
        entry.stats.last_error = None
        if fetched:
            logger.info("follow: pulled %d item(s) from %s", fetched, producer.hex())
        return fetched

    async def _sync_blob(self, entry: _Follow, item) -> int:
        held = self._server.forwarder.cs.get(item.name.without_content_hash())
        if held is not None and (
            item.content_hash is None or held.metadata.content_hash == item.content_hash
        ):
            return 0
        got = await self._ask(
            entry,
            Interest(name=item.name.without_content_hash(), lifetime_ms=self._lifetime_ms),
        )
        return 1 if got is not None else 0

    async def _sync_stream(self, entry: _Follow, prefix: Name, item) -> int:
        cs = self._server.forwarder.cs
        newest = cs.get_prefix(prefix, child=ChildSelector.LATEST)
        held_max = newest.metadata.sequence if newest is not None else None
        if (
            held_max is not None
            and item.latest_sequence is not None
            and held_max >= item.latest_sequence
        ):
            return 0
        floor = 0 if held_max is None else held_max + 1
        # The manifest says where the stream ends, so the walk stops there
        # instead of sending one more Interest and waiting for it to time out.
        last = item.latest_sequence
        fetched = 0
        while fetched < self._max_items and (last is None or floor <= last):
            got = await self._ask(
                entry,
                Interest(
                    name=prefix,
                    lifetime_ms=self._lifetime_ms,
                    can_be_prefix=True,
                    selector=InterestSelector(child=ChildSelector.OLDEST, min_sequence=floor),
                ),
            )
            if got is None:
                break
            seq = got.metadata.sequence
            if seq is None or seq < floor:
                break  # upstream ignored the selector; don't guess
            floor = seq + 1
            fetched += 1
        return fetched

    async def _ask(self, entry: _Follow, interest: Interest) -> Data | None:
        """Send an Interest out the followed face, bypassing the local cache.

        Catch-up exists to find out what upstream has that we don't, so a local
        hit would defeat it. The answer is cached by the forwarder's normal
        solicited-Data path.
        """
        if entry.face_id not in self._server._faces:
            return None
        asked = interest.name.without_content_hash()
        self._asking.append(asked)
        try:
            return await self._server.forwarder._forward(interest, 0, [entry.face_id])
        finally:
            self._asking.remove(asked)

    @staticmethod
    def _label_wanted(entry: _Follow, name: Name) -> bool:
        if not entry.labels:
            return True
        if name.len() <= 1:
            return False
        return name.components[1].decode("utf-8", errors="replace") in entry.labels
