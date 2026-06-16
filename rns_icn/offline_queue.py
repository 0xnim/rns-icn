"""OfflineQueue — queue content for offline subscribers, deliver on reconnect.

When a subscriber's link drops (face becomes unreachable), any data published
to their subscribed stream during the offline period is held here. On reconnect
and re-subscribe, the queued data is drained to the new face.

Design:
  - Keyed by stream *name* (the Name the subscriber subscribed to), not face_id.
    This is deliberate: a subscriber reconnects with a new face_id but the same
    stream name, so we deliver queued data to the new face.
  - Each item is a (timestamp, Data) tuple for TTL-based pruning.
  - Thread-safe within the asyncio event loop — no locks needed.
"""

from __future__ import annotations

import logging
import time

from .face import FaceId
from .name import Name
from .packet import Data

logger = logging.getLogger(__name__)


class OfflineQueue:
    """Holds Data packets for subscribers whose faces were unreachable.

    Args:
        server: ICNServer instance (for access to _faces dict for drain).
        max_age_seconds: TTL for queued items. Items older than this are
            dropped by prune(). Default 24h.
    """

    def __init__(self, server=None, max_age_seconds: int = 86400):
        self._server = server
        self._max_age = max_age_seconds
        # stream_name -> [(timestamp_unix, Data)]
        self._queue: dict[Name, list[tuple[float, Data]]] = {}

    # ── Public API ──────────────────────────────────────────────────────

    def put(self, stream_name: Name, data: Data) -> None:
        """Queue a Data packet for an offline subscriber stream."""
        if stream_name not in self._queue:
            self._queue[stream_name] = []
        self._queue[stream_name].append((time.time(), data))

    async def drain(self, stream_name: Name, face_id: FaceId) -> int:
        """Deliver all queued data for *stream_name* to *face_id*.

        Returns the number of Data packets successfully sent.
        If the face is no longer available the items are re-queued.
        """
        items = self._queue.pop(stream_name, [])
        if not items:
            return 0
        if self._server is None:
            self._queue[stream_name] = items
            return 0
        face = self._server._faces.get(face_id)
        if face is None:
            # Face dropped before we could drain — put it all back
            self._queue[stream_name] = items
            return 0
        sent = 0
        for _, data in items:
            try:
                await face.send_data(data)
                sent += 1
            except Exception:
                logger.warning(
                    "drain to face %s failed for %s", face.id(), stream_name, exc_info=True
                )
        return sent

    # ── Inspection / management ─────────────────────────────────────────

    def pending_count(self, stream_name: Name) -> int:
        """Number of queued Data packets for a stream."""
        return len(self._queue.get(stream_name, []))

    def total_pending(self) -> int:
        """Total queued Data packets across all streams."""
        return sum(len(items) for items in self._queue.values())

    def stream_count(self) -> int:
        """Number of distinct streams with queued data."""
        return len(self._queue)

    def peek(self, stream_name: Name, limit: int = 5) -> list[tuple[str, int, float]]:
        """Preview queued items for a stream.

        Returns list of (content_hash_prefix, size_bytes, age_seconds).
        """
        items = self._queue.get(stream_name, [])
        now = time.time()
        preview = []
        for ts, data in items[:limit]:
            h = data.metadata.content_hash[:4].hex() if data.metadata.content_hash else "none"
            preview.append((h, len(data.content), now - ts))
        return preview

    # ── Cleanup ─────────────────────────────────────────────────────────

    def prune(self) -> int:
        """Remove all items older than *max_age_seconds*.

        Returns the number of expired items removed.
        """
        now = time.time()
        removed = 0
        for stream_name in list(self._queue.keys()):
            items = self._queue[stream_name]
            fresh = [(t, d) for t, d in items if now - t < self._max_age]
            expired = len(items) - len(fresh)
            if expired > 0:
                removed += expired
            if fresh:
                self._queue[stream_name] = fresh
            else:
                del self._queue[stream_name]
        return removed

    def cleanup(self, stream_name: Name) -> int:
        """Remove ALL queued items for a stream. Returns count removed."""
        items = self._queue.pop(stream_name, [])
        return len(items)

    def clear(self) -> int:
        """Remove all queued items. Returns total count cleared."""
        total = self.total_pending()
        self._queue.clear()
        return total
