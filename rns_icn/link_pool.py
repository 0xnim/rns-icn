"""LinkPool — shared RNS Link management: reuse, health, announce injection."""

from __future__ import annotations

import asyncio
import contextlib
import time

import RNS

from .config import KnownPeer
from .metrics import metrics


class LinkPool:
    """Manages outbound RNS Links: reuse, health monitoring, announce injection.

    Used by both ICNClient and ICNServer for consistent link handling.
    """

    def __init__(
        self,
        identity: RNS.Identity,
        app_name: str,
        aspect: str,
        known_peers: list[KnownPeer],
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        self.identity = identity
        self.app_name = app_name
        self.aspect = aspect
        self.known_peers = {p.destination_hash: p for p in known_peers}
        self._loop = loop or asyncio.get_event_loop()
        self._links: dict[bytes, RNS.Link] = {}      # peer_hash -> link
        self._health: dict[bytes, float] = {}        # peer_hash -> last_activity_ts
        self._monitor_task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        """Start background health monitor."""
        if self._running:
            return
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_links())

    async def stop(self) -> None:
        """Stop monitor and teardown all links."""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._monitor_task
        for link in list(self._links.values()):
            try:
                link.teardown()
            except Exception as e:
                RNS.log(f"ICN LinkPool: link teardown on stop failed: {e}", RNS.LOG_DEBUG)
        await asyncio.sleep(0.2)
        self._links.clear()
        self._health.clear()

    async def get_link(self, peer_hash: bytes) -> RNS.Link | None:
        """Get existing active link or create new one."""
        # Return existing active link
        if peer_hash in self._links:
            link = self._links[peer_hash]
            if link.status == RNS.Link.ACTIVE:
                self._health[peer_hash] = time.time()
                return link
            else:
                # Dead link — remove and recreate
                del self._links[peer_hash]
                self._health.pop(peer_hash, None)

        # Create new link (resolves identity + path internally)
        link = await self._create_link(peer_hash)
        if link:
            self._links[peer_hash] = link
            self._health[peer_hash] = time.time()
            # Record link up
            metrics.record_link_up(peer_hash.hex())
        return link

    def _resolve_identity(self, peer_hash: bytes) -> RNS.Identity | None:
        """Resolve the peer's RNS.Identity from config or the known-destinations table."""
        peer_config = self.known_peers.get(peer_hash.hex())
        if peer_config and peer_config.identity_path:
            identity = RNS.Identity.from_file(peer_config.identity_path)
            if identity:
                return identity
        # Fall back to whatever RNS already learned via announces.
        return RNS.Identity.recall(peer_hash)

    async def _ensure_path(self, peer_hash: bytes, timeout: float = 30.0) -> bool:
        """Ensure RNS Transport has a path (next hop) to the peer destination."""
        if RNS.Transport.has_path(peer_hash):
            return True
        RNS.Transport.request_path(peer_hash)
        start = time.time()
        while time.time() - start < timeout:
            if RNS.Transport.has_path(peer_hash):
                return True
            await asyncio.sleep(0.25)
        return RNS.Transport.has_path(peer_hash)

    async def _create_link(self, peer_hash: bytes) -> RNS.Link | None:
        """Resolve identity + path, then establish an outbound Link to the peer."""
        identity = self._resolve_identity(peer_hash)
        if identity is None:
            RNS.log(f"ICN LinkPool: no identity for peer {peer_hash.hex()[:16]}", RNS.LOG_DEBUG)
            return None

        if not await self._ensure_path(peer_hash):
            RNS.log(f"ICN LinkPool: no path to peer {peer_hash.hex()[:16]}", RNS.LOG_DEBUG)
            return None

        dest = RNS.Destination(
            identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            self.app_name,
            self.aspect,
        )

        link = RNS.Link(dest)
        timeout = 120.0
        start = time.time()
        while link.status != RNS.Link.ACTIVE:
            if link.status == RNS.Link.CLOSED:
                return None
            await asyncio.sleep(0.1)
            if time.time() - start > timeout:
                try:
                    link.teardown()
                except Exception as e:
                    RNS.log(f"ICN LinkPool: teardown of timed-out link failed: {e}", RNS.LOG_DEBUG)
                return None

        return link

    async def _monitor_links(self) -> None:
        """Periodic health check — remove dead links."""
        while self._running:
            await asyncio.sleep(30)
            now = time.time()
            dead = [h for h, t in self._health.items() if now - t > 120]
            for h in dead:
                if h in self._links:
                    try:
                        self._links[h].teardown()
                    except Exception as e:
                        RNS.log(f"ICN LinkPool: teardown of idle link failed: {e}", RNS.LOG_DEBUG)
                    del self._links[h]
                    del self._health[h]
                    # Record link down
                    metrics.record_link_down(h.hex())

    def get_link_status(self, peer_hash: bytes) -> str | None:
        """Get status of a link if it exists."""
        link = self._links.get(peer_hash)
        if link:
            return str(link.status)
        return None

    @property
    def active_link_count(self) -> int:
        return sum(1 for link in self._links.values() if link.status == RNS.Link.ACTIVE)