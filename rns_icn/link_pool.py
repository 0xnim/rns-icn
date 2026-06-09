"""LinkPool — shared RNS Link management: reuse, health, announce injection."""

from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional

import RNS

from .config import KnownPeer


class LinkPool:
    """Manages outbound RNS Links: reuse, health monitoring, announce injection.

    Used by both ICNClient and ICNServer for consistent link handling.
    """

    def __init__(
        self,
        identity: RNS.Identity,
        app_name: str,
        aspect: str,
        known_peers: List[KnownPeer],
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.identity = identity
        self.app_name = app_name
        self.aspect = aspect
        self.known_peers = {p.destination_hash: p for p in known_peers}
        self._loop = loop or asyncio.get_event_loop()
        self._links: Dict[bytes, RNS.Link] = {}      # peer_hash -> link
        self._health: Dict[bytes, float] = {}        # peer_hash -> last_activity_ts
        self._monitor_task: Optional[asyncio.Task] = None
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
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        for link in list(self._links.values()):
            try:
                link.teardown()
            except Exception:
                pass
        await asyncio.sleep(0.2)
        self._links.clear()
        self._health.clear()

    async def get_link(self, peer_hash: bytes) -> Optional[RNS.Link]:
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

        # Ensure announce in table
        await self._ensure_announce(peer_hash)

        # Create new link
        link = await self._create_link(peer_hash)
        if link:
            self._links[peer_hash] = link
            self._health[peer_hash] = time.time()
        return link

    async def _ensure_announce(self, peer_hash: bytes) -> None:
        """Ensure peer announce is in RNS transport table."""
        if peer_hash in RNS.Transport.announce_table:
            return

        peer_config = self.known_peers.get(peer_hash.hex())
        if peer_config and peer_config.identity_path:
            identity = RNS.Identity.from_file(peer_config.identity_path)
            if identity:
                dest = RNS.Destination(
                    identity,
                    RNS.Destination.IN,
                    RNS.Destination.SINGLE,
                    self.app_name,
                    self.aspect,
                )
                RNS.Transport.announce_table[peer_hash] = dest
                return

        # Fallback: request path and wait for announce
        RNS.Transport.request_path(peer_hash, None, None, False)
        await self._wait_for_announce(peer_hash, timeout=30.0)

    async def _create_link(self, peer_hash: bytes) -> Optional[RNS.Link]:
        """Create and establish a new Link to peer."""
        dest = RNS.Transport.announce_table.get(peer_hash)
        if not dest:
            return None

        link = RNS.Link(dest)
        timeout = 120.0
        start = time.time()

        while link.status != RNS.Link.ACTIVE:
            if link.status == RNS.Link.CLOSED:
                return None
            await asyncio.sleep(0.1)
            if time.time() - start > timeout:
                return None

        return link

    async def _wait_for_announce(self, peer_hash: bytes, timeout: float) -> None:
        """Wait for announce to arrive in transport table."""
        start = time.time()
        while time.time() - start < timeout:
            if peer_hash in RNS.Transport.announce_table:
                return
            await asyncio.sleep(5.0)
            RNS.Transport.request_path(peer_hash, None, None, False)

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
                    except Exception:
                        pass
                    del self._links[h]
                    del self._health[h]

    def get_link_status(self, peer_hash: bytes) -> Optional[str]:
        """Get status of a link if it exists."""
        link = self._links.get(peer_hash)
        if link:
            return str(link.status)
        return None

    @property
    def active_link_count(self) -> int:
        return sum(1 for link in self._links.values() if link.status == RNS.Link.ACTIVE)