"""LinkPool — shared RNS Link management: reuse, health, announce injection."""

from __future__ import annotations

import asyncio
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
        connect_timeout: float = 120.0,
        path_request_timeout: float = 30.0,
        idle_timeout: float | None = 120.0,
    ):
        self.identity = identity
        self.app_name = app_name
        self.aspect = aspect
        self.known_peers = {p.destination_hash: p for p in known_peers}
        self._loop = loop or asyncio.get_event_loop()
        self.connect_timeout = connect_timeout
        self.path_request_timeout = path_request_timeout
        # Seconds a link may go unused before the monitor closes it; None keeps
        # links for as long as Reticulum considers them alive (servers, whose
        # upstream links carry forwarded traffic and pushes).
        self.idle_timeout = idle_timeout
        self._links: dict[bytes, RNS.Link] = {}      # peer_hash -> link
        self._health: dict[bytes, float] = {}        # peer_hash -> last_activity_ts
        self._monitor_task: asyncio.Task | None = None
        self._running = False
        self._stopped = False  # get_link is supported before the first start().
        self._pending: dict[bytes, asyncio.Task] = {}
        self._stop_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start monitoring, or explicitly reopen a stopped pool."""
        if self._stop_task is not None:
            await asyncio.shield(self._stop_task)
        if self._running:
            return
        self._stopped = False
        self._stop_task = None
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_links())

    async def stop(self) -> None:
        """Reject new acquisitions and drain all owned links/establishments.

        Cancellation of a stop caller does not interrupt shared cleanup. A
        subsequent stop/start waits for that cleanup before returning/reopening.
        """
        if self._stop_task is None:
            self._stopped = True
            self._running = False
            self._stop_task = asyncio.create_task(self._stop())
        await asyncio.shield(self._stop_task)

    async def _stop(self) -> None:
        tasks = list(self._pending.values())
        if self._monitor_task is not None:
            tasks.append(self._monitor_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._pending.clear()
        self._monitor_task = None
        links = list(self._links.values())
        self._links.clear()
        self._health.clear()
        for link in links:
            self._teardown(link)
        await asyncio.sleep(0.2)

    @staticmethod
    def _teardown(link: RNS.Link) -> None:
        try:
            link.teardown()
        except Exception as e:
            RNS.log(f"ICN LinkPool: link teardown failed: {e}", RNS.LOG_DEBUG)

    def invalidate(self, peer_hash: bytes, expected_link: RNS.Link) -> bool:
        """Remove only the expected cached Link, synchronously on the owner loop.

        The caller owns teardown. A delayed old-face callback must not evict a
        replacement Link; pending establishments are not affected.
        """
        if self._links.get(peer_hash) is not expected_link:
            return False
        del self._links[peer_hash]
        self._health.pop(peer_hash, None)
        return True

    async def get_link(self, peer_hash: bytes) -> RNS.Link | None:
        """Reuse an ACTIVE Link or share one establishment per peer.

        Allowed before first start(); after stop(), start() must reopen the pool.
        Canceling an acquisition only cancels that waiter, not the shared work.
        """
        if self._stopped:
            raise RuntimeError("ICN LinkPool is stopped; call start() before get_link()")
        link = self._links.get(peer_hash)
        if link is not None:
            if link.status == RNS.Link.ACTIVE:
                self._health[peer_hash] = time.time()
                return link
            self.invalidate(peer_hash, link)

        task = self._pending.get(peer_hash)
        if task is None:
            task = asyncio.create_task(self._establish(peer_hash))
            self._pending[peer_hash] = task
            # Observe errors even if every shielded waiter was canceled.
            task.add_done_callback(self._observe_establishment)
        # wait() shields the owned task from caller cancellation without
        # shield() reporting its late error for each abandoned waiter.
        await asyncio.wait((task,))
        link = task.result()
        if self._stopped:
            raise RuntimeError("ICN LinkPool is stopped; call start() before get_link()")
        if link is None or link.status != RNS.Link.ACTIVE:
            return None
        if self._links.get(peer_hash) is not link:
            # Retirement/shutdown may run after establishment completes but
            # before this suspended acquirer resumes. Never hand out that Link.
            return None
        return link

    @staticmethod
    def _observe_establishment(task: asyncio.Task) -> None:
        if not task.cancelled():
            task.exception()

    async def _establish(self, peer_hash: bytes) -> RNS.Link | None:
        task = asyncio.current_task()
        try:
            link = await self._create_link(peer_hash)
            if link is not None:
                if self._stopped or self._pending.get(peer_hash) is not task:
                    self._teardown(link)
                    return None
                self._links[peer_hash] = link
                self._health[peer_hash] = time.time()
                metrics.record_link_up(peer_hash.hex())
            return link
        finally:
            if self._pending.get(peer_hash) is task:
                del self._pending[peer_hash]

    def _resolve_identity(self, peer_hash: bytes) -> RNS.Identity | None:
        """Resolve the peer's RNS.Identity from config or the known-destinations table."""
        peer_config = self.known_peers.get(peer_hash.hex())
        if peer_config and peer_config.identity_path:
            identity = RNS.Identity.from_file(peer_config.identity_path)
            if identity:
                return identity
        # Fall back to whatever RNS already learned via announces.
        return RNS.Identity.recall(peer_hash)

    async def _ensure_path(self, peer_hash: bytes, timeout: float | None = None) -> bool:
        """Ensure RNS Transport has a path (next hop) to the peer destination."""
        timeout = self.path_request_timeout if timeout is None else timeout
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
        # Path first: the answer to a path request is the peer's announce, which
        # is also where its identity comes from when none is configured.
        if not await self._ensure_path(peer_hash):
            RNS.log(f"ICN LinkPool: no path to peer {peer_hash.hex()[:16]}", RNS.LOG_NOTICE)
            return None

        identity = self._resolve_identity(peer_hash)
        if identity is None:
            RNS.log(f"ICN LinkPool: no identity for peer {peer_hash.hex()[:16]}", RNS.LOG_NOTICE)
            return None

        dest = RNS.Destination(
            identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            self.app_name,
            self.aspect,
        )

        link = RNS.Link(dest)
        try:
            start = time.monotonic()
            while link.status != RNS.Link.ACTIVE:
                if link.status == RNS.Link.CLOSED:
                    return None
                await asyncio.sleep(0.1)
                if time.monotonic() - start > self.connect_timeout:
                    self._teardown(link)
                    return None
            return link
        except BaseException:
            # The pool owns this native Link until successfully returned. In
            # particular, cancellation during handshake must not leak it.
            self._teardown(link)
            raise

    def _link_is_idle(self, link) -> bool:
        """Whether the link itself has carried no data for the idle timeout.

        ``_health`` only records when a caller last asked the pool for the
        link. A caller that holds the link and keeps using it (a face) never
        asks again, so the link's own activity has to be checked too — or an
        in-flight transfer gets torn down underneath it.
        """
        no_data_for = getattr(link, "no_data_for", None)
        if no_data_for is None or self.idle_timeout is None:
            return True
        try:
            return no_data_for() > self.idle_timeout
        except Exception:
            return True

    async def _monitor_links(self) -> None:
        """Periodic health check — remove dead links."""
        while self._running:
            await asyncio.sleep(30)
            if self.idle_timeout is None:
                continue
            now = time.time()
            dead = [
                h for h, t in self._health.items()
                if now - t > self.idle_timeout and self._link_is_idle(self._links.get(h))
            ]
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
