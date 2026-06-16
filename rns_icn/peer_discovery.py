"""PeerDiscoveryManager — auto-discover ICN servers via RNS announces.

RNSICNServers announce themselves on the RNS mesh. The PeerDiscoveryManager
listens for these announces, maintains a registry of seen peers, and can
optionally auto-connect to establish links.

Capabilities are exchanged on each link establishment, so both sides know
what the other can do (role, version, features bitmask).

Flow:
  1. Server A announces on /icn/<aspect> with app_data=b"icn"
  2. PeerDiscoveryManager on Server B receives the announce
  3. Server B records the peer in its registry
  4. (optionally) Server B establishes a link to Server A
  5. On link establishment, both sides exchange CapPeer packets
  6. Capabilities stored on each server's PeerInfo entry
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import RNS

from .face import FaceId
from .packet import CapPeer

if TYPE_CHECKING:
    from .rns_server import ICNServer as RNSICNServer

logger = logging.getLogger(__name__)


class _AnnounceHandler:
    """RNS announce handler adapter for v1.2.8 API.

    RNS.Transport.register_announce_handler() takes a single object with:
      - aspect_filter: string (e.g. "icn/default") or callable filter
      - received_announce(destination_hash, announced_identity, app_data, ...)
    """

    def __init__(
        self,
        aspect_filter: str,
        on_announce: Callable,
    ):
        self.aspect_filter = aspect_filter
        self._on_announce = on_announce

    def received_announce(
        self,
        destination_hash: bytes,
        announced_identity: RNS.Identity,
        app_data: bytes,
    ) -> None:
        self._on_announce(destination_hash, announced_identity, app_data)


@dataclass
class PeerInfo:
    """Information about a discovered ICN server peer."""

    # RNS destination hex hash (16 bytes → 32 hex chars)
    hash: str
    # RNS Identity of the discovered server
    identity: RNS.Identity
    # App data from the latest announce
    app_data: bytes
    # Timestamps
    first_seen: float = 0.0
    last_seen: float = 0.0
    # Current link state
    face_id: FaceId | None = None
    # Capabilities (set after CapPeer exchange on link)
    capabilities: CapPeer | None = None

    @property
    def is_connected(self) -> bool:
        return self.face_id is not None

    @property
    def age_seconds(self) -> float:
        return time.time() - self.last_seen


# Callback type for peer discovery events
# (peer_hash: str, peer_info: PeerInfo)
DiscoveryCallback = Callable[[str, PeerInfo], None]


class PeerDiscoveryManager:
    """Listens for ICN announces on the RNS mesh and tracks peers.

    Usage:
        pdm = PeerDiscoveryManager(server)
        pdm.start(app_name="icn")
        ...
        pdm.stop()
    """

    def __init__(self, server: RNSICNServer | None = None):
        self._server = server
        self._handler_id: _AnnounceHandler | None = None
        self._peers: dict[str, PeerInfo] = {}
        self._callbacks: list[DiscoveryCallback] = []
        self._app_name: str = ""
        self._aspect: str = ""

    # ── Lifecycle ──

    def start(self, app_name: str = "icn", aspect: str = "default") -> None:
        """Register the announce handler to start listening for peers.

        Args:
            app_name: RNS app name to listen for (must match servers).
            aspect: RNS aspect to listen for.
        """
        self._app_name = app_name
        self._aspect = aspect
        try:
            # RNS v1.2.8 API: register_announce_handler takes a single handler
            # object with aspect_filter + received_announce callback
            handler = _AnnounceHandler(
                aspect_filter=f"{app_name}/{aspect}",
                on_announce=self._on_announce,
            )
            RNS.Transport.register_announce_handler(handler)
            self._handler_id = handler
            logger.info(
                "PeerDiscovery: listening for announces on /%s/%s",
                app_name, aspect,
            )
        except Exception as e:
            logger.warning("PeerDiscovery: failed to register announce handler: %s", e)

    def stop(self) -> None:
        """Unregister the announce handler."""
        if self._handler_id is not None:
            try:
                RNS.Transport.deregister_announce_handler(self._handler_id)
            except Exception:
                logger.debug("deregister_announce_handler failed", exc_info=True)
        self._peers.clear()

    # ── Callbacks ──

    def add_callback(self, cb: DiscoveryCallback) -> None:
        """Register a callback for new peer discovery events.

        Called when a new announce is received. Receives (peer_hash, PeerInfo).
        """
        self._callbacks.append(cb)

    def remove_callback(self, cb: DiscoveryCallback) -> None:
        """Remove a previously registered callback."""
        if cb in self._callbacks:
            self._callbacks.remove(cb)

    # ── Peer registry ──

    def get_peer(self, peer_hash: str) -> PeerInfo | None:
        """Look up a peer by its RNS destination hex hash."""
        return self._peers.get(peer_hash)

    def get_peers(self) -> dict[str, PeerInfo]:
        """Return a copy of all known peers."""
        return dict(self._peers)

    def peer_count(self) -> int:
        return len(self._peers)

    def remove_peer(self, peer_hash: str) -> None:
        """Remove a peer from the registry (e.g. on link teardown)."""
        self._peers.pop(peer_hash, None)

    def update_peer_capabilities(
        self, peer_hash: str, capabilities: CapPeer
    ) -> None:
        """Update a peer's capabilities after exchange."""
        info = self._peers.get(peer_hash)
        if info is not None:
            info.capabilities = capabilities

    def update_peer_face(
        self, peer_hash: str, face_id: FaceId
    ) -> None:
        """Associate a face with a discovered peer after link establishment."""
        info = self._peers.get(peer_hash)
        if info is not None:
            info.face_id = face_id

    def clear_face(self, face_id: FaceId) -> None:
        """Detach a face from its peer (e.g. on link teardown).

        The peer entry is kept so a later re-announce can trigger reconnect;
        only the live face association is cleared.
        """
        for info in self._peers.values():
            if info.face_id == face_id:
                info.face_id = None

    def peer_hash_for_face(self, face_id: FaceId) -> str | None:
        """Resolve a face ID back to the peer's hex hash.

        Search is O(n) — peers are usually few (<100).
        """
        for h, info in self._peers.items():
            if info.face_id == face_id:
                return h
        return None

    # ── Internal ──

    def _on_announce(
        self, dest_hash: bytes, announced_identity: RNS.Identity, app_data: bytes
    ) -> None:
        """RNS Transport announce callback.

        Called on the RNS thread whenever an announce matching our
        app_name/aspect is received.

        Args:
            dest_hash: 16-byte destination hash
            announced_identity: RNS.Identity that announced
            app_data: announce application data
        """
        hex_hash = dest_hash.hex()
        now = time.time()

        existing = self._peers.get(hex_hash)
        if existing is not None:
            # Update existing entry
            existing.last_seen = now
            existing.app_data = app_data
            existing.identity = announced_identity
            logger.debug(
                "PeerDiscovery: re-announced %s (age=%.0fs)",
                hex_hash[:16], existing.age_seconds,
            )
            # A re-announce from a peer we are *not* currently linked to is the
            # reconnect signal: fire callbacks so the owner can re-establish the
            # link and re-install its FIB route (dynamic FIB re-install). When a
            # link is live (face_id set) we stay quiet to avoid churn.
            if existing.face_id is None:
                for cb in self._callbacks:
                    try:
                        cb(hex_hash, existing)
                    except Exception as e:
                        logger.warning("PeerDiscovery: callback error: %s", e)
            return

        # New peer discovered
        info = PeerInfo(
            hash=hex_hash,
            identity=announced_identity,
            app_data=app_data,
            first_seen=now,
            last_seen=now,
        )
        self._peers[hex_hash] = info

        logger.info(
            "PeerDiscovery: discovered %s (identity=%s, app_data=%s)",
            hex_hash[:16], str(announced_identity)[:16], app_data.hex(),
        )

        # Notify callbacks
        for cb in self._callbacks:
            try:
                cb(hex_hash, info)
            except Exception as e:
                logger.warning("PeerDiscovery: callback error: %s", e)

    # ── Auto-connect helper ──

    async def auto_connect(self, peer_hash: str) -> FaceId | None:
        """Establish an outbound link to a discovered peer.

        Connects to the peer via the server's connect() method, then
        exchanges capabilities.

        Args:
            peer_hash: RNS destination hex hash of the peer.

        Returns:
            FaceId if connected, None on failure.
        """
        if self._server is None:
            return None

        face_id = await self._server.connect(peer_hash)
        if face_id is not None:
            self.update_peer_face(peer_hash, face_id)
            # Capability exchange is handled by the server's link lifecycle
            # (_send_capabilities is called from _on_incoming_link / after connect)
            return face_id

        return None
