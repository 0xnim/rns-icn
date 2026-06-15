"""PropagationManager — ICN Propagation Node.

ICN servers peer and sync streams so content propagates across the mesh.
A consumer connects to any peered server and gets the full stream even
if the original producer is offline.

Modeled on RNS Propagation Nodes: peered servers exchange manifests,
subscribe to each other's streams via APS, and forward pushed content
to all peers so it replicates across the network.

Flow:
  1. Peering handshake (PropPeer packet) — two servers declare peer relationship
  2. Manifest exchange — each peer fetches the other's content manifest
  3. Stream subscription — each peer APS-subscribes to discovered streams
  4. Content propagation — publish_pushed() forwards to all peers
  5. Catch-up — new peers get existing content via start_from_now=False subscribe
"""

from __future__ import annotations

import logging
from typing import Optional

from .face import FaceId
from .name import Name
from .packet import Data, PropPeer

logger = logging.getLogger(__name__)


class PropagationError(Exception):
    """Raised on propagation failures (peering, sync, forward)."""
    ...


class PropagationManager:
    """Manages peered propagation relationships between ICN servers.

    Each server has one PropagationManager that tracks peered servers
    (identified by their RNS address prefix and the FaceId of the
    peering link). When content is published via publish_pushed(),
    the PropagationManager forwards it to all peers, which in turn
    forward to their peers (preventing loops via PIT nonce tracking).

    On peering, each side fetches the other's manifest and subscribes
    to all discovered streams using APS with start_from_now=False,
    which triggers a push of existing content.
    """

    def __init__(self, server: "ICNServer" = None):  # noqa: F821
        self._server = server
        # peer_face -> peer's RNS address (16 bytes)
        self._peers: dict[FaceId, bytes] = {}
        # synced streams — set of producer-prefix Names we've already synced
        self._synced_producers: set[Name] = set()
        # downstream peers — peers whose manifests we summarize into ours
        self._downstream_peers: set[FaceId] = set()

    # ── Peer management ──

    def add_peer(self, face_id: FaceId, rns_addr: bytes) -> None:
        """Register a face as a propagation peer.

        Args:
            face_id: The face representing the link to the peer.
            rns_addr: 16-byte RNS address of the peered server.
        """
        self._peers[face_id] = rns_addr

    def remove_peer(self, face_id: FaceId) -> None:
        """Unregister a propagation peer (on link teardown)."""
        self._peers.pop(face_id, None)
        self._downstream_peers.discard(face_id)
        self._synced_producers.discard(
            self._producer_name_for_face(face_id)
        )

    def is_peer(self, face_id: FaceId) -> bool:
        """Check if a face is a registered propagation peer."""
        return face_id in self._peers

    def peer_count(self) -> int:
        """Number of peered servers."""
        return len(self._peers)

    def peer_prefixes(self) -> list[Name]:
        """Return the producer prefix Names for all peers."""
        return [Name(addr) for addr in self._peers.values()]

    # ── Downstream peer tracking ──

    def mark_downstream(self, face_id: FaceId) -> None:
        """Mark a peer as downstream — its manifest is summarized into ours.

        Downstream peers are typically origin/cache servers whose content
        should appear in the parent (propagation/root) server's manifest,
        enabling a hierarchical content directory.
        """
        if self.is_peer(face_id):
            self._downstream_peers.add(face_id)

    def mark_upstream(self, face_id: FaceId) -> None:
        """Mark a peer as upstream — remove from downstream set."""
        self._downstream_peers.discard(face_id)

    @property
    def downstream_faces(self) -> list[FaceId]:
        """Faces of peers marked as downstream."""
        return list(self._downstream_peers)

    def is_downstream(self, face_id: FaceId) -> bool:
        """Check if a peer face is marked as downstream."""
        return face_id in self._downstream_peers

    # ── Manifest fetching ──

    async def fetch_peer_manifest(self, face_id: FaceId) -> Optional["Manifest"]:  # noqa: F821
        """Fetch and parse a peer's content manifest.

        First checks the local ContentStore (manifest may be cached from
        peering sync), then falls back to express Interest/Data exchange
        via the forwarder if not cached.

        Returns:
            Parsed Manifest, or None on failure.
        """
        if self._server is None:
            return None
        addr = self._peers.get(face_id)
        if addr is None:
            return None

        # Check CS cache first
        manifest_name = Name(addr, [b"manifest"])
        cached_data = self._server.forwarder.cs.get(manifest_name)
        if cached_data is not None:
            from .manifest import Manifest
            try:
                return Manifest.from_data(cached_data)
            except Exception:
                pass

        # Fall back to express Interest fetch
        data = await self.fetch_peer_manifest_raw(face_id)
        if data is None:
            return None
        from .manifest import Manifest
        try:
            return Manifest.from_data(data)
        except Exception:
            return None

    async def fetch_downstream_manifests(self) -> dict[bytes, "Manifest"]:  # noqa: F821
        """Fetch manifests from all downstream peers.

        First checks the local ContentStore (manifest may have been cached
        during peering sync). Falls back to express Interest/Data fetch
        if not cached. Returns a dict mapping peer RNS address (16 bytes)
        -> parsed Manifest. Peers that fail to respond are silently skipped.
        """
        results: dict[bytes, Manifest] = {}
        for face_id in self._downstream_peers:
            addr = self._peers.get(face_id)
            if addr is None:
                continue

            manifest_data = None
            # Check CS cache first (manifest may be cached from peering sync)
            if self._server is not None:
                manifest_name = Name(addr, [b"manifest"])
                manifest_data = self._server.forwarder.cs.get(manifest_name)

            # Fall back to express Interest/Data fetch if not cached
            if manifest_data is None:
                manifest_data = await self.fetch_peer_manifest_raw(face_id)

            if manifest_data is not None:
                from .manifest import Manifest
                try:
                    results[addr] = Manifest.from_data(manifest_data)
                except Exception:
                    pass
        return results

    async def fetch_peer_manifest_raw(self, face_id: FaceId) -> Optional[Data]:
        """Fetch a peer's manifest Data directly via express Interest.

        Returns raw Data packet (not parsed Manifest), or None on failure.
        """
        if self._server is None:
            return None
        addr = self._peers.get(face_id)
        if addr is None:
            return None
        manifest_name = Name(addr, [b"manifest"])
        from .packet import Interest
        interest = Interest(name=manifest_name, lifetime_ms=10000)
        try:
            return await self._server.forwarder.express(interest, face_id)
        except Exception as e:
            logger.warning("Failed to fetch manifest from peer %s: %s", addr.hex(), e)
            return None

    def _producer_name_for_face(self, face_id: FaceId) -> Name:
        """Get the producer Name for a peered face."""
        addr = self._peers.get(face_id)
        if addr is None:
            return Name(bytes(16))
        return Name(addr)

    # ── Sync on peering ──

    async def sync_from_peer(self, face_id: FaceId) -> int:
        """Fetch a peer's manifest and subscribe to all streams.

        Called after the PROP_PEER handshake completes. Fetches the
        peer's content manifest, then subscribes to each stream with
        start_from_now=False to pull existing content.

        Returns:
            Number of streams subscribed to.
        """
        if self._server is None:
            return 0

        addr = self._peers.get(face_id)
        if addr is None:
            raise PropagationError("face is not a peered server")

        peer_prefix = Name(addr)
        self._synced_producers.add(peer_prefix)

        # Fetch peer's manifest
        manifest_name = Name(addr, [b"manifest"])
        try:
            manifest_data = await self._manifest_fetch(face_id, manifest_name)
            if manifest_data is None:
                logger.info("No manifest from peer %s (empty or unreachable)", addr.hex())
                return 0
        except Exception as e:
            logger.warning("Failed to fetch manifest from peer %s: %s", addr.hex(), e)
            return 0

        # Parse manifest and subscribe to all stream entries
        from .manifest import Manifest
        try:
            manifest = Manifest.from_data(manifest_data)
        except Exception as e:
            logger.warning("Failed to parse peer manifest: %s", e)
            return 0

        subscribed = 0
        for entry in manifest.entries:
            if entry.kind.value == "stream" or entry.name is not None:
                sub_name = entry.name.without_content_hash()
                await self._aps_subscribe(face_id, sub_name, start_from_now=False)
                subscribed += 1

        logger.info(
            "Synced %d streams from peer %s (sequence %d)",
            subscribed, addr.hex(), manifest.sequence,
        )
        return subscribed

    async def _manifest_fetch(self, face_id: FaceId, manifest_name: Name) -> Optional[Data]:
        """Express an Interest for the peer's manifest and wait for Data."""
        from .packet import Interest
        interest = Interest(
            name=manifest_name,
            lifetime_ms=10000,
        )
        face = self._server._faces.get(face_id)
        if face is None:
            return None
        return await face.express_interest(interest)

    async def _aps_subscribe(self, face_id: FaceId, stream_name: Name,
                             start_from_now: bool = False) -> None:
        """Send an APS Subscribe for a peer's stream.

        Sends the APSubscribe packet to the peer's face. The peer handles
        it in handle_subscribe, which both registers the subscription and
        pushes existing content if start_from_now is False.
        """
        from .packet import APSubscribe
        sub = APSubscribe(name=stream_name, start_from_now=start_from_now)
        face = self._server._faces.get(face_id)
        if face is not None:
            await face.send_raw(sub.to_bytes())

    # ── Content propagation ──

    async def propagate(self, data: Data, exclude_face: FaceId = None) -> int:
        """Forward a Data packet to all peered servers.

        Called after publish_pushed() to replicate content across
        the mesh. Skips the face that originally sent us the data
        to prevent echo, and skips unseen peers (no face reference).

        Returns:
            Number of peers the data was forwarded to.
        """
        if self._server is None:
            return 0

        forwarded = 0
        for face_id in list(self._peers.keys()):
            if face_id == exclude_face:
                continue
            face = self._server._faces.get(face_id)
            if face is not None:
                try:
                    await face.send_data(data)
                    forwarded += 1
                except Exception as e:
                    logger.warning("Failed to propagate to face %d: %s", face_id, e)
        return forwarded

    async def propagate_to_new_peer(self, face_id: FaceId) -> int:
        """Push all local content to a newly peered server.

        Subscribes the new peer's face to all local stream prefixes
        with start_from_now=False so the peer receives existing content.

        Returns:
            Number of local streams pushed to the new peer.
        """
        if self._server is None:
            return 0

        # Build local manifest to discover streams
        from .manifest import EntryKind
        entries = list(self._server._build_manifest_entries())
        if not entries:
            return 0

        pushed = 0
        for entry in entries:
            if entry.kind == EntryKind.STREAM or entry.name is not None:
                stream_name = entry.name.without_content_hash()
                # Send APS Subscribe on the peer's behalf
                # (the peer's handle_subscribe will push existing content)
                await self._aps_subscribe(face_id, stream_name, start_from_now=False)
                pushed += 1

        # Also send the manifest itself
        manifest_name = Name(self._server.rns_addr, [b"manifest"])
        await self._aps_subscribe(face_id, manifest_name, start_from_now=False)
        pushed += 1

        return pushed

    async def handle_peer_data(self, data: Data, in_face: FaceId) -> None:
        """Handle Data received from a peered server.

        Caches the Data locally, resolves any pending PIT entries
        (for Interests that were forwarded to this peer), and
        propagates to other peers (excluding the sender). This ensures
        content from any peer replicates to all other peers.
        """
        if self._server is None:
            return

        # Cache locally AND resolve any pending PIT entries
        await self._server.forwarder.receive_data(data, in_face)

        # Propagate to other peers
        await self.propagate(data, exclude_face=in_face)

    async def handle_peer_handshake(self, peer: PropPeer, face_id: FaceId) -> None:
        """Handle an incoming PROP_PEER handshake.

        Registers the sending server as a peer and triggers bidirectional
        content sync.
        """
        addr = peer.rns_addr
        self.add_peer(face_id, addr)

        # Add FIB route to the peer's producer prefix
        peer_prefix = Name(addr)
        self._server.forwarder.add_route(peer_prefix, face_id, cost=20)

        logger.info(
            "Peered with %s on face %d (wants_sync=%s)",
            addr.hex(), face_id, peer.wants_sync,
        )

        # If peer wants sync, send our content
        if peer.wants_sync:
            await self.propagate_to_new_peer(face_id)

        # Always sync from peer
        await self.sync_from_peer(face_id)
