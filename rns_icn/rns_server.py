"""RNSICNServer — ICN server on the Reticulum mesh.

Wires the ICN Forwarder into RNS Destinations and Links.
Like an LXMF Propagation Node but for ICN content:
- Listens on an RNS Destination for incoming Links
- Each incoming Link becomes a LinkFace
- Can establish outbound Links to peers
- Publishes a manifest of available content
- Routes Interest/Data over the mesh
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import RNS

from .face import FaceId, LinkFace
from .manifest import EntryKind, Manifest, ManifestEntry
from .name import Name
from .packet import (
    CapPeer,
    Data,
    FEATURE_APS,
    FEATURE_CHUNKED,
    FEATURE_MANIFEST,
    FEATURE_OFFLINE_QUEUE,
    FEATURE_PROPAGATION,
    parse_packet,
)
from .peer_discovery import PeerDiscoveryManager
from .resource_transport import (
    DEFAULT_RESOURCE_THRESHOLD,
    LargeContentPublisher,
    ResourceListener,
    ResourcePublisher,
    ResourceTransportError,
)
from .rns_utils import load_or_create_identity
from .server import ICNServer, ServerRole


class RNSICNServer(ICNServer):
    """ICN Server integrated with RNS networking.

    Creates a RNS Destination for the 'icn' application,
    accepts incoming Links as Faces, and can establish
    outbound Links to known peers.
    """

    def __init__(
        self,
        identity: Optional[RNS.Identity] = None,
        identity_path: Optional[str] = None,
        app_name: str = "icn",
        aspect: str = "default",
        cs_max: int = 10000,
    ):
        if identity_path is not None:
            identity = load_or_create_identity(identity_path)
        self.identity = identity or RNS.Identity()
        self.app_name = app_name
        self.aspect = aspect

        # Base ICNServer uses the 16-byte RNS address
        super().__init__(self.identity.hash, cs_max=cs_max)

        # Created lazily in start()
        self.destination: Optional[RNS.Destination] = None

        self._links: dict[FaceId, RNS.Link] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Resource transport (for large chunks > threshold)
        self._resource_listener: Optional[ResourceListener] = None
        self._resource_threshold: int = DEFAULT_RESOURCE_THRESHOLD

        # Announce management
        self._announce_task: Optional[asyncio.Task] = None
        self._announce_interval: float = 300  # seconds between re-announces

        # Peer discovery
        self.discovery: PeerDiscoveryManager = PeerDiscoveryManager(self)

        # Compute feature bitmask for capability exchange
        self._features: int = self._compute_features()

    def _icn_app_data(self) -> bytes:
        """Build announce app_data with server role encoded."""
        return b"icn" + bytes([self.role.value])

    def start(self) -> None:
        """Start the server. Must be called from an async context with RNS initialized."""
        self._loop = asyncio.get_running_loop()

        # Create the destination that clients connect to
        self.destination = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            self.app_name,
            self.aspect,
        )
        self.destination.set_link_established_callback(self._on_incoming_link)

        # Force immediate announce so peers discover us via transport table
        self.destination.announce(app_data=self._icn_app_data())
        RNS.log(f"ICN: Announced destination {self.destination.hexhash} on /{self.app_name}/{self.aspect}")

        # Start periodic re-announce loop to keep transport table fresh
        self._announce_task = asyncio.ensure_future(self._announce_loop())

        # Start peer discovery
        self.discovery.start(app_name=self.app_name, aspect=self.aspect)

        RNS.log(f"ICN Server: {str(self.identity)}")
        RNS.log(f"ICN Destination: {self.destination.hexhash}")
        RNS.log(f"Listening on /{self.app_name}/{self.aspect}")

    def stop(self) -> None:
        """Stop the server, tear down all links and cancel announce loop."""
        # Stop peer discovery
        self.discovery.stop()

        # Cancel periodic announce loop
        if self._announce_task is not None:
            self._announce_task.cancel()
            self._announce_task = None

        for fid, link in list(self._links.items()):
            try:
                link.teardown()
            except Exception:
                pass
        self._links.clear()

    def announce(self, app_data: Optional[bytes] = None) -> None:
        """Force an announce of this server's destination.

        Broadcasts the destination over the mesh so peers can discover
        us via the RNS transport table. The announce includes optional
        application-specific data (default: b'icn').

        Args:
            app_data: Optional bytes to include in the announce.
        """
        if self.destination is None:
            raise RuntimeError("Server not started. Call start() first.")
        self.destination.announce(app_data=app_data or self._icn_app_data())
        RNS.log(f"ICN: Announced destination {self.destination.hexhash}")

    async def _announce_loop(self) -> None:
        """Periodically re-announce the destination to keep transport table fresh.

        RNS transport entries eventually expire; periodic re-announcement
        ensures peers can still route to us. Runs at self._announce_interval.
        """
        try:
            while True:
                await asyncio.sleep(self._announce_interval)
                if self.destination is not None:
                    self.destination.announce(app_data=self._icn_app_data())
                    log_hexhash = self.destination.hexhash
                    RNS.log(f"ICN: Re-announced destination {log_hexhash}")
        except asyncio.CancelledError:
            pass

    def _on_incoming_link(self, link: RNS.Link) -> None:
        """Called when a remote peer establishes a Link."""
        face = self._new_face()
        self._make_link_face(face.id(), link)
        self._links[face.id()] = link

        peer_hash = link.hash.hex()  # RNS v1.2.8: Link has .hash (bytes), not .hexhash
        link_id_str = peer_hash[:16]
        RNS.log(f"ICN: Incoming link from {link.get_remote_identity()} → face #{face.id()} (peer={link_id_str})")

        link.set_remote_identified_callback(
            lambda lk, ident: RNS.log(f"ICN: Link identity verified: {ident}")
        )

        # Register in discovery and send our capabilities
        self.discovery.update_peer_face(peer_hash, face.id())
        if self._loop is not None:
            asyncio.run_coroutine_threadsafe(
                self._send_capabilities(face.id(), peer_hash), self._loop
            )

    async def connect(self, peer_hash: str) -> Optional[FaceId]:
        """Establish an outbound Link to a peer ICN server.

        Args:
            peer_hash: RNS destination hex hash
        Returns:
            FaceId if connected, None on timeout
        """
        if self._loop is None or self.destination is None:
            raise RuntimeError("Server not started. Call start() first.")

        dest = RNS.Destination(
            self.identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
            self.app_name, self.aspect,
        )
        # Override the computed hash with the peer's destination hash
        dest.hash = bytes.fromhex(peer_hash)
        dest.hexhash = peer_hash

        link = RNS.Link(dest)
        timeout = 120.0
        start_time = asyncio.get_event_loop().time()
        while link.status != RNS.Link.ACTIVE:
            if link.status in (RNS.Link.CLOSED,):
                RNS.log(f"ICN: Link failed (reason: {link.teardown_reason})")
                return None
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start_time > timeout:
                RNS.log("ICN: Link establishment timed out")
                return None

        face = self._new_face()
        self._make_link_face(face.id(), link)
        self._links[face.id()] = link

        # Register in discovery and send capabilities
        self.discovery.update_peer_face(peer_hash, face.id())
        await self._send_capabilities(face.id(), peer_hash)

        RNS.log(f"ICN: Outbound link established → face #{face.id()}")
        return face.id()

    def _make_link_face(self, face_id: FaceId, link: RNS.Link) -> LinkFace:
        link_face = LinkFace(face_id, link, loop=self._loop)
        self.forwarder.register_face(link_face)
        self._faces[face_id] = link_face

        # Wire resource listener for large chunks arriving via RNS.Resource
        resource_listener = ResourceListener(link)
        resource_listener.set_on_data(self._on_resource_data)

        if self._loop is not None:
            asyncio.run_coroutine_threadsafe(
                self._process_link(face_id, link_face), self._loop
            )

        return link_face

    async def _process_link(self, face_id: FaceId, link_face: LinkFace) -> None:
        recv_queue = link_face._recv_queue
        while not link_face._closed:
            try:
                raw = await asyncio.wait_for(recv_queue.get(), timeout=5.0)
                if not raw:
                    break
                await self.handle_incoming(face_id, raw)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                RNS.log(f"ICN: Error on link {face_id}: {e}")
                break

    def publish_content(self, name: Name, content: bytes, sequence: Optional[int] = None) -> None:
        """Publish content into the ContentStore."""
        data = Data.new(name=name, content=content)
        if sequence is not None:
            data.with_sequence(sequence)
        self.forwarder.cs.insert(name, data)
        RNS.log(f"ICN: Published {name} ({len(content)} bytes)")

    def publish_manifest(self) -> None:
        """Build manifest from CS and cache it."""
        our_prefix = Name(self.rns_addr, [])
        groups: dict[str, list[Data]] = {}
        # Group CS entries by label
        for entry_name in list(self.forwarder.cs._entries.keys()):
            if entry_name.starts_with(our_prefix) and not entry_name.is_root():
                label = entry_name.components[1].decode("utf-8", errors="replace")
                # Get the actual data from CS
                data = self.forwarder.cs.get(entry_name)
                if data is not None:
                    groups.setdefault(label, []).append(data)

        entries = []
        for label, data_list in groups.items():
            sequences = [d.metadata.sequence for d in data_list if d.metadata.sequence is not None]
            if sequences:
                first = data_list[0]
                entries.append(
                    ManifestEntry(
                        kind=EntryKind.STREAM,
                        label=label,
                        name=first.name.without_content_hash(),
                        latest_sequence=max(sequences),
                        total_items=len(sequences),
                        start_time=int(time.time()) - len(sequences) * 60,
                        end_time=int(time.time()),
                    )
                )
            else:
                data = data_list[0]
                entries.append(
                    ManifestEntry(
                        kind=EntryKind.BLOB,
                        label=label,
                        name=data.name.without_content_hash(),
                        content_hash=data.metadata.content_hash,
                        size=len(data.content),
                    )
                )

        manifest = Manifest.create(producer=self.rns_addr, entries=entries)
        content = manifest.to_json()
        data = Data.new(name=manifest.manifest_name(), content=content)
        data.with_sequence(manifest.sequence)
        self.forwarder.cs.insert(data.name, data)
        RNS.log(f"ICN: Published manifest with {len(entries)} entries")

    @property
    def hexhash(self) -> str:
        d = self.destination
        return d.hexhash if d else self.identity.hexhash

    # ── Capability exchange ──

    def _compute_features(self) -> int:
        """Build the feature bitmask for capability exchange.

        Returns a 32-bit integer where each bit represents a supported
        feature that this server instance can provide.
        """
        features = 0
        # APS push subscriptions
        features |= FEATURE_APS
        # Content manifest
        features |= FEATURE_MANIFEST
        # Offline queue for disconnected subscribers
        features |= FEATURE_OFFLINE_QUEUE
        # Chunked content (if the chunker module is available)
        try:
            from .chunker import chunk_content
            features |= FEATURE_CHUNKED
        except ImportError:
            pass
        # Content propagation (if the propagation module is available)
        try:
            from .propagation import PropagationManager
            features |= FEATURE_PROPAGATION
        except ImportError:
            pass
        return features

    async def _send_capabilities(self, face_id: FaceId, peer_hash: str) -> None:
        """Send our CapPeer to a peer on the given face.

        Called immediately after link establishment (both incoming and
        outgoing) so the peer learns our role, version, and features.
        """
        cap = CapPeer(
            version=1,
            role=self.role.value,
            features=self._features,
        )
        face = self._faces.get(face_id)
        if face is not None:
            try:
                await face.send_raw(cap.to_bytes())
                RNS.log(f"ICN: Sent capabilities to peer {peer_hash[:16]} (role={self.role.name}, features={self._features:#010x})")
            except Exception as e:
                RNS.log(f"ICN: Failed to send capabilities to peer {peer_hash[:16]}: {e}")

    def _on_cap_peer(self, cap: CapPeer, face_id: FaceId, peer_hash: str) -> None:
        """Handle incoming capabilities from a peer.

        Stores the peer's capabilities in the discovery registry and
        logs the peer's role and features.
        """
        self.discovery.update_peer_capabilities(peer_hash, cap)
        role_names = ["ORIGIN", "CACHE", "PROPAGATION"]
        role_name = role_names[cap.role] if 0 <= cap.role < len(role_names) else f"UNKNOWN({cap.role})"
        RNS.log(f"ICN: Received capabilities from peer {peer_hash[:16]} (role={role_name}, features={cap.features:#010x}, version={cap.version})")

    # ── Packet handling override ──

    async def handle_incoming(self, face_id: FaceId, raw: bytes) -> None:
        """Handle incoming packets, including CapPeer for discovery."""
        try:
            from .packet import parse_packet
            pkt = parse_packet(raw)
        except (ValueError, Exception):
            return

        # Handle CapPeer — capability exchange on link
        if pkt.cap_peer is not None:
            peer_hash = self.discovery.peer_hash_for_face(face_id)
            if peer_hash is None:
                # Unknown peer — use a placeholder
                peer_hash = f"face_{face_id}"
            self._on_cap_peer(pkt.cap_peer, face_id, peer_hash)
            return

        # Fall through to base class for standard packet types
        await super().handle_incoming(face_id, raw)

    # ── Resource transport ──

    def _on_resource_data(self, data: Data) -> None:
        """Called when a Data packet arrives via RNS.Resource."""
        self.forwarder.cs.insert(data.name, data)
        RNS.log(f"ICN: Received Data via Resource: {data.name} ({len(data.content)} bytes)")

    def create_resource_publisher(self, link: RNS.Link) -> ResourcePublisher:
        """Create a ResourcePublisher for sending large Data over a Link.

        Use this when you need to send a Data packet that exceeds the
        configured threshold and should use RNS.Resource transport.

        Args:
            link: An established RNS.Link to send over.

        Returns:
            A ResourcePublisher bound to the link.
        """
        return ResourcePublisher(link)

    def create_large_content_publisher(
        self,
        link: RNS.Link,
        threshold: Optional[int] = None,
    ) -> LargeContentPublisher:
        """Create a LargeContentPublisher for chunked content.

        Automatically uses RNS.Resource for Data packets whose serialised
        size exceeds *threshold* (defaults to *resource_threshold*).

        Args:
            link: An established RNS.Link.
            threshold: Size threshold in bytes (default: 100 KB).

        Returns:
            A LargeContentPublisher bound to the link.
        """
        return LargeContentPublisher(
            link,
            resource_threshold=threshold or self._resource_threshold,
        )

    @property
    def resource_threshold(self) -> int:
        return self._resource_threshold

    @resource_threshold.setter
    def resource_threshold(self, value: int) -> None:
        self._resource_threshold = value

    @property
    def rns_identity(self) -> RNS.Identity:
        return self.identity

    def __str__(self) -> str:
        return f"RNSICNServer({self.identity})"
