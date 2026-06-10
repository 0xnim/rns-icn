"""RNSICNServer — ICN server on the Reticulum mesh (renamed to ICNServer).

Like an LXMF Propagation Node but for ICN content:
- Listens on an RNS Destination for incoming Links
- Each incoming Link becomes a LinkFace
- Can establish outbound Links to peers
- Publishes a manifest of available content
- Routes Interest/Data over the mesh
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import RNS

from .config import ServerConfig
from .link_pool import LinkPool
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
from .server import ICNServer as BaseICNServer, ServerRole
from .logging import setup_logging
from .metrics import metrics
from .health import is_health_interest, handle_health_interest, setup_http_api


class ICNServer(BaseICNServer):
    """ICN Server integrated with RNS networking.

    Creates a RNS Destination for the 'icn' application,
    accepts incoming Links as Faces, and can establish
    outbound Links to known peers.

    Accepts ServerConfig for configuration and uses LinkPool
    for consistent link management.
    """

    def __init__(
        self,
        config: ServerConfig,
        link_pool: Optional[LinkPool] = None,
    ):
        # Load identity from config
        identity = load_or_create_identity(config.identity_path)

        self.config = config
        self.identity = identity
        self.app_name = config.app_name
        self.aspect = config.aspect

        # Base ICNServer uses the 16-byte RNS address
        super().__init__(self.identity.hash, cs_max=config.cs_max_entries, role=config.role)

        # Replace in-memory ContentStore with SQLite-backed persistent store
        from .content_store import ContentStore as SQLiteContentStore
        self.forwarder.cs = SQLiteContentStore(
            path=config.cs_path,
            max_entries=config.cs_max_entries,
            default_ttl=config.cs_ttl_seconds,
            prefix_ttls=config.cs_prefix_ttls,
        )

        # Destination created in start()
        self.destination: Optional[RNS.Destination] = None

        # Use shared LinkPool or create one
        self._link_pool = link_pool or LinkPool(
            identity=self.identity,
            app_name=self.app_name,
            aspect=self.aspect,
            known_peers=config.known_peers,
        )

        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Resource transport
        self._resource_listener: Optional[ResourceListener] = None
        self._resource_threshold: int = config.resource_threshold

        # Announce management
        self._announce_task: Optional[asyncio.Task] = None
        self._announce_interval: float = config.announce_interval

        # Peer discovery
        self.discovery: PeerDiscoveryManager = PeerDiscoveryManager(self)

        # Compute feature bitmask for capability exchange
        self._features: int = self._compute_features()

        # Track if we started RNS (to stop on shutdown)
        self._started_rns = False

    def _icn_app_data(self) -> bytes:
        """Build announce app_data with server role encoded."""
        return b"icn" + bytes([self.role.value])

    async def __aenter__(self) -> "ICNServer":
        await self.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.shutdown()

    async def start(self) -> None:
        """Start the server. Must be called from an async context."""
        if self._loop is not None:
            return  # Already started

        self._loop = asyncio.get_running_loop()

        # Setup logging first
        setup_logging(self.config)

        # Track start time for uptime metrics
        self._started_at = time.time()

        # Initialize RNS if not already started
        if not hasattr(RNS, "Reticulum") or RNS.Reticulum is None:
            RNS.Reticulum()
            self._started_rns = True

        # Create the destination that clients connect to
        self.destination = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            self.app_name,
            self.aspect,
        )
        self.destination.set_link_established_callback(self._on_incoming_link)

        # Start link pool (handles outbound links)
        await self._link_pool.start()

        # Inject known peers into announce table
        await self._inject_known_peers()

        # Force immediate announce so peers discover us via transport table
        self.destination.announce(app_data=self._icn_app_data())
        RNS.log(f"ICN: Announced destination {self.destination.hexhash} on /{self.app_name}/{self.aspect}")

        # Start periodic re-announce loop to keep transport table fresh
        self._announce_task = asyncio.ensure_future(self._announce_loop())

        # Start peer discovery
        self.discovery.start(app_name=self.app_name, aspect=self.aspect)

        # Start HTTP API if enabled
        if self.config.http_enabled:
            self._http_runner = await setup_http_api(
                self,
                metrics,
                self.config.http_host,
                self.config.http_port,
            )
            RNS.log(f"ICN: HTTP API started on http://{self.config.http_host}:{self.config.http_port}")

        RNS.log(f"ICN Server: {str(self.identity)}")
        RNS.log(f"ICN Destination: {self.destination.hexhash}")
        RNS.log(f"Listening on /{self.app_name}/{self.aspect}")

    async def shutdown(self) -> None:
        """Stop the server, tear down all links and cancel announce loop."""
        # Stop HTTP API if running
        if hasattr(self, "_http_runner") and self._http_runner:
            await self._http_runner.cleanup()
            self._http_runner = None

        # Stop peer discovery
        self.discovery.stop()

        # Cancel periodic announce loop
        if self._announce_task is not None:
            self._announce_task.cancel()
            try:
                await self._announce_task
            except asyncio.CancelledError:
                pass
            self._announce_task = None

        # Stop link pool (tears down all links)
        await self._link_pool.stop()

        # Stop RNS if we started it
        if self._started_rns and hasattr(RNS, "Reticulum") and RNS.Reticulum:
            RNS.Reticulum().exit()

        self._loop = None

    def announce(self, app_data: Optional[bytes] = None) -> None:
        """Force an announce of this server's destination."""
        if self.destination is None:
            raise RuntimeError("Server not started. Call start() first.")
        self.destination.announce(app_data=app_data or self._icn_app_data())
        RNS.log(f"ICN: Announced destination {self.destination.hexhash}")

    async def _announce_loop(self) -> None:
        """Periodically re-announce the destination to keep transport table fresh."""
        try:
            while True:
                await asyncio.sleep(self._announce_interval)
                if self.destination is not None:
                    self.destination.announce(app_data=self._icn_app_data())
                    log_hexhash = self.destination.hexhash
                    RNS.log(f"ICN: Re-announced destination {log_hexhash}")
        except asyncio.CancelledError:
            pass

    async def _inject_known_peers(self) -> None:
        """Load all configured known_peers into RNS.Transport.announce_table."""
        for peer_hash, peer_config in self._link_pool.known_peers.items():
            if peer_config.identity_path:
                identity = RNS.Identity.from_file(peer_config.identity_path)
                if identity:
                    dest = RNS.Destination(
                        identity,
                        RNS.Destination.IN,
                        RNS.Destination.SINGLE,
                        self.app_name,
                        self.aspect,
                    )
                    RNS.Transport.announce_table[bytes.fromhex(peer_hash)] = dest
                    RNS.log(f"ICN: Injected known peer {peer_config.name} ({peer_hash[:16]}) into announce table")

    def _on_incoming_link(self, link: RNS.Link) -> None:
        """Called when a remote peer establishes a Link."""
        face = self._new_face()
        self._make_link_face(face.id(), link)

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
        """Establish an outbound Link to a peer ICN server using LinkPool.

        Args:
            peer_hash: RNS destination hex hash
        Returns:
            FaceId if connected, None on timeout
        """
        if self._loop is None or self.destination is None:
            raise RuntimeError("Server not started. Call start() first.")

        peer_hash_bytes = bytes.fromhex(peer_hash)

        # Use LinkPool to get or create link
        link = await self._link_pool.get_link(peer_hash_bytes)
        if not link:
            RNS.log(f"ICN: Failed to establish link to {peer_hash}")
            return None

        face = self._new_face()
        self._make_link_face(face.id(), link)

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
        """Build the feature bitmask for capability exchange."""
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
        """Send our CapPeer to a peer on the given face."""
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
        """Handle incoming capabilities from a peer."""
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

        # Handle health check Interest
        if pkt.interest is not None and is_health_interest(pkt.interest.name):
            health = await handle_health_interest(self, pkt.interest.name, face_id)
            if health:
                from .packet import Data
                health_data = Data.new(
                    name=pkt.interest.name,
                    content=json.dumps(health).encode("utf-8"),
                )
                face = self._faces.get(face_id)
                if face:
                    await face.send_data(health_data)
                return

        # Fall through to base class for standard packet types
        await super().handle_incoming(face_id, raw)

    # ── Resource transport ──

    def _on_resource_data(self, data: Data) -> None:
        """Called when a Data packet arrives via RNS.Resource."""
        self.forwarder.cs.insert(data.name, data)
        RNS.log(f"ICN: Received Data via Resource: {data.name} ({len(data.content)} bytes)")

    def create_resource_publisher(self, link: RNS.Link) -> ResourcePublisher:
        """Create a ResourcePublisher for sending large Data over a Link."""
        return ResourcePublisher(link)

    def create_large_content_publisher(
        self,
        link: RNS.Link,
        threshold: Optional[int] = None,
    ) -> LargeContentPublisher:
        """Create a LargeContentPublisher for chunked content."""
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

    @property
    def link_pool(self) -> LinkPool:
        return self._link_pool

    def __str__(self) -> str:
        return f"ICNServer({self.identity})"