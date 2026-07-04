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
from collections.abc import Callable
from typing import TYPE_CHECKING

import RNS

if TYPE_CHECKING:
    from aiohttp import web

import contextlib

from . import access, discovery
from .config import ServerConfig
from .face import FaceId, LinkFace
from .health import handle_health_interest, is_health_interest
from .icn_logging import setup_logging
from .link_pool import LinkPool
from .manifest import EntryKind, Manifest, ManifestEntry
from .metrics import metrics
from .name import Name
from .packet import (
    FEATURE_APS,
    FEATURE_CHUNKED,
    FEATURE_MANIFEST,
    FEATURE_NACK,
    FEATURE_OFFLINE_QUEUE,
    FEATURE_PROPAGATION,
    CapPeer,
    Data,
    Invalidate,
)
from .peer_discovery import PeerDiscoveryManager
from .resource_transport import (
    LargeContentPublisher,
    ResourceListener,
    ResourcePublisher,
)
from .rns_utils import load_or_create_identity
from .server import ICNServer as BaseICNServer


def _verify_invalidation(inv: Invalidate) -> bool:
    """Verify an Invalidate against the producer recalled from its name.

    Self-certifying: the producer address embedded in the name is the identity
    whose key must have signed the invalidation. Returns False when unsigned or
    when the producer key can't be recalled.
    """
    if inv.signature is None:
        return False
    identity = RNS.Identity.recall(inv.name.rns_addr, from_identity_hash=True)
    if identity is None:
        return False
    return inv.verify_signature(identity.validate)


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
        link_pool: LinkPool | None = None,
    ):
        # Load identity from config
        identity = load_or_create_identity(config.identity_path)

        self.config = config
        self.identity = identity
        self.app_name = config.app_name
        self.aspect = config.aspect

        # The producer signs originated Data with its own (self-certifying)
        # identity: the name is the hash of this key, so consumers verify by
        # recalling it. The attribute is kept for the signer/capability paths.
        self.signing_identity = identity

        # Base ICNServer uses the 16-byte RNS address; pass the identity's
        # Ed25519 signer so origin-produced Data is signed (Phase 3.1/3.2).
        super().__init__(
            self.identity.hash,
            cs_max=config.cs_max_entries,
            role=config.role,
            signer=self.signing_identity.sign,
            invalidation_verifier=_verify_invalidation,
            pit_max=config.pit_max_entries,
        )

        # Per-face features advertised by each peer's CapPeer handshake, used to
        # gate optional behaviour (e.g. only NACK peers that support it).
        self._peer_features: dict[FaceId, int] = {}

        # Replace in-memory ContentStore with SQLite-backed persistent store
        from .content_store import ContentStore as SQLiteContentStore
        self.forwarder.cs = SQLiteContentStore(
            path=config.cs_path,
            max_entries=config.cs_max_entries,
            default_ttl=config.cs_ttl_seconds,
            prefix_ttls=config.cs_prefix_ttls,
        )

        # Enable stale-while-revalidate on the forwarding strategy when configured.
        from .strategy import BestRoute
        if isinstance(self.forwarder.strategy, BestRoute):
            self.forwarder.strategy = BestRoute(
                stale_while_revalidate=config.cs_stale_while_revalidate,
            )

        # Per-prefix access control (Phase 3.3): builds an AccessController from
        # config.access_rules so restricted prefixes are encrypted at publish and
        # capabilities can be issued to authorized consumers. The CEK is derived
        # from the producer identity (the namespace owner).
        self._access = access.AccessController(
            producer_identity=self.identity,
            producer_addr=self.rns_addr,
            rules=[
                access.AccessRule(
                    prefix=Name(self.rns_addr, [lbl.encode() for lbl in rule.prefix]),
                    consumers={bytes.fromhex(c) for c in rule.consumers},
                )
                for rule in config.access_rules
            ],
        )

        # Destination created in start()
        self.destination: RNS.Destination | None = None

        # Use shared LinkPool or create one
        self._link_pool = link_pool or LinkPool(
            identity=self.identity,
            app_name=self.app_name,
            aspect=self.aspect,
            known_peers=config.known_peers,
        )

        self._loop: asyncio.AbstractEventLoop | None = None

        # HTTP health/metrics API runner (set when http_enabled)
        self._http_runner: web.AppRunner | None = None

        # Resource transport
        self._resource_listener: ResourceListener | None = None
        self._resource_threshold: int = config.resource_threshold

        # Announce management
        self._announce_task: asyncio.Task | None = None
        self._announce_interval: float = config.announce_interval
        self._pit_age_task: asyncio.Task | None = None

        # Peer discovery
        self.discovery: PeerDiscoveryManager = PeerDiscoveryManager(self)

        # Optional hook fired (on the event loop) after a face's routes are
        # withdrawn on link close. The router sets this to re-establish the link
        # and re-install the route (dynamic FIB re-install on reconnect).
        self.on_face_closed: Callable[[FaceId], None] | None = None

        # Compute feature bitmask for capability exchange
        self._features: int = self._compute_features()

        # Track if we started RNS (to stop on shutdown)
        self._started_rns = False

    def _icn_app_data(self) -> bytes:
        """Build announce app_data with server role encoded."""
        return b"icn" + bytes([self.role.value])

    async def __aenter__(self) -> ICNServer:
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

        # Initialize RNS only if not already running (it's a process-global singleton;
        # calling RNS.Reticulum() when one exists raises "Attempt to reinitialise").
        if RNS.Reticulum.get_instance() is None:
            RNS.Reticulum(configdir=self.config.rns_configdir)
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

        # Start periodic PIT aging so expired in-flight Interests and loop-nonce
        # state are reclaimed even during quiet periods between traffic.
        self._pit_age_task = asyncio.ensure_future(self._pit_age_loop())

        # Start peer discovery
        RNS.log("ICN: Starting peer discovery...")
        self.discovery.start(app_name=self.app_name, aspect=self.aspect)
        RNS.log("ICN: Peer discovery started")

        # Start HTTP API if enabled
        if self.config.http_enabled:
            RNS.log("ICN: Starting HTTP API...")
            try:
                from .health import setup_http_api
                self._http_runner = await setup_http_api(
                    self,
                    metrics,
                    self.config.http_host,
                    self.config.http_port,
                )
                RNS.log(f"ICN: HTTP API started on http://{self.config.http_host}:{self.config.http_port}")
            except Exception as e:
                RNS.log(f"ICN: HTTP API startup failed: {e}")
                import traceback
                traceback.print_exc()
        else:
            RNS.log(f"ICN: HTTP API disabled (http_enabled={self.config.http_enabled})")

        RNS.log(f"ICN Server: {self.identity!s}")
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
            with contextlib.suppress(asyncio.CancelledError):
                await self._announce_task
            self._announce_task = None

        # Cancel periodic PIT aging
        if self._pit_age_task is not None:
            self._pit_age_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pit_age_task
            self._pit_age_task = None

        # Stop link pool (tears down all links)
        await self._link_pool.stop()

        # Stop RNS if we started it (exit_handler is static and idempotent).
        if self._started_rns and RNS.Reticulum.get_instance() is not None:
            RNS.Reticulum.exit_handler()

        self._loop = None

    def announce(self, app_data: bytes | None = None) -> None:
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

    async def _pit_age_loop(self) -> None:
        """Periodically purge expired PIT/nonce state and sample PIT metrics."""
        try:
            while True:
                await asyncio.sleep(self.config.pit_purge_interval)
                pit = self.forwarder.pit
                pit.purge_expired()
                metrics.record_pit(len(pit), pit.evictions)
        except asyncio.CancelledError:
            pass

    async def _inject_known_peers(self) -> None:
        """Warm RNS path resolution for configured known_peers.

        Issues a path request for each known peer so a route is available by
        the time we try to establish a Link. Identity resolution itself is
        handled in LinkPool from each peer's ``identity_path``.
        """
        for peer_hash, peer_config in self._link_pool.known_peers.items():
            try:
                peer_hash_bytes = bytes.fromhex(peer_hash)
            except ValueError:
                continue
            if not RNS.Transport.has_path(peer_hash_bytes):
                RNS.Transport.request_path(peer_hash_bytes)
                RNS.log(f"ICN: Requested path to known peer {peer_config.name} ({peer_hash[:16]})")

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

    async def connect(self, peer_hash: str) -> FaceId | None:
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
        link_face = LinkFace(
            face_id, link, loop=self._loop, on_closed=self._on_face_closed
        )
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

    def _on_face_closed(self, face_id: FaceId) -> None:
        """LinkFace closed-callback hook — runs on the RNS thread.

        Schedules FIB/face cleanup onto the event loop so a dropped link stops
        being a black-hole route. RNS already detects link death via keepalive,
        so this is the right event to ride rather than polling for liveness.
        """
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._cleanup_closed_face, face_id)

    def _cleanup_closed_face(self, face_id: FaceId) -> None:
        """Withdraw a closed face's routes and drop it (event-loop side)."""
        self.forwarder.withdraw_face(face_id)
        self._faces.pop(face_id, None)
        self._peer_features.pop(face_id, None)
        self.discovery.clear_face(face_id)
        RNS.log(f"ICN: face #{face_id} closed — FIB routes withdrawn")
        if self.on_face_closed is not None:
            try:
                self.on_face_closed(face_id)
            except Exception:
                RNS.log("ICN: on_face_closed hook failed", RNS.LOG_DEBUG)

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

    def publish_content(
        self,
        name: Name,
        content: bytes,
        sequence: int | None = None,
        latest_under: Name | None = None,
    ) -> None:
        """Publish content into the ContentStore.

        Content under a restricted prefix (config.access_rules) is encrypted with
        the prefix CEK before storage, so the stored/served ciphertext is what
        caches relay and what the producer signs — only consumers holding a
        capability can read it.

        Also refreshes the collection's verifiable latest-version pointer
        (rns_icn.discovery) so consumers can fetch the authenticated latest;
        ``latest_under`` sets the collection prefix (default: the name's parent).
        """
        content_bytes, encrypted = self._access.encrypt_content(name, content)
        data = Data.new(name=name, content=content_bytes)
        data.metadata.encrypted = encrypted
        if sequence is not None:
            data.with_sequence(sequence)
        self.forwarder.cs.insert(name, data)
        self._publish_latest_pointer(name, data, sequence, latest_under)
        suffix = " (encrypted)" if encrypted else ""
        RNS.log(f"ICN: Published {name} ({len(content)} bytes){suffix}")

    async def publish_post(
        self,
        name: Name,
        content: bytes,
        sequence: int,
        latest_under: Name | None = None,
    ) -> None:
        """Publish one immutable edition as pullable-latest *and* live push.

        Composes ``publish_content`` (CS insert + verifiable latest-pointer)
        and ``publish_pushed`` (APS push to subscribers + peer propagation)
        as a single operation with one CS insert. ``sequence`` is mandatory:
        an edition stream without monotonic sequences has neither rollback
        protection nor a meaningful latest-pointer.

        The Data is signed eagerly (like ``publish_pushed``) so the pushed
        copy is verifiable; serve-time ``_maybe_sign`` skips already-signed
        Data, so pull answers carry the same signature.
        """
        content_bytes, encrypted = self._access.encrypt_content(name, content)
        data = Data.new(name=name, content=content_bytes)
        data.metadata.encrypted = encrypted
        data.with_sequence(sequence)
        self._maybe_sign(data)
        self.forwarder.cs.insert(name, data)
        self._publish_latest_pointer(name, data, sequence, latest_under)
        await self.aps.publish(data, offline_queue=self.offline_queue)
        await self.propagation.propagate(data)
        suffix = " (encrypted)" if encrypted else ""
        RNS.log(f"ICN: Published post {name} seq={sequence} ({len(content)} bytes){suffix}")

    def _publish_latest_pointer(
        self, name: Name, data: Data, sequence: int | None, latest_under: Name | None
    ) -> None:
        """Refresh the verifiable latest-version pointer for ``name``'s collection.

        The pointer (rns_icn.discovery) names the just-published version,
        content-hash pinned so a consumer's eventual fetch is self-certifying.
        ``latest_under`` overrides the collection prefix; by default it is the
        name's parent (all components but the leaf). A producer-root blob (no
        parent) gets no pointer.
        """
        content_hash = data.metadata.content_hash
        if content_hash is None:
            return  # Data.new always hashes; defensive, keeps the pointer pinned
        prefix = latest_under
        if prefix is None:
            if name.len() <= 1:
                return
            prefix = Name(self.rns_addr, list(name.components[1:-1]))
        target = name.with_content_hash(content_hash)
        meta_name = discovery.meta_name(prefix)
        pointer = Data.new(name=meta_name, content=discovery.encode_meta(target))
        if sequence is not None:
            pointer.with_sequence(sequence)
        pointer.with_freshness_period(self.config.meta_freshness_period)
        # Stored unsigned like all origin Data; _maybe_sign signs it at serve time
        # (meta_name.rns_addr == self.rns_addr), so the served pointer is
        # producer-authenticated and rollback-checkable.
        self.forwarder.cs.insert(meta_name, pointer)

    def issue_capability(
        self,
        prefix_labels: list[str],
        consumer: RNS.Identity,
        ttl_seconds: int = 0,
    ) -> access.Capability:
        """Mint a capability letting ``consumer`` read a restricted prefix.

        ``prefix_labels`` are the labels under our namespace (e.g. ``["private"]``
        for ``/<us>/private``). Must match a configured access rule that lists the
        consumer. Signed with our signing identity so clients verify it like any
        producer signature.
        """
        prefix = Name(self.rns_addr, [lbl.encode() for lbl in prefix_labels])
        return self._access.issue_capability(
            prefix, consumer, self.signing_identity.sign, ttl_seconds
        )

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
            from .chunker import chunk_content  # noqa: F401  (availability probe)
            features |= FEATURE_CHUNKED
        except ImportError:
            pass
        # Content propagation (if the propagation module is available)
        try:
            from .propagation import PropagationManager  # noqa: F401  (availability probe)
            features |= FEATURE_PROPAGATION
        except ImportError:
            pass
        # Interest NACK for fast multi-path failover
        features |= FEATURE_NACK
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
        self._peer_features[face_id] = cap.features
        role_names = ["ORIGIN", "CACHE", "PROPAGATION"]
        role_name = role_names[cap.role] if 0 <= cap.role < len(role_names) else f"UNKNOWN({cap.role})"
        RNS.log(f"ICN: Received capabilities from peer {peer_hash[:16]} (role={role_name}, features={cap.features:#010x}, version={cap.version})")

    def _peer_supports_nack(self, face_id: FaceId) -> bool:
        """True if the peer on this face advertised FEATURE_NACK in its handshake."""
        return bool(self._peer_features.get(face_id, 0) & FEATURE_NACK)

    # ── Packet handling override ──

    async def handle_incoming(self, face_id: FaceId, raw: bytes) -> None:
        """Handle incoming packets, including CapPeer for discovery."""
        try:
            from .packet import parse_packet
            pkt = parse_packet(raw)
        except Exception:
            metrics.record_malformed_packet()
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
        threshold: int | None = None,
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


# Backward-compatible alias: this class was renamed from RNSICNServer to ICNServer.
RNSICNServer = ICNServer