"""ICNServer — ICN server that runs on a RNS destination.

Like an LXMF Propagation Node, but for content:
- Listens for incoming RNS Links on a destination
- Each Link becomes a Face
- Routes Interests via Forwarder
- Serves content from ContentStore
- Publishes a manifest of available content
"""

from __future__ import annotations

import asyncio
import enum
import hashlib
import time
from typing import Callable, Optional

from .aps import APSManager
from .content_store import ContentStore
from .face import Face, FaceCapabilities, FaceId
from .fib import Fib
from .forwarder import Forwarder
from .manifest import EntryKind, Manifest, ManifestEntry
from .name import Name
from .offline_queue import OfflineQueue
from .packet import APSubscribe, Data, Interest, parse_packet
from .pit import Pit
from .propagation import PropagationManager

_face_counter = 0


class _ServerFace(Face):
    """A Face representing a single RNS Link connection to the server."""

    def __init__(self, face_id: FaceId, send_q: asyncio.Queue):
        self._id = face_id
        self._send_q = send_q

    async def express_interest(self, interest: Interest) -> Optional[Data]:
        # Server faces don't originate Interests (consumers do)
        # But if they do, forward it out
        await self._send_q.put(interest.to_bytes())
        return None

    async def send_data(self, data: Data) -> None:
        await self._send_q.put(data.to_bytes())

    async def send_raw(self, raw: bytes) -> None:
        await self._send_q.put(raw)

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(disruption_tolerance_ms=3600000, mtu=500, is_local=False)

    def id(self) -> FaceId:
        return self._id


class ServerRole(enum.IntEnum):
    """Role of an ICN server in the network topology.

    ORIGIN — authoritative content source. Publishes original content,
        serves it, and propagates to peered servers.

    CACHE — stores and serves cached copies. Does not originate content
        but participates in peering and propagation.

    PROPAGATION — relays content between Origin and Cache servers.
        Focuses on forwarding with minimal local storage.
    """
    ORIGIN = 0
    CACHE = 1
    PROPAGATION = 2


class ICNServer:
    """ICN content server.

    Owns a Forwarder, accepts incoming Link connections as Faces,
    processes Interest/Data exchanges, and maintains a manifest.
    """

    def __init__(self, rns_identity: bytes, cs_max: int = 10000,
                 role: ServerRole = ServerRole.ORIGIN,
                 signer: Optional[Callable[[bytes], bytes]] = None):
        """Args:
            rns_identity: 16-byte RNS address of this server
            role: ServerRole (ORIGIN, CACHE, or PROPAGATION)
            signer: optional callable (typically RNS.Identity.sign) used to
                sign Data this server originates. Only Data whose producer
                address equals our own rns_addr is signed.
        """
        self.role = role
        self.rns_addr = rns_identity
        self._signer = signer
        self.forwarder = Forwarder(cs_max=cs_max)
        self.offline_queue = OfflineQueue(self, max_age_seconds=86400)
        self.aps = APSManager(self)
        self.propagation = PropagationManager(self)
        self._next_face_id: FaceId = 100
        self._faces: dict[FaceId, Face] = {}
        self._face_send_queues: dict[FaceId, asyncio.Queue] = {}

    def _icn_app_data(self) -> bytes:
        """Build announce app_data with server role encoded."""
        return b"icn" + bytes([self.role.value])

    def _new_face(self) -> _ServerFace:
        global _face_counter
        _face_counter += 1
        fid = self._next_face_id
        self._next_face_id += 1
        send_q: asyncio.Queue = asyncio.Queue()
        face = _ServerFace(fid, send_q)
        self._faces[fid] = face
        self._face_send_queues[fid] = send_q
        self.forwarder.register_face(face)
        return face

    def _maybe_sign(self, data: Optional[Data]) -> Optional[Data]:
        """Sign Data we originate, in place.

        Only signs when a signer is configured, the Data is not already
        signed, and its producer address is our own — so caches/propagation
        nodes relay an upstream producer's signature untouched rather than
        re-signing with the wrong key.
        """
        if (
            data is not None
            and self._signer is not None
            and data.signature is None
            and data.name.rns_addr == self.rns_addr
        ):
            data.sign(self._signer)
        return data

    def _serve_from_cs(self, interest: Interest, in_face_id: FaceId) -> Optional[Data]:
        """Check local ContentStore for matching data."""
        if interest.can_be_prefix:
            return self._maybe_sign(self.forwarder.cs.get_prefix(interest.name))
        return self._maybe_sign(self.forwarder.cs.get(interest.name))

    async def _build_manifest_data(self, include_downstream: bool = True) -> Data:
        """Build a Data packet containing our content manifest.

        When ``include_downstream`` is True and the server has a PropagationManager
        with downstream peers, fetches manifests from each downstream peer and
        includes:
        - A MANIFEST-kind entry pointing to the downstream peer's full manifest
        - Inline copies of the peer's content entries (BLOB, STREAM), with
          labels prefixed by the peer's address to identify the origin

        This creates a hierarchical manifest: propagation servers aggregate
        downstream content, and root servers see a complete content directory.
        """
        entries = list(self._build_manifest_entries())

        if include_downstream and self.propagation is not None:
            downstream_mfs = await self.propagation.fetch_downstream_manifests()
            for peer_addr, peer_manifest in downstream_mfs.items():
                # Add MANIFEST entry pointing to downstream peer's full manifest
                peer_label = peer_addr.hex()[:12]
                entries.append(
                    ManifestEntry(
                        kind=EntryKind.MANIFEST,
                        label=f"_peer:{peer_label}",
                        name=Name(peer_addr, [b"manifest"]),
                    )
                )
                # Inline the peer's content entries so parent sees all content
                for entry in peer_manifest.entries:
                    if entry.kind != EntryKind.MANIFEST:
                        entries.append(
                            ManifestEntry(
                                kind=entry.kind,
                                label=f"peer:{peer_addr.hex()[:8]}/{entry.label}",
                                name=entry.name,
                                content_hash=entry.content_hash,
                                size=entry.size,
                                latest_sequence=entry.latest_sequence,
                                total_items=entry.total_items,
                                start_time=entry.start_time,
                                end_time=entry.end_time,
                            )
                        )

        manifest = Manifest.create(
            producer=self.rns_addr,
            entries=entries,
        )
        content = manifest.to_json()
        content_hash = hashlib.blake2b(content, digest_size=32).digest()  # noqa
        data = Data.new(
            name=manifest.manifest_name(),
            content=content,
        )
        data.metadata.sequence = manifest.sequence
        self._maybe_sign(data)
        return data

    def _build_manifest_entries(self) -> list[ManifestEntry]:
        """Build manifest entries from CS contents. Override for custom."""
        our_prefix = Name(self.rns_addr, [])
        groups: dict[str, list[Data]] = {}
        # Group CS entries by their label (second component)
        for entry_name in list(getattr(self.forwarder.cs, "_entries", {}).keys()):
            if entry_name.starts_with(our_prefix) and not entry_name.is_root():
                label_bytes = entry_name.components[1] if len(entry_name.components) > 1 else b""
                label = label_bytes.decode("utf-8", errors="replace")
                data = self.forwarder.cs.get(entry_name)
                if data is not None:
                    groups.setdefault(label, []).append(data)

        entries = []
        for label, data_list in groups.items():
            # Check if this group looks like a stream (has sequence numbers)
            sequences = [d.metadata.sequence for d in data_list if d.metadata.sequence is not None]
            if sequences:
                # Stream entry with computed metadata
                first = data_list[0]
                entries.append(
                    ManifestEntry(
                        kind=EntryKind.STREAM,
                        label=label,
                        name=first.name.without_content_hash(),
                        content_hash=None,
                        size=None,
                        latest_sequence=max(sequences),
                        total_items=len(sequences),
                        start_time=int(time.time()) - len(sequences) * 60,  # approximate
                        end_time=int(time.time()),
                    )
                )
            else:
                # Blob entry — use the first data we found for this label
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
        return entries

    async def handle_interest(self, interest: Interest, face_id: FaceId) -> None:
        """Process an incoming Interest."""
        # Check local CS
        data = self._serve_from_cs(interest, face_id)
        if data is not None:
            face = self._faces.get(face_id)
            if face:
                await face.send_data(data)
            return

        # If the name is our manifest, generate it
        if interest.name == Name(self.rns_addr, [b"manifest"]):
            manifest_data = await self._build_manifest_data()
            self.forwarder.cs.insert(manifest_data.name, manifest_data)
            face = self._faces.get(face_id)
            if face:
                await face.send_data(manifest_data)
            return

        # Forward upstream if we have a route
        result = await self.forwarder.express(interest, face_id)
        if result is not None:
            face = self._faces.get(face_id)
            if face:
                await face.send_data(result)

    async def handle_data(self, data: Data, face_id: FaceId) -> None:
        """Process an incoming Data packet."""
        await self.forwarder.receive_data(data, face_id)

    async def handle_subscribe(self, sub: APSubscribe, face_id: FaceId) -> None:
        """Process an incoming APS Subscribe request.

        Registers the face as a subscriber for the stream name.
        If start_from_now is False, also sends any existing content
        from the local ContentStore that matches the stream name.
        Then drains any offline-queued content for this stream.
        """
        self.aps.subscribe(sub.name, face_id)

        # If not start_from_now, send existing CS content for this stream
        if not sub.start_from_now:
            # Walk CS for matching entries
            for entry_name in list(getattr(self.forwarder.cs, "_entries", {}).keys()):
                if entry_name.starts_with(sub.name):
                    data = self._maybe_sign(self.forwarder.cs.get(entry_name))
                    if data is not None:
                        face = self._faces.get(face_id)
                        if face:
                            await face.send_data(data)

        # Drain any offline-queued content for this stream
        drained = await self.offline_queue.drain(sub.name, face_id)
        if drained:
            pass  # log-friendly placeholder

    async def publish_pushed(self, data: Data) -> None:
        """Push a Data packet to all APS subscribers of its stream.

        Inserts the Data into the local ContentStore, then pushes
        it to every face subscribed to a matching stream name.
        Faces that are unreachable have their data queued in the
        OfflineQueue for delivery on reconnect.
        Also propagates to all peered servers for mesh-wide replication.
        """
        self._maybe_sign(data)
        self.forwarder.cs.insert(data.name, data)
        await self.aps.publish(data, offline_queue=self.offline_queue)
        await self.propagation.propagate(data)

    async def handle_incoming(self, face_id: FaceId, raw: bytes) -> None:
        """Handle a raw incoming packet from a Link."""
        try:
            pkt = parse_packet(raw)
        except (ValueError, Exception):
            # Silently drop invalid packets
            return

        if pkt.interest is not None:
            await self.handle_interest(pkt.interest, face_id)
        elif pkt.data is not None:
            # If this is from a propagation peer, propagate to other peers
            if self.propagation.is_peer(face_id):
                await self.propagation.handle_peer_data(pkt.data, face_id)
            else:
                await self.handle_data(pkt.data, face_id)
        elif pkt.subscribe is not None:
            await self.handle_subscribe(pkt.subscribe, face_id)
        elif pkt.peer is not None:
            await self.propagation.handle_peer_handshake(pkt.peer, face_id)

    def get_face_send_queue(self, face_id: FaceId) -> Optional[asyncio.Queue]:
        return self._face_send_queues.get(face_id)

    @property
    def face_count(self) -> int:
        return len(self._faces)

    @property
    def cs(self) -> ContentStore:
        return self.forwarder.cs

    @property
    def fib(self) -> Fib:
        return self.forwarder.fib

    @property
    def pit(self) -> Pit:
        return self.forwarder.pit
