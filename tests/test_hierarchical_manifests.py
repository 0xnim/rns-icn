"""Tests for 5.5 Hierarchical manifests.

Tests cover:
- Downstream peer tracking (mark_downstream, is_downstream, downstream_faces)
- Backward compat: server without downstream peers builds normal manifest
- Propagation server includes MANIFEST entries + inline entries from downstream
- End-to-end manifest fetch from downstream peer via Interest/Data exchange
- Multi-level hierarchy: root → propagation → origin (3 servers)
- Content directory flattening (filters MANIFEST entries)
- Edge cases: empty downstream manifest, no downstream peers, no propagation mgr
"""

import asyncio

import pytest

from rns_icn.face import FaceId
from rns_icn.manifest import (
    EntryKind,
    Manifest,
    ManifestEntry,
    flatten_content_directory,
)
from rns_icn.name import Name, RNS_ADDR_BYTES
from rns_icn.packet import Data, PropPeer
from rns_icn.propagation import PropagationManager
from rns_icn.server import ICNServer


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


def build_manifest_data(producer: bytes, entries: list[ManifestEntry]) -> Data:
    """Build a Data packet containing a manifest (helper for test setup)."""
    import hashlib
    import time
    manifest = Manifest.create(
        producer=producer,
        entries=entries,
    )
    content = manifest.to_json()
    data = Data.new(
        name=manifest.manifest_name(),
        content=content,
    )
    data.metadata.sequence = manifest.sequence
    return data

def server_link(
    server_a: ICNServer, face_a_id: FaceId,
    server_b: ICNServer, face_b_id: FaceId,
) -> asyncio.Task:
    """Wire two faces bidirectionally so bytes from one server arrive at the other."""
    async def _pipe():
        q_a = server_a.get_face_send_queue(face_a_id)
        q_b = server_b.get_face_send_queue(face_b_id)
        if q_a is None or q_b is None:
            return

        async def forward_a_to_b():
            while True:
                raw = await q_a.get()
                await server_b.handle_incoming(face_b_id, raw)

        async def forward_b_to_a():
            while True:
                raw = await q_b.get()
                await server_a.handle_incoming(face_a_id, raw)

        await asyncio.gather(
            forward_a_to_b(),
            forward_b_to_a(),
        )

    task = asyncio.create_task(_pipe())
    return task


# ═══════════════════════════════════════════
# Downstream peer tracking
# ═══════════════════════════════════════════


class TestDownstreamPeerTracking:
    def test_default_no_downstream(self):
        """New PropagationManager has no downstream peers."""
        mgr = PropagationManager()
        assert mgr.downstream_faces == []

    def test_mark_downstream_requires_peer_first(self):
        """mark_downstream has no effect on a non-peer face."""
        mgr = PropagationManager()
        mgr.mark_downstream(42)
        assert mgr.downstream_faces == []

    def test_mark_and_is_downstream(self):
        """mark_downstream makes is_downstream return True."""
        mgr = PropagationManager()
        mgr.add_peer(100, rns_addr(0xBB))
        mgr.mark_downstream(100)
        assert mgr.is_downstream(100)
        assert 100 in mgr.downstream_faces

    def test_mark_upstream_removes(self):
        """mark_upstream removes a peer from downstream set."""
        mgr = PropagationManager()
        mgr.add_peer(100, rns_addr(0xBB))
        mgr.mark_downstream(100)
        assert mgr.is_downstream(100)
        mgr.mark_upstream(100)
        assert not mgr.is_downstream(100)
        assert mgr.downstream_faces == []

    def test_remove_peer_clears_downstream(self):
        """remove_peer removes from downstream tracking."""
        mgr = PropagationManager()
        mgr.add_peer(100, rns_addr(0xBB))
        mgr.mark_downstream(100)
        mgr.remove_peer(100)
        assert mgr.downstream_faces == []

    def test_multiple_downstream_peers(self):
        """Multiple downstream peers tracked correctly."""
        mgr = PropagationManager()
        mgr.add_peer(1, rns_addr(0xBB))
        mgr.add_peer(2, rns_addr(0xCC))
        mgr.add_peer(3, rns_addr(0xDD))
        mgr.mark_downstream(1)
        mgr.mark_downstream(3)
        assert set(mgr.downstream_faces) == {1, 3}


# ═══════════════════════════════════════════
# Manifest structure — backward compat
# ═══════════════════════════════════════════


class TestManifestBackwardCompat:
    @pytest.mark.asyncio
    async def test_server_without_downstream_produces_normal_manifest(self):
        """A server with no downstream peers builds a manifest with only its own
        content — MANIFEST entries never appear. Same as pre-hierarchy behaviour."""
        server = ICNServer(rns_addr(0xAA))

        # Publish local content
        data = Data.new(name=Name(rns_addr(0xAA), [b"hello"]), content=b"world")
        server.forwarder.cs.insert(data.name, data)

        manifest_data = await server._build_manifest_data()
        manifest = Manifest.from_data(manifest_data)

        assert manifest.producer == rns_addr(0xAA)
        assert len(manifest.entries) == 1
        entry = manifest.entries[0]
        assert entry.kind == EntryKind.BLOB
        assert entry.label == "hello"

        # No MANIFEST entries when no downstream peers
        manifest_entries = [e for e in manifest.entries if e.kind == EntryKind.MANIFEST]
        assert len(manifest_entries) == 0

    @pytest.mark.asyncio
    async def test_server_without_propagation_still_builds_manifest(self):
        """Servers without a PropagationManager still build manifests normally.
        (Edge case: manual construct or test fixture)"""
        server = ICNServer(rns_addr(0xAA))
        server.propagation = None  # Remove propagation

        data = Data.new(name=Name(rns_addr(0xAA), [b"test"]), content=b"data")
        server.forwarder.cs.insert(data.name, data)

        # Should not crash — no propagation means no downstream to fetch
        manifest_data = await server._build_manifest_data()
        manifest = Manifest.from_data(manifest_data)

        assert manifest.producer == rns_addr(0xAA)
        assert len(manifest.entries) == 1

    @pytest.mark.asyncio
    async def test_include_downstream_false_omits_downstream(self):
        """Passing include_downstream=False skips downstream manifest fetch."""
        server = ICNServer(rns_addr(0xAA))
        data = Data.new(name=Name(rns_addr(0xAA), [b"test"]), content=b"data")
        server.forwarder.cs.insert(data.name, data)

        manifest_data = await server._build_manifest_data(include_downstream=False)
        manifest = Manifest.from_data(manifest_data)

        assert len(manifest.entries) == 1
        assert manifest.entries[0].kind == EntryKind.BLOB


# ═══════════════════════════════════════════
# End-to-end hierarchical manifest
# ── End-to-end hierarchical manifest ──


class TestHierarchicalManifestE2E:
    @pytest.mark.asyncio
    async def test_propagation_server_includes_downstream_manifest_entry(self):
        """A propagation server with one downstream peer includes:
        - A MANIFEST entry pointing to the downstream peer's manifest
        - Inline copies of the downstream peer's content entries
        """
        # Server B is an origin with content
        server_b = ICNServer(rns_addr(0xBB))
        b_content = Data.new(
            name=Name(rns_addr(0xBB), [b"sensor_data"]),
            content=b"temperature=25.3",
        )
        server_b.forwarder.cs.insert(b_content.name, b_content)

        # Pre-build B's manifest Data and cache it in A's CS
        # (simulates what would happen during peering sync)
        b_entries = server_b._build_manifest_entries()
        b_manifest_data = build_manifest_data(rns_addr(0xBB), b_entries)
        b_manifest = Manifest.from_data(b_manifest_data)

        # Server A is a propagation/root with B's manifest cached
        server_a = ICNServer(rns_addr(0xAA))
        server_a.forwarder.cs.insert(b_manifest_data.name, b_manifest_data)

        # Set up peering and mark B as downstream
        face_a = server_a._new_face()
        server_a.propagation.add_peer(face_a.id(), rns_addr(0xBB))
        server_a.propagation.mark_downstream(face_a.id())

        # Build A's hierarchical manifest
        manifest_data = await server_a._build_manifest_data()
        manifest = Manifest.from_data(manifest_data)

        entries = manifest.entries

        # Find the MANIFEST entry
        manifest_entries = [e for e in entries if e.kind == EntryKind.MANIFEST]
        assert len(manifest_entries) == 1
        me = manifest_entries[0]
        assert "peer:" in me.label
        assert me.name.rns_addr == rns_addr(0xBB)
        assert me.name.components[-1] == b"manifest"

        # Find inline peer content
        inline_entries = [
            e for e in entries
            if e.kind != EntryKind.MANIFEST and "peer:" in e.label
        ]
        assert len(inline_entries) >= 1
        inline = inline_entries[0]
        assert "sensor_data" in inline.label
        assert inline.name.rns_addr == rns_addr(0xBB)

    @pytest.mark.asyncio
    async def test_downstream_content_appears_in_directory(self):
        """Root server's content directory includes downstream content after
        flattening MANIFEST entries."""
        server_b = ICNServer(rns_addr(0xBB))
        b_stream = Data.new(
            name=Name(rns_addr(0xBB), [b"telemetry"]),
            content=b"v1",
        ).with_sequence(1)
        server_b.forwarder.cs.insert(b_stream.name, b_stream)

        # Cache B's manifest on A
        b_entries = server_b._build_manifest_entries()
        b_manifest_data = build_manifest_data(rns_addr(0xBB), b_entries)

        server_a = ICNServer(rns_addr(0xAA))
        a_blob = Data.new(
            name=Name(rns_addr(0xAA), [b"config"]),
            content=b"config_data",
        )
        server_a.forwarder.cs.insert(a_blob.name, a_blob)
        server_a.forwarder.cs.insert(b_manifest_data.name, b_manifest_data)

        # Set up peering and mark B as downstream
        face_a = server_a._new_face()
        server_a.propagation.add_peer(face_a.id(), rns_addr(0xBB))
        server_a.propagation.mark_downstream(face_a.id())

        manifest_data = await server_a._build_manifest_data()
        manifest = Manifest.from_data(manifest_data)

        # Flatten: remove MANIFEST entries, keep only BLOB/STREAM
        directory = flatten_content_directory(manifest)

        # Should have A's local config + B's telemetry
        labels = [e.label for e in directory]
        config_entries = [l for l in labels if "config" in l]
        telemetry_entries = [l for l in labels if "telemetry" in l]

        assert len(config_entries) >= 1  # A's local entry
        assert len(telemetry_entries) >= 1  # B's inlined entry

        # No MANIFEST entries remain after flattening
        assert all(e.kind != EntryKind.MANIFEST for e in directory)

    @pytest.mark.asyncio
    async def test_multi_level_hierarchy(self):
        """Three-level hierarchy: Root(A) → Propagation(B) → Origin(C).

        A peers with B (B is downstream of A).
        B peers with C (C is downstream of B).
        C has content.
        A's manifest should include B's and C's content (C via B's manifest).
        """
        # Level 3: Origin C
        server_c = ICNServer(rns_addr(0xCC))
        c_data = Data.new(
            name=Name(rns_addr(0xCC), [b"deep_data"]),
            content=b"I'm deep down!",
        )
        server_c.forwarder.cs.insert(c_data.name, c_data)

        # Level 2: Propagation B
        server_b = ICNServer(rns_addr(0xBB))
        b_data = Data.new(
            name=Name(rns_addr(0xBB), [b"mid_data"]),
            content=b"I'm in the middle!",
        )
        server_b.forwarder.cs.insert(b_data.name, b_data)

        # Cache C's manifest on B (simulates B peering with C downstream)
        c_entries = server_c._build_manifest_entries()
        c_manifest_data = build_manifest_data(rns_addr(0xCC), c_entries)
        server_b.forwarder.cs.insert(c_manifest_data.name, c_manifest_data)

        # Build B's manifest — which should include C's content
        # (since B has C's manifest cached and has C as downstream)
        face_b_to_c = server_b._new_face()
        server_b.propagation.add_peer(face_b_to_c.id(), rns_addr(0xCC))
        server_b.propagation.mark_downstream(face_b_to_c.id())

        # Level 1: Root A
        server_a = ICNServer(rns_addr(0xAA))

        # Cache B's manifest on A
        b_entries_with_c = await server_b._build_manifest_data()
        server_a.forwarder.cs.insert(b_entries_with_c.name, b_entries_with_c)

        face_a_to_b = server_a._new_face()
        server_a.propagation.add_peer(face_a_to_b.id(), rns_addr(0xBB))
        server_a.propagation.mark_downstream(face_a_to_b.id())

        # Build A's manifest
        manifest_data = await server_a._build_manifest_data()
        manifest = Manifest.from_data(manifest_data)

        directory = flatten_content_directory(manifest)
        labels = [e.label for e in directory]

        # B's content should be present
        assert any("mid_data" in l for l in labels), (
            f"Expected mid_data in labels: {labels}"
        )
        # C's content should also be present (passed through B's manifest)
        assert any("deep_data" in l for l in labels), (
            f"Expected deep_data in labels: {labels}"
        )

        # Verify MANIFEST entries exist
        manifest_entries = [e for e in manifest.entries if e.kind == EntryKind.MANIFEST]
        assert len(manifest_entries) >= 1  # At least B's manifest ref

    @pytest.mark.asyncio
    async def test_downstream_with_no_content(self):
        """A downstream peer with empty CS doesn't cause failures."""
        server_b = ICNServer(rns_addr(0xBB))  # No content published

        # Build B's manifest (empty)
        b_entries = server_b._build_manifest_entries()
        b_manifest_data = build_manifest_data(rns_addr(0xBB), b_entries)

        server_a = ICNServer(rns_addr(0xAA))
        a_data = Data.new(
            name=Name(rns_addr(0xAA), [b"local_only"]),
            content=b"just me",
        )
        server_a.forwarder.cs.insert(a_data.name, a_data)
        server_a.forwarder.cs.insert(b_manifest_data.name, b_manifest_data)

        face_a = server_a._new_face()
        server_a.propagation.add_peer(face_a.id(), rns_addr(0xBB))
        server_a.propagation.mark_downstream(face_a.id())

        manifest_data = await server_a._build_manifest_data()
        manifest = Manifest.from_data(manifest_data)

        # Should have A's local entry, and a MANIFEST entry for B
        manifest_entries = [e for e in manifest.entries if e.kind == EntryKind.MANIFEST]
        assert len(manifest_entries) == 1

        # Local entry should always be there
        assert any("local_only" in e.label for e in manifest.entries)

    @pytest.mark.asyncio
    async def test_server_with_multiple_downstream_peers(self):
        """Root with two downstream peers: manifest includes both."""
        server_b = ICNServer(rns_addr(0xBB))
        b_data = Data.new(
            name=Name(rns_addr(0xBB), [b"b_stream"]),
            content=b"from B",
        ).with_sequence(1)
        server_b.forwarder.cs.insert(b_data.name, b_data)

        server_c = ICNServer(rns_addr(0xCC))
        c_data = Data.new(
            name=Name(rns_addr(0xCC), [b"c_blob"]),
            content=b"from C",
        )
        server_c.forwarder.cs.insert(c_data.name, c_data)

        # Cache both manifests on A
        b_manifest_data = build_manifest_data(
            rns_addr(0xBB), server_b._build_manifest_entries(),
        )
        c_manifest_data = build_manifest_data(
            rns_addr(0xCC), server_c._build_manifest_entries(),
        )

        server_a = ICNServer(rns_addr(0xAA))
        server_a.forwarder.cs.insert(b_manifest_data.name, b_manifest_data)
        server_a.forwarder.cs.insert(c_manifest_data.name, c_manifest_data)

        # Peer A ↔ B and A ↔ C
        face_a_b = server_a._new_face()
        server_a.propagation.add_peer(face_a_b.id(), rns_addr(0xBB))
        server_a.propagation.mark_downstream(face_a_b.id())

        face_a_c = server_a._new_face()
        server_a.propagation.add_peer(face_a_c.id(), rns_addr(0xCC))
        server_a.propagation.mark_downstream(face_a_c.id())

        manifest_data = await server_a._build_manifest_data()
        manifest = Manifest.from_data(manifest_data)

        directory = flatten_content_directory(manifest)
        labels = [e.label for e in directory]

        assert any("b_stream" in l for l in labels), (
            f"Expected b_stream in {labels}"
        )
        assert any("c_blob" in l for l in labels), (
            f"Expected c_blob in {labels}"
        )

        # Should have 2 MANIFEST entries (one for each peer)
        manifest_entries = [e for e in manifest.entries if e.kind == EntryKind.MANIFEST]
        assert len(manifest_entries) == 2

    @pytest.mark.asyncio
    async def test_downstream_manifest_fetch_via_forwarder(self):
        """fetch_peer_manifest() can serve a cached manifest from CS.

        The manifest Data is cached during peering sync and can be
        retrieved without a fresh Interest round-trip.
        """
        server_b = ICNServer(rns_addr(0xBB))
        b_data = Data.new(
            name=Name(rns_addr(0xBB), [b"stream1"]),
            content=b"content_B",
        ).with_sequence(1)
        server_b.forwarder.cs.insert(b_data.name, b_data)

        server_a = ICNServer(rns_addr(0xAA))

        # Wire peers with server_link for the Interest round-trip during peering
        face_a = server_a._new_face()
        face_b = server_b._new_face()
        link = server_link(server_a, face_a.id(), server_b, face_b.id())
        try:
            peer_b = PropPeer(rns_addr=rns_addr(0xBB), wants_sync=False)
            await server_a.handle_incoming(face_a.id(), peer_b.to_bytes())
            peer_a = PropPeer(rns_addr=rns_addr(0xAA), wants_sync=False)
            await server_b.handle_incoming(face_b.id(), peer_a.to_bytes())

            # Mark B as downstream of A
            server_a.propagation.mark_downstream(face_a.id())

            # Wait for peering sync to complete (manifest Data round-trip)
            # The manifest may be cached in CS. If the async round-trip
            # didn't complete, fetch_peer_manifest falls back to express.
            await asyncio.sleep(0.15)

            # Try via CS cache first
            mn = Name(rns_addr(0xBB), [b"manifest"])
            cached = server_a.forwarder.cs.get(mn)

            if cached is not None:
                # Already cached from peering — can read directly
                manifest = Manifest.from_data(cached)
            else:
                # Fall back to express Interest round-trip
                manifest = await server_a.propagation.fetch_peer_manifest(face_a.id())
                if manifest is None:
                    # If express also fails, the manifest Data should be
                    # cached from the fallback — try CS one more time
                    cached = server_a.forwarder.cs.get(mn)
                    if cached is not None:
                        manifest = Manifest.from_data(cached)

            assert manifest is not None
            assert manifest.producer == rns_addr(0xBB)
            assert len(manifest.entries) == 1
            assert manifest.entries[0].label == "stream1"
        finally:
            link.cancel()
            try:
                await link
            except asyncio.CancelledError:
                pass
# ═══════════════════════════════════════════


class TestFlattenContentDirectory:
    def test_empty_manifest(self):
        """Empty manifest yields empty directory."""
        addr = rns_addr(0xAA)
        m = Manifest.create(producer=addr, entries=[])
        directory = flatten_content_directory(m)
        assert directory == []

    def test_only_blob_entries(self):
        """Manifest with only BLOB entries returns them all."""
        addr = rns_addr(0xAA)
        m = Manifest.create(producer=addr, entries=[
            ManifestEntry(EntryKind.BLOB, "file1", Name(addr, [b"file1"])),
            ManifestEntry(EntryKind.BLOB, "file2", Name(addr, [b"file2"])),
        ])
        directory = flatten_content_directory(m)
        assert len(directory) == 2

    def test_manifest_entries_filtered(self):
        """MANIFEST-kind entries are filtered out."""
        addr = rns_addr(0xAA)
        m = Manifest.create(producer=addr, entries=[
            ManifestEntry(EntryKind.BLOB, "local", Name(addr, [b"local"])),
            ManifestEntry(
                EntryKind.MANIFEST, "_peer:bb...", Name(rns_addr(0xBB), [b"manifest"]),
            ),
            ManifestEntry(
                EntryKind.STREAM, "peer:bb/sensor", Name(rns_addr(0xBB), [b"sensor"]),
            ),
        ])
        directory = flatten_content_directory(m)
        assert len(directory) == 2
        assert all(e.kind != EntryKind.MANIFEST for e in directory)
        labels = [e.label for e in directory]
        assert "local" in labels
        assert "peer:bb/sensor" in labels

    def test_deduplicates_by_label(self):
        """Duplicate labels are deduplicated (same content via multiple paths)."""
        addr = rns_addr(0xAA)
        m = Manifest.create(producer=addr, entries=[
            ManifestEntry(EntryKind.BLOB, "dup", Name(addr, [b"dup"])),
            ManifestEntry(EntryKind.BLOB, "dup", Name(addr, [b"dup"])),
        ])
        directory = flatten_content_directory(m)
        assert len(directory) == 1

    def test_mixed_kinds(self):
        """Mixed BLOB, STREAM, MANIFEST entries — only non-MANIFEST returned."""
        addr = rns_addr(0xAA)
        m = Manifest.create(producer=addr, entries=[
            ManifestEntry(EntryKind.BLOB, "a", Name(addr, [b"a"])),
            ManifestEntry(EntryKind.STREAM, "b", Name(addr, [b"b"])),
            ManifestEntry(EntryKind.MANIFEST, "_peer:cc", Name(rns_addr(0xCC), [b"manifest"])),
            ManifestEntry(EntryKind.STREAM, "peer:cc/s", Name(rns_addr(0xCC), [b"s"])),
        ])
        directory = flatten_content_directory(m)
        assert len(directory) == 3  # BLOB + STREAM + remote STREAM
        assert all(e.kind != EntryKind.MANIFEST for e in directory)
