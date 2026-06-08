"""Focused integration tests for capability exchange (5.2).

Tests the non-link-establishment parts:
- compute_features returns expected features
- CapPeer dispatched via handle_incoming override
- CapPeer stored in discovery registry
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from rns_icn.packet import CapPeer, FEATURE_APS, FEATURE_MANIFEST, FEATURE_OFFLINE_QUEUE
from rns_icn.rns_server import RNSICNServer


def _make_mock_identity(byte_val: int):
    ident = MagicMock()
    ident.hash = bytes([byte_val] * 16)
    ident.hexhash = f"{byte_val:02x}" * 16
    return ident


@pytest.fixture
def make_server():
    """Factory fixture for a minimal mock RNSICNServer."""
    servers = []

    def _make(byte_val=0x0A):
        identity = _make_mock_identity(byte_val)
        with (
            patch("RNS.Identity", return_value=identity),
            patch("RNS.Destination"),
            patch("RNS.log"),
            patch("RNS.Transport"),
        ):
            server = RNSICNServer(app_name="icn", aspect="test")
            server._loop = asyncio.get_running_loop()
            servers.append(server)
            return server

    yield _make
    for s in servers:
        try:
            s.stop()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_compute_features_includes_aps(make_server):
    """_compute_features always includes APS, MANIFEST, OFFLINE_QUEUE."""
    server = make_server(0x0F)
    assert server._features & FEATURE_APS
    assert server._features & FEATURE_MANIFEST
    assert server._features & FEATURE_OFFLINE_QUEUE


@pytest.mark.asyncio
async def test_cap_peer_parsed_and_stored(make_server):
    """handle_incoming dispatches CapPeer and stores in discovery."""
    server = make_server(0x0B)

    # Register a peer with a face — simulates prior announce + link
    peer_hash = "cc" * 16
    server.discovery._peers[peer_hash] = MagicMock(
        hash=peer_hash,
        face_id=100,
        capabilities=None,
    )
    fake_face = MagicMock()
    fake_face.id.return_value = 100
    server._faces[100] = fake_face

    # Create and send a CapPeer as if it arrived on the link
    cap = CapPeer(version=1, role=1, features=FEATURE_APS | FEATURE_MANIFEST)
    await server.handle_incoming(100, cap.to_bytes())

    # Verify stored in discovery
    info = server.discovery.get_peer(peer_hash)
    assert info is not None
    assert info.capabilities is not None
    assert info.capabilities.role == 1
    assert info.capabilities.features == (FEATURE_APS | FEATURE_MANIFEST)


@pytest.mark.asyncio
async def test_cap_peer_unknown_face_no_crash(make_server):
    """CapPeer from unknown face gets placeholder hash, doesn't crash."""
    server = make_server(0x0C)

    cap = CapPeer(version=1, role=0, features=0)
    # Unknown face ID 999 — should not crash
    await server.handle_incoming(999, cap.to_bytes())
    # No assertion needed — we just verify no exception


@pytest.mark.asyncio
async def test_standard_packets_still_work(make_server):
    """Non-CapPeer packets still route to base handle_incoming."""
    server = make_server(0x0D)

    # Register a peer
    peer_hash = "dd" * 16
    server.discovery._peers[peer_hash] = MagicMock(
        hash=peer_hash,
        face_id=200,
        capabilities=None,
    )

    # Send an invalid packet (not CapPeer) — should be silently dropped
    # by base handle_incoming
    await server.handle_incoming(200, b"\xff\xff\xff\xff")
    # No assertion needed — verifies fallthrough doesn't crash


@pytest.mark.asyncio
async def test_cap_peer_called_on_incoming_link(make_server):
    """_on_incoming_link schedules CapPeer send for new link."""
    server = make_server(0x0E)
    server.destination = MagicMock()
    server.destination.hexhash = "ee" * 16

    mock_link = MagicMock()
    mock_link.hexhash = "ee" * 16
    mock_link.hash = bytes.fromhex("ee" * 16)

    # Register peer in discovery
    server.discovery._peers["ee" * 16] = MagicMock(
        hash="ee" * 16, face_id=None, capabilities=None,
    )

    # This should not raise — schedules coroutine for later
    server._on_incoming_link(mock_link)
