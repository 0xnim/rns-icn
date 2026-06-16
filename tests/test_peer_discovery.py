"""Tests for PeerDiscoveryManager and capability exchange (5.2).

Tests the announce-based peer discovery, CapPeer serialization,
capability exchange on link establishment, and peer registry.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rns_icn.packet import (
    FEATURE_APS,
    FEATURE_CHUNKED,
    FEATURE_MANIFEST,
    FEATURE_OFFLINE_QUEUE,
    FEATURE_PROPAGATION,
    CapPeer,
    PacketType,
    parse_packet,
)
from rns_icn.peer_discovery import PeerDiscoveryManager, PeerInfo

# ── CapPeer packet tests ──


class TestCapPeerSerialization:
    def test_default_cap_peer(self):
        c = CapPeer()
        assert c.version == 1
        assert c.role == 0
        assert c.features == 0

    def test_cap_peer_to_bytes_type_byte(self):
        c = CapPeer()
        b = c.to_bytes()
        assert b[0] == PacketType.CAP_PEER  # 0x05

    def test_cap_peer_round_trip_default(self):
        c = CapPeer()
        b = c.to_bytes()
        c2 = CapPeer.from_bytes(b)
        assert c2.version == 1
        assert c2.role == 0
        assert c2.features == 0

    def test_cap_peer_round_trip_orign(self):
        c = CapPeer(version=1, role=0, features=FEATURE_APS | FEATURE_MANIFEST)
        b = c.to_bytes()
        c2 = CapPeer.from_bytes(b)
        assert c2.version == 1
        assert c2.role == 0
        assert c2.features == (FEATURE_APS | FEATURE_MANIFEST)

    def test_cap_peer_round_trip_cache(self):
        c = CapPeer(version=1, role=1, features=FEATURE_APS | FEATURE_OFFLINE_QUEUE)
        b = c.to_bytes()
        c2 = CapPeer.from_bytes(b)
        assert c2.role == 1
        assert c2.features == (FEATURE_APS | FEATURE_OFFLINE_QUEUE)

    def test_cap_peer_round_trip_propagation(self):
        c = CapPeer(version=1, role=2, features=FEATURE_PROPAGATION | FEATURE_MANIFEST)
        b = c.to_bytes()
        c2 = CapPeer.from_bytes(b)
        assert c2.role == 2
        assert c2.features == (FEATURE_PROPAGATION | FEATURE_MANIFEST)

    def test_cap_peer_all_features(self):
        all_features = (
            FEATURE_APS | FEATURE_PROPAGATION | FEATURE_OFFLINE_QUEUE
            | FEATURE_MANIFEST | FEATURE_CHUNKED
        )
        c = CapPeer(version=1, role=0, features=all_features)
        b = c.to_bytes()
        c2 = CapPeer.from_bytes(b)
        assert c2.features == all_features

    def test_cap_peer_wire_size(self):
        """CapPeer wire format: 1 type + 1 version + 1 role + 4 features = 7 bytes."""
        c = CapPeer()
        assert len(c.to_bytes()) == 7

    def test_cap_peer_from_bytes_short_buffer(self):
        with pytest.raises(ValueError, match="too short"):
            CapPeer.from_bytes(b"\x05\x01\x02")

    def test_cap_peer_from_bytes_wrong_type(self):
        with pytest.raises(ValueError, match="expected CAP_PEER"):
            CapPeer.from_bytes(b"\x00\x01\x02\x03\x04\x05\x06")

    def test_parse_packet_cap_peer(self):
        c = CapPeer(version=1, role=2, features=FEATURE_PROPAGATION)
        pkt = parse_packet(c.to_bytes())
        assert pkt.cap_peer is not None
        assert pkt.type == PacketType.CAP_PEER
        assert pkt.cap_peer.role == 2
        assert pkt.cap_peer.features == FEATURE_PROPAGATION


# ── Feature constants ──


class TestFeatureConstants:
    def test_feature_values(self):
        assert FEATURE_APS == 0x00000001
        assert FEATURE_PROPAGATION == 0x00000002
        assert FEATURE_OFFLINE_QUEUE == 0x00000004
        assert FEATURE_MANIFEST == 0x00000008
        assert FEATURE_CHUNKED == 0x00000010

    def test_features_are_exclusive(self):
        """Each feature occupies a distinct bit."""
        all_bits = (
            FEATURE_APS
            | FEATURE_PROPAGATION
            | FEATURE_OFFLINE_QUEUE
            | FEATURE_MANIFEST
            | FEATURE_CHUNKED
        )
        assert bin(all_bits).count("1") == 5


# ── PeerInfo tests ──


class TestPeerInfo:
    def test_defaults(self):
        identity = MagicMock()
        info = PeerInfo(hash="abcd", identity=identity, app_data=b"icn")
        assert info.hash == "abcd"
        assert info.is_connected is False
        assert info.face_id is None
        assert info.capabilities is None

    def test_is_connected_with_face(self):
        info = PeerInfo(hash="abcd", identity=MagicMock(), app_data=b"icn")
        info.face_id = 101
        assert info.is_connected is True

    def test_is_connected_no_face(self):
        info = PeerInfo(hash="abcd", identity=MagicMock(), app_data=b"icn")
        assert info.is_connected is False

    def test_capabilities_stored(self):
        info = PeerInfo(hash="abcd", identity=MagicMock(), app_data=b"icn")
        cap = CapPeer(version=1, role=0, features=FEATURE_APS | FEATURE_MANIFEST)
        info.capabilities = cap
        assert info.capabilities.role == 0
        assert info.capabilities.features == (FEATURE_APS | FEATURE_MANIFEST)


# ── PeerDiscoveryManager tests ──


class TestPeerDiscoveryManager:
    @pytest.fixture
    def manager(self):
        """Create a PeerDiscoveryManager with no real server."""
        pdm = PeerDiscoveryManager(server=None)
        return pdm

    def test_new_manager_empty(self, manager):
        assert manager.peer_count() == 0
        assert manager.get_peers() == {}

    def test_on_announce_new_peer(self, manager):
        """A new announce creates a PeerInfo entry."""
        dest_hash = b"\xaa" * 16
        identity = MagicMock()
        manager._on_announce(dest_hash, identity, b"icn")

        assert manager.peer_count() == 1
        info = manager.get_peer("aa" * 16)
        assert info is not None
        assert info.app_data == b"icn"
        assert info.face_id is None

    def test_on_announce_update_existing(self, manager):
        """A re-announce updates last_seen and app_data."""
        dest_hash = b"\xbb" * 16
        identity = MagicMock()
        manager._on_announce(dest_hash, identity, b"icn")

        # Re-announce with different app_data
        manager._on_announce(dest_hash, identity, b"icn-v2")

        assert manager.peer_count() == 1  # still one peer
        info = manager.get_peer("bb" * 16)
        assert info.app_data == b"icn-v2"

    def test_multiple_peers(self, manager):
        """Multiple distinct announces create multiple peer entries."""
        for i in range(3):
            dest_hash = bytes([i] * 16)
            manager._on_announce(dest_hash, MagicMock(), b"icn")

        assert manager.peer_count() == 3

    def test_remove_peer(self, manager):
        dest_hash = b"\xcc" * 16
        manager._on_announce(dest_hash, MagicMock(), b"icn")
        assert manager.peer_count() == 1

        manager.remove_peer("cc" * 16)
        assert manager.peer_count() == 0

    def test_update_peer_face(self, manager):
        dest_hash = b"\xdd" * 16
        hex_hash = "dd" * 16
        manager._on_announce(dest_hash, MagicMock(), b"icn")

        manager.update_peer_face(hex_hash, 42)
        info = manager.get_peer(hex_hash)
        assert info.face_id == 42
        assert info.is_connected is True

    def test_update_peer_capabilities(self, manager):
        dest_hash = b"\xee" * 16
        hex_hash = "ee" * 16
        manager._on_announce(dest_hash, MagicMock(), b"icn")

        cap = CapPeer(version=1, role=1, features=FEATURE_APS)
        manager.update_peer_capabilities(hex_hash, cap)
        info = manager.get_peer(hex_hash)
        assert info.capabilities is not None
        assert info.capabilities.role == 1
        assert info.capabilities.features == FEATURE_APS

    def test_peer_hash_for_face(self, manager):
        dest_hash = b"\xff" * 16
        hex_hash = "ff" * 16
        manager._on_announce(dest_hash, MagicMock(), b"icn")
        manager.update_peer_face(hex_hash, 77)

        found = manager.peer_hash_for_face(77)
        assert found == hex_hash

    def test_peer_hash_for_face_unknown(self, manager):
        assert manager.peer_hash_for_face(999) is None

    def test_update_peer_face_unknown_peer(self, manager):
        """Updating face for an unknown peer should not crash."""
        manager.update_peer_face("nonexistent", 42)  # no crash

    def test_update_peer_capabilities_unknown_peer(self, manager):
        """Updating capabilities for an unknown peer should not crash."""
        manager.update_peer_capabilities("nonexistent", CapPeer())  # no crash

    def test_remove_peer_unknown(self, manager):
        """Removing an unknown peer should not crash."""
        manager.remove_peer("unknown")

    def test_callback_fired_on_new_announce(self, manager):
        """Callbacks registered via add_callback fire on new announces."""
        results = []

        def cb(peer_hash, info):
            results.append((peer_hash, info))

        manager.add_callback(cb)

        dest_hash = b"\x11" * 16
        manager._on_announce(dest_hash, MagicMock(), b"icn")

        assert len(results) == 1
        assert results[0][0] == "11" * 16
        assert results[0][1].hash == "11" * 16

    def test_callback_not_fired_on_re_announce_while_connected(self, manager):
        """Re-announces from a *linked* peer stay quiet (no reconnect churn).

        While disconnected, a re-announce is the reconnect signal and does fire
        (see test_dynamic_fib); once a face is associated we go silent.
        """
        count = 0

        def cb(peer_hash, info):
            nonlocal count
            count += 1

        manager.add_callback(cb)
        dest_hash = b"\x22" * 16

        manager._on_announce(dest_hash, MagicMock(), b"icn")  # new peer → fires
        manager.update_peer_face("22" * 16, 7)                # now linked
        manager._on_announce(dest_hash, MagicMock(), b"icn-again")  # quiet

        assert count == 1  # only the initial discovery fired

    def test_remove_callback(self, manager):
        """Removed callbacks no longer fire."""
        count = 0

        def cb(peer_hash, info):
            nonlocal count
            count += 1

        manager.add_callback(cb)
        manager.remove_callback(cb)

        dest_hash = b"\x33" * 16
        manager._on_announce(dest_hash, MagicMock(), b"icn")

        assert count == 0

    def test_get_peers_returns_copy(self, manager):
        """get_peers() returns a copy, not the internal dict."""
        manager._on_announce(b"\x44" * 16, MagicMock(), b"icn")
        peers_copy = manager.get_peers()
        peers_copy.clear()
        assert manager.peer_count() == 1  # internal dict unchanged
