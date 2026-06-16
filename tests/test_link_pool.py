"""Tests for LinkPool — link reuse, health monitoring, eviction.

Links and RNS Transport/Identity are mocked so no real network is touched.
"""

from unittest.mock import MagicMock, patch

import pytest
import RNS

from rns_icn.config import KnownPeer
from rns_icn.link_pool import LinkPool


def _mock_link(status=RNS.Link.ACTIVE):
    link = MagicMock()
    link.status = status
    return link


def _pool(known_peers=None):
    return LinkPool(
        identity=MagicMock(),
        app_name="icn",
        aspect="default",
        known_peers=known_peers or [],
        loop=MagicMock(),  # never driven; LinkPool only stores it
    )


@pytest.mark.asyncio
async def test_get_link_reuses_active_link():
    pool = _pool()
    peer = b"\x01" * 16
    link = _mock_link()
    pool._links[peer] = link
    pool._health[peer] = 0.0

    result = await pool.get_link(peer)

    assert result is link
    # Reuse refreshes the activity timestamp.
    assert pool._health[peer] > 0.0


@pytest.mark.asyncio
async def test_get_link_evicts_dead_link_and_recreates():
    pool = _pool()
    peer = b"\x02" * 16
    pool._links[peer] = _mock_link(status=RNS.Link.CLOSED)
    pool._health[peer] = 123.0
    new_link = _mock_link()

    with patch.object(pool, "_create_link", return_value=new_link) as mk:
        result = await pool.get_link(peer)

    mk.assert_awaited_once_with(peer)
    assert result is new_link
    assert pool._links[peer] is new_link


@pytest.mark.asyncio
async def test_get_link_returns_none_when_create_fails():
    pool = _pool()
    peer = b"\x03" * 16

    with patch.object(pool, "_create_link", return_value=None):
        result = await pool.get_link(peer)

    assert result is None
    assert peer not in pool._links


def test_active_link_count_and_status():
    pool = _pool()
    pool._links[b"\x01" * 16] = _mock_link()
    pool._links[b"\x02" * 16] = _mock_link(status=RNS.Link.CLOSED)

    assert pool.active_link_count == 1
    assert pool.get_link_status(b"\x01" * 16) == str(RNS.Link.ACTIVE)
    assert pool.get_link_status(b"\xff" * 16) is None


@pytest.mark.asyncio
async def test_monitor_links_evicts_idle_links():
    pool = _pool()
    peer = b"\x04" * 16
    link = _mock_link()
    pool._links[peer] = link
    pool._health[peer] = 0.0  # ancient — far older than the 120s threshold
    pool._running = True

    async def fake_sleep(_seconds):
        # Run the loop body exactly once, then let the while-condition exit.
        pool._running = False

    with patch("rns_icn.link_pool.asyncio.sleep", side_effect=fake_sleep):
        await pool._monitor_links()

    link.teardown.assert_called_once()
    assert peer not in pool._links
    assert peer not in pool._health


def test_resolve_identity_prefers_config_then_recall():
    peer = b"\x05" * 16
    configured = MagicMock()
    pool = _pool(known_peers=[
        KnownPeer(name="p", destination_hash=peer.hex(), identity_path="/some/path"),
    ])

    with patch.object(RNS.Identity, "from_file", return_value=configured):
        assert pool._resolve_identity(peer) is configured

    # No config entry → fall back to whatever RNS learned via announces.
    recalled = MagicMock()
    pool2 = _pool()
    with patch.object(RNS.Identity, "recall", return_value=recalled):
        assert pool2._resolve_identity(peer) is recalled


@pytest.mark.asyncio
async def test_ensure_path_returns_true_when_path_exists():
    pool = _pool()
    peer = b"\x06" * 16

    with patch.object(RNS.Transport, "has_path", return_value=True):
        assert await pool._ensure_path(peer) is True
