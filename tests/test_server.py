"""Tests for RNSICNServer — announce propagation and lifecycle.
Also tests for server role model.
"""

from unittest.mock import MagicMock, patch

import pytest

from rns_icn.config import ServerConfig
from rns_icn.rns_server import RNSICNServer
from rns_icn.server import ICNServer, ServerRole

# Role-encoded app_data for an ORIGIN server
_ORIGIN_APP_DATA = b"icn\x00"
_CACHE_APP_DATA = b"icn\x01"
_PROP_APP_DATA = b"icn\x02"


def _make_mock_identity(prefix_byte: int):
    """Create a mocked RNS.Identity with deterministic hash."""
    mock_id = MagicMock()
    mock_id.hash = bytes([prefix_byte]) + b"\x00" * 15
    mock_id.hexhash = f"{prefix_byte:02x}" + "00" * 15
    return mock_id


def _cfg(aspect: str = "test", role: ServerRole = ServerRole.ORIGIN) -> ServerConfig:
    """Minimal in-memory ServerConfig for tests."""
    return ServerConfig(
        identity_path="/unused",
        app_name="icn",
        aspect=aspect,
        cs_path=":memory:",
        role=role,
    )


async def _start(server):
    """Start a server with peer discovery stubbed out (no real RNS traffic)."""
    server.discovery = MagicMock()
    await server.start()

def _make_mock_dest(prefix_byte: int):
    """Create a mocked RNS.Destination."""
    mock_dest = MagicMock()
    mock_dest.hexhash = f"{prefix_byte:02x}" + "00" * 15
    return mock_dest


@pytest.mark.asyncio
async def test_announce_called_on_start():
    """Server calls destination.announce() during start()."""
    with (
        patch("rns_icn.rns_server.load_or_create_identity", return_value=_make_mock_identity(0x01)),
        patch("RNS.Reticulum"),
        patch("RNS.Destination") as mock_dest_class,
    ):
        mock_dest = MagicMock()
        mock_dest.hexhash = "01" + "00" * 15
        mock_dest_class.return_value = mock_dest

        server = RNSICNServer(_cfg())
        await _start(server)

        mock_dest_class.assert_called_once()
        mock_dest.announce.assert_called_once_with(app_data=_ORIGIN_APP_DATA)
        mock_dest.set_link_established_callback.assert_called_once()

        await server.shutdown()


@pytest.mark.asyncio
async def test_announce_public_method():
    """Public announce() method calls destination.announce()."""
    with (
        patch("rns_icn.rns_server.load_or_create_identity", return_value=_make_mock_identity(0x02)),
        patch("RNS.Reticulum"),
        patch("RNS.Destination") as mock_dest_class,
    ):
        mock_dest = MagicMock()
        mock_dest.hexhash = "02" + "00" * 15
        mock_dest_class.return_value = mock_dest

        server = RNSICNServer(_cfg())
        await _start(server)

        mock_dest.announce.reset_mock()

        server.announce(app_data=b"test-data")

        mock_dest.announce.assert_called_once_with(app_data=b"test-data")

        await server.shutdown()


@pytest.mark.asyncio
async def test_announce_loop_cancelled_on_stop():
    """Periodic announce loop is cancelled when server stops."""
    with (
        patch("rns_icn.rns_server.load_or_create_identity", return_value=_make_mock_identity(0x03)),
        patch("RNS.Reticulum"),
        patch("RNS.Destination") as mock_dest_class,
    ):
        mock_dest = MagicMock()
        mock_dest.hexhash = "03" + "00" * 15
        mock_dest_class.return_value = mock_dest

        server = RNSICNServer(_cfg())
        server._announce_interval = 0.1
        await _start(server)

        assert server._announce_task is not None
        assert not server._announce_task.done()

        await server.shutdown()

        task = server._announce_task
        assert task is None or task.cancelled()


@pytest.mark.asyncio
async def test_announce_errors_before_start():
    """announce() raises RuntimeError if called before start()."""
    with (
        patch("rns_icn.rns_server.load_or_create_identity", return_value=_make_mock_identity(0x04)),
    ):
        server = RNSICNServer(_cfg())
        with pytest.raises(RuntimeError, match="not started"):
            server.announce()


@pytest.mark.asyncio
async def test_announce_default_app_data():
    """announce() uses role-encoded app_data as default."""
    with (
        patch("rns_icn.rns_server.load_or_create_identity", return_value=_make_mock_identity(0x05)),
        patch("RNS.Reticulum"),
        patch("RNS.Destination") as mock_dest_class,
    ):
        mock_dest = MagicMock()
        mock_dest.hexhash = "05" + "00" * 15
        mock_dest_class.return_value = mock_dest

        server = RNSICNServer(_cfg())
        await _start(server)

        mock_dest.announce.reset_mock()

        server.announce()

        mock_dest.announce.assert_called_once_with(app_data=_ORIGIN_APP_DATA)

        await server.shutdown()


# ── ServerRole model tests ──


def test_server_role_enum_values():
    """ServerRole enum has correct integer values."""
    assert ServerRole.ORIGIN.value == 0
    assert ServerRole.CACHE.value == 1
    assert ServerRole.PROPAGATION.value == 2


def test_server_role_names():
    """ServerRole enum has expected names."""
    assert set(m.name for m in ServerRole) == {"ORIGIN", "CACHE", "PROPAGATION"}


def test_icn_server_default_role():
    """ICNServer defaults to ORIGIN role."""
    server = ICNServer(rns_identity=b"\xaa" + b"\x00" * 15)
    assert server.role == ServerRole.ORIGIN


def test_icn_server_explicit_role_origin():
    """ICNServer accepts explicit ORIGIN role."""
    server = ICNServer(
        rns_identity=b"\xbb" + b"\x00" * 15,
        role=ServerRole.ORIGIN,
    )
    assert server.role == ServerRole.ORIGIN


def test_icn_server_explicit_role_cache():
    """ICNServer accepts explicit CACHE role."""
    server = ICNServer(
        rns_identity=b"\xcc" + b"\x00" * 15,
        role=ServerRole.CACHE,
    )
    assert server.role == ServerRole.CACHE


def test_icn_server_explicit_role_propagation():
    """ICNServer accepts explicit PROPAGATION role."""
    server = ICNServer(
        rns_identity=b"\xdd" + b"\x00" * 15,
        role=ServerRole.PROPAGATION,
    )
    assert server.role == ServerRole.PROPAGATION


def test_icn_app_data_origin():
    """_icn_app_data() returns b'icn' + \\x00 for ORIGIN role."""
    server = ICNServer(rns_identity=b"\xee" + b"\x00" * 15)
    assert server._icn_app_data() == _ORIGIN_APP_DATA


def test_icn_app_data_cache():
    """_icn_app_data() returns b'icn' + \\x01 for CACHE role."""
    server = ICNServer(
        rns_identity=b"\xff" + b"\x00" * 15,
        role=ServerRole.CACHE,
    )
    assert server._icn_app_data() == _CACHE_APP_DATA


def test_icn_app_data_propagation():
    """_icn_app_data() returns b'icn' + \\x02 for PROPAGATION role."""
    server = ICNServer(
        rns_identity=b"\x10" + b"\x00" * 15,
        role=ServerRole.PROPAGATION,
    )
    # Note: this calls ICNServer._icn_app_data which doesn't exist on the base class
    # The method exists on RNSICNServer. Let's test the base class method.
    assert server._icn_app_data() == _PROP_APP_DATA


@pytest.mark.asyncio
async def test_rns_server_announce_with_cache_role():
    """RNSICNServer with CACHE role announces with \\x01 byte."""
    with (
        patch("rns_icn.rns_server.load_or_create_identity", return_value=_make_mock_identity(0x11)),
        patch("RNS.Reticulum"),
        patch("RNS.Destination") as mock_dest_class,
    ):
        mock_dest = MagicMock()
        mock_dest.hexhash = "11" + "00" * 15
        mock_dest_class.return_value = mock_dest

        server = RNSICNServer(_cfg(aspect="cache-test", role=ServerRole.CACHE))
        await _start(server)

        mock_dest.announce.assert_called_once_with(app_data=_CACHE_APP_DATA)
        await server.shutdown()


@pytest.mark.asyncio
async def test_rns_server_announce_with_propagation_role():
    """RNSICNServer with PROPAGATION role announces with \\x02 byte."""
    with (
        patch("rns_icn.rns_server.load_or_create_identity", return_value=_make_mock_identity(0x12)),
        patch("RNS.Reticulum"),
        patch("RNS.Destination") as mock_dest_class,
    ):
        mock_dest = MagicMock()
        mock_dest.hexhash = "12" + "00" * 15
        mock_dest_class.return_value = mock_dest

        server = RNSICNServer(_cfg(aspect="prop-test", role=ServerRole.PROPAGATION))
        await _start(server)

        mock_dest.announce.assert_called_once_with(app_data=_PROP_APP_DATA)
        await server.shutdown()
