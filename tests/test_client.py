"""Basic lifecycle tests for ICNClient and ICNServer."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from rns_icn.config import load_client_config, load_server_config
from rns_icn.client import ICNClient
from rns_icn.rns_server import ICNServer


def test_load_client_config():
    """Test loading client config from TOML."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write("""
[client]
identity_path = "~/.icn/client_identity"
mesh_interfaces = ["UTN Oregon"]
fetch_timeout = 30.0

[[client.known_peers]]
name = "test-peer"
destination_hash = "24cb54c7ec86294f0723e1d04015b8aa"
identity_path = "~/.icn/test_identity"
""")
        path = f.name

    try:
        cfg = load_client_config(path)
        assert cfg.identity_path is not None
        assert cfg.mesh_interfaces == ["UTN Oregon"]
        assert cfg.fetch_timeout == 30.0
        assert len(cfg.known_peers) == 1
        assert cfg.known_peers[0].name == "test-peer"
        assert cfg.known_peers[0].destination_hash == "24cb54c7ec86294f0723e1d04015b8aa"
    finally:
        Path(path).unlink()


def test_load_server_config():
    """Test loading server config from TOML."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write("""
[server]
identity_path = "/etc/rns-icn/identity"
app_name = "icn"
aspect = "default"
role = "ORIGIN"
announce_interval = 30.0
cs_max_entries = 10000
resource_threshold = 100000

[[server.known_peers]]
name = "peer-1"
destination_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
identity_path = "~/.icn/peer1_identity"
""")
        path = f.name

    try:
        cfg = load_server_config(path)
        assert cfg.identity_path is not None
        assert cfg.app_name == "icn"
        assert cfg.role.name == "ORIGIN"
        assert cfg.announce_interval == 30.0
        assert cfg.cs_max_entries == 10000
        assert len(cfg.known_peers) == 1
        assert cfg.known_peers[0].name == "peer-1"
    finally:
        Path(path).unlink()


@pytest.mark.asyncio
async def test_client_lifecycle():
    """Test ICNClient async context manager starts/stops cleanly."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write("""
[client]
identity_path = "~/.icn/test_client_identity"
mesh_interfaces = ["UTN Oregon"]
""")
        path = f.name

    cfg = load_client_config(path)

    try:
        async with ICNClient(cfg) as client:
            # Client should be initialized
            assert client.identity is not None
            assert client.forwarder is not None
            assert client.link_pool is not None

            # Identity should be valid
            assert len(client.identity.hash) == 16
    finally:
        Path(path).unlink()


@pytest.mark.asyncio
async def test_server_lifecycle():
    """Test ICNServer async context manager starts/stops cleanly."""
    # Create a temp identity file for testing
    with tempfile.TemporaryDirectory() as tmpdir:
        identity_path = Path(tmpdir) / "test_server_identity"

        # Generate a test identity. RNS.Reticulum() is a process-global singleton:
        # only initialise it if one isn't already running (another test may have
        # started it), and leave it up so the server's start() reuses the instance —
        # exiting + reinitialising in the same process raises "Attempt to reinitialise".
        import RNS
        if RNS.Reticulum.get_instance() is None:
            RNS.Reticulum()
        identity = RNS.Identity()
        identity.to_file(str(identity_path))

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(f"""
[server]
identity_path = "{identity_path}"
app_name = "icn"
aspect = "default"
role = "ORIGIN"
announce_interval = 30.0
""")
            path = f.name

        cfg = load_server_config(path)

        try:
            async with ICNServer(cfg) as server:
                # Server should be initialized
                assert server.identity is not None
                assert server.destination is not None
                assert server.hexhash is not None
                assert server.link_pool is not None

                # Destination should be valid
                assert len(server.destination.hash) == 16
        finally:
            Path(path).unlink()


if __name__ == "__main__":
    # Run tests manually without pytest
    test_load_client_config()
    print("✓ test_load_client_config")

    test_load_server_config()
    print("✓ test_load_server_config")

    asyncio.run(test_client_lifecycle())
    print("✓ test_client_lifecycle")

    asyncio.run(test_server_lifecycle())
    print("✓ test_server_lifecycle")

    print("\nAll tests passed!")