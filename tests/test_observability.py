"""Tests for ICN observability — logging, health, metrics."""

import asyncio
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from rns_icn.content_store import ContentStore
from rns_icn.health import (
    health_handler,
    is_health_interest,
    metrics_handler,
)
from rns_icn.metrics import metrics
from rns_icn.name import Name
from rns_icn.packet import Data


def test_name_from_string():
    """Test Name.from_string parsing."""
    # Basic name (16-byte RNS address = 32 hex chars)
    name = Name.from_string("/" + "a" * 32 + "/test/item")
    assert name.rns_addr == bytes.fromhex("a" * 32)
    assert name.components[1] == b"test"
    assert name.components[2] == b"item"
    assert name.content_hash is None

    # With hash (32 bytes = 64 hex chars)
    name = Name.from_string("/" + "a" * 32 + "/test/item?hash=" + "f" * 64)
    assert name.content_hash == bytes.fromhex("f" * 64)


def test_health_interest_detection():
    """Test health check Interest detection."""
    producer = b"\x00" * 16

    # Health name
    health_name = Name(producer, [b"health"])
    assert is_health_interest(health_name) is True

    # Regular name
    regular_name = Name(producer, [b"manifest"])
    assert is_health_interest(regular_name) is False

    # Root name
    root_name = Name(producer, [])
    assert is_health_interest(root_name) is False

    # Deep name
    deep_name = Name(producer, [b"a", b"b", b"health"])
    assert is_health_interest(deep_name) is False


def test_metrics_collector():
    """Test MetricsCollector basic operations."""
    m = metrics
    m.reset()

    # Fetch recording
    m.record_fetch(0.1, success=True)
    m.record_fetch(0.2, success=True)
    m.record_fetch(0.5, success=False)

    stats = m.get_fetch_stats()
    assert stats["count"] == 3
    assert stats["mean"] == 0.26666666666666666
    assert stats["p50"] == 0.2

    counters = m.get_counters()
    assert counters["fetch_total"] == 3
    assert counters["fetch_errors"] == 1
    assert counters["malformed_packets"] == 0

    # Malformed-packet counter (dropped unparseable packets)
    m.record_malformed_packet()
    m.record_malformed_packet()
    assert m.get_counters()["malformed_packets"] == 2

    # Link uptime
    m.record_link_up("peer1")
    import time
    time.sleep(0.01)
    uptime = m.get_link_uptime("peer1")
    assert uptime is not None and uptime >= 0.01

    m.record_link_down("peer1")
    total = m.get_link_uptime("peer1")
    assert total is not None and total >= 0.01

    m.reset()


@pytest.mark.asyncio
async def test_http_health_endpoint():
    """Test HTTP /health endpoint."""
    # Create a mock server-like object
    class MockContentStore:
        def __len__(self): return 5
        @property
        def capacity(self): return 10000
        @property
        def size_bytes(self): return 1024
        @property
        def hits(self): return 8
        @property
        def misses(self): return 2

    class MockLinkPool:
        def __init__(self):
            self._links = {"peer1": None}
        @property
        def active_link_count(self): return 1

    class MockServer:
        def __init__(self):
            self.identity = type("Id", (), {"hexhash": "abcd"})()
            self.hexhash = "abcd1234"
            self.forwarder = type("Fwd", (), {"cs": MockContentStore()})()
            self.link_pool = MockLinkPool()
            self._started_at = 0

    from rns_icn.metrics import MetricsCollector
    collector = MetricsCollector()

    app = web.Application()
    app["server"] = MockServer()
    app["metrics"] = collector
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)

    async with TestServer(app) as server, TestClient(server) as client:
        resp = await client.get("/health")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "healthy"
        assert data["content_store"]["entries"] == 5
        assert data["links"]["active"] == 1

        resp = await client.get("/metrics")
        assert resp.status == 200
        text = await resp.text()
        assert "icn_content_store_entries 5" in text
        assert "icn_links_active 1" in text


@pytest.mark.asyncio
async def test_content_store_persistence():
    """Test SQLite ContentStore persistence across restarts."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_cs.db"

        # First instance - insert data
        store1 = ContentStore(str(db_path), max_entries=10, default_ttl=86400)
        producer = b"\x00" * 16
        name = Name(producer, [b"test", b"item"])
        data = Data.new(name, b"persistent content")
        store1.insert(name, data)
        store1.close()

        # Second instance - read data
        store2 = ContentStore(str(db_path), max_entries=10, default_ttl=86400)
        retrieved = store2.get(name)
        assert retrieved is not None
        assert retrieved.content == b"persistent content"

        # Test prefix match
        prefixed = store2.get_prefix(Name(producer, [b"test"]))
        assert prefixed is not None
        assert prefixed.content == b"persistent content"

        store2.close()


@pytest.mark.asyncio
async def test_server_health_interest():
    """Test RNS health Interest handling."""
    # This would require a full integration test with RNS mesh
    # For now, just verify the function exists and is importable
    from rns_icn.health import handle_health_interest, is_health_interest
    assert callable(handle_health_interest)
    assert callable(is_health_interest)


if __name__ == "__main__":
    # Run basic tests
    test_name_from_string()
    print("✓ test_name_from_string")

    test_health_interest_detection()
    print("✓ test_health_interest_detection")

    test_metrics_collector()
    print("✓ test_metrics_collector")

    asyncio.run(test_http_health_endpoint())
    print("✓ test_http_health_endpoint")

    asyncio.run(test_content_store_persistence())
    print("✓ test_content_store_persistence")

    asyncio.run(test_server_health_interest())
    print("✓ test_server_health_interest")

    print("\nAll observability tests passed!")