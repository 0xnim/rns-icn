"""Health endpoints for ICN — HTTP and RNS."""

from __future__ import annotations

import json
import time
from typing import Optional, Dict, Any, TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from rns_icn.rns_server import ICNServer
else:
    ICNServer = object

from rns_icn.metrics import MetricsCollector
from rns_icn.content_store import ContentStore


async def health_handler(request: web.Request) -> web.Response:
    """HTTP /health endpoint — returns JSON health status."""
    server: ICNServer = request.app["server"]
    collector: MetricsCollector = request.app.get("metrics", MetricsCollector())

    content_store = server.forwarder.cs

    health = {
        "status": "healthy",
        "identity": server.identity.hexhash,
        "destination": server.hexhash,
        "uptime_seconds": time.time() - getattr(server, "_started_at", time.time()),
        "content_store": {
            "entries": len(content_store),
            "capacity": content_store.capacity,
            "size_bytes": content_store.size_bytes,
            "hits": content_store.hits,
            "misses": content_store.misses,
            "hit_rate": content_store.hits / max(1, content_store.hits + content_store.misses),
        },
        "links": {
            "active": server.link_pool.active_link_count,
            "total": len(server.link_pool._links),
        },
    }
    return web.json_response(health)


async def metrics_handler(request: web.Request) -> web.Response:
    """HTTP /metrics endpoint — returns Prometheus text format."""
    server: ICNServer = request.app["server"]
    collector: MetricsCollector = request.app.get("metrics", MetricsCollector())
    content_store = server.forwarder.cs

    fetch_stats = collector.get_fetch_stats()
    link_stats = collector.get_link_stats()
    counters = collector.get_counters()

    lines = [
        f"icn_content_store_entries {len(content_store)}",
        f"icn_content_store_capacity {content_store.capacity}",
        f"icn_content_store_size_bytes {content_store.size_bytes}",
        f"icn_content_store_hits_total {content_store.hits}",
        f"icn_content_store_misses_total {content_store.misses}",
        f"icn_content_store_hit_rate {content_store.hits / max(1, content_store.hits + content_store.misses):.4f}",
        f"icn_fetch_total {counters['fetch_total']}",
        f"icn_fetch_errors_total {counters['fetch_errors']}",
        f"icn_fetch_latency_mean {fetch_stats.get('mean', 0):.4f}",
        f"icn_fetch_latency_p50 {fetch_stats.get('p50', 0):.4f}",
        f"icn_fetch_latency_p95 {fetch_stats.get('p95', 0):.4f}",
        f"icn_fetch_latency_p99 {fetch_stats.get('p99', 0):.4f}",
        f"icn_links_active {server.link_pool.active_link_count}",
        f"icn_links_total {len(server.link_pool._links)}",
        f"icn_uptime_seconds {time.time() - getattr(server, '_started_at', time.time()):.1f}",
    ]

    # Per-link uptime
    for peer_hash, stats in link_stats.items():
        peer_short = peer_hash[:16]
        lines.append(f'icn_link_uptime_seconds{{peer="{peer_short}"}} {stats["current_uptime"]:.1f}')
        lines.append(f'icn_link_total_uptime_seconds{{peer="{peer_short}"}} {stats["total_uptime"]:.1f}')
        lines.append(f'icn_link_is_up{{peer="{peer_short}"}} {1 if stats["is_up"] else 0}')

    return web.Response(text="\n".join(lines), content_type="text/plain; version=0.0.4")


async def setup_http_api(
    server: ICNServer,
    collector: MetricsCollector,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> web.AppRunner:
    """Setup and start HTTP API server."""
    app = web.Application()
    app["server"] = server
    app["metrics"] = collector
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner


# ── RNS Health Interest Handling ──

HEALTH_NAME_COMPONENT = b"health"


def is_health_interest(name) -> bool:
    """Check if an Interest is a health check request."""
    if name.is_root():
        return False
    # Health check: /<identity>/health
    return name.len() == 2 and name.components[1] == HEALTH_NAME_COMPONENT


async def handle_health_interest(server: ICNServer, name, in_face) -> Optional[Dict[str, Any]]:
    """Handle health check Interest — returns Data with health JSON."""
    if not is_health_interest(name):
        return None

    content_store = server.forwarder.cs
    collector: MetricsCollector = getattr(server, "_metrics", MetricsCollector())

    health = {
        "status": "healthy",
        "identity": server.identity.hexhash,
        "destination": server.hexhash,
        "uptime_seconds": time.time() - getattr(server, "_started_at", time.time()),
        "content_store": {
            "entries": len(content_store),
            "capacity": content_store.capacity,
            "size_bytes": content_store.size_bytes,
            "hits": content_store.hits,
            "misses": content_store.misses,
            "hit_rate": content_store.hits / max(1, content_store.hits + content_store.misses),
        },
        "links": {
            "active": server.link_pool.active_link_count,
            "total": len(server.link_pool._links),
        },
        "metrics": {
            "fetch": collector.get_counters(),
            "fetch_latency": collector.get_fetch_stats(),
        },
    }

    return health