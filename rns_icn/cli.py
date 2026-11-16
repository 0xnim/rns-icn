"""ICN CLI — icn-client and icn-server binaries."""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from .client import ICNClient
from .config import load_client_config, load_server_config
from .metrics import metrics
from .name import Name
from .rns_server import ICNServer
from .server import ServerRole


async def client_main() -> int:
    """icn-client binary entry point."""
    parser = argparse.ArgumentParser(prog="icn-client", description="ICN Client")
    parser.add_argument("--config", default="icn.toml", help="Config file path")
    parser.add_argument("--fetch", help="Name to fetch (e.g., /producer/manifest)")
    parser.add_argument("--peer", help="Peer destination hex hash")
    parser.add_argument("--timeout", type=float, default=30.0, help="Fetch timeout seconds")
    parser.add_argument("--max-retries", type=int, help="Max retries (overrides config)")
    args = parser.parse_args()

    config = load_client_config(args.config)

    # Override config from CLI
    if args.max_retries is not None:
        config.max_retries = args.max_retries

    if not args.fetch or not args.peer:
        parser.print_help()
        return 1

    try:
        peer_hash = bytes.fromhex(args.peer)
    except ValueError:
        print(f"Error: Invalid peer hash: {args.peer}", file=sys.stderr)
        return 1

    # Parse name
    try:
        name = Name.from_string(args.fetch)
    except Exception as e:
        print(f"Error parsing name: {e}", file=sys.stderr)
        return 1

    async with ICNClient(config) as client:
        try:
            data = await client.fetch(name, peer_hash, timeout=args.timeout)
            if data:
                print(f"✓ Received {len(data.content)} bytes")
                if data.content:
                    # Try to decode as UTF-8
                    try:
                        print(data.content.decode("utf-8"))
                    except UnicodeDecodeError:
                        print(f"[binary: {len(data.content)} bytes]")
                return 0
            else:
                print("✗ No data received (timeout)", file=sys.stderr)
                return 1
        except Exception as e:
            print(f"✗ Fetch failed: {e}", file=sys.stderr)
            return 1


async def server_main() -> int:
    """icn-server binary entry point."""
    parser = argparse.ArgumentParser(prog="icn-server", description="ICN Server")
    parser.add_argument("--config", default="icn.toml", help="Config file path")
    args = parser.parse_args()

    config = load_server_config(args.config)

    # Setup graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # Windows

    print("Starting ICN Server...")
    print(f"  Identity: {config.identity_path}")
    print(f"  CS: {config.cs_path} (max {config.cs_max_entries}, TTL {config.cs_ttl_seconds}s)")

    server = ICNServer(config)
    try:
        await server.start()
        print(f"  Destination: {server.hexhash}")
        print(f"  Listening on /{config.app_name}/{config.aspect}")
        # Held (not directly used) so the announce loop is not garbage-collected.
        _pages_task = _start_pages(server, config)
        print("Ready. Press Ctrl+C to stop.")

        # If HTTP enabled, start it
        if config.http_enabled:
            from .health import setup_http_api
            from .metrics import metrics
            # Held (not directly used) to keep the aiohttp runner alive for the
            # server's lifetime; GC of the runner would tear down the HTTP API.
            _runner = await setup_http_api(server, metrics, config.http_host, config.http_port)
            print(f"  HTTP API: http://{config.http_host}:{config.http_port} (health, metrics)")

        await stop_event.wait()
        print("\nShutting down...")
    finally:
        await server.shutdown()
    return 0


def _start_pages(server: ICNServer, config) -> asyncio.Task | None:
    """Serve the node's content to NomadNet-compatible page browsers.

    Returns the periodic-announce task (hold a reference to keep it alive), or
    None when the front end is disabled.
    """
    if not config.pages_enabled:
        return None
    import RNS

    from .pages import PageNode

    nicknames: dict[bytes, str] = {}
    for peer in config.known_peers:
        if peer.identity_path:
            identity = RNS.Identity.from_file(peer.identity_path)
            if identity is not None:
                nicknames[identity.hash] = peer.name
    node = PageNode(server, node_name=config.pages_node_name, nicknames=nicknames)
    print(f"  Pages: {node.hexhash} (NomadNet page node \"{config.pages_node_name}\")")

    async def _announce_loop() -> None:
        while True:
            node.announce()
            await asyncio.sleep(max(60.0, config.pages_announce_interval))

    return asyncio.ensure_future(_announce_loop())


def _peer_route_cost(dest_hash_hex: str, default: int = 10) -> int:
    """Derive FIB cost from RNS transport hop count when known.

    RNS already tracks hops-to-destination in its path table, so reusing it
    makes primary/backup ordering reflect real mesh distance instead of a
    constant. Falls back to ``default`` when no path is known yet.
    """
    import RNS
    try:
        hops = RNS.Transport.hops_to(bytes.fromhex(dest_hash_hex))
    except Exception:
        return default
    if isinstance(hops, int) and 0 < hops < 128:
        return hops * 10
    return default


async def _install_peer_route(server: ICNServer, peer) -> tuple[str, Name] | None:
    """Connect to one peer and install (or refresh) its FIB route.

    ICN names are self-certifying: content under ``/<peer-identity-hash>/...``
    is authoritative to that peer, so the route is keyed by the peer's identity
    hash pointing at the Link face. ``connect()`` uses the RNS *destination*
    hash while the FIB prefix is the *identity* hash — they differ, so the
    peer's ``identity_path`` is required to derive the routable prefix.

    Idempotent: ``connect`` reuses an existing link via the LinkPool and
    ``add_route`` upserts, so this is safe to call again on reconnect.
    """
    import RNS

    face_id = await server.connect(peer.destination_hash)
    if face_id is None:
        print(f"  ! Could not link to peer '{peer.name}' ({peer.destination_hash[:16]})",
              file=sys.stderr)
        return None
    if not peer.identity_path:
        print(f"  ! Peer '{peer.name}' has no identity_path; cannot install route",
              file=sys.stderr)
        return None
    identity = RNS.Identity.from_file(peer.identity_path)
    if identity is None:
        print(f"  ! Could not load identity for peer '{peer.name}' from {peer.identity_path}",
              file=sys.stderr)
        return None
    prefix = Name(identity.hash)
    cost = _peer_route_cost(peer.destination_hash)
    server.forwarder.add_route(prefix, face_id, cost=cost)
    print(f"  Route: /{identity.hash.hex()} → {peer.name} (face #{face_id}, cost {cost})")
    if getattr(peer, "follow", False):
        _start_follow(server, peer, identity, face_id)
    return (peer.name, prefix)


def _start_follow(server: ICNServer, peer, identity, face_id: int) -> None:
    """Keep a followed peer's content in the local cache ahead of demand.

    Pushes from the peer are cached on the strength of the link being to that
    producer, so refuse to follow when the configured destination is not the
    one derived from the peer's identity file.
    """
    import RNS

    expected = RNS.Destination.hash(
        identity, server.config.app_name, server.config.aspect
    ).hex()
    if expected != peer.destination_hash.lower():
        print(f"  ! Not following '{peer.name}': destination_hash does not belong "
              f"to the identity in {peer.identity_path}", file=sys.stderr)
        return
    server.follower.follow(
        face_id,
        identity.hash,
        labels=list(peer.follow_labels),
        resync_interval=server.config.follow_resync_interval,
    )
    scope = ", ".join(peer.follow_labels) if peer.follow_labels else "everything"
    print(f"  Follow: {peer.name} ({scope})")


async def _install_peer_routes(server: ICNServer, config) -> list[tuple[str, Name]]:
    """Connect to each known peer and install a FIB route for its content.

    Interests for an installed prefix are forwarded upstream by the Forwarder on
    a CS miss (reverse-path Data is cached at this hop).
    """
    installed: list[tuple[str, Name]] = []
    for peer in config.known_peers:
        result = await _install_peer_route(server, peer)
        if result is not None:
            installed.append(result)
    return installed


def _wire_route_reinstall(server: ICNServer, config) -> None:
    """Re-install an upstream route when its peer re-announces after a drop.

    Dynamic FIB re-install, event-driven off RNS announces: when a link closes,
    the server withdraws the route and clears the peer's face. The peer keeps
    announcing periodically; the discovery layer fires this callback on the next
    announce (only while disconnected), and we re-establish + re-install. This
    rides RNS's own keepalive (for the drop) and announce cadence (for recovery)
    instead of polling.
    """
    peers_by_dest = {p.destination_hash: p for p in config.known_peers}

    def _on_rediscovered(peer_hash: str, info) -> None:
        peer = peers_by_dest.get(peer_hash)
        if peer is None:
            return  # not a configured upstream

        async def _run() -> None:
            result = await _install_peer_route(server, peer)
            if result is not None:
                print(f"  Reconnect: re-installed route for {peer.name}")

        # Discovery callbacks fire on the RNS thread → hop onto the event loop.
        if server._loop is not None:
            asyncio.run_coroutine_threadsafe(_run(), server._loop)

    server.discovery.add_callback(_on_rediscovered)


def _wire_holder_routes(server: ICNServer) -> None:
    """Route producers nobody configured to the nearest announced holder.

    When a node announces that it holds a producer's content and there is no
    route for that producer, link to the nearest such node and route to it. The
    route stays until its link closes; the next announce installs a new one.
    A holder that turns out not to have an item costs a NACK, and Data is still
    checked against the producer's signature by whoever consumes it.
    """
    pending: set[bytes] = set()

    async def _install(producer: bytes) -> None:
        try:
            if server.forwarder.fib.lookup(Name(producer)):
                return
            holders = server.holders.holders_of(producer)
            own = server.destination.hash if server.destination is not None else None
            holders = [h for h in holders if h.destination_hash != own]
            if not holders:
                return
            dest_hex = holders[0].destination_hash.hex()
            face_id = await server.connect(dest_hex)
            if face_id is None:
                return
            cost = _peer_route_cost(dest_hex)
            server.forwarder.add_route(Name(producer), face_id, cost=cost)
            print(f"  Route: /{producer.hex()} → holder {dest_hex[:16]} "
                  f"(face #{face_id}, cost {cost})")
        finally:
            pending.discard(producer)

    def _on_holder(producer: bytes, destination_hash: bytes) -> None:
        # Fires on the RNS thread.
        if producer == server.rns_addr or server.follower.is_following(producer):
            return
        loop = server._loop
        if loop is None or producer in pending:
            return
        pending.add(producer)
        asyncio.run_coroutine_threadsafe(_install(producer), loop)

    server.holders.add_callback(_on_holder)


async def router_main() -> int:
    """icn-router binary entry point.

    Runs an ICN server in a forwarding role: links to configured upstream
    peers, installs FIB routes for their content prefixes, and forwards
    Interests on a local cache miss while caching reverse-path Data.
    """
    parser = argparse.ArgumentParser(prog="icn-router", description="ICN Router")
    parser.add_argument("--config", default="icn.toml", help="Config file path")
    args = parser.parse_args()

    config = load_server_config(args.config)
    # A router forwards/caches rather than originating content. Honour an
    # explicit role from config, but default ORIGIN configs to CACHE here.
    if config.role == ServerRole.ORIGIN:
        config.role = ServerRole.CACHE

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # Windows

    print("Starting ICN Router...")
    print(f"  Identity: {config.identity_path}")
    print(f"  Role: {config.role.name}")
    print(f"  CS: {config.cs_path} (max {config.cs_max_entries}, TTL {config.cs_ttl_seconds}s)")

    server = ICNServer(config)
    try:
        await server.start()
        print(f"  Destination: {server.hexhash}")
        print(f"  Listening on /{config.app_name}/{config.aspect}")

        if not config.known_peers:
            print("  ! No known_peers configured — router has no upstream routes",
                  file=sys.stderr)
        installed = await _install_peer_routes(server, config)
        # Withdraw routes on link drop and re-install on the peer's next announce.
        _wire_route_reinstall(server, config)
        if config.route_via_holders:
            _wire_holder_routes(server)
        # Held (not directly used) so the announce loop is not garbage-collected.
        _pages_task = _start_pages(server, config)
        print(f"Ready. {len(installed)} upstream route(s) installed. Press Ctrl+C to stop.")

        if config.http_enabled:
            from .health import setup_http_api
            # Held (not directly used) to keep the aiohttp runner alive for the
            # server's lifetime; GC of the runner would tear down the HTTP API.
            _runner = await setup_http_api(server, metrics, config.http_host, config.http_port)
            print(f"  HTTP API: http://{config.http_host}:{config.http_port} (health, metrics)")

        await stop_event.wait()
        print("\nShutting down...")
    finally:
        await server.shutdown()
    return 0


def client_main_sync() -> int:
    """Synchronous wrapper for icn-client entry point."""
    return asyncio.run(client_main())


def server_main_sync() -> int:
    """Synchronous wrapper for icn-server entry point."""
    return asyncio.run(server_main())


def router_main_sync() -> int:
    """Synchronous wrapper for icn-router entry point."""
    return asyncio.run(router_main())


def main() -> int:
    """Entry point for both binaries."""
    if "icn-client" in sys.argv[0] or (len(sys.argv) > 0 and sys.argv[0].endswith("icn-client")):
        return asyncio.run(client_main())
    elif "icn-server" in sys.argv[0] or (len(sys.argv) > 0 and sys.argv[0].endswith("icn-server")):
        return asyncio.run(server_main())
    elif "icn-router" in sys.argv[0] or (len(sys.argv) > 0 and sys.argv[0].endswith("icn-router")):
        return asyncio.run(router_main())
    else:
        # Called as module: python -m rns_icn.cli client|server|router
        if len(sys.argv) < 2:
            print("Usage: python -m rns_icn.cli [client|server|router] [args...]")
            return 1
        subcommand = sys.argv[1]
        sys.argv = [sys.argv[0], *sys.argv[2:]]  # Remove subcommand
        if subcommand == "client":
            return asyncio.run(client_main())
        elif subcommand == "server":
            return asyncio.run(server_main())
        elif subcommand == "router":
            return asyncio.run(router_main())
        else:
            print(f"Unknown subcommand: {subcommand}")
            return 1


if __name__ == "__main__":
    sys.exit(main())