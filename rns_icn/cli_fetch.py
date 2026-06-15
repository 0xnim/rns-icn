#!/usr/bin/env python3
"""CLI entry point for icn-fetch (installed via pip)."""

import asyncio
import os
import sys
import tempfile

import RNS

from rns_icn.config import ServerConfig
from rns_icn.rns_server import ICNServer
from rns_icn.name import Name
from rns_icn.packet import Interest

try:
    from rns_icn.manifest import Manifest
except ImportError:
    Manifest = None


def _ephemeral_config() -> ServerConfig:
    """Build a throwaway ServerConfig for a one-shot CLI client invocation."""
    workdir = tempfile.mkdtemp(prefix="icn_cli_")
    # Keep stdout clean for content output by sending RNS's own logs to a file.
    RNS.logdest = RNS.LOG_FILE
    RNS.logfile = os.path.join(workdir, "rns.log")
    return ServerConfig(
        identity_path=os.path.join(workdir, "identity"),
        app_name="icn",
        aspect="default",
        cs_path=os.path.join(workdir, "content_store.db"),
        http_enabled=False,
    )


def main():
    """Entry point for `icn-fetch` console script."""
    args = sys.argv[1:]
    if len(args) < 2:
        print(
            "Usage: icn-fetch <peer_hash> <name> [output|-]\n"
            "Fetch content from an ICN server over RNS.\n"
            "  peer_hash   RNS destination hex hash\n"
            "  name        Content path, or 'manifest' for server index\n"
            "  output      Output file, or '-' for stdout (default: '-')",
            file=sys.stderr,
        )
        sys.exit(1)

    peer_hash = args[0]
    name_str = args[1]
    output_path = args[2] if len(args) > 2 else "-"

    asyncio.run(_fetch(peer_hash, name_str, output_path))


async def _fetch(peer_hash: str, name_str: str, output_path: str):
    print(f"[fetch] Initializing RNS...", file=sys.stderr)

    peer_addr_raw = bytes.fromhex(peer_hash)
    server = ICNServer(_ephemeral_config())
    await server.start()

    print(f"[fetch] Local identity: {server.identity.hexhash}", file=sys.stderr)
    print(f"[fetch] Target peer dest: {peer_hash}", file=sys.stderr)

    # Wait for a path to the peer (request + poll) so its identity can be
    # recalled and a Link established.
    print(f"[fetch] Waiting for path to peer...", file=sys.stderr)
    for attempt in range(24):  # up to 2 minutes
        if RNS.Transport.has_path(peer_addr_raw):
            hops = RNS.Transport.hops_to(peer_addr_raw)
            print(f"[fetch] Path found! hops={hops}", file=sys.stderr)
            break
        RNS.Transport.request_path(peer_addr_raw)
        await asyncio.sleep(5)
    else:
        print(f"[fetch] WARNING: path not found after 2min, trying anyway...", file=sys.stderr)

    print(f"[fetch] Connecting to peer: {peer_hash}", file=sys.stderr)

    face_id = await server.connect(peer_hash)
    if face_id is None:
        print("[fetch] FAILED — could not establish link to peer", file=sys.stderr)
        await server.shutdown()
        sys.exit(1)

    print(f"[fetch] Link established (face #{face_id})", file=sys.stderr)

    # Content is named under the producer's *identity* hash, but Links target the
    # *destination* hash. Recall the peer identity to build the correct Name prefix.
    peer_identity = RNS.Identity.recall(peer_addr_raw)
    producer_addr = peer_identity.hash if peer_identity is not None else peer_addr_raw
    server.forwarder.add_route(Name(producer_addr, []), face_id, 10)

    await asyncio.sleep(1)

    is_manifest = name_str == "manifest"
    if is_manifest:
        target_name = Name(producer_addr, [b"manifest"])
    else:
        path_components = [c.encode("utf-8") for c in name_str.split("/") if c]
        target_name = Name(producer_addr, path_components)

    print(f"[fetch] Expressing Interest: {target_name}", file=sys.stderr)

    interest = (
        Interest(name=target_name).with_can_be_prefix().with_lifetime(30000)
    )
    result = await server.forwarder.express(interest, 0)

    if result is None:
        print(f"[fetch] FAILED — no response for '{name_str}' (timeout)", file=sys.stderr)
        await server.shutdown()
        sys.exit(1)

    content = result.content
    content_len = len(content)
    hash_hex = (
        result.metadata.content_hash.hex()
        if result.metadata.content_hash
        else "N/A"
    )

    print(f"[fetch] Received: {result.name}", file=sys.stderr)
    print(f"[fetch]   Size: {content_len} bytes", file=sys.stderr)
    print(f"[fetch]   Hash: {hash_hex}", file=sys.stderr)

    if is_manifest and Manifest is not None:
        try:
            manifest = Manifest.from_data(result)
            print(f"[fetch]   Sequence: v{manifest.sequence}", file=sys.stderr)
            print(f"[fetch]   Entries: {len(manifest.entries)}", file=sys.stderr)
        except Exception:
            pass

    if output_path == "-":
        sys.stdout.buffer.write(content)
        sys.stdout.buffer.flush()
    else:
        try:
            with open(output_path, "wb") as f:
                f.write(content)
        except IOError as e:
            print(f"[fetch] ERROR: Cannot write to {output_path}: {e}", file=sys.stderr)
            await server.shutdown()
            sys.exit(1)

    print(f"[fetch] Done — wrote {content_len} bytes", file=sys.stderr)
    await server.shutdown()


if __name__ == "__main__":
    main()
