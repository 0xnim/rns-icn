#!/usr/bin/env python3
"""CLI entry point for icn-fetch (installed via pip)."""

import asyncio
import os
import sys

import RNS

from rns_icn.rns_server import RNSICNServer
from rns_icn.name import Name
from rns_icn.packet import Interest

try:
    from rns_icn.manifest import Manifest
except ImportError:
    Manifest = None


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
    RNS.Reticulum()

    peer_addr_raw = bytes.fromhex(peer_hash)
    server = RNSICNServer(app_name="icn", aspect="default")
    server.start()

    print(f"[fetch] Local identity: {server.identity.hexhash}", file=sys.stderr)
    print(f"[fetch] Target peer dest: {peer_hash}", file=sys.stderr)

    # Wait for path to peer to appear in transport table
    print(f"[fetch] Waiting for path to peer...", file=sys.stderr)
    for attempt in range(24):  # up to 2 minutes
        if peer_addr_raw in list(RNS.Transport.path_table.keys()):
            hops = RNS.Transport.hops_to(peer_addr_raw)
            print(f"[fetch] Path found! hops={hops}", file=sys.stderr)
            break
        await asyncio.sleep(5)
    else:
        print(f"[fetch] WARNING: path not found after 2min, trying anyway...", file=sys.stderr)

    print(f"[fetch] Connecting to peer: {peer_hash}", file=sys.stderr)

    face_id = await server.connect(peer_hash)
    if face_id is None:
        print("[fetch] FAILED — could not establish link to peer", file=sys.stderr)
        server.stop()
        sys.exit(1)

    print(f"[fetch] Link established (face #{face_id})", file=sys.stderr)

    peer_addr = bytes.fromhex(peer_hash)
    server.forwarder.add_route(Name(peer_addr, []), face_id, 10)

    await asyncio.sleep(1)

    is_manifest = name_str == "manifest"
    if is_manifest:
        target_name = Name(peer_addr, [b"manifest"])
    else:
        path_components = [c.encode("utf-8") for c in name_str.split("/") if c]
        target_name = Name(peer_addr, path_components)

    print(f"[fetch] Expressing Interest: {target_name}", file=sys.stderr)

    interest = (
        Interest(name=target_name).with_can_be_prefix().with_lifetime(30000)
    )
    result = await server.forwarder.express(interest, 0)

    if result is None:
        print(f"[fetch] FAILED — no response for '{name_str}' (timeout)", file=sys.stderr)
        server.stop()
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
            server.stop()
            sys.exit(1)

    print(f"[fetch] Done — wrote {content_len} bytes", file=sys.stderr)
    server.stop()


if __name__ == "__main__":
    main()
