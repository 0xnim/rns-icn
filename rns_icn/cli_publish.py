#!/usr/bin/env python3
"""CLI entry point for icn-publish (installed via pip)."""

import asyncio
import os
import sys

import RNS

from rns_icn.rns_server import RNSICNServer
from rns_icn.name import Name
from rns_icn.packet import Data


def main():
    """Entry point for `icn-publish` console script."""
    args = sys.argv[1:]
    if len(args) < 2:
        print(
            "Usage: icn-publish <peer_hash> <name> [file|-]\n"
            "Publish content to an ICN server over RNS.\n"
            "  peer_hash   RNS destination hex hash\n"
            "  name        Content path (e.g. 'hello', 'docs/manual')\n"
            "  file        Input file, or '-' for stdin (default: '-')",
            file=sys.stderr,
        )
        sys.exit(1)

    peer_hash = args[0]
    name_str = args[1]
    file_path = args[2] if len(args) > 2 else "-"

    # Read content
    if file_path == "-":
        content = sys.stdin.buffer.read()
    else:
        try:
            with open(file_path, "rb") as f:
                content = f.read()
        except FileNotFoundError:
            print(f"ERROR: File not found: {file_path}", file=sys.stderr)
            sys.exit(1)
        except PermissionError:
            print(f"ERROR: Permission denied: {file_path}", file=sys.stderr)
            sys.exit(1)

    if not content:
        print("ERROR: Empty content — nothing to publish", file=sys.stderr)
        sys.exit(1)

    asyncio.run(_publish(peer_hash, name_str, content))


async def _publish(peer_hash: str, name_str: str, content: bytes):
    print(f"[publish] Initializing RNS...", file=sys.stderr)
    RNS.Reticulum()

    server = RNSICNServer(app_name="icn", aspect="default")
    server.start()

    print(f"[publish] Local identity: {server.identity.hexhash}", file=sys.stderr)
    print(f"[publish] Connecting to peer: {peer_hash}", file=sys.stderr)

    face_id = await server.connect(peer_hash)
    if face_id is None:
        print("[publish] FAILED — could not establish link to peer", file=sys.stderr)
        server.stop()
        sys.exit(1)

    print(f"[publish] Link established (face #{face_id})", file=sys.stderr)

    peer_addr = bytes.fromhex(peer_hash)
    server.forwarder.add_route(Name(peer_addr, []), face_id, 10)

    await asyncio.sleep(1)

    path_components = [c.encode("utf-8") for c in name_str.split("/") if c]
    content_name = Name(peer_addr, path_components)

    data = Data.new(name=content_name, content=content)
    data.with_sequence(1)

    link_face = server._faces.get(face_id)
    if link_face is None:
        print("[publish] ERROR: Link face not found", file=sys.stderr)
        server.stop()
        sys.exit(1)

    await link_face.send_data(data)

    await asyncio.sleep(0.5)

    # Trigger manifest rebuild
    from rns_icn.packet import Interest

    manifest_name = Name(peer_addr, [b"manifest"])
    manifest_interest = (
        Interest(name=manifest_name).with_can_be_prefix().with_lifetime(8000)
    )
    manifest_result = await server.forwarder.express(manifest_interest, 0)
    if manifest_result is not None:
        print(f"[publish] Manifest confirmed ({len(manifest_result.content)} bytes)", file=sys.stderr)
    else:
        print(f"[publish] Warning: Could not verify manifest update", file=sys.stderr)

    print(f"[publish] Published '{name_str}' ({len(content)} bytes) → {peer_hash}", file=sys.stderr)

    server.stop()


if __name__ == "__main__":
    main()
