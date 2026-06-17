#!/usr/bin/env python3
"""CLI entry point for icn-subscribe (installed via pip).

Subscribes to a stream prefix on an ICN producer and prints (or saves) each
Data packet the producer pushes. Unlike icn-fetch (one-shot request/response),
this upgrades the link to push mode via an APS Subscribe handshake and then
runs until interrupted or until --count messages have arrived.
"""

import argparse
import asyncio
import os
import re
import signal
import sys
import tempfile

import RNS

from rns_icn.config import ServerConfig
from rns_icn.name import Name
from rns_icn.packet import APSubscribe, Data
from rns_icn.rns_server import ICNServer


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


def _safe_filename(name: Name) -> str:
    """Derive a filesystem-safe name for a pushed Data when writing to a dir."""
    parts = [c.decode("utf-8", "replace") for c in name.components[1:]]
    joined = "_".join(parts) if parts else name.rns_addr.hex()
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", joined)
    return safe or "data"


def main():
    """Entry point for `icn-subscribe` console script."""
    parser = argparse.ArgumentParser(
        prog="icn-subscribe",
        description="Subscribe to a stream on an ICN producer and print pushed Data.",
    )
    parser.add_argument("peer_hash", help="RNS destination hex hash of the producer")
    parser.add_argument("name", help="Stream prefix (e.g. 'feed', 'sensors/temp')")
    parser.add_argument(
        "--from-now",
        action="store_true",
        help="Only receive content produced after subscribing (skip existing backlog)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Exit after receiving this many messages (default: run until Ctrl+C)",
    )
    parser.add_argument(
        "--out-dir",
        help="Write each message to a file in this directory instead of stdout",
    )
    args = parser.parse_args()

    if args.out_dir:
        try:
            os.makedirs(args.out_dir, exist_ok=True)
        except OSError as e:
            print(f"[subscribe] ERROR: cannot use --out-dir: {e}", file=sys.stderr)
            sys.exit(1)

    sys.exit(asyncio.run(_subscribe(args)))


async def _subscribe(args) -> int:
    print("[subscribe] Initializing RNS...", file=sys.stderr)

    peer_addr_raw = bytes.fromhex(args.peer_hash)
    server = ICNServer(_ephemeral_config())
    await server.start()

    print(f"[subscribe] Local identity: {server.identity.hexhash}", file=sys.stderr)
    print(f"[subscribe] Target peer dest: {args.peer_hash}", file=sys.stderr)

    # Wait for a path to the peer so its identity can be recalled + a Link made.
    print("[subscribe] Waiting for path to peer...", file=sys.stderr)
    for _attempt in range(24):  # up to 2 minutes
        if RNS.Transport.has_path(peer_addr_raw):
            hops = RNS.Transport.hops_to(peer_addr_raw)
            print(f"[subscribe] Path found! hops={hops}", file=sys.stderr)
            break
        RNS.Transport.request_path(peer_addr_raw)
        await asyncio.sleep(5)
    else:
        print("[subscribe] WARNING: path not found after 2min, trying anyway...",
              file=sys.stderr)

    print(f"[subscribe] Connecting to peer: {args.peer_hash}", file=sys.stderr)
    face_id = await server.connect(args.peer_hash)
    if face_id is None:
        print("[subscribe] FAILED — could not establish link to peer", file=sys.stderr)
        await server.shutdown()
        return 1
    print(f"[subscribe] Link established (face #{face_id})", file=sys.stderr)

    # Content is named under the producer's *identity* hash, but Links target the
    # *destination* hash. Recall the peer identity to build the correct prefix.
    peer_identity = RNS.Identity.recall(peer_addr_raw)
    producer_addr = peer_identity.hash if peer_identity is not None else peer_addr_raw

    path_components = [c.encode("utf-8") for c in args.name.split("/") if c]
    stream_name = Name(producer_addr, path_components)

    # Surface pushed Data: it carries no PIT entry on a leaf consumer, so the
    # forwarder would otherwise drop it. The observer fires on the event loop.
    stop_event = asyncio.Event()
    received = 0

    def on_data(data: Data) -> None:
        nonlocal received
        # Once the count is reached, ignore further pushes that may already be
        # queued for this event-loop wakeup so --count is a firm upper bound.
        if args.count and received >= args.count:
            return
        if not data.name.starts_with(stream_name):
            return
        received += 1
        _emit(data, received, args.out_dir)
        if args.count and received >= args.count:
            stop_event.set()

    server.forwarder.set_data_callback(on_data)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # Windows

    link_face = server._faces.get(face_id)
    if link_face is None:
        print("[subscribe] ERROR: Link face not found", file=sys.stderr)
        await server.shutdown()
        return 1

    sub = APSubscribe(name=stream_name, start_from_now=args.from_now)
    await link_face.send_raw(sub.to_bytes())
    print(f"[subscribe] Subscribed to {stream_name} "
          f"({'new only' if args.from_now else 'incl. existing'}). "
          f"{'Waiting for ' + str(args.count) + ' message(s).' if args.count else 'Ctrl+C to stop.'}",
          file=sys.stderr)

    try:
        await stop_event.wait()
    finally:
        print(f"\n[subscribe] Done — received {received} message(s)", file=sys.stderr)
        await server.shutdown()
    return 0


def _emit(data: Data, index: int, out_dir: str | None) -> None:
    content = data.content
    seq = data.metadata.sequence if data.metadata else None
    hash_hex = (
        data.metadata.content_hash.hex()
        if data.metadata and data.metadata.content_hash
        else "N/A"
    )
    seq_str = f"v{seq}" if seq is not None else "?"
    print(f"[subscribe] #{index} {data.name} ({len(content)} bytes, seq {seq_str}, "
          f"hash {hash_hex[:16]})", file=sys.stderr)

    if out_dir:
        fname = f"{index:06d}_{_safe_filename(data.name)}"
        path = os.path.join(out_dir, fname)
        try:
            with open(path, "wb") as f:
                f.write(content)
        except OSError as e:
            print(f"[subscribe]   ! cannot write {path}: {e}", file=sys.stderr)
        else:
            print(f"[subscribe]   wrote {path}", file=sys.stderr)
    else:
        sys.stdout.buffer.write(content)
        if not content.endswith(b"\n"):
            sys.stdout.buffer.write(b"\n")
        sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
