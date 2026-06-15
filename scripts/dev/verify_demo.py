#!/usr/bin/env python3
"""Fetch signed Data from a remote ICN origin over the RNS mainnet and verify it.

Usage: verify_demo.py <peer_destination_hash> [label]

Exercises the real packet verification path: recalls the producer identity from
the name's producer (identity) hash and validates Data.signature, then shows a
tamper check.
"""

import asyncio
import os
import sys
import tempfile

import RNS

from rns_icn.config import ServerConfig
from rns_icn.name import Name
from rns_icn.packet import Interest
from rns_icn.rns_server import ICNServer


def _cfg() -> ServerConfig:
    wd = tempfile.mkdtemp(prefix="icn_demo_")
    RNS.logdest = RNS.LOG_FILE
    RNS.logfile = os.path.join(wd, "rns.log")
    return ServerConfig(
        identity_path=os.path.join(wd, "id"),
        app_name="icn",
        aspect="default",
        cs_path=os.path.join(wd, "cs.db"),
        http_enabled=False,
    )


async def main(peer_hash: str, label: str) -> int:
    peer = bytes.fromhex(peer_hash)
    srv = ICNServer(_cfg())
    await srv.start()
    print(f"[demo] local identity   : {srv.identity.hexhash}")
    print(f"[demo] resolving path to {peer_hash} over the mainnet ...")
    for _ in range(36):
        if RNS.Transport.has_path(peer):
            break
        RNS.Transport.request_path(peer)
        await asyncio.sleep(5)
    if not RNS.Transport.has_path(peer):
        print("[demo] no path to peer after 3 min")
        return 1
    print(f"[demo] path found, hops  : {RNS.Transport.hops_to(peer)}")

    fid = await srv.connect(peer_hash)
    if fid is None:
        print("[demo] link FAILED")
        return 1
    print(f"[demo] link established   : face #{fid}")

    peer_ident = RNS.Identity.recall(peer)  # recall by destination hash
    producer = peer_ident.hash if peer_ident else peer
    print(f"[demo] producer (id hash) : {producer.hex()}")
    srv.forwarder.add_route(Name(producer, []), fid, 10)
    await asyncio.sleep(1)

    name = Name(producer, [label.encode()])
    interest = Interest(name=name).with_can_be_prefix().with_lifetime(30000)
    print(f"[demo] expressing Interest: {name}")
    data = await srv.forwarder.express(interest, 0)
    if data is None:
        print("[demo] no Data returned (timeout)")
        await srv.shutdown()
        return 1

    sig = data.signature
    print()
    print(f"[demo] RECEIVED  : {data.name}")
    print(f"[demo]   content : {data.content!r}")
    print(f"[demo]   hash ok : {data.verify_content_hash()}")
    if sig is None:
        print("[demo]   signature: MISSING  <-- origin did not sign")
        await srv.shutdown()
        return 1
    print(f"[demo]   signature: present, {len(sig)} bytes ({sig[:8].hex()}...)")

    prod_ident = RNS.Identity.recall(producer, from_identity_hash=True)
    print(f"[demo]   producer key recalled over mesh: {prod_ident is not None}")
    verified = prod_ident is not None and data.verify_signature(prod_ident.validate)
    print(f"[demo]   >>> SIGNATURE VERIFIED: {verified}")

    # Tamper check: a cache that flips bytes cannot keep a valid signature.
    original = data.content
    data.content = original + b" [tampered]"
    tampered_ok = data.verify_signature(prod_ident.validate)
    data.content = original
    print(f"[demo]   >>> tampered content verifies: {tampered_ok}  (expect False)")

    await srv.shutdown()
    return 0 if verified and not tampered_ok else 1


if __name__ == "__main__":
    peer = sys.argv[1]
    lbl = sys.argv[2] if len(sys.argv) > 2 else "hello"
    sys.exit(asyncio.run(main(peer, lbl)))
