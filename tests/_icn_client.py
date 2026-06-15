"""Client subprocess for the multi-hop RNS integration test.

Runs a standalone Reticulum instance (TCP client to the router only), links to
the router, routes the origin's content prefix *via the router*, then fetches
the origin's manifest and a blob — exercising two ICN hops over two real RNS
Links. Prints a single ``CLIENT_RESULT <json>`` line summarising what it got.

A separate process is required because RNS.Reticulum is a hard process
singleton and cannot be re-initialised within the parent test process (which
already brings up its own instance for the single-hop test).

Not a test module — the leading underscore keeps pytest from collecting it.
Invoked as::

    python tests/_icn_client.py <configdir> <router_port> <router_hexhash> \
        <router_identity_path> <origin_identity_path>
"""

import asyncio
import json
import os
import sys

import RNS

from rns_icn.config import KnownPeer, ServerConfig
from rns_icn.rns_server import ICNServer
from rns_icn.name import Name
from rns_icn.packet import Interest

APP_NAME = "icn"
ASPECT = "default"


def _write_config(configdir: str, router_port: int) -> None:
    os.makedirs(configdir, exist_ok=True)
    config = f"""[reticulum]
  enable_transport = No
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = 3

[interfaces]
  [[TCP Client to Router]]
    type = TCPClientInterface
    interface_enabled = yes
    target_host = 127.0.0.1
    target_port = {router_port}
"""
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(config)


async def main() -> None:
    configdir = sys.argv[1]
    router_port = int(sys.argv[2])
    router_hexhash = sys.argv[3]
    router_identity_path = sys.argv[4]
    origin_identity_path = sys.argv[5]

    _write_config(configdir, router_port)
    RNS.Reticulum(configdir=configdir)

    origin_identity = RNS.Identity.from_file(origin_identity_path)
    origin_addr = origin_identity.hash

    client = ICNServer(
        ServerConfig(
            identity_path=os.path.join(configdir, "client_identity"),
            app_name=APP_NAME,
            aspect=ASPECT,
            cs_path=os.path.join(configdir, "client_cs.db"),
            announce_interval=5.0,
            http_enabled=False,
            known_peers=[
                KnownPeer(
                    name="router",
                    destination_hash=router_hexhash,
                    identity_path=router_identity_path,
                )
            ],
        )
    )
    await client.start()

    result = {"linked": False, "manifest": False, "content_hex": None, "name_ok": False}
    try:
        # Link to the router and route the origin's prefix *via the router*.
        router_face = None
        for _ in range(30):
            router_face = await client.connect(router_hexhash)
            if router_face is not None:
                break
            await asyncio.sleep(1)
        if router_face is None:
            print("CLIENT_RESULT " + json.dumps(result), flush=True)
            return
        result["linked"] = True
        client.forwarder.add_route(Name(origin_addr), router_face, 10)

        # Manifest discovery — forwarded router → origin and back.
        manifest = await client.forwarder.express(
            Interest(name=Name(origin_addr, [b"manifest"]))
            .with_can_be_prefix()
            .with_lifetime(20000),
            0,
        )
        result["manifest"] = manifest is not None

        # Content fetch — two ICN hops over two real RNS Links.
        hello = Name(origin_addr, [b"hello"])
        data = await client.forwarder.express(
            Interest(name=hello, lifetime_ms=20000), 0
        )
        if data is not None:
            result["content_hex"] = data.content.hex()
            result["name_ok"] = data.name == hello

        print("CLIENT_RESULT " + json.dumps(result), flush=True)
    finally:
        await client.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
