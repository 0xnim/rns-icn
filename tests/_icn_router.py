"""Router subprocess for the multi-hop RNS integration test.

Runs a standalone Reticulum instance that is both:
- a TCP *server* (so the parent test process can link to it as a client), and
- a TCP *client* to the origin (so it can link upstream to the origin server).

The ICNServer runs in CACHE role: it links to the origin, installs a FIB route
for the origin's content prefix, then forwards Interests on a CS miss and caches
the reverse-path Data. Prints a single ``ROUTER_READY <json>`` line on stdout
once the upstream route is installed.

Not a test module — the leading underscore keeps pytest from collecting it.
Invoked as::

    python tests/_icn_router.py <configdir> <listen_port> <origin_port> \
        <origin_hexhash> <origin_identity_path>
"""

import asyncio
import json
import os
import sys

import RNS

from rns_icn.config import KnownPeer, ServerConfig
from rns_icn.rns_server import ICNServer
from rns_icn.server import ServerRole
from rns_icn.name import Name

APP_NAME = "icn"
ASPECT = "default"


def _write_config(configdir: str, listen_port: int, origin_port: int) -> None:
    os.makedirs(configdir, exist_ok=True)
    config = f"""[reticulum]
  enable_transport = Yes
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = 3

[interfaces]
  [[TCP Server Interface]]
    type = TCPServerInterface
    interface_enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {listen_port}

  [[TCP Client to Origin]]
    type = TCPClientInterface
    interface_enabled = yes
    target_host = 127.0.0.1
    target_port = {origin_port}
"""
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(config)


async def main() -> None:
    configdir = sys.argv[1]
    listen_port = int(sys.argv[2])
    origin_port = int(sys.argv[3])
    origin_hexhash = sys.argv[4]
    origin_identity_path = sys.argv[5]

    _write_config(configdir, listen_port, origin_port)
    RNS.Reticulum(configdir=configdir)

    identity_path = os.path.join(configdir, "router_identity")
    cs_path = os.path.join(configdir, "router_cs.db")
    server = ICNServer(
        ServerConfig(
            identity_path=identity_path,
            app_name=APP_NAME,
            aspect=ASPECT,
            role=ServerRole.CACHE,
            cs_path=cs_path,
            announce_interval=5.0,
            http_enabled=False,
            known_peers=[
                KnownPeer(
                    name="origin",
                    destination_hash=origin_hexhash,
                    identity_path=origin_identity_path,
                )
            ],
        )
    )
    await server.start()

    # Link upstream to the origin and install a FIB route for its content.
    # Retry while the path/announce propagates over the fresh TCP interface.
    face_id = None
    for _ in range(30):
        face_id = await server.connect(origin_hexhash)
        if face_id is not None:
            break
        await asyncio.sleep(1)
    if face_id is None:
        print("ROUTER_ERROR could not link to origin", flush=True)
        await server.shutdown()
        return

    origin_identity = RNS.Identity.from_file(origin_identity_path)
    server.forwarder.add_route(Name(origin_identity.hash), face_id, 10)

    print(
        "ROUTER_READY "
        + json.dumps(
            {
                "hexhash": server.hexhash,
                "identity_path": identity_path,
                "cs_path": cs_path,
            }
        ),
        flush=True,
    )

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await server.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
