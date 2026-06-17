"""Router subprocess for the real-RNS chaos tests.

A forwarding (CACHE-role) ICN node that links upstream to the origin and uses
the *production* route wiring from ``rns_icn.cli``:

- ``_install_peer_routes`` — connect to the origin and install its FIB route
  (retried while the path/announce propagates over the fresh TCP interface), and
- ``_wire_route_reinstall`` — re-establish + re-install the route when the
  origin re-announces after a link drop (dynamic FIB re-install on reconnect).

Using the real wiring is the point: the chaos test exercises the same withdraw-
on-close / reinstall-on-reannounce path that ``icn-router`` runs in production.

Both a TCP *server* (so clients link to it) and a TCP *client* (to the origin).
Prints ``ROUTER_READY <json>`` once its upstream route is installed, then reads
one command per line from stdin:

    DROP_UPSTREAM   tear down the upstream RNS link to the origin — the same
                    event RNS keepalive would eventually raise on a dead peer,
                    injected deterministically so the test doesn't wait minutes
                    for keepalive. Fires the real close hook → route withdrawn.
    ROUTES          print ``ROUTES <json>`` with the live next-hop count for the
                    origin prefix (0 after a withdraw, 1 after reinstall).
    QUIT            shut down and exit.

Recovery is *not* injected: the origin keeps announcing on its real 5s cadence,
and ``_wire_route_reinstall`` re-establishes the route off that announce.

Not a test module — the leading underscore keeps pytest from collecting it.
Invoked as::

    python tests/_chaos_router.py <configdir> <listen_port> <origin_port> \
        <origin_hexhash> <origin_identity_path>
"""

import asyncio
import json
import os
import sys

import RNS

from rns_icn.cli import _install_peer_routes, _wire_route_reinstall
from rns_icn.config import KnownPeer, ServerConfig
from rns_icn.name import Name
from rns_icn.rns_server import ICNServer
from rns_icn.server import ServerRole

APP_NAME = "icn"
ASPECT = "default"


def _origin_routes(server: ICNServer, origin_addr: bytes) -> list:
    """Live FIB next-hops for the origin's content prefix."""
    return server.forwarder.fib.lookup(Name(origin_addr, [b"probe"])) or []


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

    origin_addr = RNS.Identity.from_file(origin_identity_path).hash

    identity_path = os.path.join(configdir, "router_identity")
    cs_path = os.path.join(configdir, "router_cs.db")
    config = ServerConfig(
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
    server = ICNServer(config)
    await server.start()

    # Install the upstream route, retrying while the path/announce propagates.
    installed: list = []
    for _ in range(30):
        installed = await _install_peer_routes(server, config)
        if installed:
            break
        await asyncio.sleep(1)
    if not installed:
        print("ROUTER_ERROR could not link to origin", flush=True)
        await server.shutdown()
        return

    # Withdraw on link drop happens automatically via the close hook; wire the
    # re-install so a later origin re-announce restores the route.
    _wire_route_reinstall(server, config)

    print(
        "ROUTER_READY "
        + json.dumps(
            {"hexhash": server.hexhash, "identity_path": identity_path, "cs_path": cs_path}
        ),
        flush=True,
    )

    loop = asyncio.get_running_loop()
    try:
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break  # stdin closed
            cmd = line.split()[0].upper() if line.split() else ""
            if cmd == "QUIT":
                break
            if cmd == "DROP_UPSTREAM":
                dropped = 0
                for face_id, _cost in _origin_routes(server, origin_addr):
                    face = server._faces.get(face_id)
                    if face is not None:
                        # Tear down the live link directly (not face.close(),
                        # which pre-sets _closed and suppresses the close hook).
                        face.link.teardown()
                        dropped += 1
                print(f"DROPPED {json.dumps({'faces': dropped})}", flush=True)
            elif cmd == "ROUTES":
                print(
                    "ROUTES " + json.dumps({"count": len(_origin_routes(server, origin_addr))}),
                    flush=True,
                )
    finally:
        await server.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
