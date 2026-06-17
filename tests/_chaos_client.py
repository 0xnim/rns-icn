"""Interactive client subprocess for the real-RNS chaos tests.

A long-lived edge ICN node driven over stdin so a single client process can
fetch repeatedly across a chaos timeline (before a fault, during it, after
recovery) — which is what lets the test exercise *true* multi-path fall-through
(primary face dead → forwarder fails over to the backup) rather than just
"the backup works once the primary is gone".

Startup links to one or more routers and installs a cost-ordered FIB route for
the origin's content prefix via each, then prints ``CLIENT_READY <json>``.
Thereafter it reads one command per line from stdin:

    FETCH <label> [lifetime_ms]   → express Interest for /<origin>/<label>,
                                     prints ``FETCH_RESULT <json>``
    QUIT                          → shut down and exit

Distinct labels are uncached everywhere, so a successful FETCH proves the
Interest reached the origin over a currently-working path.

Not a test module — the leading underscore keeps pytest from collecting it.
Invoked as::

    python tests/_chaos_client.py <configdir> <origin_identity_path> <routes_json>

where ``routes_json`` is a JSON list of
``{"port": int, "hexhash": str, "identity_path": str, "cost": int}``.
"""

import asyncio
import json
import os
import sys

import RNS

from rns_icn.config import KnownPeer, ServerConfig
from rns_icn.name import Name
from rns_icn.packet import Interest
from rns_icn.rns_server import ICNServer

APP_NAME = "icn"
ASPECT = "default"


def _write_config(configdir: str, routes: list[dict]) -> None:
    os.makedirs(configdir, exist_ok=True)
    iface_blocks = []
    for i, r in enumerate(routes):
        iface_blocks.append(
            f"""  [[TCP Client {i}]]
    type = TCPClientInterface
    interface_enabled = yes
    target_host = 127.0.0.1
    target_port = {r['port']}
"""
        )
    config = f"""[reticulum]
  enable_transport = No
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = 3

[interfaces]
{''.join(iface_blocks)}"""
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(config)


async def _connect_routes(client: ICNServer, origin_addr: bytes, routes: list[dict]) -> list[str]:
    """Link to each router and install a cost-ordered route to the origin.

    Returns the hexhashes of routers that linked successfully. A router that
    can't be reached (e.g. already killed) is skipped, not fatal.
    """
    connected: list[str] = []
    for r in routes:
        face = None
        for _ in range(30):
            face = await client.connect(r["hexhash"])
            if face is not None:
                break
            await asyncio.sleep(1)
        if face is None:
            continue
        client.forwarder.add_route(Name(origin_addr), face, r["cost"])
        connected.append(r["hexhash"])
    return connected


async def main() -> None:
    configdir = sys.argv[1]
    origin_identity_path = sys.argv[2]
    routes = json.loads(sys.argv[3])

    _write_config(configdir, routes)
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
                KnownPeer(name=f"router{i}", destination_hash=r["hexhash"],
                          identity_path=r["identity_path"])
                for i, r in enumerate(routes)
            ],
        )
    )
    await client.start()

    connected = await _connect_routes(client, origin_addr, routes)
    print("CLIENT_READY " + json.dumps({"connected": connected}), flush=True)

    loop = asyncio.get_running_loop()
    try:
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break  # stdin closed
            parts = line.split()
            if not parts:
                continue
            cmd = parts[0].upper()
            if cmd == "QUIT":
                break
            if cmd == "FETCH":
                label = parts[1]
                lifetime = int(parts[2]) if len(parts) > 2 else 15000
                name = Name(origin_addr, [label.encode()])
                start = loop.time()
                try:
                    data = await client.forwarder.express(
                        Interest(name=name, lifetime_ms=lifetime), 0
                    )
                except Exception:
                    # A send over a face whose link just died can raise; for the
                    # consumer that's an unsatisfied Interest, same as a timeout.
                    data = None
                elapsed_ms = int((loop.time() - start) * 1000)
                result = {
                    "label": label,
                    "ok": data is not None,
                    "content_hex": data.content.hex() if data is not None else None,
                    "name_ok": (data.name == name) if data is not None else False,
                    "elapsed_ms": elapsed_ms,
                }
                print("FETCH_RESULT " + json.dumps(result), flush=True)
    finally:
        await client.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
