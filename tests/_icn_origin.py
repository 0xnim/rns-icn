"""Origin-server subprocess for the RNS integration test.

Runs a standalone Reticulum instance with a single TCPServerInterface and an
ICNServer that publishes one blob + a manifest, then stays alive. Prints a
single ``ORIGIN_READY <json>`` line on stdout once content is published so the
parent test can hand-shake (destination hash + identity path).

Not a test module — the leading underscore keeps pytest from collecting it.
Invoked as: ``python tests/_icn_origin.py <configdir> <port> [--connect]``

By default the origin listens on ``port``. With ``--connect`` it instead
dials 127.0.0.1:``port`` as a TCP client — used when the peer is the test
process itself, whose shared Reticulum instance (see ``conftest.py``) owns
the listening side.
"""

import asyncio
import json
import os
import sys

import RNS

from rns_icn.config import ServerConfig
from rns_icn.name import Name
from rns_icn.rns_server import ICNServer

APP_NAME = "icn"
ASPECT = "default"
CONTENT = b"Hello from ICN server A!"


def _write_config(configdir: str, port: int, connect: bool) -> None:
    os.makedirs(configdir, exist_ok=True)
    if connect:
        interface = f"""  [[TCP Client Interface]]
    type = TCPClientInterface
    interface_enabled = yes
    target_host = 127.0.0.1
    target_port = {port}"""
    else:
        interface = f"""  [[TCP Server Interface]]
    type = TCPServerInterface
    interface_enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {port}"""
    config = f"""[reticulum]
  enable_transport = Yes
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = 3

[interfaces]
{interface}
"""
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(config)


async def main() -> None:
    configdir = sys.argv[1]
    port = int(sys.argv[2])
    _write_config(configdir, port, connect="--connect" in sys.argv[3:])

    RNS.Reticulum(configdir=configdir)

    identity_path = os.path.join(configdir, "origin_identity")
    server = ICNServer(
        ServerConfig(
            identity_path=identity_path,
            app_name=APP_NAME,
            aspect=ASPECT,
            cs_path=os.path.join(configdir, "origin_cs.db"),
            announce_interval=5.0,
            http_enabled=False,
        )
    )
    await server.start()

    hello = Name(server.identity.hash, [b"hello"])
    server.publish_content(hello, CONTENT)
    server.publish_manifest()

    print(
        "ORIGIN_READY "
        + json.dumps({"hexhash": server.hexhash, "identity_path": identity_path}),
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
