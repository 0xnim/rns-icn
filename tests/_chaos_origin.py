"""Origin subprocess for the real-RNS chaos tests.

Like ``_icn_origin.py`` but publishes a *set* of distinctly named blobs
(``item0 … item{N-1}``) instead of a single ``hello``. Distinct names matter
for chaos testing: each fetch of an as-yet-unfetched name is uncached at every
hop (client CS, router CS), so a successful fetch genuinely proves the Interest
reached the origin — letting a test distinguish "the route recovered" from "a
cache answered".

The identity is persisted at ``<configdir>/origin_identity`` and the listen
port is fixed by the caller, so the process can be killed and re-spawned with
the same RNS address + destination — simulating an origin crash and recovery.

Not a test module — the leading underscore keeps pytest from collecting it.
Invoked as::

    python tests/_chaos_origin.py <configdir> <listen_port> [num_items]
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


def content_for(label: str) -> bytes:
    """Deterministic content for a label — recomputable by the test side."""
    return b"chaos-content-" + label.encode()


def _write_config(configdir: str, listen_port: int) -> None:
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
"""
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(config)


async def main() -> None:
    configdir = sys.argv[1]
    listen_port = int(sys.argv[2])
    num_items = int(sys.argv[3]) if len(sys.argv) > 3 else 40

    _write_config(configdir, listen_port)
    RNS.Reticulum(configdir=configdir)

    identity_path = os.path.join(configdir, "origin_identity")
    # A fresh CS each boot is fine — content is re-published deterministically.
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

    for i in range(num_items):
        label = f"item{i}"
        server.publish_content(Name(server.identity.hash, [label.encode()]), content_for(label))
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
