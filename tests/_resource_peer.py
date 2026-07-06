"""Publisher-side subprocess for the resource-transport integration tests.

Runs a standalone Reticulum instance that dials into the test process's
shared instance over localhost TCP, links to the destination named in the
spec file, and publishes ICN Data packets over that link as RNS.Resources.
The receiving side — ResourceListener and every assertion — lives in the
parent test (two processes are required: RNS has no path to a destination
living in the same instance).

Spec file (JSON):
  app_name, aspect     the parent's IN destination name
  dest_hexhash         the parent destination to link to (hex)
  name_root_hex        root (identity hash) for published Names (hex)
  mode                 "data" — one ResourcePublisher.publish_data per item
                       "chunked" — chunk item 0 and send every Data packet
                       plus its manifest via LargeContentPublisher
  items                [{"labels": [str, ...], "content_b64": str}, ...]
  chunk_size           chunked mode: chunker chunk size in bytes
  resource_threshold   chunked mode: LargeContentPublisher threshold

Prints ``PEER_DONE`` once everything is published and idles until killed;
prints ``PEER_ERROR <reason>`` and exits non-zero on any failure.

Not a test module — the leading underscore keeps pytest from collecting it.
Invoked as: ``python tests/_resource_peer.py <configdir> <connect_port> <spec_path>``
"""

import asyncio
import base64
import json
import os
import sys

import RNS

from rns_icn.chunker import chunk_content
from rns_icn.name import Name
from rns_icn.packet import Data
from rns_icn.resource_transport import LargeContentPublisher, ResourcePublisher

RESOLVE_TIMEOUT = 30.0
LINK_TIMEOUT = 30.0
PUBLISH_TIMEOUT = 120.0


def _fail(reason: str) -> None:
    print(f"PEER_ERROR {reason}", flush=True)
    sys.exit(1)


def _write_config(configdir: str, connect_port: int) -> None:
    os.makedirs(configdir, exist_ok=True)
    config = f"""[reticulum]
  enable_transport = Yes
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = 3

[interfaces]
  [[TCP Client Interface]]
    type = TCPClientInterface
    interface_enabled = yes
    target_host = 127.0.0.1
    target_port = {connect_port}
"""
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(config)


async def _resolve_identity(dest_hash: bytes) -> RNS.Identity:
    """The parent destination's identity, via path request if not yet known."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + RESOLVE_TIMEOUT
    while True:
        identity = RNS.Identity.recall(dest_hash)
        if identity is not None:
            return identity
        if loop.time() >= deadline:
            _fail("no announce from parent destination")
        RNS.Transport.request_path(dest_hash)
        await asyncio.sleep(0.5)


async def _establish_link(dest: RNS.Destination) -> RNS.Link:
    link = RNS.Link(dest)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + LINK_TIMEOUT
    while link.status != RNS.Link.ACTIVE:
        if loop.time() >= deadline:
            _fail("link establishment timed out")
        await asyncio.sleep(0.1)
    return link


async def _publish(spec: dict, link: RNS.Link) -> None:
    root = bytes.fromhex(spec["name_root_hex"])
    if spec["mode"] == "data":
        publisher = ResourcePublisher(link)
        for item in spec["items"]:
            name = Name(root, [label.encode() for label in item["labels"]])
            data = Data.new(name=name, content=base64.b64decode(item["content_b64"]))
            if not await publisher.publish_data(data, timeout=PUBLISH_TIMEOUT):
                _fail(f"publish_data failed for {name}")
    elif spec["mode"] == "chunked":
        item = spec["items"][0]
        name = Name(root, [label.encode() for label in item["labels"]])
        result = chunk_content(
            base64.b64decode(item["content_b64"]), name, chunk_size=spec["chunk_size"]
        )
        lcp = LargeContentPublisher(link, resource_threshold=spec["resource_threshold"])
        for data in result.data_packets:
            if not await lcp.publish_data_packet(data, timeout=PUBLISH_TIMEOUT):
                _fail(f"publish_data_packet failed for {data.name}")
        manifest_data = Data.new(name=name, content=result.manifest.to_json())
        if not await lcp.publish_data_packet(manifest_data, timeout=PUBLISH_TIMEOUT):
            _fail("publish_data_packet failed for manifest")
    else:
        _fail(f"unknown mode {spec['mode']!r}")


async def main() -> None:
    configdir = sys.argv[1]
    connect_port = int(sys.argv[2])
    with open(sys.argv[3]) as f:
        spec = json.load(f)

    _write_config(configdir, connect_port)
    RNS.Reticulum(configdir=configdir)

    dest_hash = bytes.fromhex(spec["dest_hexhash"])
    identity = await _resolve_identity(dest_hash)
    dest = RNS.Destination(
        identity,
        RNS.Destination.OUT,
        RNS.Destination.SINGLE,
        spec["app_name"],
        spec["aspect"],
    )
    link = await _establish_link(dest)
    await _publish(spec, link)

    # Stay alive until the parent kills us: tearing the link down while the
    # receiver is still processing concluded resources would race the test.
    print("PEER_DONE", flush=True)
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
