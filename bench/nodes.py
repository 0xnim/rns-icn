"""Benchmark node processes: origin, hub and reader.

Each runs its own Reticulum instance in its own config directory and talks to
the others over localhost TCP (through the harness's shapers). Run by
``bench/run.py``; not meant to be started by hand.

    nodes.py origin <dir> <listen_port> <items_json>
    nodes.py hub    <dir> <listen_port> <origin_port> <origin_hash> <origin_identity> <follow:0|1>
    nodes.py reader <dir> <hub_port> <mode> <spec_json>

The origin serves every item twice: as ICN Data under ``/<origin>/feed/<label>``
and as a page on a NomadNet-compatible node destination
(``nomadnetwork.node``, request path ``/page/<label>.mu``), which is how
existing Reticulum clients fetch content today.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import RNS

from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig, KnownPeer, ServerConfig
from rns_icn.name import Name
from rns_icn.rns_server import ICNServer
from rns_icn.server import ServerRole

PAGE_APP = ("nomadnetwork", "node")
# Announces cross the shaped links and would swamp a 1200 bit/s channel; keep
# the periodic ones out of the measurement window.
QUIET_ANNOUNCE = 3600.0


def write_config(configdir: str, transport: bool, interfaces: list[str]) -> None:
    os.makedirs(configdir, exist_ok=True)
    body = "\n".join(interfaces)
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(f"""[reticulum]
  enable_transport = {"Yes" if transport else "No"}
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = {os.environ.get('BENCH_LOGLEVEL', '2')}

[interfaces]
{body}
""")


def tcp_server(port: int) -> str:
    return f"""  [[Listen {port}]]
    type = TCPServerInterface
    interface_enabled = yes
    ingress_control = false
    listen_ip = 127.0.0.1
    listen_port = {port}
"""


def tcp_client(port: int) -> str:
    return f"""  [[Connect {port}]]
    type = TCPClientInterface
    interface_enabled = yes
    ingress_control = false
    target_host = 127.0.0.1
    target_port = {port}
"""


def say(tag: str, payload: dict) -> None:
    print(f"{tag} {json.dumps(payload)}", flush=True)


# ── origin ──


async def origin(configdir: str, listen_port: int, items: dict[str, str]) -> None:
    write_config(configdir, transport=True, interfaces=[tcp_server(listen_port)])
    RNS.Reticulum(configdir=configdir)

    identity_path = os.path.join(configdir, "identity")
    server = ICNServer(ServerConfig(
        identity_path=identity_path,
        cs_path=os.path.join(configdir, "cs.db"),
        announce_interval=QUIET_ANNOUNCE,
        http_enabled=False,
    ))
    await server.start()

    pages: dict[str, bytes] = {}
    for seq, (label, text) in enumerate(items.items(), start=1):
        content = text.encode()
        server.publish_content(Name(server.identity.hash, [b"feed", label.encode()]),
                               content, sequence=seq)
        pages[f"/page/{label}.mu"] = content

    # NomadNet-compatible page node on the same identity.
    page_dest = RNS.Destination(
        server.identity, RNS.Destination.IN, RNS.Destination.SINGLE, *PAGE_APP
    )

    def serve(path, data, request_id, link_id, remote_identity, requested_at):
        return pages.get(path)

    for path in pages:
        page_dest.register_request_handler(
            path, response_generator=serve, allow=RNS.Destination.ALLOW_ALL
        )
    page_dest.announce()

    say("READY", {
        "icn_hash": server.hexhash,
        "page_hash": page_dest.hexhash,
        "identity_path": identity_path,
        "producer": server.identity.hash.hex(),
    })
    try:
        # Commands from the harness: {"publish": {label: text}, "seq": n}
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            cmd = json.loads(line)
            if cmd.get("announce"):
                # A page node announces periodically; the harness triggers it
                # once the hub is up so the hub holds a path, as it would on a
                # running network.
                page_dest.announce()
                say("ANNOUNCED", {})
                continue
            for label, text in cmd.get("publish", {}).items():
                await server.publish_post(
                    Name(server.identity.hash, [b"feed", label.encode()]),
                    text.encode(), sequence=cmd["seq"],
                )
                path = f"/page/{label}.mu"
                pages[path] = text.encode()
                page_dest.register_request_handler(
                    path, response_generator=serve, allow=RNS.Destination.ALLOW_ALL
                )
            say("PUBLISHED", {})
    finally:
        await server.shutdown()


# ── hub ──


async def hub(configdir: str, listen_port: int, origin_port: int, origin_hash: str,
              origin_identity_path: str, follow: bool) -> None:
    write_config(configdir, transport=True,
                 interfaces=[tcp_server(listen_port), tcp_client(origin_port)])
    RNS.Reticulum(configdir=configdir)

    identity_path = os.path.join(configdir, "identity")
    server = ICNServer(ServerConfig(
        identity_path=identity_path,
        role=ServerRole.CACHE,
        cs_path=os.path.join(configdir, "cs.db"),
        announce_interval=QUIET_ANNOUNCE,
        http_enabled=False,
        known_peers=[KnownPeer(name="origin", destination_hash=origin_hash,
                               identity_path=origin_identity_path)],
    ))
    await server.start()

    face_id = None
    for _ in range(60):
        face_id = await server.connect(origin_hash)
        if face_id is not None:
            break
        await asyncio.sleep(1)
    if face_id is None:
        say("ERROR", {"error": "could not link to origin"})
        await server.shutdown()
        return

    origin_identity = RNS.Identity.from_file(origin_identity_path)
    producer = origin_identity.hash
    server.forwarder.add_route(Name(producer), face_id, 10)
    if follow:
        await server.follower.follow(face_id, producer)

    def stats() -> dict:
        s = server.follower.stats().get(producer.hex())
        return dict(s.__dict__) if s is not None else {}

    say("READY", {"icn_hash": server.hexhash, "identity_path": identity_path,
                  "follow": stats()})
    try:
        loop = asyncio.get_running_loop()
        while True:  # any line from the harness -> follower counters
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            say("STATS", stats())
    finally:
        await server.shutdown()


# ── reader ──


async def wait_for_path(dest: bytes, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    asked = float("-inf")
    while time.monotonic() < deadline:
        if RNS.Transport.has_path(dest):
            return True
        if time.monotonic() - asked > 15:
            RNS.Transport.request_path(dest)
            asked = time.monotonic()
        await asyncio.sleep(0.05)
    return False


async def read_page(spec: dict, timeout: float) -> bytes | None:
    """Fetch the way a NomadNet browser does: link to the node, request a page."""
    dest_hash = bytes.fromhex(spec["page_hash"])
    if not await wait_for_path(dest_hash, timeout):
        return None
    identity = RNS.Identity.recall(dest_hash)
    if identity is None:
        return None
    dest = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, *PAGE_APP)

    loop = asyncio.get_running_loop()
    done: asyncio.Future = loop.create_future()

    def finish(value):
        loop.call_soon_threadsafe(lambda: done.done() or done.set_result(value))

    def established(link):
        link.request(
            f"/page/{spec['label']}.mu",
            data=None,
            response_callback=lambda receipt: finish(receipt.response),
            failed_callback=lambda receipt: finish(None),
            timeout=timeout,
        )

    link = RNS.Link(dest, established_callback=established,
                    closed_callback=lambda link: finish(None))
    try:
        response = await asyncio.wait_for(done, timeout)
    except asyncio.TimeoutError:
        response = None
    link.teardown()
    if isinstance(response, str):
        response = response.encode()
    return response


async def read_icn(configdir: str, spec: dict, timeout: float) -> bytes | None:
    """Fetch through the hub with the real client, verification included."""
    client = ICNClient(ClientConfig(
        identity_path=os.path.join(configdir, "identity"),
        known_peers=[KnownPeer(name="hub", destination_hash=spec["hub_hash"],
                               identity_path=spec["hub_identity"])],
        connect_timeout=timeout,
        fetch_timeout=timeout,
        path_request_timeout=timeout,
        max_retries=0,
    ))
    await client.start()
    try:
        name = Name(bytes.fromhex(spec["producer"]), [b"feed", spec["label"].encode()])
        data = await client.fetch(name, bytes.fromhex(spec["hub_hash"]), timeout=timeout)
        return data.content if data is not None else None
    finally:
        await client.shutdown()


async def reader(configdir: str, hub_port: int, mode: str, spec: dict) -> None:
    write_config(configdir, transport=False, interfaces=[tcp_client(hub_port)])
    RNS.Reticulum(configdir=configdir)
    timeout = float(spec.get("timeout", 300))
    await asyncio.sleep(1.0)  # let the TCP interface come up before timing

    started = time.monotonic()
    error = None
    try:
        if mode == "page":
            content = await read_page(spec, timeout)
        else:
            content = await read_icn(configdir, spec, timeout)
    except Exception as e:  # a failed fetch is a result, not a harness crash
        content, error = None, f"{type(e).__name__}: {e}"
    elapsed = time.monotonic() - started
    say("RESULT", {
        "ok": content is not None,
        "error": error,
        "seconds": round(elapsed, 3),
        "size": len(content) if content is not None else 0,
        "matches": content == spec["expect"].encode() if content is not None else False,
    })


def main() -> None:
    role, configdir = sys.argv[1], sys.argv[2]
    if role == "origin":
        coro = origin(configdir, int(sys.argv[3]), json.loads(sys.argv[4]))
    elif role == "hub":
        coro = hub(configdir, int(sys.argv[3]), int(sys.argv[4]), sys.argv[5],
                   sys.argv[6], sys.argv[7] == "1")
    elif role == "reader":
        coro = reader(configdir, int(sys.argv[3]), sys.argv[4], json.loads(sys.argv[5]))
    else:
        raise SystemExit(f"unknown role {role}")
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
