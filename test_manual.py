import asyncio
import sys
sys.path.insert(0, "/opt/rns-icn")

# Don't start RNS here - let the server handle it

from rns_icn.config import load_server_config, ClientConfig
from rns_icn.rns_server import ICNServer
from rns_icn.client import ICNClient
from rns_icn.name import Name
from rns_icn.manifest import Manifest, EntryKind

async def main():
    config = load_server_config("/opt/rns-icn/icn.toml")
    server = ICNServer(config)
    await server.start()
    print("Server started:", server.hexhash)

    producer = server.rns_addr

    # Publish content
    server.publish_content(Name(producer, [b"test", b"hello"]), b"Hello World!")
    server.publish_content(Name(producer, [b"test", b"data"]), b"Test data content")

    # Check CS
    print("CS entries:", len(server.forwarder.cs))
    for name in server.forwarder.cs._entries:
        print("  ", name)

    server.publish_manifest()
    mf = server.forwarder.cs.get(Name(producer, [b"manifest"]))
    if mf:
        m = Manifest.from_data(mf)
        print("Manifest entries:", len(m.entries))
        for e in m.entries:
            print("  ", e.kind.value, e.label)

    # Test client
    client_config = ClientConfig(
        identity_path="/tmp/test_client_identity",
        mesh_interfaces=["UTN Oregon"],
        known_peers=config.known_peers,
    )
    async with ICNClient(client_config) as client:
        print("Client started:", client.identity.hexhash)
        manifest = await client.fetch_manifest(producer, timeout=30)
        if manifest:
            print("Client fetched manifest:", len(manifest.entries))
            for e in manifest.entries:
                print("  ", e.kind.value, e.label)
                if e.kind in (EntryKind.BLOB, EntryKind.STREAM):
                    content = await client.fetch_content(e, producer, timeout=10)
                    if content:
                        print("    Content:", content[:60])
        else:
            print("Client failed to fetch manifest")

    print("Test done!")
    await server.shutdown()

asyncio.run(main())