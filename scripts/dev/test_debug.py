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
    print("Producer RNS addr:", producer.hex())
    print("Server rns_addr:", server.rns_addr.hex())
    print("Identity hash:", server.identity.hash.hex())

    # Publish content
    name1 = Name(producer, [b"test", b"hello"])
    name2 = Name(producer, [b"test", b"data"])
    print("Name1:", name1)
    print("Name1 components:", [c.hex() if isinstance(c, bytes) else c for c in name1.components])
    print("Name1 rns_addr:", name1.rns_addr.hex())
    print("Name1 starts_with producer:", name1.starts_with(Name(producer, [])))
    print("Name1 len:", name1.len())
    print("Prefix len:", Name(producer, []).len())

    server.publish_content(name1, b"Hello World!")
    server.publish_content(name2, b"Test data content")

    # Check CS - debug
    print("\n--- CS Debug ---")
    cs = server.forwarder.cs
    print("CS len:", len(cs))
    print("CS _entries type:", type(cs._entries))
    for entry_name in cs._entries.keys():
        print("  Entry name:", entry_name)
        print("  Entry name components:", entry_name.components)
        print("  Entry name starts_with:", entry_name.starts_with(Name(producer, [])))
        print("  Entry name is_root:", entry_name.is_root())
        if entry_name.starts_with(Name(producer, [])) and not entry_name.is_root():
            label = entry_name.components[1].decode("utf-8", errors="replace")
            print("  Label:", label)
            data = cs.get(entry_name)
            print("  Data:", data.content if data else None)

    server.publish_manifest()
    mf = server.forwarder.cs.get(Name(producer, [b"manifest"]))
    if mf:
        m = Manifest.from_data(mf)
        print("\nManifest entries:", len(m.entries))
        for e in m.entries:
            print("  ", e.kind.value, e.label)

    print("\nTest done!")
    await server.shutdown()

asyncio.run(main())