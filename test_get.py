import asyncio
import sys
sys.path.insert(0, "/opt/rns-icn")

from rns_icn.config import load_server_config
from rns_icn.rns_server import ICNServer
from rns_icn.name import Name

async def main():
    config = load_server_config("/opt/rns-icn/icn.toml")
    server = ICNServer(config)
    await server.start()

    producer = server.rns_addr
    name1 = Name(producer, [b"test", b"hello"])
    
    server.publish_content(name1, b"Hello World!")
    
    cs = server.forwarder.cs
    print("CS len:", len(cs))
    
    # Check what _entries returns
    entries = cs._entries
    print("Entries keys:", list(entries.keys()))
    
    for entry_name in entries.keys():
        print("\n--- Testing get ---")
        print("Entry name:", entry_name)
        print("Entry name type:", type(entry_name))
        print("Entry name bytes:", entry_name.to_bytes().hex())
        
        # Manual hash check
        name_hash = cs._name_hash(entry_name)
        print("Computed name_hash:", name_hash.hex())
        
        # Check DB directly
        import sqlite3
        conn = sqlite3.connect(cs._path)
        row = conn.execute("SELECT name_hash, name_bytes FROM content").fetchone()
        if row:
            print("DB name_hash:", row[0].hex())
            print("DB name_bytes:", row[1].hex())
        
        # Try get
        data = cs.get(entry_name)
        print("cs.get result:", data.content if data else "None")
        
        # Try get with original name object
        data2 = cs.get(name1)
        print("cs.get(name1) result:", data2.content if data2 else "None")
    
    await server.shutdown()

asyncio.run(main())