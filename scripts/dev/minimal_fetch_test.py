"""Minimal test: isolated config, connect to VPS, wait 5 min."""
import RNS, os, tempfile, time, sys

configdir = tempfile.mkdtemp(prefix="icn_fetch_min_")
os.makedirs(f"{configdir}/storage", exist_ok=True)

config = """[reticulum]
enable_transport = Yes
share_instance = No

[logging]
loglevel = 3

[interfaces]
  [[VPS Direct]]
    type = TCPClientInterface
    enabled = yes
    target_host = 172.81.133.81
    target_port = 49200
    ifac_size = 6
"""
with open(f"{configdir}/config", "w") as f:
    f.write(config)

os.environ["RNS_CONFIGDIR"] = configdir
try:
    RNS.Reticulum(configdir=configdir)
except TypeError:
    RNS.Reticulum(configdir=configdir, loglevel=3)

db = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
print("Waiting for VPS path...", flush=True)
for i in range(60):
    time.sleep(5)
    known = db in list(RNS.Transport.path_table.keys())
    sys.stdout.write(f"\r[{i*5:3d}s] paths={len(RNS.Transport.path_table)} known={known}")
    sys.stdout.flush()
    if known:
        entry = RNS.Transport.path_table[db]
        print(f"\nFOUND! hops={entry[2]}")
        
        # Now try link
        id = RNS.Identity()
        dest = RNS.Destination(id, RNS.Destination.OUT, RNS.Destination.SINGLE, "icn", "default")
        dest.hash = db
        dest.hexhash = "d005497acc72fb103257d20e0a2314db"
        
        print("Linking...", flush=True)
        link = RNS.Link(dest)
        start = time.time()
        while link.status != RNS.Link.ACTIVE:
            time.sleep(0.2)
            if link.status == RNS.Link.CLOSED:
                print(f"Closed: reason={link.teardown_reason}")
                break
            if time.time() - start > 30:
                print("Timed out")
                break
        else:
            print(f"ACTIVE! ({time.time()-start:.1f}s)!")
            # Try Interest
            from rns_icn.face import LinkFace
            from rns_icn.packet import Interest
            from rns_icn.name import Name
            lf = LinkFace(1, link, asyncio.get_event_loop() if hasattr(asyncio, 'get_event_loop') else None)
            import asyncio
            lf._loop = asyncio.get_event_loop()
            name = Name(db, [b"manifest"])
            interest = Interest(name=name).with_can_be_prefix().with_lifetime(30000)
            result = asyncio.get_event_loop().run_until_complete(lf.express_interest(interest))
            if result:
                print(f"Got data: {result.content[:200]}")
            else:
                print("No response")
            link.teardown()
        break
else:
    print("\nNot found after 5 minutes")
