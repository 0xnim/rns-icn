"""Test link with normal config, wait for backbone path."""
import RNS, time, sys

RNS.Reticulum()

db = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
print("Waiting for VPS path...", flush=True)

for i in range(36):
    time.sleep(5)
    known = db in list(RNS.Transport.path_table.keys())
    sys.stdout.write(f"\r[{i*5:3d}s] paths={len(RNS.Transport.path_table)} known={known}")
    sys.stdout.flush()
    if known:
        entry = RNS.Transport.path_table[db]
        hops = entry[2]
        print(f"\nFOUND! hops={hops}")
        
        # Try link with proper identity
        vps_id = RNS.Identity()
        vps_id.hash = bytes.fromhex("f6b084e021f71c7a109d713365bae960")
        
        dest = RNS.Destination(vps_id, RNS.Destination.OUT, RNS.Destination.SINGLE, "icn", "default")
        print(f"Dest hash: {dest.hash.hex()}")
        
        link = RNS.Link(dest)
        start = time.time()
        while link.status != RNS.Link.ACTIVE:
            time.sleep(0.2)
            if link.status == RNS.Link.CLOSED:
                print(f"Closed: reason={link.teardown_reason}")
                if hasattr(link, 'packet') and link.packet:
                    print(f"  Packet sent: {link.packet.sent}")
                    print(f"  Packet raw[:50]: {link.packet.raw[:50].hex()}")
                break
            if time.time() - start > 30:
                print(f"Timeout: status={link.status}")
                break
        else:
            print(f"ACTIVE! ({time.time()-start:.1f}s)")
            link.teardown()
        break
else:
    print("\nNot found after 3min")
