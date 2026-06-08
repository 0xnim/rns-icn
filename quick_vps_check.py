"""Quick check if VPS path appears after recent re-announce."""
import RNS, time, sys
RNS.Reticulum()

db = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
print("Checking path table...")

for i in range(24):
    time.sleep(5)
    known = db in list(RNS.Transport.path_table.keys())
    sys.stdout.write(f"\r[{i*5}s] paths={len(RNS.Transport.path_table)} known={known}")
    sys.stdout.flush()
    if known:
        entry = RNS.Transport.path_table[db]
        print(f"\nFOUND! hops={entry[2] if len(entry)>2 else '?'}")
        
        # Try link with hash override
        id = RNS.Identity()
        dest = RNS.Destination(id, RNS.Destination.OUT, RNS.Destination.SINGLE, "icn", "default")
        dest.hash = db
        dest.hexhash = "d005497acc72fb103257d20e0a2314db"
        
        print("Link attempt...")
        link = RNS.Link(dest)
        start = time.time()
        while link.status != RNS.Link.ACTIVE:
            time.sleep(0.2)
            if time.time() - start > 15:
                print(f"  Timed out. status={link.status} reason={link.teardown_reason}")
                if hasattr(link, 'packet') and link.packet:
                    print(f"  Packet sent: {link.packet.sent}")
                break
            if link.status == RNS.Link.CLOSED:
                print(f"  Closed. reason={link.teardown_reason}")
                break
        else:
            print(f"  ACTIVE! ({time.time()-start:.1f}s)")
            link.teardown()
        break
else:
    print("\nNot found after 2 minutes")
