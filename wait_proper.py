"""Wait for VPS path with progress."""
import RNS, time, sys
RNS.Reticulum()

db = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
print("Waiting for VPS destination...", flush=True)

for i in range(36):
    time.sleep(5)
    known = db in list(RNS.Transport.path_table.keys())
    pts = len(RNS.Transport.path_table)
    sys.stdout.write(f"\r  [{i*5:3d}s] paths={pts} dest={known}")
    sys.stdout.flush()
    if known:
        entry = RNS.Transport.path_table[db]
        hops = entry[2] if len(entry) > 2 else "?"
        print(f"\nFOUND! hops={hops}")
        
        # Now try the link
        id = RNS.Identity()
        dest = RNS.Destination(id, RNS.Destination.OUT, RNS.Destination.SINGLE, "icn", "default")
        dest.hash = db
        dest.hexhash = "d005497acc72fb103257d20e0a2314db"
        
        print("Creating link...", flush=True)
        link = RNS.Link(dest)
        start = time.time()
        while link.status != RNS.Link.ACTIVE:
            time.sleep(0.2)
            elapsed = time.time() - start
            if elapsed > 30:
                print(f"Link failed. status={link.status} reason={link.teardown_reason}")
                break
            if link.status == RNS.Link.CLOSED:
                print(f"Link closed. reason={link.teardown_reason}")
                break
        else:
            print(f"Link ACTIVE! ({elapsed:.1f}s)")
            link.teardown()
        break
else:
    print("\nNot found after 3 minutes")
