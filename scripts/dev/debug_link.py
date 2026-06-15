"""Debug which interface the VPS path uses."""
import RNS, time, sys
RNS.Reticulum()

db = bytes.fromhex("d005497acc72fb103257d20e0a2314db")

for i in range(36):
    time.sleep(5)
    if db in list(RNS.Transport.path_table.keys()):
        entry = RNS.Transport.path_table[db]
        hops = entry[2] if len(entry) > 2 else "?"
        via_if = entry[0] if len(entry) > 0 else None  # IDX_PT_RVCD_IF
        
        # Find which interface has this as its hash
        for iface in RNS.Transport.interfaces:
            iface_hash = getattr(iface, "hash", None)
            if iface_hash and iface_hash == via_if:
                print(f"FOUND at [{i*5}s]! hops={hops}")
                print(f"  Interface: {getattr(iface, 'name', '?')} ({type(iface).__name__})")
                if hasattr(iface, "parent_interface") and iface.parent_interface:
                    print(f"  Parent: {getattr(iface.parent_interface, 'name', '?')}")
                break
        else:
            print(f"FOUND at [{i*5}s]! hops={hops}")
            print(f"  via_if hash: {via_if.hex() if isinstance(via_if, bytes) else via_if}")
            # List all iface hashes for comparison
            for iface in RNS.Transport.interfaces:
                ih = getattr(iface, "hash", None)
                print(f"  iface '{getattr(iface,'name','?')}' hash={ih.hex() if isinstance(ih,bytes) else ih}")
        
        # Now try to establish link and debug
        id = RNS.Identity()
        dest = RNS.Destination(id, RNS.Destination.OUT, RNS.Destination.SINGLE, "icn", "default")
        dest.hash = db
        dest.hexhash = "d005497acc72fb103257d20e0a2314db"
        
        print("Creating link...")
        link = RNS.Link(dest)
        start = time.time()
        while link.status != RNS.Link.ACTIVE:
            time.sleep(0.2)
            if time.time() - start > 15:
                print(f"Timed out. status={link.status} reason={link.teardown_reason}")
                # Check if the packet was sent at all
                if hasattr(link, 'packet') and link.packet:
                    print(f"  Packet sent: {link.packet.sent}")
                break
            if link.status == RNS.Link.CLOSED:
                print(f"Closed. reason={link.teardown_reason}")
                break
        else:
            print(f"ACTIVE! ({time.time()-start:.1f}s)")
            link.teardown()
        break
    sys.stdout.write(f"\r[{i*5}s] waiting...")
    sys.stdout.flush()
else:
    print("\nNot found after 3min")
