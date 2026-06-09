#!/usr/bin/env python3
"""Check if VPS combined ICN destination is reachable."""
import RNS, time

RNS.Reticulum()
time.sleep(10)

d = bytes.fromhex("a3f67d22a5a72f50cf905bceb9cdaf62")
print("Path:", d in RNS.Transport.path_table)
print("Announce:", d in RNS.Transport.announce_table)
print("Paths:", len(RNS.Transport.path_table))
print("Announces:", len(RNS.Transport.announce_table))

identity = RNS.Identity()
dest = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "icn", "default")
dest.hash = d

link = RNS.Link(dest)
start = time.time()
while link.status == RNS.Link.PENDING and time.time() - start < 60:
    time.sleep(0.5)

if link.status == RNS.Link.ACTIVE:
    print("SUCCESS: LINK ACTIVE!", link.rtt*1000, "ms")
    link.teardown()
else:
    print("FAIL: Link status =", link.status, "time =", time.time()-start)
