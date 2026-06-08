"""Minimal link test: connect to VPS via direct TCP."""
import asyncio, RNS, time

RNS.Reticulum()
time.sleep(2)  # wait for interfaces to come up

async def test():
    dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
    known = dest in list(RNS.Transport.path_table.keys())
    hops = RNS.Transport.hops_to(dest)
    print(f"VPS dest in path table: {known}")
    print(f"Hops: {hops}")

    if hops >= 128:
        print("No path — waiting...")
        for i in range(24):
            await asyncio.sleep(5)
            known = dest in list(RNS.Transport.path_table.keys())
            if known:
                hops = RNS.Transport.hops_to(dest)
                print(f"Path found after {i*5}s! hops={hops}")
                break

    # Now try to establish a link
    id = RNS.Identity()
    out_dest = RNS.Destination(
        id, RNS.Destination.OUT, RNS.Destination.SINGLE,
        "icn", "default",
    )
    out_dest.hash = dest
    out_dest.hexhash = "d005497acc72fb103257d20e0a2314db"

    print("Creating link...")
    link = RNS.Link(out_dest)
    start = time.time()
    while link.status != RNS.Link.ACTIVE:
        await asyncio.sleep(0.2)
        elapsed = time.time() - start
        if elapsed > 30:
            print(f"Link failed after 30s. status={link.status} reason={link.teardown_reason}")
            break
        if link.status == RNS.Link.CLOSED:
            print(f"Link closed. reason={link.teardown_reason}")
            break
    else:
        print(f"Link ACTIVE! ({elapsed:.1f}s)")
        link.teardown()

asyncio.run(test())
