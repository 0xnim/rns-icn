"""Try direct RNS link to VPS."""
import RNS, asyncio, os

RNS.Reticulum()

async def go():
    identity = RNS.Identity()
    dest = RNS.Destination(
        identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
        "icn", "default",
    )
    dest.hash = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
    dest.hexhash = "d005497acc72fb103257d20e0a2314db"

    print(f"[Client] Identity: {identity.hexhash}")
    print(f"[Client] Dest: {dest.hexhash}")
    print(f"[Client] Interface check:")
    for iface in RNS.Transport.interfaces:
        name = getattr(iface, 'name', '?')
        active = getattr(iface, 'online', '?')
        print(f"  {name} ({type(iface).__name__}) online={active}")

    # Check if there's a path to the VPS
    hops = RNS.Transport.hops_to(dest.hash)
    print(f"\n[Client] Hops to VPS dest: {hops}")

    print(f"\n[Client] Establishing link...")
    link = RNS.Link(dest)

    start = asyncio.get_event_loop().time()
    while link.status != RNS.Link.ACTIVE:
        await asyncio.sleep(0.2)
        if asyncio.get_event_loop().time() - start > 30:
            print("[Client] ✗ Link timed out")
            return

    print("[Client] ✓ Link established!")

    # Now try Interest exchange via Channel
    from rns_icn.face import LinkFace
    loop = asyncio.get_running_loop()
    lf = LinkFace(1, link, loop=loop)

    from rns_icn.name import Name
    from rns_icn.packet import Interest
    vps_addr = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
    name = Name(vps_addr, [b"manifest"])
    interest = Interest(name=name).with_can_be_prefix().with_lifetime(15000)
    print(f"\n[Client] Expressing Interest: {name}")
    result = await lf.express_interest(interest)
    if result:
        print(f"[Client] ✓ Got Data! ({len(result.content)} bytes)")
        print(f"[Client] Content: {result.content[:200]}")
    else:
        print("[Client] ✗ No Data response")
    link.teardown()

asyncio.run(go())
