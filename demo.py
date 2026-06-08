#!/usr/bin/env python3
"""ICN Demo — local in-process server-to-server content exchange over TestFace.

No RNS required. Two ICNServers communicate via TestFace pairs
to demonstrate the full Interest/Data/manifest pipeline.
"""

import asyncio
import time

from rns_icn import (
    ICNServer,
    Name,
    Interest,
    Data,
    Manifest,
    ManifestEntry,
    EntryKind,
    test_face_pair,
)


async def main():
    print("╔══════════════════════════════════════════════╗")
    print("║        rns-icn — Local Pipeline Demo         ║")
    print("╚══════════════════════════════════════════════╝\n")

    # Two servers with random identities
    import RNS
    id_producer = RNS.Identity()
    id_consumer = RNS.Identity()

    producer_hash = id_producer.hash  # 16 bytes
    consumer_hash = id_consumer.hash

    print(f"Producer: {producer_hash.hex()}")
    print(f"Consumer: {consumer_hash.hex()}\n")

    # ── Producer side ──
    producer = ICNServer(producer_hash)

    # Publish hello content
    hello_name = Name(producer_hash, [b"hello"])
    hello_content = b"Hello from rns-icn!\nThis content was fetched by name, not location."
    producer.cs.insert(hello_name, Data.new(name=hello_name, content=hello_content))

    # Publish manifest
    manifest = Manifest.create(
        producer=producer_hash,
        entries=[
            ManifestEntry(
                kind=EntryKind.BLOB,
                label="hello",
                name=hello_name,
            ),
        ],
    )
    manifest_data = Data.new(
        name=manifest.manifest_name(),
        content=manifest.to_json(),
    )
    manifest_data.with_sequence(manifest.sequence)
    producer.cs.insert(manifest_data.name, manifest_data)

    print(f"[Producer] Published 'hello' ({len(hello_content)} bytes)")
    print(f"[Producer] Published manifest v{manifest.sequence}\n")

    # ── Consumer side ──
    consumer = ICNServer(consumer_hash)

    # Wire them together with TestFace
    face_a, face_b = test_face_pair()
    consumer.forwarder.register_face(face_a)
    consumer.forwarder.add_route(Name(producer_hash), face_a.id(), 10)

    # Spawn producer handler
    async def producer_handler():
        while True:
            raw = await face_b.recv_raw()
            if raw is not None and raw:
                ptype = raw[0]
                if ptype == 0x01:  # Interest
                    interest = Interest.from_bytes(raw)
                    data = producer.cs.get(interest.name)
                    if data:
                        await face_b.send_data(data)
            await asyncio.sleep(0.01)

    asyncio.create_task(producer_handler())
    await asyncio.sleep(0.05)

    # ── Fetch manifest ──
    manifest_interest = (
        Interest(name=manifest.manifest_name())
        .with_can_be_prefix()
        .with_lifetime(5000)
    )

    print(f"[Consumer] Express Interest: {manifest.manifest_name()}")
    result = await consumer.forwarder.express(manifest_interest, 0)

    if result:
        recv_manifest = Manifest.from_data(result)
        print(f"[Consumer] ✓ Got manifest v{recv_manifest.sequence}")
        for entry in recv_manifest.entries:
            print(f"  - {entry.label} ({entry.kind.value}) → {entry.name}")

            # Fetch each content
            content_interest = Interest(name=entry.name).with_lifetime(5000)
            content_result = await consumer.forwarder.express(content_interest, 0)
            if content_result:
                print(f"  ✓ Received '{entry.label}' ({len(content_result.content)} bytes)")
                text = content_result.content.decode("utf-8", errors="replace")
                for line in text.split("\n"):
                    print(f"    {line}")
            else:
                print(f"  ✗ Timeout fetching '{entry.label}'")
    else:
        print("[Consumer] ✗ Manifest not found")

    print("\n╔══════════════════════════════════════════════╗")
    print("║              Pipeline Complete               ║")
    print("╚══════════════════════════════════════════════╝")


if __name__ == "__main__":
    asyncio.run(main())
