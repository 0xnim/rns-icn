import asyncio
import os
import sys
import tempfile

# Ensure local rns_icn is in path
sys.path.insert(0, os.path.dirname(__file__))

from rns_icn.config import load_server_config, ClientConfig
from rns_icn.rns_server import ICNServer
from rns_icn.client import ICNClient
from rns_icn.name import Name
from rns_icn.packet import Data
from rns_icn.manifest import Manifest, ManifestEntry, EntryKind


async def run_integration_test():
    print("=" * 60)
    print("ICN Phase 1 Integration Test")
    print("=" * 60)

    # Load server config
    config = load_server_config("/opt/rns-icn/icn.toml")
    print(f"\n[1/5] Starting ICN Server...")
    print(f"      Identity: {config.identity_path}")

    server = ICNServer(config)
    await server.start()
    print(f"      Running at {server.hexhash}")

    # Give RNS time to establish mesh paths
    print("\n[2/5] Waiting for mesh connectivity...")
    await asyncio.sleep(5)

    # Publish test content
    print("\n[3/5] Publishing test content...")
    producer_addr = server.rns_addr

    # Content 1: Simple blob
    blob_name = Name(producer_addr, [b"test", b"hello"])
    blob_content = b"Hello from ICN VPS server! This is a test blob content."
    server.publish_content(blob_name, blob_content)
    print(f"      Published: {blob_name} ({len(blob_content)} bytes)")

    # Content 2: Another blob
    blob2_name = Name(producer_addr, [b"test", b"data"])
    blob2_content = b"Second test blob with different content for verification."
    server.publish_content(blob2_name, blob2_content)
    print(f"      Published: {blob2_name} ({len(blob2_content)} bytes)")

    # Content 3: Stream (sequence)
    for i in range(3):
        seq_name = Name(producer_addr, [b"stream", b"counter"])
        seq_content = f"Stream item #{i+1}".encode()
        server.publish_content(seq_name, seq_content, sequence=i)
        await asyncio.sleep(0.1)
    print(f"      Published: stream/counter (3 sequenced items)")

    # Build and publish manifest
    print("\n[4/5] Building manifest...")
    server.publish_manifest()
    manifest_name = Name(producer_addr, [b"manifest"])
    print(f"      Manifest: {manifest_name}")

    # Verify manifest in ContentStore
    manifest_data = server.forwarder.cs.get(manifest_name)
    if manifest_data:
        manifest = Manifest.from_data(manifest_data)
        print(f"      Manifest entries: {len(manifest.entries)}")
        for entry in manifest.entries:
            print(f"        - {entry.kind.value}: {entry.label} ({entry.name})")

    # Give mesh time to propagate
    print("\n[5/5] Testing client fetch...")
    await asyncio.sleep(3)

    # Now test ICNClient fetch in separate process
    print("      Starting client fetch test...")

    client_config = ClientConfig(
        identity_path="/tmp/test_client_identity",
        mesh_interfaces=["UTN Oregon"],
        known_peers=config.known_peers,
        connect_timeout=60.0,
        fetch_timeout=30.0,
        log_level="INFO",
    )

    async with ICNClient(client_config) as client:
        print(f"      Client identity: {client.identity.hexhash}")

        # Fetch manifest
        print(f"      Fetching manifest from {producer_addr.hex()}...")
        manifest = await client.fetch_manifest(producer_addr, timeout=30.0)

        if manifest:
            print(f"      ✓ Manifest received! Sequence: {manifest.sequence}")
            print(f"      Entries:")
            for entry in manifest.entries:
                print(f"        - {entry.kind.value}: {entry.label}")

            # Fetch each blob
            for entry in manifest.entries:
                if entry.kind in (EntryKind.BLOB, EntryKind.STREAM):
                    print(f"      Fetching {entry.label}...")
                    content = await client.fetch_content(entry, producer_addr, timeout=10.0)
                    if content:
                        print(f"        ✓ Received {len(content)} bytes: {content[:50]}...")
                    else:
                        print(f"        ✗ Failed to fetch {entry.label}")
            return True
        else:
            print(f"      ✗ Failed to fetch manifest")
            return False


if __name__ == "__main__":
    try:
        result = asyncio.run(run_integration_test())
        print("\n" + "=" * 60)
        if result:
            print("INTEGRATION TEST PASSED!")
        else:
            print("INTEGRATION TEST FAILED!")
        print("=" * 60)
    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()