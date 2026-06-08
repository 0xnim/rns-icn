"""rns-icn — Information-Centric Networking for reticulum-py.

Content-addressed named data over the Reticulum mesh. Self-certifying names
encode the producer's RNS identity. Manifests are the discovery mechanism:
know a producer → fetch manifest → discover content → fetch content.

## Architecture

```
ICNServer
├── Forwarder (FIB/PIT/CS/Strategy)
│   ├── Name       — /<rns-addr:16>/<path>[?hash=<32b>]
│   ├── Interest   — "I want this named data"
│   ├── Data       — content + optional signature + metadata
│   ├── Manifest   — producer's content index (JSON, versioned)
│   ├── CS         — LRU cache of Data packets
│   ├── FIB        — prefix → faces, longest-prefix-match
│   ├── PIT        — Interest aggregation, reverse-path
│   ├── Face (ABC) — communication endpoint (TestFace / LinkFace)
│   └── Strategy   — pluggable forwarding decisions
└── Faces (to peers, consumers, producers)
```
"""

from .name import Name, NameError, MAX_COMPONENTS
from .offline_queue import OfflineQueue
from .packet import (
    APSubscribe,
    PacketType,
    PropPeer,
    CapPeer,
    FEATURE_APS,
    FEATURE_PROPAGATION,
    FEATURE_OFFLINE_QUEUE,
    FEATURE_MANIFEST,
    FEATURE_CHUNKED,
    Interest,
    InterestError,
    Data,
    DataError,
    DataMetadata,
    Freshness,
    Packet,
    SubscribeError,
    parse_packet,
)
from .face import Face, FaceId, FaceCapabilities, TestFace, LinkFace, test_face_pair
from .fib import Fib, FibEntry
from .pit import Pit, PitEntry, PitOp
from .content_store import ContentStore
from .strategy import Strategy, StrategyDecision, BestRoute
from .aps import APSManager
# from .propagation import PropagationManager, PropagationError  # TODO: uncomment when propagation module exists
from .forwarder import Forwarder
from .server import ICNServer, ServerRole
from .rns_server import RNSICNServer
from .peer_discovery import PeerDiscoveryManager, PeerInfo
from .rns_utils import (
    load_or_create_identity,
    default_identity_path,
)
from .resource_transport import (
    DEFAULT_RESOURCE_THRESHOLD,
    LargeContentPublisher,
    ResourceListener,
    ResourcePublisher,
    ResourceTransportError,
    ResourceTimeoutError,
)
from .manifest import Manifest, ManifestEntry, EntryKind, ContentManifest, ChunkRef, ContentManifestError
from .chunker import ChunkResult, ChunkerError, EmptyContentError, chunk_content, DEFAULT_CHUNK_SIZE
from .assembler import (
    AssemblyError,
    MissingChunkError,
    HashMismatchError,
    IntegrityError,
    assemble,
    assemble_verified,
    assemble_fast,
    verify_chunk,
    verify_chunks,
    missing_labels,
)

__all__ = [
    "Name", "NameError", "MAX_COMPONENTS",
    "OfflineQueue",
    "APSubscribe", "SubscribeError",
    "PropPeer",
    "CapPeer",
    "FEATURE_APS", "FEATURE_PROPAGATION", "FEATURE_OFFLINE_QUEUE", "FEATURE_MANIFEST", "FEATURE_CHUNKED",
    "PacketType", "Interest", "InterestError",
    "Data", "DataError", "DataMetadata", "Freshness",
    "Packet", "parse_packet",
    "Face", "FaceId", "FaceCapabilities", "TestFace", "LinkFace", "test_face_pair",
    "Fib", "FibEntry",
    "Pit", "PitEntry", "PitOp",
    "ContentStore",
    "Strategy", "StrategyDecision", "BestRoute",
    "APSManager",
    "PropagationManager", "PropagationError",
    "Forwarder",
    "ICNServer",
    "ServerRole",
    "RNSICNServer",
    "PeerDiscoveryManager", "PeerInfo",
    "load_or_create_identity",
    "default_identity_path",
    # Resource transport
    "DEFAULT_RESOURCE_THRESHOLD",
    "LargeContentPublisher",
    "ResourceListener",
    "ResourcePublisher",
    "ResourceTransportError",
    "ResourceTimeoutError",
    "Manifest", "ManifestEntry", "EntryKind",
    "ContentManifest", "ChunkRef", "ContentManifestError",
    # Chunker
    "ChunkResult", "ChunkerError", "EmptyContentError",
    "chunk_content", "DEFAULT_CHUNK_SIZE",
    # Assembler
    "AssemblyError", "MissingChunkError", "HashMismatchError", "IntegrityError",
    "assemble", "assemble_verified", "assemble_fast",
    "verify_chunk", "verify_chunks", "missing_labels",
]
