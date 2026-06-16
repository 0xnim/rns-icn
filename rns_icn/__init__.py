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

from .aps import APSManager
from .assembler import (
    AssemblyError,
    HashMismatchError,
    IntegrityError,
    MissingChunkError,
    assemble,
    assemble_fast,
    assemble_verified,
    missing_labels,
    verify_chunk,
    verify_chunks,
)
from .chunker import (
    DEFAULT_CHUNK_SIZE,
    ChunkerError,
    ChunkResult,
    EmptyContentError,
    chunk_content,
)
from .content_store import ContentStore
from .face import Face, FaceCapabilities, FaceId, LinkFace, TestFace, test_face_pair
from .fib import Fib, FibEntry
from .forwarder import Forwarder
from .manifest import (
    ChunkRef,
    ContentManifest,
    ContentManifestError,
    EntryKind,
    Manifest,
    ManifestEntry,
)
from .name import MAX_COMPONENTS, Name, NameError
from .offline_queue import OfflineQueue
from .packet import (
    DEFAULT_HOP_LIMIT,
    FEATURE_APS,
    FEATURE_CHUNKED,
    FEATURE_MANIFEST,
    FEATURE_OFFLINE_QUEUE,
    FEATURE_PROPAGATION,
    APSubscribe,
    CapPeer,
    Data,
    DataError,
    DataMetadata,
    Freshness,
    Interest,
    InterestError,
    Packet,
    PacketType,
    PropPeer,
    SubscribeError,
    parse_packet,
)
from .peer_discovery import PeerDiscoveryManager, PeerInfo
from .pit import Pit, PitEntry, PitOp
from .propagation import PropagationError, PropagationManager
from .resource_transport import (
    DEFAULT_RESOURCE_THRESHOLD,
    LargeContentPublisher,
    ResourceListener,
    ResourcePublisher,
    ResourceTimeoutError,
    ResourceTransportError,
)
from .rns_server import ICNServer as RNSICNServer
from .rns_utils import (
    default_identity_path,
    load_or_create_identity,
)
from .server import ICNServer, ServerRole
from .strategy import BestRoute, Strategy, StrategyDecision

__all__ = [
    "Name", "NameError", "MAX_COMPONENTS",
    "OfflineQueue",
    "APSubscribe", "SubscribeError",
    "PropPeer",
    "CapPeer",
    "FEATURE_APS", "FEATURE_PROPAGATION", "FEATURE_OFFLINE_QUEUE", "FEATURE_MANIFEST", "FEATURE_CHUNKED",
    "PacketType", "Interest", "InterestError", "DEFAULT_HOP_LIMIT",
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
