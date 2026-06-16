"""ICNClient — ICN consumer client with config-driven setup and link reuse."""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Optional

import RNS

from . import rotation
from .config import ClientConfig
from .face import LinkFace
from .forwarder import Forwarder
from .icn_logging import setup_logging
from .link_pool import LinkPool
from .manifest import Manifest, ManifestEntry
from .metrics import metrics
from .name import Name
from .packet import Data, Interest


class ICNClient:
    """ICN Consumer client: expresses Interests, fetches Data over RNS mesh.

    Manages RNS initialization, identity, Forwarder, and LinkPool.
    Use as async context manager for proper lifecycle.
    """

    def __init__(self, config: ClientConfig):
        self.config = config
        self._started_rns = False
        self._identity: Optional[RNS.Identity] = None
        self._link_pool: Optional[LinkPool] = None
        self._forwarder: Optional[Forwarder] = None
        self._face_counter = 1000
        # name → highest authenticated (signed_at, sequence) accepted so far,
        # used for rollback detection when config.reject_rollback is set.
        self._seen_signed_key: dict[bytes, tuple[int, int]] = {}
        # producer addr → its key-rotation chain (rns_icn.rotation), loaded from
        # config.rotation_chains at start(); empty means "no rotation known".
        self._rotation_store: dict[bytes, list[rotation.KeyRotation]] = {}

    async def __aenter__(self) -> "ICNClient":
        return await self.start()

    async def __aexit__(self, *exc) -> None:
        await self.shutdown()

    async def start(self) -> "ICNClient":
        """Initialize RNS, identity, forwarder, and link pool."""
        # Setup logging first
        setup_logging(self.config)

        # Initialize RNS only if not already running (it's a process-global
        # singleton; calling RNS.Reticulum() when one exists raises).
        if RNS.Reticulum.get_instance() is None:
            RNS.Reticulum()
            self._started_rns = True
        else:
            self._started_rns = False

        # Load or create identity
        if self.config.identity_path:
            from .rns_utils import load_or_create_identity
            path = Path(self.config.identity_path).expanduser()
            self._identity = load_or_create_identity(str(path))
        else:
            self._identity = RNS.Identity()

        # Create local forwarder (no destination — we're a consumer)
        self._forwarder = Forwarder(cs_max=1000)

        # Create link pool for outbound connections
        self._link_pool = LinkPool(
            identity=self._identity,
            app_name="icn",
            aspect="default",
            known_peers=self.config.known_peers,
        )
        await self._link_pool.start()

        self._load_rotation_chains()

        return self

    def _load_rotation_chains(self) -> None:
        """Load and validate configured key-rotation chains, keyed by producer.

        Each file's anchor (the first link's signer key) determines the producer
        address it applies to. A malformed or unverifiable chain is skipped with
        a warning rather than failing startup.
        """
        for path in self.config.rotation_chains:
            try:
                certs = rotation.load_rotation_chain(path)
                if not certs:
                    continue
                producer = rotation.addr_of_public_key(certs[0].prev_public_key)
                # Validate up front so a bad chain never reaches verification.
                rotation.verify_rotation_chain(producer, certs)
                self._rotation_store[producer] = certs
            except Exception as e:
                RNS.log(f"ICN: skipping invalid rotation chain {path}: {e}", RNS.LOG_WARNING)

    async def shutdown(self) -> None:
        """Gracefully stop link pool and RNS."""
        if self._link_pool:
            await self._link_pool.stop()
        if self._started_rns and RNS.Reticulum.get_instance() is not None:
            RNS.Reticulum.exit_handler()

    async def fetch(
        self,
        name: Name,
        peer_hash: bytes,
        timeout: Optional[float] = None,
        max_retries: Optional[int] = None,
    ) -> Optional[Data]:
        """Express Interest to peer, wait for Data with retry/backoff."""
        if self._link_pool is None or self._forwarder is None:
            raise RuntimeError("Client not started. Call start() or use as context manager.")
        timeout = timeout or self.config.fetch_timeout
        max_retries = max_retries if max_retries is not None else self.config.max_retries
        base_delay = self.config.base_retry_delay
        max_delay = self.config.max_retry_delay

        last_error = None
        fetch_start = time.time()
        for attempt in range(max_retries + 1):
            try:
                link = await self._link_pool.get_link(peer_hash)
                if not link:
                    raise RuntimeError(f"Failed to establish link to {peer_hash.hex()}")

                face_id = self._get_or_create_face_id(link)

                # Express interest with lifetime in milliseconds
                # Add unique nonce for duplicate detection
                interest = Interest(name=name)
                interest.with_lifetime(int(timeout * 1000))
                interest.nonce = os.urandom(8)

                result = await asyncio.wait_for(
                    self._forwarder.express(interest, face_id),
                    timeout=timeout,
                )

                if result and result.verify_content_hash():
                    sig_ok, sig_err = self._check_signature(result)
                    if not sig_ok:
                        last_error = sig_err
                        continue
                    rb_ok, rb_err = self._check_rollback(result)
                    if not rb_ok:
                        last_error = rb_err
                        continue
                    # Record successful fetch latency
                    fetch_latency = time.time() - fetch_start
                    metrics.record_fetch(fetch_latency, success=True)
                    return result
                elif result:
                    # Hash mismatch - treat as failure and retry
                    last_error = ValueError("Data content hash verification failed")
                    continue

            except asyncio.TimeoutError:
                last_error = TimeoutError(f"Fetch timed out after {timeout}s")
            except Exception as e:
                last_error = e

            # Exponential backoff before retry
            if attempt < max_retries:
                delay = min(base_delay * (2 ** attempt), max_delay)
                await asyncio.sleep(delay)

        # All retries exhausted
        fetch_latency = time.time() - fetch_start
        metrics.record_fetch(fetch_latency, success=False)
        if last_error:
            raise last_error
        return None

    def _check_signature(self, data: Data) -> tuple[bool, Optional[Exception]]:
        """Verify a Data packet's producer signature per policy.

        Trust anchor is the producer's RNS identity, recalled from the name's
        producer address (``name.rns_addr``). A present-but-invalid signature
        is always rejected. A missing signature, or one whose producer key we
        can't recall, is rejected only when ``require_signature`` is set.

        When a key-rotation chain is known for the producer, the Data is
        accepted if it verifies against any key the chain authorizes (anchor or
        a delegated key); the chain is self-certifying, so no recall is needed.
        """
        if data.signature is not None:
            # If we hold a rotation chain for this producer, the set of valid
            # signing keys is the chain's authorized keys (self-certifying).
            certs = self._rotation_store.get(data.name.rns_addr)
            if certs:
                try:
                    validators = rotation.authorized_validators(
                        data.name.rns_addr, certs
                    )
                except rotation.RotationError as e:
                    return False, ValueError(f"invalid rotation chain: {e}")
                if any(data.verify_signature(v) for v in validators):
                    return True, None
                return False, ValueError(
                    "Data signature not authorized by producer's rotation chain"
                )
            # The producer address in a name is the producer's *identity* hash
            # (not a destination hash), so recall accordingly.
            identity = RNS.Identity.recall(
                data.name.rns_addr, from_identity_hash=True
            )
            if identity is not None:
                if data.verify_signature(identity.validate):
                    return True, None
                return False, ValueError("Data signature verification failed")
            # Signed, but we don't have the producer's key to verify.
            if self.config.require_signature:
                return False, ValueError(
                    "producer identity unknown; cannot verify signature"
                )
            return True, None
        # Unsigned.
        if self.config.require_signature:
            return False, ValueError("Data is unsigned but signature is required")
        return True, None

    def _check_rollback(self, data: Data) -> tuple[bool, Optional[Exception]]:
        """Reject signed Data that rolls back to an older authenticated version.

        Tracks the highest authenticated ``(signed_at, sequence)`` accepted per
        name; a later fetch carrying an older key for the same name is a relay
        or cache replaying a stale-but-validly-signed version. No-op unless
        ``config.reject_rollback`` is set, and only acts on signed Data (an
        unsigned key is not trustworthy). Must run after signature verification.
        """
        if not self.config.reject_rollback:
            return True, None
        key = data.freshness_key()
        if key is None:
            return True, None
        name_key = data.name.to_bytes()
        previous = self._seen_signed_key.get(name_key)
        if previous is not None and key < previous:
            return False, ValueError(
                "Data rolls back to an older signed version "
                f"({key} < {previous})"
            )
        if previous is None or key > previous:
            self._seen_signed_key[name_key] = key
        return True, None

    async def fetch_manifest(
        self,
        producer_addr: bytes,
        timeout: Optional[float] = None,
    ) -> Optional[Manifest]:
        """Fetch and parse manifest from producer."""
        manifest_name = Name(producer_addr, [b"manifest"])
        data = await self.fetch(manifest_name, producer_addr, timeout)
        if data:
            return Manifest.from_data(data)
        return None

    async def fetch_content(
        self,
        entry: ManifestEntry,
        producer_addr: bytes,
        timeout: Optional[float] = None,
    ) -> Optional[bytes]:
        """Fetch content by manifest entry."""
        data = await self.fetch(entry.name, producer_addr, timeout)
        return data.content if data else None

    def _get_or_create_face_id(self, link: RNS.Link) -> int:
        """Get existing face ID for link or create new one."""
        if not self._forwarder:
            raise RuntimeError("Forwarder not initialized. Call start() first.")

        # Check if link already has a registered face
        for face in self._forwarder.faces.values():
            if hasattr(face, "_link") and face._link is link:
                return face.id()

        # Create new face
        face_id = self._face_counter
        self._face_counter += 1

        link_face = LinkFace(face_id, link, loop=asyncio.get_event_loop())
        self._forwarder.register_face(link_face)
        return face_id

    @property
    def identity(self) -> RNS.Identity:
        if not self._identity:
            raise RuntimeError("Client not started. Call start() or use as context manager.")
        return self._identity

    @property
    def forwarder(self) -> Forwarder:
        if not self._forwarder:
            raise RuntimeError("Client not started. Call start() or use as context manager.")
        return self._forwarder

    @property
    def link_pool(self) -> LinkPool:
        if not self._link_pool:
            raise RuntimeError("Client not started. Call start() or use as context manager.")
        return self._link_pool