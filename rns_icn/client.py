"""ICNClient — ICN consumer client with config-driven setup and link reuse."""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import RNS

from . import access, discovery
from .config import ClientConfig
from .face import LinkFace
from .forwarder import Forwarder
from .icn_logging import setup_logging
from .link_pool import LinkPool
from .manifest import Manifest, ManifestEntry
from .metrics import metrics
from .name import Name
from .packet import ChildSelector, Data, Interest, InterestSelector


class ICNClient:
    """ICN Consumer client: expresses Interests, fetches Data over RNS mesh.

    Manages RNS initialization, identity, Forwarder, and LinkPool.
    Use as async context manager for proper lifecycle.
    """

    def __init__(self, config: ClientConfig):
        self.config = config
        self._started_rns = False
        self._identity: RNS.Identity | None = None
        self._link_pool: LinkPool | None = None
        self._forwarder: Forwarder | None = None
        self._face_counter = 1000
        # name → highest authenticated (signed_at, sequence) accepted so far,
        # used for rollback detection when config.reject_rollback is set.
        self._seen_signed_key: dict[bytes, tuple[int, int]] = {}
        # producer addr → capabilities (rns_icn.access) granting this client read
        # access to restricted prefixes, loaded from config.capabilities.
        self._capability_store: dict[bytes, list[access.Capability]] = {}

    async def __aenter__(self) -> ICNClient:
        return await self.start()

    async def __aexit__(self, *exc) -> None:
        await self.shutdown()

    async def start(self) -> ICNClient:
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

        self._load_capabilities()

        return self

    def _load_capabilities(self) -> None:
        """Load configured capability files, keyed by producer address.

        A malformed file is skipped with a warning rather than failing startup.
        Signatures are checked lazily at decryption time against the producer's
        authorized key.
        """
        for path in self.config.capabilities:
            try:
                cap = access.load_capability(path)
                self._capability_store.setdefault(cap.producer, []).append(cap)
            except Exception as e:
                RNS.log(f"ICN: skipping invalid capability {path}: {e}", RNS.LOG_WARNING)

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
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> Data | None:
        """Express Interest to peer, wait for Data with retry/backoff.

        Verified Data is returned only after its signature and rollback checks
        pass; encrypted content is decrypted in place when a capability is held.
        """
        return await self._fetch_verified(
            name, peer_hash, timeout=timeout, max_retries=max_retries
        )

    async def _fetch_verified(
        self,
        name: Name,
        peer_hash: bytes,
        *,
        timeout: float | None = None,
        max_retries: int | None = None,
        must_be_fresh: bool = False,
        can_be_prefix: bool = False,
        selector: InterestSelector | None = None,
        require_signature: bool | None = None,
        raise_on_failure: bool = True,
    ) -> Data | None:
        """Express an Interest with retry/backoff and run the verify pipeline.

        Shared by ``fetch`` (exact, raising) and ``fetch_latest`` (discovery,
        non-raising fallbacks). ``require_signature`` overrides the config
        default for this call; ``raise_on_failure`` controls whether exhausted
        retries raise the last error or return None.
        """
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
                interest.must_be_fresh = must_be_fresh
                interest.can_be_prefix = can_be_prefix
                interest.selector = selector

                result = await asyncio.wait_for(
                    self._forwarder.express(interest, face_id),
                    timeout=timeout,
                )

                if result and result.verify_content_hash():
                    sig_ok, sig_err = self._check_signature(
                        result, require_signature=require_signature
                    )
                    if not sig_ok:
                        last_error = sig_err
                        continue
                    rb_ok, rb_err = self._check_rollback(result)
                    if not rb_ok:
                        last_error = rb_err
                        continue
                    result = self._maybe_decrypt(result)
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
        if last_error and raise_on_failure:
            raise last_error
        return None

    async def fetch_latest(
        self,
        prefix: Name,
        peer_hash: bytes,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> Data | None:
        """Fetch the authenticated latest version under ``prefix``.

        Discovery-first: fetch the producer-signed latest-version pointer
        (rns_icn.discovery) with ``must_be_fresh`` so a stale cache revalidates
        it to the origin, verify its signature, then fetch the exact
        content-hash-pinned target it names — making "latest" a producer
        assertion rather than a cache's ranking, and engaging rollback
        protection on the pointer.

        Falls back to the best-effort ``LATEST`` child selector (whatever the
        answering node holds, unverifiable ranking) when no pointer is
        available — e.g. a producer that never published one. Returns None if
        nothing satisfies either path.
        """
        meta = await self._fetch_verified(
            discovery.meta_name(prefix),
            peer_hash,
            timeout=timeout,
            max_retries=max_retries,
            must_be_fresh=True,
            require_signature=True,
            raise_on_failure=False,
        )
        if meta is not None:
            try:
                target = discovery.decode_meta(meta.content)
            except discovery.DiscoveryError as e:
                RNS.log(f"ICN: malformed latest pointer for {prefix}: {e}", RNS.LOG_WARNING)
                target = None
            if target is not None:
                if target.rns_addr != prefix.rns_addr or not target.starts_with(prefix):
                    RNS.log(
                        f"ICN: latest pointer {target} escapes {prefix}; ignoring",
                        RNS.LOG_WARNING,
                    )
                else:
                    return await self.fetch(target, peer_hash, timeout, max_retries)

        # Best-effort fallback: ask any node for its newest version. No
        # must_be_fresh — when the origin is unreachable a cached latest is the
        # graceful degradation; the ranking is unverifiable (see PROTOCOL.md §7).
        return await self._fetch_verified(
            prefix,
            peer_hash,
            timeout=timeout,
            max_retries=max_retries,
            can_be_prefix=True,
            selector=InterestSelector(child=ChildSelector.LATEST),
            raise_on_failure=False,
        )

    def _check_signature(
        self, data: Data, require_signature: bool | None = None
    ) -> tuple[bool, Exception | None]:
        """Verify a Data packet's producer signature per policy.

        Trust anchor is the producer's RNS identity, recalled from the name's
        producer address (``name.rns_addr``). A present-but-invalid signature
        is always rejected. A missing signature, or one whose producer key we
        can't recall, is rejected only when ``require`` is set. ``require``
        defaults to ``config.require_signature`` but callers (e.g. a latest
        pointer fetch) may force it on for a single call.
        """
        require = (
            self.config.require_signature
            if require_signature is None
            else require_signature
        )
        if data.signature is not None:
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
            if require:
                return False, ValueError(
                    "producer identity unknown; cannot verify signature"
                )
            return True, None
        # Unsigned.
        if require:
            return False, ValueError("Data is unsigned but signature is required")
        return True, None

    def _check_rollback(self, data: Data) -> tuple[bool, Exception | None]:
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

    def _producer_validators(self, producer_addr: bytes):
        """Validator authorized to sign for a producer, or None if unknown.

        The producer address is the identity hash of the (self-certifying) name,
        so the authorized key is recalled directly from the mesh. Returns None
        when the identity is not available (offline / never announced).
        """
        identity = RNS.Identity.recall(producer_addr, from_identity_hash=True)
        if identity is not None:
            return [identity.validate]
        return None

    def _maybe_decrypt(self, data: Data) -> Data:
        """Decrypt restricted Data in place if we hold a capability for it.

        For encrypted Data, looks for a valid capability covering the name,
        verifies the producer's signature on it (when the producer key is
        known), unwraps the CEK with our identity, and returns a plaintext Data.
        If no usable capability is found the ciphertext Data is returned
        unchanged (the ``encrypted`` flag stays set so the caller can tell).

        Note the AEAD content decryption and ECIES CEK unwrap are themselves
        authenticated, so a forged capability fails closed even if its signature
        could not be checked offline.
        """
        if not data.metadata.encrypted:
            return data
        caps = self._capability_store.get(data.name.rns_addr)
        if not caps:
            return data
        now = int(time.time())
        for cap in caps:
            if not cap.covers(data.name, now):
                continue
            validators = self._producer_validators(cap.producer)
            if validators is not None and not any(
                cap.verify_signature(v) for v in validators
            ):
                continue
            try:
                cek = cap.unwrap(self.identity)
                plaintext = access.decrypt_content(data.content, cek)
            except access.AccessError:
                continue
            out = Data.new(name=data.name, content=plaintext)
            out.metadata.sequence = data.metadata.sequence
            out.metadata.freshness = data.metadata.freshness
            out.metadata.freshness_period = data.metadata.freshness_period
            out.metadata.signed_at = data.metadata.signed_at
            return out
        return data

    async def fetch_manifest(
        self,
        producer_addr: bytes,
        timeout: float | None = None,
    ) -> Manifest | None:
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
        timeout: float | None = None,
    ) -> bytes | None:
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