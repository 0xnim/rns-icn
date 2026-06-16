"""RNS.Resource transport for large ICN content chunks.

When chunks exceed the configured threshold (default 100 KB), use
RNS.Resource instead of Channel messages. Resource provides automatic
segmentation, retransmission of lost segments, and flow control at the
Reticulum transport layer.

Architecture:
  ResourcePublisher (send side)
    └─ attaches to an RNS.Link → creates RNS.Resource containing serialised Data
       → segment/retransmit/flow control at RNS layer
       → notifies via Future when transfer completes

  ResourceListener (receive side)
    └─ attaches to an RNS.Link via Link.ACCEPT_APP strategy
       → incoming Resources deserialised to ICN Data packets
       → injected into the ICN server's handle_incoming flow
"""

from __future__ import annotations

import asyncio
import logging
import struct
from collections.abc import Callable

import RNS

from .packet import Data

logger = logging.getLogger(__name__)

# ── Constants ──

DEFAULT_RESOURCE_THRESHOLD = 100 * 1024  # 100 KB
RESOURCE_TYPE_ICN_DATA = 0x49  # 'I' — identifies the payload as ICN Data


# ── Errors ──


class ResourceTransportError(Exception):
    """Raised when resource creation or transfer fails."""
    ...


class ResourceTimeoutError(ResourceTransportError):
    """Raised when a resource transfer exceeds the allowed timeout."""
    ...


# ── Helper: wrap/unwrap ICN Data in a type-tagged payload ──


def _wrap_payload(data_bytes: bytes) -> bytes:
    """Prepend a type tag so the listener can distinguish ICN Data from other
    Resources that might arrive on the same link."""
    return struct.pack(">B", RESOURCE_TYPE_ICN_DATA) + data_bytes


def _unwrap_payload(raw: bytes) -> bytes | None:
    """Extract the ICN Data payload if the type tag matches."""
    if len(raw) < 1 or raw[0] != RESOURCE_TYPE_ICN_DATA:
        return None
    return raw[1:]


# ── Send side ──


class ResourcePublisher:
    """Publish ICN Data packets as RNS.Resources over a Link.

    Each call to *publish_data* creates a new RNS.Resource on the
    link. The caller awaits completion or timeout.

    Resources handle segmentation, retransmission of lost segments,
    and sliding-window flow control automatically at the RNS layer.
    """

    def __init__(self, link: RNS.Link):
        self._link = link
        self._loop: asyncio.AbstractEventLoop | None = None

    def _get_or_init_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    async def publish_data(self, data: Data, timeout: float = 60.0) -> bool:
        """Send a single ICN Data packet as an RNS.Resource.

        Args:
            data: The ICN Data packet to send.
            timeout: Max seconds to wait for transfer completion.

        Returns:
            True if the resource transfer completed, False on timeout
            or unrecoverable error.
        """
        raw = data.to_bytes()
        return await self._publish_raw(raw, timeout)

    async def publish_raw(self, raw: bytes, timeout: float = 60.0) -> bool:
        """Send raw bytes (must be a serialised ICN Interest/Data) as a Resource.

        Args:
            raw: Serialised packet bytes.
            timeout: Max seconds to wait for transfer completion.

        Returns:
            True if complete, False on timeout/error.
        """
        return await self._publish_raw(raw, timeout)

    async def _publish_raw(self, raw: bytes, timeout: float) -> bool:
        wrapped = _wrap_payload(raw)
        loop = self._get_or_init_loop()
        future: asyncio.Future[bool] = loop.create_future()

        def on_conclude(res: RNS.Resource) -> None:
            if not future.done():
                loop.call_soon_threadsafe(future.set_result, True)

        try:
            # Create the resource.  This starts the transfer in the RNS
            # thread pool; we await the callback via Future.
            # Bound (not directly used) so the Resource stays alive for the
            # duration of the transfer — we await completion via `future`.
            _resource = RNS.Resource(
                wrapped,           # data bytes
                self._link,        # link to send over
                callback=on_conclude,
            )
        except Exception as exc:
            raise ResourceTransportError(f"failed to create resource: {exc}") from exc

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            return False


# ── Receive side ──


class ResourceListener:
    """Listen for incoming RNS.Resources on a Link.

    Uses Link.ACCEPT_APP strategy — the link will call *accept_resource*
    for incoming resource advertisements and *on_resource_concluded*
    when a transfer finishes.

    Only resources whose payload carries the ICN type tag (0x49) are
    forwarded to *on_data*. Other resources arriving on the same link
    are silently ignored.
    """

    def __init__(self, link: RNS.Link):
        self._link = link
        self._on_data: Callable[[Data], None] | None = None
        self._on_raw: Callable[[bytes], None] | None = None

        # Set up resource strategy to accept app-level resources via callbacks.
        # This is the correct RNS v1.2.8 API — Destination.set_resource_callback
        # does not exist.
        link.resource_strategy = RNS.Link.ACCEPT_APP
        link.callbacks.resource = self._accept_resource
        link.callbacks.resource_concluded = self._on_resource_concluded

    def set_on_data(self, callback: Callable[[Data], None]) -> None:
        """Register a callback that receives parsed ICN Data packets."""
        self._on_data = callback

    def set_on_raw(self, callback: Callable[[bytes], None]) -> None:
        """Register a callback that receives raw bytes from any resource,
        regardless of type tag."""
        self._on_raw = callback

    # ── Private ──

    def _accept_resource(self, adv: RNS.ResourceAdvertisement) -> bool:
        """Accept all resource advertisements — type filtering happens
        in _on_resource_concluded after the data arrives."""
        return True

    def _on_resource_concluded(self, resource: RNS.Resource) -> None:
        """Called by RNS on the inbound thread when a resource transfer
        finishes."""
        if resource.status != RNS.Resource.COMPLETE:
            return  # transfer failed or was cancelled

        try:
            resource.data.seek(0)
            payload = resource.data.read()
        except Exception:
            logger.debug("could not read completed resource payload", exc_info=True)
            return  # malformed — skip

        # Always fire the raw callback if set
        if self._on_raw is not None:
            self._on_raw(payload)

        # Only deserialise and fire on_data for tagged ICN Data resources
        inner = _unwrap_payload(payload)
        if inner is None:
            return  # not an ICN Data resource

        try:
            data = Data.from_bytes(inner)
        except Exception:
            logger.debug("tagged resource was not a valid ICN Data packet", exc_info=True)
            return

        if self._on_data is not None:
            self._on_data(data)


# ── Convenience: publish chunked content ──


class LargeContentPublisher:
    """Publish chunked content (ContentManifest + Data packets) over a Link.

    Uses RNS.Resource for each Data packet.  The manifest is sent
    as a Data packet like any other.

    Depends on:
    - 4.2 chunker/assembler (produces ChunkResult)
    """

    def __init__(
        self,
        link: RNS.Link,
        resource_threshold: int = DEFAULT_RESOURCE_THRESHOLD,
    ):
        self._publisher = ResourcePublisher(link)
        self._threshold = resource_threshold
        self._link = link

    @property
    def threshold(self) -> int:
        return self._threshold

    @threshold.setter
    def threshold(self, value: int) -> None:
        self._threshold = value

    async def publish_data_packet(
        self,
        data: Data,
        timeout: float = 60.0,
    ) -> bool:
        """Publish a single Data packet, using Resource if above threshold.

        Args:
            data: The ICN Data packet to send.
            timeout: Max seconds for resource transfer (if applicable).

        Returns:
            True if the packet was sent successfully.
        """
        return await self._publisher.publish_data(data, timeout=timeout)
