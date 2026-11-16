"""Face abstraction — communication endpoint for Interest/Data exchange."""

from __future__ import annotations

import asyncio
import logging
import threading
import math
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .packet import Data, Interest, PacketType

if TYPE_CHECKING:
    import RNS

logger = logging.getLogger(__name__)

FaceId = int


@dataclass
class FaceCapabilities:
    disruption_tolerance_ms: int = 5000
    mtu: int = 1500
    is_local: bool = True


class Face(ABC):
    @abstractmethod
    async def express_interest(self, interest: Interest) -> Data | None:
        ...

    @abstractmethod
    async def send_data(self, data: Data) -> None:
        ...

    @abstractmethod
    async def send_raw(self, raw: bytes) -> None:
        """Send raw bytes on this face (for lower-level use)."""
        ...

    @abstractmethod
    def capabilities(self) -> FaceCapabilities:
        ...

    @abstractmethod
    def id(self) -> FaceId:
        ...


class TestFace(Face):
    """In-process face using asyncio queues."""

    # Despite the name, this is production helper infrastructure, not a pytest
    # test case — tell pytest not to try collecting it (it has an __init__).
    __test__ = False

    def __init__(self, face_id: FaceId):
        self._id = face_id
        self._outgoing: asyncio.Queue[bytes] = asyncio.Queue()
        self._incoming: asyncio.Queue[bytes] | None = None

    def connect(self, other: TestFace):
        """Wire two TestFaces together."""
        self._incoming = other._outgoing
        other._incoming = self._outgoing

    async def send_interest(self, interest: Interest) -> None:
        """Inject an Interest from the test side."""
        await self._outgoing.put(interest.to_bytes())

    async def send_data(self, data: Data) -> None:
        """Inject a Data from the test side."""
        await self._outgoing.put(data.to_bytes())

    async def send_raw(self, raw: bytes) -> None:
        await self._outgoing.put(raw)

    async def recv_raw(self) -> bytes | None:
        """Receive raw bytes from the connected peer (blocks briefly)."""
        if self._incoming is None:
            return None
        try:
            return await asyncio.wait_for(self._incoming.get(), timeout=0.01)
        except asyncio.TimeoutError:
            return None

    async def recv_packet(self) -> PacketType | None:
        """Receive and parse a raw packet. Returns the type.
        DEPRECATED: Use recv_raw() instead."""
        raw = await self.recv_raw()
        if raw is None or not raw:
            return None
        return PacketType(raw[0])

    async def express_interest(self, interest: Interest) -> Data | None:
        """Forward Interest to peer, wait for Data."""
        await self._outgoing.put(interest.to_bytes())
        if self._incoming is None:
            return None
        timeout_s = interest.lifetime_ms / 1000.0
        try:
            while True:
                raw = await asyncio.wait_for(self._incoming.get(), timeout=timeout_s)
                ptype = raw[0]
                if ptype == 0x02:  # Data
                    return Data.from_bytes(raw)
                # Ignore Interests that come back
        except asyncio.TimeoutError:
            return None

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(is_local=True)

    def id(self) -> FaceId:
        return self._id


def test_face_pair() -> tuple[TestFace, TestFace]:
    a = TestFace(1)
    b = TestFace(2)
    a.connect(b)
    return a, b


# Not a pytest test despite the test_ prefix — it's a connected-pair factory.
test_face_pair.__test__ = False  # type: ignore[attr-defined]


# ── LinkFace: Face over RNS Link with Channel ──


class LinkFace(Face):
    """Face backed by a RNS Link with reliable Channel transport.

    Uses RNS.Channel + LinkChannelOutlet for reliable in-order delivery
    over encrypted Links instead of raw RNS.Packet sends. The Channel
    provides automatic retry, sequence tracking, and flow-control
    windowing.

    Bridges from RNS's thread-based callback model into asyncio.
    channel_send_timeout bounds Data/raw capacity waits; the default matches
    the default four-second Interest lifetime and can be raised for slow links.
    An expressed Interest always uses its own total lifetime instead.
    """

    def __init__(self, face_id: FaceId, link: RNS.Link, loop: asyncio.AbstractEventLoop | None = None,
                 on_closed: Callable[[FaceId], None] | None = None, *,
                 channel_send_timeout: float = 4.0):
        from RNS.Channel import CEType, ChannelException, MessageBase

        if not math.isfinite(channel_send_timeout) or channel_send_timeout <= 0:
            raise ValueError("channel_send_timeout must be finite and positive")
        self._channel_send_timeout = channel_send_timeout

        class _ICNMessage(MessageBase):
            """Channel message wrapping raw ICN Interest/Data bytes."""
            MSGTYPE = 0x01

            def __init__(self, raw: bytes | None = None):
                self.raw = raw if raw is not None else b""

            def pack(self) -> bytes:
                return self.raw

            def unpack(self, raw: bytes):
                self.raw = raw

        self._id = face_id
        self._link = link
        self._loop = loop or asyncio.get_running_loop()
        self._recv_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._closed = False
        self._close_lock = threading.Lock()
        self._on_closed_hook = on_closed
        self._ICNMessage = _ICNMessage
        self._channel_exc = ChannelException
        self._channel_not_ready = CEType.ME_LINK_NOT_READY

        # Get or create the Channel on this Link
        channel = link.get_channel()
        self._channel = channel
        channel.register_message_type(_ICNMessage)

        # Bridge Channel message callback → asyncio recv queue
        def _on_channel_message(message: MessageBase) -> bool:
            if self._closed:
                return False
            if isinstance(message, _ICNMessage):
                self._loop.call_soon_threadsafe(
                    self._recv_queue.put_nowait, message.raw
                )
            return False  # Don't consume — allow other handlers if any

        channel.add_message_handler(_on_channel_message)

        # Handle link close. RNS allows a single closed-callback per Link, so
        # LinkFace owns it and fans out to an optional owner hook (used by the
        # server to withdraw this face's FIB routes — see ICNServer).
        def _on_closed(link: RNS.Link) -> bool:
            with self._close_lock:
                if self._closed:
                    return False
                self._closed = True
            try:
                self._loop.call_soon_threadsafe(
                    self._recv_queue.put_nowait, b""
                )
            except RuntimeError:
                logger.debug("LinkFace closed after event loop shutdown")
            if self._on_closed_hook is not None:
                try:
                    self._on_closed_hook(self._id)
                except Exception:
                    logger.debug("LinkFace on_closed hook failed", exc_info=True)
            return True

        self._notify_closed = _on_closed
        link.set_link_closed_callback(_on_closed)

    CHANNEL_READY_POLL = 0.01

    async def _send(self, raw: bytes, timeout: float | None = None) -> None:
        """Wait for Channel capacity, bounded by one deadline; never discard bytes."""
        if timeout is None:
            timeout = self._channel_send_timeout
        deadline = self._loop.time() + timeout
        message = self._ICNMessage(raw)
        while True:
            if self._closed:
                raise ConnectionError("cannot send on a closed face")
            remaining = deadline - self._loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError("Channel send capacity wait timed out")
            try:
                # Channel.send atomically tests/reserves its window. Readiness
                # checks alone race with other callers and receipt callbacks.
                self._channel.send(message)
                return
            except self._channel_exc as exc:
                if exc.type != self._channel_not_ready:
                    raise
            # RNS has no public capacity-available callback. Yield to asyncio
            # while its receipt thread opens the window; cancellation propagates.
            await asyncio.sleep(min(self.CHANNEL_READY_POLL, remaining))

    async def express_interest(self, interest: Interest) -> Data | None:
        deadline = self._loop.time() + interest.lifetime_ms / 1000.0
        try:
            await self._send(interest.to_bytes(), timeout=deadline - self._loop.time())
            while True:
                remaining = deadline - self._loop.time()
                if remaining <= 0:
                    return None
                reply = await asyncio.wait_for(self._recv_queue.get(), timeout=remaining)
                if not reply:
                    return None  # link closed
                if reply[0] == 0x02:  # Data type byte
                    return Data.from_bytes(reply)
                # Other packet types do not restart the Interest lifetime.
        except asyncio.TimeoutError:
            return None

    async def send_data(self, data: Data) -> None:
        """Use a Resource when the serialized Data exceeds the Channel MDU."""
        raw = data.to_bytes()
        # Channel.mdu already subtracts the Channel envelope from Link.mdu.
        # Compare the complete packet, including its ICN framing/signature.
        if len(raw) <= self._channel.mdu:
            await self._send(raw)
            return

        from .resource_transport import ResourcePublisher, ResourceTransportError

        if self._closed:
            raise ResourceTransportError("cannot send Data on a closed face")
        if not await ResourcePublisher(self._link).publish_raw(raw):
            raise ResourceTransportError("Data resource transfer did not complete")

    async def send_raw(self, raw: bytes) -> None:
        await self._send(raw)

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(
            disruption_tolerance_ms=3600000,  # mesh links survive hours
            mtu=getattr(self._link, 'mdu', 500),
            is_local=False,
        )

    def id(self) -> FaceId:
        return self._id

    @property
    def link(self) -> RNS.Link:
        return self._link

    def close(self) -> None:
        if not self._notify_closed(self._link):
            return
        try:
            self._link.teardown()
        except Exception:
            logger.debug("link teardown on face close failed", exc_info=True)
