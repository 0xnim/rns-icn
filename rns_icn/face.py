"""Face abstraction — communication endpoint for Interest/Data exchange."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from .packet import Data, Interest, PacketType

if TYPE_CHECKING:
    import RNS

FaceId = int


@dataclass
class FaceCapabilities:
    disruption_tolerance_ms: int = 5000
    mtu: int = 1500
    is_local: bool = True


class Face(ABC):
    @abstractmethod
    async def express_interest(self, interest: Interest) -> Optional[Data]:
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
        self._incoming: Optional[asyncio.Queue[bytes]] = None

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

    async def recv_raw(self) -> Optional[bytes]:
        """Receive raw bytes from the connected peer (blocks briefly)."""
        if self._incoming is None:
            return None
        try:
            return await asyncio.wait_for(self._incoming.get(), timeout=0.01)
        except asyncio.TimeoutError:
            return None

    async def recv_packet(self) -> Optional[PacketType]:
        """Receive and parse a raw packet. Returns the type.
        DEPRECATED: Use recv_raw() instead."""
        raw = await self.recv_raw()
        if raw is None or not raw:
            return None
        return PacketType(raw[0])

    async def express_interest(self, interest: Interest) -> Optional[Data]:
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
test_face_pair.__test__ = False


# ── LinkFace: Face over RNS Link with Channel ──


class LinkFace(Face):
    """Face backed by a RNS Link with reliable Channel transport.

    Uses RNS.Channel + LinkChannelOutlet for reliable in-order delivery
    over encrypted Links instead of raw RNS.Packet sends. The Channel
    provides automatic retry, sequence tracking, and flow-control
    windowing.

    Bridges from RNS's thread-based callback model into asyncio.
    """

    def __init__(self, face_id: FaceId, link: "RNS.Link", loop: asyncio.AbstractEventLoop | None = None):
        from RNS.Channel import ChannelException, MessageBase

        class _ICNMessage(MessageBase):
            """Channel message wrapping raw ICN Interest/Data bytes."""
            MSGTYPE = 0x01

            def __init__(self, raw: bytes = None):
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
        self._ICNMessage = _ICNMessage
        self._channel_exc = ChannelException

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

        # Handle link close
        def _on_closed(link: RNS.Link) -> None:
            self._closed = True
            self._loop.call_soon_threadsafe(
                self._recv_queue.put_nowait, b""
            )

        link.set_link_closed_callback(_on_closed)

    def _send(self, raw: bytes) -> None:
        """Send raw bytes over the Channel."""
        try:
            self._channel.send(self._ICNMessage(raw))
        except self._channel_exc:
            # Channel not ready (window full, link not ready) — silently drop
            # The caller's timeout will handle it
            pass

    async def express_interest(self, interest: Interest) -> Optional[Data]:
        raw = interest.to_bytes()
        self._send(raw)

        timeout_s = interest.lifetime_ms / 1000.0
        try:
            while True:
                reply = await asyncio.wait_for(
                    self._recv_queue.get(), timeout=timeout_s
                )
                if not reply:
                    return None  # link closed
                if reply[0] == 0x02:  # Data type byte
                    return Data.from_bytes(reply)
                # Ignore other packet types (e.g. Interests forwarded back)
        except asyncio.TimeoutError:
            return None

    async def send_data(self, data: Data) -> None:
        self._send(data.to_bytes())

    async def send_raw(self, raw: bytes) -> None:
        self._send(raw)

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(
            disruption_tolerance_ms=3600000,  # mesh links survive hours
            mtu=getattr(self._link, 'mdu', 500),
            is_local=False,
        )

    def id(self) -> FaceId:
        return self._id

    @property
    def link(self) -> "RNS.Link":
        return self._link

    def close(self) -> None:
        self._closed = True
        try:
            self._link.teardown()
        except Exception:
            pass
