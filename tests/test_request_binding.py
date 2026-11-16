"""Request binding (PROTOCOL.md §3): Interests as RNS link requests.

Covers the three pieces without a live Reticulum: the announce flag, the face
routing Interests to ``link.request``, and the server's request handler.
"""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from rns_icn.face import LinkFace
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import APSubscribe, Data, Interest, parse_packet
from rns_icn.rns_server import (
    ANNOUNCE_FEATURE_REQUEST,
    REQUEST_PATH,
    ICNServer,
    peer_supports_request,
)

ADDR = bytes([0xAA] + [0] * (RNS_ADDR_BYTES - 1))


def test_announce_flag_parsing():
    assert peer_supports_request(b"icn\x00" + bytes([ANNOUNCE_FEATURE_REQUEST]))
    assert peer_supports_request(b"icn\x01\x03trailing")
    assert not peer_supports_request(b"icn\x00")          # pre-binding announce
    assert not peer_supports_request(b"icn\x00\x00")
    assert not peer_supports_request(b"xyz\x00\x01")
    assert not peer_supports_request(None)


class _Channel:
    mdu = 400

    def __init__(self):
        self.sent = []

    def register_message_type(self, _t):
        pass

    def add_message_handler(self, _h):
        pass

    def send(self, message):
        self.sent.append(message.raw)


class _Link:
    def __init__(self):
        self.channel = _Channel()
        self.requests = []
        self.mdu = 431

    def get_channel(self):
        return self.channel

    def set_link_closed_callback(self, _cb):
        pass

    def request(self, path, data=None, response_callback=None,
                failed_callback=None, timeout=None):
        self.requests.append((path, data, timeout))
        self._respond = response_callback
        return object()


@pytest.mark.asyncio
async def test_face_sends_interests_as_requests_and_delivers_the_response():
    link = _Link()
    face = LinkFace(1, link, request_path=REQUEST_PATH)
    interest = Interest(name=Name(ADDR, [b"feed", b"x"]), lifetime_ms=9000)

    await face.send_raw(interest.to_bytes())
    assert link.channel.sent == []
    path, data, timeout = link.requests[0]
    assert path == REQUEST_PATH and data == interest.to_bytes() and timeout == 9.0

    reply = Data.new(name=interest.name, content=b"hello").to_bytes()
    # Reticulum calls back on its own thread.
    t = threading.Thread(target=link._respond, args=(SimpleNamespace(response=reply),))
    t.start()
    t.join()
    assert await asyncio.wait_for(face._recv_queue.get(), 1) == reply


@pytest.mark.asyncio
async def test_face_keeps_non_interest_packets_on_the_channel():
    link = _Link()
    face = LinkFace(1, link, request_path=REQUEST_PATH)
    sub = APSubscribe(name=Name(ADDR, [b"feed"]), start_from_now=True).to_bytes()
    await face.send_raw(sub)
    assert link.requests == [] and link.channel.sent == [sub]


@pytest.mark.asyncio
async def test_face_without_request_path_uses_the_channel_for_interests():
    link = _Link()
    face = LinkFace(1, link)
    raw = Interest(name=Name(ADDR, [b"feed", b"x"])).to_bytes()
    await face.send_raw(raw)
    assert link.requests == [] and link.channel.sent == [raw]


@pytest_asyncio.fixture
async def server(tmp_path):
    import RNS

    from rns_icn.config import ServerConfig
    from rns_icn.rns_utils import load_or_create_identity

    path = str(tmp_path / "identity")
    load_or_create_identity(path)
    assert RNS.Identity.from_file(path) is not None
    return ICNServer(ServerConfig(
        identity_path=path, cs_path=str(tmp_path / "cs.db"), http_enabled=False,
    ))


def _call_handler(server, payload):
    """Invoke the request handler from a worker thread, as Reticulum does."""
    return asyncio.get_running_loop().run_in_executor(
        None, server._serve_request, REQUEST_PATH, payload, b"id", b"link", None, 0.0
    )


@pytest.mark.asyncio
async def test_request_handler_answers_from_the_store(server):
    server._loop = asyncio.get_running_loop()
    name = Name(server.rns_addr, [b"feed", b"x"])
    server.forwarder.cs.insert(name, Data.new(name=name, content=b"cached"))

    raw = await _call_handler(server, Interest(name=name, lifetime_ms=500).to_bytes())
    reply = parse_packet(raw).data
    assert reply.content == b"cached"
    assert reply.signature is not None       # origin Data is signed on the way out


@pytest.mark.asyncio
async def test_request_handler_nacks_what_it_cannot_answer(server):
    server._loop = asyncio.get_running_loop()
    missing = Name(ADDR, [b"nope"])
    raw = await _call_handler(server, Interest(name=missing, lifetime_ms=200).to_bytes())
    assert parse_packet(raw).nack.name == missing


@pytest.mark.asyncio
async def test_request_handler_ignores_garbage(server):
    server._loop = asyncio.get_running_loop()
    assert await _call_handler(server, b"\xff\xff") is None
    assert await _call_handler(server, {"not": "bytes"}) is None


@pytest.mark.asyncio
async def test_announce_advertises_the_request_binding(server):
    assert peer_supports_request(server._icn_app_data())


@pytest.mark.asyncio
async def test_capabilities_are_sent_on_first_channel_packet_not_on_link(server):
    server._loop = asyncio.get_running_loop()
    server._send_capabilities = MagicMock(side_effect=lambda *a: asyncio.sleep(0))
    link = MagicMock()
    link.hash = bytes(16)
    server._make_link_face = MagicMock()

    server._on_incoming_link(link)
    await asyncio.sleep(0)
    server._send_capabilities.assert_not_called()   # request-only readers get none

    face_id = next(iter(server._caps_pending))
    await server.handle_incoming(face_id, Interest(name=Name(ADDR, [b"x"]), lifetime_ms=50).to_bytes())
    server._send_capabilities.assert_called_once()
    await server.handle_incoming(face_id, b"\xff")
    server._send_capabilities.assert_called_once()   # only once per link
