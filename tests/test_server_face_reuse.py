"""Shared outbound Links must have one callback owner and one receive pump."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import RNS

from rns_icn.face import TestFace
from rns_icn.rns_server import ICNServer
from rns_icn.server import ICNServer as BaseServer


def server_for(link):
    server = object.__new__(ICNServer)
    BaseServer.__init__(server, bytes(16))
    server._loop = asyncio.get_running_loop()
    server.destination = object()
    server._link_pool = SimpleNamespace(get_link=AsyncMock(return_value=link))
    server.discovery = SimpleNamespace(update_peer_face=Mock(), clear_face=Mock())
    server._peer_features = {}
    server._caps_pending = set()
    server.on_face_closed = None
    server.installed = []

    def install(face_id, native_link):
        face = TestFace(face_id)
        face._link, face._closed = native_link, False
        server._faces[face_id] = face
        server.forwarder.register_face(face)
        server.installed.append(face)
        return face

    server._make_link_face = install
    server._send_capabilities = AsyncMock()
    return server


@pytest.mark.asyncio
async def test_same_link_reuses_face_during_inflight_handshake():
    link = SimpleNamespace(status=RNS.Link.ACTIVE)
    server = server_for(link)
    started, release = asyncio.Event(), asyncio.Event()

    async def handshake(*args):
        started.set()
        await release.wait()

    server._send_capabilities.side_effect = handshake
    first = asyncio.create_task(server.connect('ab' * 16))
    try:
        await asyncio.wait_for(started.wait(), 1)
        others = await asyncio.wait_for(asyncio.gather(
            *(server.connect('ab' * 16) for _ in range(16))), 1)
        release.set()
        owner = await first
        assert others == [owner] * 16
        assert len(server.installed) == 1
        assert server._send_capabilities.await_count == 1
        assert server.discovery.update_peer_face.call_count == 1
    finally:
        release.set()
        await asyncio.gather(first, return_exceptions=True)
        server.forwarder.cs.close()


@pytest.mark.asyncio
async def test_replacement_link_gets_new_face_and_old_cleanup_preserves_it():
    server = server_for(SimpleNamespace(status=RNS.Link.ACTIVE))
    try:
        old = await server.connect('ab' * 16)
        server._link_pool.get_link.return_value = SimpleNamespace(status=RNS.Link.ACTIVE)
        new = await server.connect('ab' * 16)
        assert old != new
        server._cleanup_closed_face(old)
        assert await server.connect('ab' * 16) == new
        assert len(server.installed) == 2
        assert server._faces[new] is server.installed[-1]
    finally:
        server.forwarder.cs.close()


@pytest.mark.asyncio
async def test_closed_face_cannot_rewrap_same_native_link():
    server = server_for(SimpleNamespace(status=RNS.Link.ACTIVE))
    try:
        old = await server.connect('ab' * 16)
        server._faces[old]._closed = True
        assert await server.connect('ab' * 16) is None
        assert len(server.installed) == 1
    finally:
        server.forwarder.cs.close()
