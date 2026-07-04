"""Tests for ICNServer.publish_post — the composed publish primitive.

One call must leave the edition pullable (CS + verifiable latest-pointer,
like publish_content) *and* pushed live (APS + propagation, like
publish_pushed), with a single CS insert and an eagerly-signed Data.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rns_icn import discovery
from rns_icn.config import ServerConfig
from rns_icn.name import Name
from rns_icn.rns_server import RNSICNServer

_FAKE_SIG = b"\x00" * 64


def _make_mock_identity(prefix_byte: int):
    mock_id = MagicMock()
    mock_id.hash = bytes([prefix_byte]) + b"\x00" * 15
    mock_id.hexhash = f"{prefix_byte:02x}" + "00" * 15
    mock_id.sign = lambda data: _FAKE_SIG
    return mock_id


def _cfg() -> ServerConfig:
    return ServerConfig(
        identity_path="/unused",
        app_name="icn",
        aspect="test",
        cs_path=":memory:",
    )


async def _make_server(prefix_byte: int = 0x21) -> RNSICNServer:
    with (
        patch(
            "rns_icn.rns_server.load_or_create_identity",
            return_value=_make_mock_identity(prefix_byte),
        ),
        patch("RNS.Reticulum"),
        patch("RNS.Destination"),
    ):
        server = RNSICNServer(_cfg())
        server.discovery = MagicMock()
        await server.start()
    server.aps = MagicMock()
    server.aps.publish = AsyncMock()
    server.propagation = MagicMock()
    server.propagation.propagate = AsyncMock()
    return server


@pytest.mark.asyncio
async def test_publish_post_pullable_and_pushed():
    server = await _make_server()
    try:
        prefix = Name(server.rns_addr, [b"wire", b"main"])
        name = Name(server.rns_addr, [b"wire", b"main", b"3"])

        await server.publish_post(name, b"hello mesh", sequence=3, latest_under=prefix)

        # Pull side: signed, sequenced Data in the CS…
        stored = server.forwarder.cs.get(name)
        assert stored is not None
        assert stored.signature == _FAKE_SIG
        assert stored.metadata.sequence == 3
        assert stored.metadata.signed_at is not None

        # …plus a latest-pointer pinned to this edition's content hash.
        meta = server.forwarder.cs.get(discovery.meta_name(prefix))
        assert meta is not None
        assert meta.metadata.sequence == 3
        assert meta.metadata.freshness_period == server.config.meta_freshness_period
        target = discovery.decode_meta(meta.content)
        assert target.without_content_hash() == name
        assert target.content_hash == stored.metadata.content_hash

        # Push side: the same signed Data went to APS (with the offline
        # queue for disconnected subscribers) and to peer propagation.
        server.aps.publish.assert_awaited_once()
        pushed = server.aps.publish.await_args.args[0]
        assert pushed.name == name
        assert pushed.signature == _FAKE_SIG
        assert server.aps.publish.await_args.kwargs["offline_queue"] is server.offline_queue
        server.propagation.propagate.assert_awaited_once()
        assert server.propagation.propagate.await_args.args[0].name == name
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_publish_post_default_latest_under_is_parent():
    server = await _make_server(0x22)
    try:
        name = Name(server.rns_addr, [b"wire", b"main", b"1"])
        await server.publish_post(name, b"x", sequence=1)
        parent = Name(server.rns_addr, [b"wire", b"main"])
        assert server.forwarder.cs.get(discovery.meta_name(parent)) is not None
    finally:
        await server.shutdown()


@pytest.mark.asyncio
async def test_publish_post_pointer_advances():
    server = await _make_server(0x23)
    try:
        prefix = Name(server.rns_addr, [b"wire", b"main"])
        for seq in (1, 2):
            await server.publish_post(
                Name(server.rns_addr, [b"wire", b"main", str(seq).encode()]),
                f"post {seq}".encode(),
                sequence=seq,
                latest_under=prefix,
            )
        meta = server.forwarder.cs.get(discovery.meta_name(prefix))
        target = discovery.decode_meta(meta.content)
        assert target.components[-1] == b"2"
        assert meta.metadata.sequence == 2
    finally:
        await server.shutdown()
