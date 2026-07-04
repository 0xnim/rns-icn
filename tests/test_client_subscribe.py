"""Tests for ICNClient.subscribe — the verified push consumer primitive."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig
from rns_icn.forwarder import Forwarder
from rns_icn.name import Name
from rns_icn.packet import APSubscribe, Data, parse_packet

ADDR = bytes(range(16))
PEER = b"\x99" * 16
PREFIX = Name(ADDR, [b"wire", b"main"])


def _post(seq: int, content: bytes = b"") -> Data:
    data = Data.new(Name(ADDR, [b"wire", b"main", str(seq).encode()]), content or b"x")
    data.with_sequence(seq)
    return data


async def _subscribed_client(callback) -> tuple[ICNClient, MagicMock]:
    client = ICNClient(ClientConfig())
    client._forwarder = Forwarder()
    face = MagicMock()
    face.send_raw = AsyncMock()
    client._forwarder._faces[1] = face
    client._get_or_create_face_id = MagicMock(return_value=1)
    client._link_pool = MagicMock()
    client._link_pool.get_link = AsyncMock(return_value=MagicMock())
    await client.subscribe(PREFIX, PEER, callback, start_from_now=True)
    return client, face


@pytest.mark.asyncio
async def test_subscribe_sends_handshake_and_registers_dispatch():
    got = []
    client, face = await _subscribed_client(got.append)

    face.send_raw.assert_awaited_once()
    pkt = parse_packet(face.send_raw.await_args.args[0])
    assert pkt.subscribe is not None
    assert pkt.subscribe.name == PREFIX
    assert pkt.subscribe.start_from_now is True

    # A pushed Data under the prefix reaches the callback…
    client._forwarder._data_callback(_post(1))
    assert [d.metadata.sequence for d in got] == [1]
    # …one outside it does not.
    client._forwarder._data_callback(Data.new(Name(ADDR, [b"other"]), b"x"))
    assert len(got) == 1


@pytest.mark.asyncio
async def test_pushed_data_is_verified():
    got = []
    client, _ = await _subscribed_client(got.append)
    bad = _post(2)
    bad.content = b"swapped after hashing"  # content hash no longer matches
    client._forwarder._data_callback(bad)
    assert got == []


@pytest.mark.asyncio
async def test_multiple_subscriptions_fan_out():
    got_a, got_b = [], []
    client, _ = await _subscribed_client(got_a.append)
    other_prefix = Name(ADDR, [b"wire", b"dev"])
    await client.subscribe(other_prefix, PEER, got_b.append)
    client._forwarder._data_callback(_post(1))
    dev_post = Data.new(Name(ADDR, [b"wire", b"dev", b"1"]), b"y")
    dev_post.with_sequence(1)
    client._forwarder._data_callback(dev_post)
    assert len(got_a) == 1 and len(got_b) == 1


def test_apsubscribe_round_trip():
    sub = APSubscribe(name=PREFIX, start_from_now=False)
    parsed = parse_packet(sub.to_bytes()).subscribe
    assert parsed.name == PREFIX
    assert parsed.start_from_now is False
