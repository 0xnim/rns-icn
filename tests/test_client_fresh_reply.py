"""Fresh-only consumer policy must also check an upstream reply."""
from unittest.mock import AsyncMock, patch

import pytest
import RNS

from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig
from rns_icn.face import TestFace
from rns_icn.forwarder import Forwarder
from rns_icn.name import Name
from rns_icn.packet import Data, Interest


class InlineFace(TestFace):
    def __init__(self, fw, reply):
        super().__init__(7)
        self.fw, self.reply = fw, reply
        self.requests = []

    async def send_raw(self, raw):
        self.requests.append(Interest.from_bytes(raw))
        await self.fw.receive_data(Data.from_bytes(self.reply.to_bytes()), self.id())


@pytest.mark.asyncio
@pytest.mark.parametrize("stale_age", [0, 100])
async def test_stale_upstream_cannot_satisfy_fresh_only_or_advance_rollback(stale_age):
    identity = RNS.Identity.from_bytes(bytes(range(64)))
    name = Name(identity.hash, [b"pointer"])
    stale = Data.new(name, b"old cached edition").with_sequence(2).with_staleness(stale_age)
    stale.sign(identity.sign, signed_at=200)
    fw = Forwarder()
    face = InlineFace(fw, stale)
    fw.register_face(face)
    client = ICNClient(ClientConfig(require_signature=True, reject_rollback=True))
    client._forwarder = fw
    client._link_pool = AsyncMock()
    client._get_or_create_face_id = lambda link: face.id()
    try:
        with patch.object(RNS.Identity, "recall", return_value=identity):
            with pytest.raises(ValueError, match="stale"):
                await client._fetch_verified(name, identity.hash, must_be_fresh=True, max_retries=0)
            assert face.requests[0].must_be_fresh
            assert client._seen_signed_key == {}
            # A rejected higher signed version must not poison accepted state.
            fresh = Data.new(name, b"acceptable fresh edition").with_sequence(1)
            fresh.sign(identity.sign, signed_at=100)
            face.reply = fresh
            got = await client._fetch_verified(name, identity.hash, must_be_fresh=True, max_retries=0)
            assert got.content == fresh.content
    finally:
        fw.cs.close()


@pytest.mark.asyncio
async def test_default_consumer_still_accepts_signed_stale_content():
    identity = RNS.Identity.from_bytes(bytes(range(64)))
    name = Name(identity.hash, [b"offline"])
    stale = Data.new(name, b"available offline").with_staleness(100).sign(identity.sign)
    fw = Forwarder()
    fw.cs.insert(name, stale)
    client = ICNClient(ClientConfig(require_signature=True))
    client._forwarder = fw
    client._link_pool = AsyncMock()
    client._get_or_create_face_id = lambda link: 7
    try:
        with patch.object(RNS.Identity, "recall", return_value=identity):
            got = await client.fetch(name, identity.hash, max_retries=0)
            assert got.content == stale.content
    finally:
        fw.cs.close()
