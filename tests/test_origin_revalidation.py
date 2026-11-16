"""An origin's affirmation must be distinguishable from its aged cache copy."""
from unittest.mock import patch

import RNS
import pytest

from rns_icn.name import Name
from rns_icn.packet import Data, Interest
from rns_icn.server import ICNServer


@pytest.mark.parametrize("prefix", [False, True])
def test_origin_revalidates_own_expired_edition_without_changing_signed_version(prefix):
    identity = RNS.Identity.from_bytes(bytes(range(64)))
    server = ICNServer(identity.hash, signer=identity.sign)
    name = Name(identity.hash, [b"collection", b"latest"])
    original = Data.new(name, b"current target").with_freshness_period(10).with_sequence(7)
    original.sign(identity.sign, signed_at=900)
    try:
        with patch("rns_icn.content_store.time.time", return_value=1000):
            server.forwarder.cs.insert(name, original)
        with patch("rns_icn.content_store.time.time", return_value=1020):
            stale = server._serve_from_cs(Interest(name), 1)
            assert not stale.metadata.freshness.fresh
            request_name = Name(identity.hash, [b"collection"]) if prefix else name
            fresh = server._serve_from_cs(Interest(request_name, must_be_fresh=True, can_be_prefix=prefix), 1)
            assert fresh.metadata.freshness.fresh
            assert fresh.metadata.freshness.age_seconds == 0
            decoded = Data.from_bytes(fresh.to_bytes())
            assert decoded.verify_signature(identity.validate)
            assert decoded.signature == original.signature
            assert decoded.freshness_key() == original.freshness_key()
            assert decoded.content == original.content
            assert not server.forwarder.cs.get(name).metadata.freshness.fresh
    finally:
        server.forwarder.cs.close()


def test_relay_cannot_affirm_another_producers_expired_data():
    identity = RNS.Identity.from_bytes(bytes(range(64)))
    relay = ICNServer(bytes(16))
    name = Name(identity.hash, [b"latest"])
    original = Data.new(name, b"current target").with_freshness_period(10).sign(identity.sign)
    try:
        with patch("rns_icn.content_store.time.time", return_value=1000):
            relay.forwarder.cs.insert(name, original)
        with patch("rns_icn.content_store.time.time", return_value=1020):
            assert relay._serve_from_cs(Interest(name, must_be_fresh=True), 1) is None
            assert not relay._serve_from_cs(Interest(name), 1).metadata.freshness.fresh
    finally:
        relay.forwarder.cs.close()


def test_origin_does_not_renew_explicitly_stale_stored_content():
    identity = RNS.Identity.from_bytes(bytes(range(64)))
    server = ICNServer(identity.hash, signer=identity.sign)
    name = Name(identity.hash, [b"explicitly-old"])
    data = Data.new(name, b"old").with_freshness_period(10).with_staleness(100)
    data.sign(identity.sign)
    try:
        server.forwarder.cs.insert(name, data)
        assert server._serve_from_cs(Interest(name, must_be_fresh=True), 1) is None
        assert not server._serve_from_cs(Interest(name), 1).metadata.freshness.fresh
    finally:
        server.forwarder.cs.close()
