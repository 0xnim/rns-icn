"""Stale age must survive cache hops, disk reopen and unsigned accounting."""
from unittest.mock import patch

import RNS
import pytest

from rns_icn.content_store import ContentStore
from rns_icn.name import Name
from rns_icn.packet import Data


@pytest.mark.parametrize("period", [None, 60])
def test_stale_age_accumulates_across_signed_wire_hops_and_restart(tmp_path, period):
    identity = RNS.Identity.from_bytes(bytes(range(64)))
    name = Name(identity.hash, [b"old"])
    original = Data.new(name=name, content=b"still authentic").with_staleness(500)
    if period is not None:
        original.with_freshness_period(period)
    original.sign(identity.sign, signed_at=100)
    first = ContentStore(path=str(tmp_path / "first.db"))
    second = ContentStore(path=str(tmp_path / "second.db"))
    try:
        with patch("rns_icn.content_store.time.time", return_value=1000):
            first.insert(name, original)
        with patch("rns_icn.content_store.time.time", return_value=1010):
            reply = first.get(name)
            assert reply.metadata.freshness.age_seconds == 510
            second.insert(name, Data.from_bytes(reply.to_bytes()))
        second.close()
        second = ContentStore(path=str(tmp_path / "second.db"))
        with patch("rns_icn.content_store.time.time", return_value=1020):
            for reply in (second.get(name), second.get_prefix(name)):
                assert not reply.metadata.freshness.fresh
                assert reply.metadata.freshness.age_seconds == 520
                assert reply.verify_signature(identity.validate)
                assert reply.content == original.content
    finally:
        first.close()
        second.close()


def test_stale_age_saturates_wire_uint64_and_clock_rollback_does_not_reduce_it():
    cs = ContentStore()
    name = Name(bytes(16), [b"old"])
    maximum = (1 << 64) - 1
    try:
        with patch("rns_icn.content_store.time.time", return_value=1000):
            cs.insert(name, Data.new(name=name, content=b"x").with_staleness(maximum - 2))
        with patch("rns_icn.content_store.time.time", return_value=990):
            assert cs.get(name).metadata.freshness.age_seconds == maximum - 2
        with patch("rns_icn.content_store.time.time", return_value=1010):
            reply = Data.from_bytes(cs.get(name).to_bytes())
            assert reply.metadata.freshness.age_seconds == maximum
            cs.insert(name, reply)
            assert cs.get(name).metadata.freshness.age_seconds == maximum
    finally:
        cs.close()
