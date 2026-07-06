"""Tests for ICNClient.resolve — destination hash → producer identity.

The resolve ABI call (ICN_APP_PLATFORM.md §7.1): apps hold a producer's
*destination* hash but need its *identity* (hash = ICN name address, keys =
signature trust anchor). RNS is mocked at the two seams resolve touches —
``Identity.recall`` and ``Transport.request_path`` — so these run without a
Reticulum instance; the announce mechanics themselves belong to RNS.
"""

import pytest
import RNS

import rns_icn.client
from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig

DEST = bytes(range(16))


def _started_client() -> ICNClient:
    client = ICNClient(ClientConfig())
    client._identity = RNS.Identity()  # started enough for resolve
    return client


@pytest.mark.asyncio
async def test_known_identity_returns_without_path_request(monkeypatch):
    client = _started_client()
    identity = RNS.Identity()
    monkeypatch.setattr(RNS.Identity, "recall", lambda dest: identity)
    requested = []
    monkeypatch.setattr(RNS.Transport, "request_path", requested.append)

    assert await client.resolve(DEST) is identity
    assert requested == []


@pytest.mark.asyncio
async def test_unknown_identity_arrives_via_path_request(monkeypatch):
    client = _started_client()
    identity = RNS.Identity()
    requested = []
    monkeypatch.setattr(RNS.Transport, "request_path", requested.append)
    # recall succeeds only once the path request has gone out — the shape of
    # a transport node answering with the producer's announce.
    monkeypatch.setattr(
        RNS.Identity, "recall", lambda dest: identity if requested else None
    )

    assert await client.resolve(DEST, timeout=5.0) is identity
    assert requested == [DEST]


@pytest.mark.asyncio
async def test_no_announce_times_out_to_none_after_rerequests(monkeypatch):
    client = _started_client()
    monkeypatch.setattr(RNS.Identity, "recall", lambda dest: None)
    requested = []
    monkeypatch.setattr(RNS.Transport, "request_path", requested.append)
    monkeypatch.setattr(rns_icn.client, "_RESOLVE_REREQUEST_INTERVAL", 0.1)

    assert await client.resolve(DEST, timeout=0.5) is None
    assert len(requested) >= 2  # kept re-asking, not one-shot


@pytest.mark.asyncio
async def test_resolve_requires_started_client():
    client = ICNClient(ClientConfig())
    with pytest.raises(RuntimeError, match="not started"):
        await client.resolve(DEST)
