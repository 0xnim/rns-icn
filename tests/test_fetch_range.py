"""Tests for the verified sequence walk: ICNClient.fetch_range and the
ContentStore rule it depends on — reserved discovery names are never ranked
by child selection (the latest-pointer carries the newest edition's sequence
and would otherwise shadow that edition in LATEST answers and walks).
"""

from unittest.mock import AsyncMock

import pytest

from rns_icn import discovery
from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig
from rns_icn.content_store import ContentStore
from rns_icn.name import Name
from rns_icn.packet import ChildSelector, Data

ADDR = bytes(range(16))
PEER = b"\x99" * 16
PREFIX = Name(ADDR, [b"wire", b"main"])


def _post(seq: int) -> Data:
    data = Data.new(Name(ADDR, [b"wire", b"main", str(seq).encode()]), f"post {seq}".encode())
    data.with_sequence(seq)
    return data


def _pointer(seq: int) -> Data:
    target = _post(seq)
    meta = Data.new(
        discovery.meta_name(PREFIX),
        discovery.encode_meta(target.name.with_content_hash(target.metadata.content_hash)),
    )
    meta.with_sequence(seq)
    return meta


# ---------------------------------------------------------------- ContentStore


def _store_with(*items: Data) -> ContentStore:
    cs = ContentStore(path=":memory:")
    for data in items:
        cs.insert(data.name, data)
    return cs


def test_child_selection_skips_reserved_names():
    cs = _store_with(_post(1), _post(2), _pointer(2))
    latest = cs.get_prefix(PREFIX, child=ChildSelector.LATEST)
    assert latest.name.components[-1] == b"2"
    oldest = cs.get_prefix(PREFIX, child=ChildSelector.OLDEST, min_sequence=2)
    assert oldest.name.components[-1] == b"2"


def test_reserved_name_still_fetchable_exactly():
    cs = _store_with(_post(2), _pointer(2))
    assert cs.get(discovery.meta_name(PREFIX)) is not None


def test_min_sequence_walk_order():
    cs = _store_with(_post(1), _post(3), _post(7), _pointer(7))
    seen = []
    floor = 0
    while True:
        data = cs.get_prefix(PREFIX, child=ChildSelector.OLDEST, min_sequence=floor)
        if data is None:
            break
        seen.append(data.metadata.sequence)
        floor = data.metadata.sequence + 1
    assert seen == [1, 3, 7]


# ------------------------------------------------------------------- Forwarder


@pytest.mark.asyncio
async def test_walk_not_answered_by_cache_above_floor():
    """A node holding only newer entries must not answer an OLDEST walk.

    Holding the latest post but not the history is the normal state of a
    consumer or cache; answering "oldest ≥ 1" with it would silently skip
    the history held elsewhere. Only an exact-floor hit is authoritative.
    """
    from rns_icn.forwarder import Forwarder
    from rns_icn.packet import Interest, InterestSelector

    fwd = Forwarder()
    post = _post(3)
    fwd.cs.insert(post.name, post)

    def walk_interest(floor: int) -> Interest:
        return Interest(
            name=PREFIX,
            can_be_prefix=True,
            selector=InterestSelector(child=ChildSelector.OLDEST, min_sequence=floor),
        )

    # No route upstream: a non-floor hit yields nothing rather than post 3…
    assert await fwd.express(walk_interest(1), in_face=0) is None
    # …while the exact-floor hit is provably the oldest ≥ floor and serves.
    hit = await fwd.express(walk_interest(3), in_face=0)
    assert hit is not None
    assert hit.metadata.sequence == 3


# ------------------------------------------------------------------ ICNClient


def _client_with_scripted_answers(answers: list[Data | None]) -> ICNClient:
    client = ICNClient(ClientConfig())
    client._fetch_verified = AsyncMock(side_effect=answers)
    return client


async def _collect(client: ICNClient, **kwargs) -> list[Data]:
    return [d async for d in client.fetch_range(PREFIX, PEER, **kwargs)]


@pytest.mark.asyncio
async def test_fetch_range_walks_until_exhausted():
    client = _client_with_scripted_answers([_post(1), _post(3), _post(7), None])
    posts = await _collect(client, start_sequence=0)
    assert [p.metadata.sequence for p in posts] == [1, 3, 7]
    # The floor ratchets past each answer: 0, 2, 4, then 8 after seq 7.
    floors = [
        call.kwargs["selector"].min_sequence
        for call in client._fetch_verified.await_args_list
    ]
    assert floors == [0, 2, 4, 8]
    assert all(
        call.kwargs["selector"].child == ChildSelector.OLDEST
        for call in client._fetch_verified.await_args_list
    )


@pytest.mark.asyncio
async def test_fetch_range_respects_max_items():
    client = _client_with_scripted_answers([_post(1), _post(2), _post(3)])
    posts = await _collect(client, max_items=2)
    assert [p.metadata.sequence for p in posts] == [1, 2]


@pytest.mark.asyncio
async def test_fetch_range_stops_on_below_floor_answer():
    """A node that ignores the selector can't wedge the walk in a loop."""
    client = _client_with_scripted_answers([_post(5), _post(5), _post(5)])
    posts = await _collect(client, start_sequence=0)
    assert [p.metadata.sequence for p in posts] == [5]


@pytest.mark.asyncio
async def test_fetch_range_stops_on_unsequenced_answer():
    blob = Data.new(Name(ADDR, [b"wire", b"main", b"x"]), b"unsequenced")
    client = _client_with_scripted_answers([_post(1), blob])
    posts = await _collect(client)
    assert [p.metadata.sequence for p in posts] == [1]


@pytest.mark.asyncio
async def test_fetch_range_empty():
    client = _client_with_scripted_answers([None])
    assert await _collect(client, start_sequence=4) == []
