"""Holder discovery: the announce holds list and the directory built from it."""

from rns_icn.holders import (
    ANNOUNCE_FEATURE_HOLDS,
    ANNOUNCE_FEATURE_REQUEST,
    MAX_ANNOUNCED_HOLDS,
    HolderDirectory,
    decode_holds,
    encode_app_data,
)
from rns_icn.rns_server import peer_supports_request

P1, P2, P3 = b"\x01" * 16, b"\x02" * 16, b"\x03" * 16
HUB_A, HUB_B, ORIGIN = b"\xa0" * 16, b"\xb0" * 16, b"\xc0" * 16


def test_app_data_without_holds_is_the_five_byte_form():
    data = encode_app_data(1, ANNOUNCE_FEATURE_REQUEST)
    assert data == b"icn\x01\x01"
    assert decode_holds(data) == []


def test_holds_round_trip_and_keep_the_request_bit():
    data = encode_app_data(2, ANNOUNCE_FEATURE_REQUEST, [P1, P2])
    assert data[4] == ANNOUNCE_FEATURE_REQUEST | ANNOUNCE_FEATURE_HOLDS
    assert decode_holds(data) == [P1, P2]
    assert peer_supports_request(data)


def test_holds_are_capped_and_bad_entries_dropped():
    many = [bytes([i]) * 16 for i in range(MAX_ANNOUNCED_HOLDS + 4)]
    assert len(decode_holds(encode_app_data(0, 0, many))) == MAX_ANNOUNCED_HOLDS
    assert decode_holds(encode_app_data(0, 0, [b"short", P1])) == [P1]


def test_malformed_holds_decode_to_nothing():
    good = encode_app_data(0, 0, [P1, P2])
    assert decode_holds(good[:-1]) == []                       # truncated
    assert decode_holds(b"icn\x00\x02\xff" + P1) == []         # count too large
    assert decode_holds(b"icn\x00\x01\x01" + P1) == []         # bit not set
    assert decode_holds(b"xyz\x00\x02\x01" + P1) == []
    assert decode_holds(None) == []


def test_directory_lists_followers_and_the_origin():
    d = HolderDirectory()
    d.observe(ORIGIN, P1, encode_app_data(0, 1), now=100)
    d.observe(HUB_A, b"\xaa" * 16, encode_app_data(1, 1, [P1, P2]), now=100)
    holders = d.holders_of(P1, now=101)
    # No distances known: a follower is preferred to the producer itself.
    assert [h.destination_hash for h in holders] == [HUB_A, ORIGIN]
    assert [h.is_origin for h in holders] == [False, True]
    assert [h.destination_hash for h in d.holders_of(P2, now=101)] == [HUB_A]
    assert d.holders_of(P3, now=101) == []


def test_directory_orders_by_hops():
    hops = {HUB_A: 4, HUB_B: 1, ORIGIN: 6}
    d = HolderDirectory(hops_to=hops.get)
    d.observe(ORIGIN, P1, encode_app_data(0, 1), now=100)
    d.observe(HUB_A, None, encode_app_data(1, 1, [P1]), now=100)
    d.observe(HUB_B, None, encode_app_data(1, 1, [P1]), now=100)
    assert [h.destination_hash for h in d.holders_of(P1, now=101)] == [HUB_B, HUB_A, ORIGIN]


def test_a_new_announce_replaces_the_previous_list():
    d = HolderDirectory()
    d.observe(HUB_A, None, encode_app_data(1, 1, [P1, P2]), now=100)
    d.observe(HUB_A, None, encode_app_data(1, 1, [P2]), now=200)
    assert d.holders_of(P1, now=201) == []
    assert [h.destination_hash for h in d.holders_of(P2, now=201)] == [HUB_A]


def test_holders_expire_and_can_be_forgotten():
    d = HolderDirectory(ttl=60)
    d.observe(HUB_A, None, encode_app_data(1, 1, [P1]), now=100)
    d.observe(HUB_B, None, encode_app_data(1, 1, [P1]), now=150)
    assert [h.destination_hash for h in d.holders_of(P1, now=170)] == [HUB_B]
    d.forget(HUB_B)
    assert d.holders_of(P1, now=170) == []
    assert d.producers() == []


def test_callbacks_fire_per_pair_and_ignore_other_apps():
    seen = []
    d = HolderDirectory()
    d.add_callback(lambda producer, dest: seen.append((producer, dest)))
    d.observe(HUB_A, b"\xaa" * 16, encode_app_data(1, 1, [P1]), now=100)
    d.observe(HUB_B, P2, b"nomadnet node", now=100)
    assert seen == [(P1, HUB_A), (b"\xaa" * 16, HUB_A)]


def test_server_uses_request_binding_only_on_links_it_opened(monkeypatch):
    import types

    import RNS

    from rns_icn.rns_server import REQUEST_PATH, ICNServer

    dest = types.SimpleNamespace(hash=HUB_A)
    advertised = {HUB_A: encode_app_data(0, ANNOUNCE_FEATURE_REQUEST, [P1])}
    monkeypatch.setattr(RNS.Identity, "recall_app_data", staticmethod(advertised.get))

    outbound = types.SimpleNamespace(initiator=True, destination=dest)
    inbound = types.SimpleNamespace(initiator=False, destination=dest)
    assert ICNServer._request_path_for(outbound) == REQUEST_PATH
    assert ICNServer._request_path_for(inbound) is None

    advertised[HUB_A] = b"icn\x00"  # peer predates the binding
    assert ICNServer._request_path_for(outbound) is None


def test_producer_key_record_certifies_itself():
    import RNS

    from rns_icn import discovery

    identity = RNS.Identity()
    public_key = identity.get_public_key()
    assert discovery.key_name(identity.hash).components[1] == b"\x00k"
    assert discovery.is_reserved(discovery.key_name(identity.hash))
    assert discovery.key_matches(identity.hash, public_key)
    assert not discovery.key_matches(identity.hash, RNS.Identity().get_public_key())
    assert not discovery.key_matches(identity.hash, public_key[:-1])
    assert not discovery.key_matches(b"\x00" * 16, public_key)
