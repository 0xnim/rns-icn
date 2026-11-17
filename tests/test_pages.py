"""Tests for the NomadNet page front end's rendering (no Reticulum needed)."""

import json

import pytest

from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import Data
from rns_icn.pages import (
    PATH_ABOUT_ICN,
    PATH_COLLECTION,
    PATH_ITEM,
    PATH_PRODUCER,
    PageRenderer,
)
from rns_icn.server import ICNServer

HUB = bytes([0xBB] + [0] * (RNS_ADDR_BYTES - 1))
WX = bytes([0xAA] + [0] * (RNS_ADDR_BYTES - 1))


def put(server, producer, labels, content, seq=None, signature=None):
    data = Data.new(name=Name(producer, labels), content=content)
    if seq is not None:
        data.with_sequence(seq)
    data.signature = signature
    server.forwarder.cs.insert(data.name, data)
    return data


@pytest.fixture
def hub():
    return ICNServer(HUB)


def test_empty_node_says_so(hub):
    assert "Nothing is cached" in PageRenderer(hub).index()


def test_index_lists_producers_with_counts_and_nicknames(hub):
    put(hub, WX, [b"feed", b"a"], b"one", 1)
    put(hub, WX, [b"feed", b"b"], b"two", 2)
    put(hub, WX, [b"manifest"], b"{}", 5)            # not content
    put(hub, WX, [b"feed", b"\x00m"], b"ptr", 2)     # reserved pointer, not content
    put(hub, HUB, [b"about"], b"me")
    page = PageRenderer(hub, node_name="Harbour hub", nicknames={WX: "weather"}).index()
    assert ">Harbour hub" in page
    assert f"weather ({WX.hex()[:12]})`:{PATH_PRODUCER}`p={WX.hex()}]  2 items" in page
    # The node's own content is listed first, directly on the home page.
    assert page.index("[about`") < page.index(">Kept from other nodes") < page.index("weather")


def test_producer_page_lists_the_latest_edition_per_collection(hub):
    put(hub, WX, [b"feed", b"mon"], b"old", 1)
    newest = put(hub, WX, [b"feed", b"tue"], b"new", 2)
    put(hub, WX, [b"docs", b"readme"], b"hi")
    page = PageRenderer(hub).producer(WX.hex())
    assert page.index("[docs`") < page.index("[feed`")
    assert f"[feed`:{PATH_ITEM}`n={newest.name.to_bytes().hex()}]  3 B, updated" in page
    assert "mon" not in page.split("[feed`")[1].split("\n")[0].split("earlier")[0]
    assert f"[1 earlier`:{PATH_COLLECTION}`p={WX.hex()}|c={b'feed'.hex()}]" in page


def test_archive_page_lists_every_edition_newest_first(hub):
    for seq in range(1, 8):
        put(hub, HUB, [b"swx", b"now", str(seq).encode()], b"{}", seq)
    renderer = PageRenderer(hub, node_name="Harbour hub")
    index = renderer.index()
    assert "6 earlier" in index and "[7`" not in index
    page = renderer.collection(HUB.hex(), b"swx/now".hex())
    assert page.index("[7`") < page.index("[6`") < page.index("[1`")
    assert "latest" in page.split("[7`")[1].split("\n")[0]
    assert "Unknown collection" in renderer.collection("zz", "zz")
    assert "Nothing from this collection" in renderer.collection(HUB.hex(), b"nope".hex())


def test_site_file_titles_orders_and_hides_collections(hub):
    put(hub, HUB, [b"swx", b"now", b"1"], b"{}", 1)
    put(hub, HUB, [b"swx", b"fc", b"1"], b"fc", 1)
    put(hub, HUB, [b"swx", b"page", b"1.mu"], b">x", 1)
    put(hub, HUB, [b"about.mu"], b">hi")
    put(hub, HUB, [b"site.json"], json.dumps({
        "titles": {"swx/page": "Space weather now", "swx/fc": "3-day forecast",
                   "about.mu": "About"},
        "hidden": ["swx/now"],
    }).encode())
    page = PageRenderer(hub).index()
    assert page.index("[About`") < page.index("[Space weather now`") < page.index("[3-day forecast`")
    assert "swx/now" not in page and "site.json" not in page
    item = put(hub, HUB, [b"swx", b"fc", b"2"], b"fc2", 2)
    crumbs = PageRenderer(hub).item(item.name.to_bytes().hex()).split("\n")[1]
    assert "[3-day forecast`:/page/collection.mu" in crumbs


def test_broken_site_file_is_ignored(hub):
    put(hub, HUB, [b"swx", b"fc", b"1"], b"fc", 1)
    put(hub, HUB, [b"site.json"], b"{not json")
    assert "[swx/fc`" in PageRenderer(hub).index()


def test_every_page_has_the_footer(hub):
    item = put(hub, HUB, [b"about.mu"], b">hi")
    r = PageRenderer(hub)
    for page in (r.index(), r.producer(HUB.hex()), r.item(item.name.to_bytes().hex()),
                 r.about_icn(), r.collection(HUB.hex(), b"x".hex())):
        assert f"[What is this?`:{PATH_ABOUT_ICN}]" in page
        assert "rns-icn" in page.splitlines()[-1]


def test_producer_page_rejects_bad_address(hub):
    assert "Unknown producer" in PageRenderer(hub).producer("zz")
    assert "No items" in PageRenderer(hub).producer(WX.hex())


def test_item_page_shows_text_verbatim_with_provenance(hub):
    item = put(hub, WX, [b"feed", b"tue"], b">not a heading\n`=tricky", 2)
    page = PageRenderer(hub).item(item.name.to_bytes().hex())
    assert "#2" in page and "unsigned" in page and "this node's cache" in page
    # Wrapped in a literal block, with a literal-toggle line in the text escaped.
    assert "`=\n>not a heading\n\\`=tricky\n`=\n" in page


def test_item_page_passes_micron_pages_through(hub):
    item = put(hub, WX, [b"site", b"index.mu"], b">Welcome\nhello")
    page = PageRenderer(hub).item(item.name.to_bytes().hex())
    assert ">Welcome\nhello\n" in page and "`=" not in page


def test_item_page_reports_signature_state(hub):
    item = put(hub, WX, [b"feed", b"x"], b"body", 1, signature=b"G" * 64)
    hexname = item.name.to_bytes().hex()
    assert "not checked" in PageRenderer(hub).item(hexname)
    good = PageRenderer(hub, validator_for=lambda a: (lambda sig, h: sig == b"G" * 64))
    assert "signature verified" in good.item(hexname)
    bad = PageRenderer(hub, validator_for=lambda a: (lambda sig, h: False))
    assert "SIGNATURE INVALID" in bad.item(hexname)


def test_item_page_handles_binary_missing_and_malformed(hub):
    blob = put(hub, WX, [b"img", b"x"], b"\xff\xfe\x00\x01")
    renderer = PageRenderer(hub)
    assert "Binary content, 4 B" in renderer.item(blob.name.to_bytes().hex())
    assert "Malformed" in renderer.item("nothex")
    missing = Name(WX, [b"feed", b"nope"]).to_bytes().hex()
    assert "could not be fetched" in renderer.item(missing)


def test_item_page_fetches_upstream_on_a_miss(hub):
    wanted = Name(WX, [b"feed", b"late"])
    asked = []

    def fetch(name):
        asked.append(name)
        return Data.new(name=name, content=b"arrived")

    page = PageRenderer(hub, fetch=fetch).item(wanted.to_bytes().hex())
    assert asked == [wanted]
    assert "fetched from upstream just now" in page and "arrived" in page
