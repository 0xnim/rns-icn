"""Publishing a directory: files become content, changes and removals follow."""

import asyncio
import os

from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import Data
from rns_icn.pages import PageRenderer
from rns_icn.publish_dir import DirectoryPublisher
from rns_icn.server import ICNServer

NODE = bytes([0xBB] + [0] * (RNS_ADDR_BYTES - 1))


def make_server():
    server = ICNServer(NODE)
    server.posted = []

    async def publish_post(name, content, sequence, latest_under=None):
        data = Data.new(name=name, content=content)
        data.with_sequence(sequence)
        server.forwarder.cs.insert(name, data)
        server.posted.append((name, sequence))

    server.publish_post = publish_post
    return server


def scan(publisher):
    return asyncio.run(publisher.scan())


def get(server, *labels):
    data = server.forwarder.cs.get(Name(NODE, [x.encode() for x in labels]))
    return None if data is None else data.content


def test_files_are_published_under_their_relative_path(tmp_path):
    (tmp_path / "about.mu").write_text(">Hello\n")
    (tmp_path / "news").mkdir()
    (tmp_path / "news" / "today.txt").write_text("calm")
    server = make_server()
    publisher = DirectoryPublisher(server, tmp_path)
    assert scan(publisher) == (2, 0)
    assert get(server, "about.mu") == b">Hello\n"
    assert get(server, "news", "today.txt") == b"calm"
    assert scan(publisher) == (0, 0)          # nothing changed


def test_changes_are_republished_and_removals_withdrawn(tmp_path):
    f = tmp_path / "note.txt"
    f.write_text("one")
    server = make_server()
    publisher = DirectoryPublisher(server, tmp_path)
    scan(publisher)
    f.write_text("two")
    assert scan(publisher) == (1, 0)
    assert get(server, "note.txt") == b"two"
    f.unlink()
    assert scan(publisher) == (0, 1)
    assert get(server, "note.txt") is None


def test_hidden_reserved_empty_and_oversized_files_are_skipped(tmp_path):
    (tmp_path / ".secret").write_text("x")
    (tmp_path / "draft.txt~").write_text("x")
    (tmp_path / "manifest").write_text("x")
    (tmp_path / "_private").mkdir()
    (tmp_path / "_private" / "a.txt").write_text("x")
    (tmp_path / "empty.txt").write_text("")
    (tmp_path / "big.bin").write_bytes(b"x" * 200)
    (tmp_path / "ok.txt").write_text("fine")
    server = make_server()
    publisher = DirectoryPublisher(server, tmp_path, max_bytes=100)
    assert scan(publisher) == (1, 0)
    assert get(server, "ok.txt") == b"fine"
    assert get(server, "big.bin") is None
    assert get(server, "manifest") is None


def test_missing_directory_publishes_nothing(tmp_path):
    assert scan(DirectoryPublisher(make_server(), tmp_path / "nope")) == (0, 0)


def test_published_files_show_up_as_pages(tmp_path):
    (tmp_path / "about.mu").write_text(">Welcome\n")
    server = make_server()
    scan(DirectoryPublisher(server, tmp_path))
    renderer = PageRenderer(server)
    assert NODE.hex() in renderer.index()
    assert ">Welcome" in renderer.item(Name(NODE, [b"about.mu"]).to_bytes().hex())


def test_editions_get_increasing_sequences_in_the_order_written(tmp_path):
    folder = tmp_path / "swx" / "now"
    folder.mkdir(parents=True)
    for i, stamp in enumerate((1000, 1000, 2000)):      # two written in one second
        f = folder / f"e{i}"
        f.write_text(f"edition {i}")
        os.utime(f, (stamp, stamp))
    other = tmp_path / "notes" / "a.txt"
    other.parent.mkdir()
    other.write_text("x")
    os.utime(other, (500, 500))
    server = make_server()
    scan(DirectoryPublisher(server, tmp_path))
    swx = [(n.components[-1], seq) for n, seq in server.posted if n.components[1] == b"swx"]
    assert swx == [(b"e0", 1000), (b"e1", 1001), (b"e2", 2000)]
    assert [seq for n, seq in server.posted if n.components[1] == b"notes"] == [500]


def test_restart_does_not_republish_unchanged_files_and_continues_the_sequence(tmp_path):
    folder = tmp_path / "swx"
    folder.mkdir()
    (folder / "a").write_text("one")
    os.utime(folder / "a", (5000, 5000))
    server = make_server()
    scan(DirectoryPublisher(server, tmp_path))
    assert len(server.posted) == 1

    restarted = DirectoryPublisher(server, tmp_path)     # same content store
    (folder / "b").write_text("two")
    os.utime(folder / "b", (4000, 4000))                 # clock went backwards
    assert scan(restarted) == (1, 0)
    assert server.posted[-1][1] == 5001
