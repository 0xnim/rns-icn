"""Publishing a directory: files become content, changes and removals follow."""

from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import Data
from rns_icn.pages import PageRenderer
from rns_icn.publish_dir import DirectoryPublisher
from rns_icn.server import ICNServer

NODE = bytes([0xBB] + [0] * (RNS_ADDR_BYTES - 1))


def make_server():
    server = ICNServer(NODE)
    if not hasattr(server, "publish_content"):
        def publish_content(name, content, sequence=None, latest_under=None):
            server.forwarder.cs.insert(name, Data.new(name=name, content=content))
        server.publish_content = publish_content
    return server


def get(server, *labels):
    data = server.forwarder.cs.get(Name(NODE, [x.encode() for x in labels]))
    return None if data is None else data.content


def test_files_are_published_under_their_relative_path(tmp_path):
    (tmp_path / "about.mu").write_text(">Hello\n")
    (tmp_path / "news").mkdir()
    (tmp_path / "news" / "today.txt").write_text("calm")
    server = make_server()
    publisher = DirectoryPublisher(server, tmp_path)
    assert publisher.scan() == (2, 0)
    assert get(server, "about.mu") == b">Hello\n"
    assert get(server, "news", "today.txt") == b"calm"
    assert publisher.scan() == (0, 0)          # nothing changed


def test_changes_are_republished_and_removals_withdrawn(tmp_path):
    f = tmp_path / "note.txt"
    f.write_text("one")
    server = make_server()
    publisher = DirectoryPublisher(server, tmp_path)
    publisher.scan()
    f.write_text("two")
    assert publisher.scan() == (1, 0)
    assert get(server, "note.txt") == b"two"
    f.unlink()
    assert publisher.scan() == (0, 1)
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
    assert publisher.scan() == (1, 0)
    assert get(server, "ok.txt") == b"fine"
    assert get(server, "big.bin") is None
    assert get(server, "manifest") is None


def test_missing_directory_publishes_nothing(tmp_path):
    assert DirectoryPublisher(make_server(), tmp_path / "nope").scan() == (0, 0)


def test_published_files_show_up_as_pages(tmp_path):
    (tmp_path / "about.mu").write_text(">Welcome\n")
    server = make_server()
    DirectoryPublisher(server, tmp_path).scan()
    renderer = PageRenderer(server)
    assert NODE.hex() in renderer.index()
    assert ">Welcome" in renderer.item(Name(NODE, [b"about.mu"]).to_bytes().hex())
