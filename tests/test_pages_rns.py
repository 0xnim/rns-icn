"""Page front end over real Reticulum: a browser-style client reads cached ICN
content from a ``nomadnetwork.node`` destination served by an ICN node.

Gated like the other real-RNS tests (``RNS_INTEGRATION=1``).
"""

import asyncio
import json
import os
import sys

import pytest

from rns_icn.config import ServerConfig
from rns_icn.name import Name
from rns_icn.pages import PageNode
from rns_icn.rns_server import ICNServer

READER = os.path.join(os.path.dirname(__file__), "_page_reader.py")
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))


@pytest.mark.skipif(
    not os.environ.get("RNS_INTEGRATION"),
    reason="Set RNS_INTEGRATION=1 to run integration tests",
)
@pytest.mark.asyncio
async def test_browser_reads_cached_content_as_pages(shared_rns, tmp_path):
    server = ICNServer(ServerConfig(
        identity_path=str(tmp_path / "identity"),
        cs_path=str(tmp_path / "cs.db"),
        announce_interval=3600.0,
        http_enabled=False,
    ))
    await server.start()
    try:
        me = server.identity.hash
        server.publish_content(Name(me, [b"feed", b"monday"]), b"Wind NW 12 knots", sequence=1)
        server.publish_content(Name(me, [b"feed", b"tuesday"]), b"Rain clearing", sequence=2)
        node = PageNode(server, node_name="Test hub")
        node.announce()

        env = dict(os.environ)
        env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        proc = await asyncio.create_subprocess_exec(
            sys.executable, READER, str(tmp_path / "reader"), str(shared_rns.port),
            node.hexhash, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, env=env,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), 120)
        line = next(row for row in out.decode().splitlines() if row.startswith("PAGES "))
        pages = json.loads(line[len("PAGES "):])

        assert "error" not in pages, pages
        assert ">Test hub" in pages["index"] and "2 items" in pages["index"]
        # Newest edition first on the producer page.
        producer = pages["producer"]
        assert producer.index("[tuesday`") < producer.index("[monday`")
        # The first item link is the newest edition; its text and provenance show.
        assert "Rain clearing" in pages["item"]
        assert "#2" in pages["item"] and "this node's cache" in pages["item"]
    finally:
        await server.shutdown()
