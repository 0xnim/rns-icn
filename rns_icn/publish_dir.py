"""Publish a directory — files dropped in a folder become this node's content.

Each file under the directory is published as ``/<this node>/<relative path>``
and re-published when its bytes change; a file that disappears is withdrawn.
This is the simplest way to put content on a node: write files with any tool
(an editor, cron, rsync) and let the server pick them up.

A file whose name ends in ``.mu`` is shown as a micron page by the NomadNet
front end (``rns_icn.pages``); anything else is shown as plain text.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from .name import Name

if TYPE_CHECKING:
    from .server import ICNServer

logger = logging.getLogger(__name__)

# Names the server uses itself; a file here would shadow them.
RESERVED_TOP_LEVEL = {"manifest", "health"}


class DirectoryPublisher:
    def __init__(self, server: ICNServer, root: str | Path, max_bytes: int = 256 * 1024):
        self._server = server
        self.root = Path(root).expanduser()
        self.max_bytes = max_bytes
        # relative path -> digest of the bytes last published
        self._published: dict[tuple[str, ...], bytes] = {}

    def _candidates(self) -> dict[tuple[str, ...], Path]:
        found: dict[tuple[str, ...], Path] = {}
        if not self.root.is_dir():
            return found
        for path in sorted(self.root.rglob("*")):
            parts = path.relative_to(self.root).parts
            # Skip hidden files and editors' temporaries anywhere in the path.
            if any(p.startswith(".") or p.endswith("~") for p in parts):
                continue
            if not path.is_file() or path.is_symlink():
                continue
            if parts[0] in RESERVED_TOP_LEVEL or parts[0].startswith("_"):
                continue
            found[parts] = path
        return found

    def scan(self) -> tuple[int, int]:
        """Publish new and changed files, withdraw removed ones.

        Returns ``(published, withdrawn)``.
        """
        published = withdrawn = 0
        candidates = self._candidates()
        for parts, path in candidates.items():
            try:
                if path.stat().st_size > self.max_bytes:
                    if parts not in self._published:
                        logger.warning("publish_dir: %s is over %d bytes, skipped",
                                       path, self.max_bytes)
                        self._published[parts] = b""
                    continue
                content = path.read_bytes()
            except OSError as e:
                logger.warning("publish_dir: cannot read %s: %s", path, e)
                continue
            if not content:
                continue
            digest = hashlib.sha256(content).digest()
            if self._published.get(parts) == digest:
                continue
            name = Name(self._server.rns_addr, [p.encode("utf-8") for p in parts])
            self._server.publish_content(name, content)
            self._published[parts] = digest
            published += 1
        for parts in [p for p in self._published if p not in candidates]:
            if self._published.pop(parts):
                name = Name(self._server.rns_addr, [p.encode("utf-8") for p in parts])
                self._server.forwarder.cs.remove(name)
                withdrawn += 1
        return published, withdrawn
