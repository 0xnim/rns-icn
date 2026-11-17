"""Publish a directory — files dropped in a folder become this node's content.

Each file under the directory is published as ``/<this node>/<relative path>``:
signed, given a sequence, and pushed to subscribers. A file that disappears is
withdrawn. This is the simplest way to put content on a node: write files with
any tool (an editor, cron, rsync) and let the server pick them up.

A folder is a collection. Readers can ask for the latest item in it, and nodes
following this one receive each new item as it appears. For something that
changes over time, write each version as a new file in one folder (for example
``swx/now/1763339400``) and delete old ones, instead of overwriting a single
file: versions with their own names can be cached anywhere without going stale.

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
        # top-level label -> last sequence issued under it
        self._last_sequence: dict[str, int] = {}

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

    def _next_sequence(self, label: str, mtime: float) -> int:
        """A sequence for a new edition under ``label``: the file's modification
        time in seconds, moved forward if that is not above the last one used.

        Sequences order a label's editions for followers, which walk them
        upwards, so each must be unique and increasing.
        """
        last = self._last_sequence.get(label)
        if last is None:
            prefix = Name(self._server.rns_addr, [label.encode("utf-8")])
            held = [seq for _, seq, _, _ in self._server.forwarder.cs.listing(prefix)
                    if seq is not None]
            last = max(held, default=0)
        sequence = max(int(mtime), last + 1)
        self._last_sequence[label] = sequence
        return sequence

    async def scan(self) -> tuple[int, int]:
        """Publish new and changed files, withdraw removed ones.

        Each new or changed file becomes a signed edition with a sequence, is
        pushed to subscribers, and updates the "latest" pointer of the folder
        it is in. A file already held with the same bytes (after a restart) is
        left alone. Returns ``(published, withdrawn)``.
        """
        published = withdrawn = 0
        candidates = self._candidates()
        pending: list[tuple[float, tuple[str, ...], bytes, bytes]] = []
        for parts, path in candidates.items():
            try:
                stat = path.stat()
                if stat.st_size > self.max_bytes:
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
            pending.append((stat.st_mtime, parts, content, digest))

        cs = self._server.forwarder.cs
        # Oldest first, so sequences follow the order the files were written.
        for mtime, parts, content, digest in sorted(pending, key=lambda item: item[:2]):
            name = Name(self._server.rns_addr, [p.encode("utf-8") for p in parts])
            if parts not in self._published:
                held = cs.get(name)
                if held is not None and held.content == content:
                    self._published[parts] = digest
                    continue
            await self._server.publish_post(
                name, content, sequence=self._next_sequence(parts[0], mtime)
            )
            self._published[parts] = digest
            published += 1
        for parts in [p for p in self._published if p not in candidates]:
            if self._published.pop(parts):
                name = Name(self._server.rns_addr, [p.encode("utf-8") for p in parts])
                cs.remove(name)
                withdrawn += 1
        return published, withdrawn
