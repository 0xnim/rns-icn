"""Forwarding Information Base — prefix → faces, longest-prefix-match."""

from __future__ import annotations

from dataclasses import dataclass, field

from .face import FaceId
from .name import Name


@dataclass
class FibEntry:
    prefix: Name
    faces: list[tuple[FaceId, int]] = field(default_factory=list)


class Fib:
    def __init__(self):
        self._entries: list[FibEntry] = []

    def insert(self, prefix: Name, face: FaceId, cost: int = 10) -> None:
        for entry in self._entries:
            if entry.prefix == prefix:
                for i, (fid, _) in enumerate(entry.faces):
                    if fid == face:
                        entry.faces[i] = (face, cost)
                        break
                else:
                    entry.faces.append((face, cost))
                entry.faces.sort(key=lambda x: x[1])
                return
        self._entries.append(FibEntry(prefix=prefix, faces=[(face, cost)]))

    def remove_face(self, prefix: Name, face: FaceId) -> None:
        new_entries = []
        for entry in self._entries:
            if entry.prefix.starts_with(prefix) or prefix.starts_with(entry.prefix):
                entry.faces = [(f, c) for f, c in entry.faces if f != face]
                if entry.faces:
                    new_entries.append(entry)
            else:
                new_entries.append(entry)
        self._entries = new_entries

    def remove_prefix(self, prefix: Name) -> None:
        self._entries = [e for e in self._entries if not e.prefix.starts_with(prefix)]

    def lookup(self, name: Name) -> list[tuple[FaceId, int]] | None:
        best: FibEntry | None = None
        for entry in self._entries:
            if name.starts_with(entry.prefix) and (
                best is None or entry.prefix.len() > best.prefix.len()
            ):
                best = entry
        return list(best.faces) if best else None

    def __len__(self) -> int:
        return len(self._entries)

    def iter(self) -> list[FibEntry]:
        return list(self._entries)
