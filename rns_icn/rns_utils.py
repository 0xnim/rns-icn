"""Utility functions for RNS ICN server identity persistence.

Provides helpers to load or create persistent RNS.Identity instances
so the ICN server retains the same hash across restarts.
"""

from __future__ import annotations

import os
from typing import Optional

import RNS


def load_or_create_identity(path: str) -> RNS.Identity:
    """Load an identity from ``path``, or create + persist a new one.

    Args:
        path: Full filesystem path to the identity file.

    Returns:
        An :class:`RNS.Identity` instance, guaranteed to exist on disk
        when this function returns.
    """
    identity = RNS.Identity.from_file(path)
    if identity is not None:
        return identity

    RNS.log(f"Identity not found at {path}, creating new identity", RNS.LOG_NOTICE)
    identity = RNS.Identity()
    identity.to_file(path)
    RNS.log(f"Saved new identity to {path} — hash: {identity.hexhash}", RNS.LOG_NOTICE)
    return identity


def default_identity_path(app_name: str = "icn") -> str:
    """Return the default path for an identity file.

    The path is ``~/.<app_name>/identity``, with the directory
    created if it doesn't exist.

    Args:
        app_name: Application subdirectory under ``~/.``.

    Returns:
        Absolute path suitable for ``load_or_create_identity``.
    """
    d = os.path.expanduser(f"~/.{app_name}")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "identity")
