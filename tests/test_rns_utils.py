"""Tests for rns_utils — identity persistence helpers."""

import os

import RNS

from rns_icn.rns_utils import (
    default_identity_path,
    load_or_create_identity,
    load_transport_identity,
)


def test_load_or_create_identity_creates_and_persists(tmp_path):
    """First call creates + writes the identity file."""
    path = str(tmp_path / "identity")
    assert not os.path.exists(path)

    identity = load_or_create_identity(path)

    assert isinstance(identity, RNS.Identity)
    assert os.path.exists(path)


def test_load_or_create_identity_reloads_same_hash(tmp_path):
    """A second call loads the persisted identity (stable hash across restarts)."""
    path = str(tmp_path / "identity")
    first = load_or_create_identity(path)
    second = load_or_create_identity(path)

    assert first.hexhash == second.hexhash


def test_default_identity_path_creates_dir(tmp_path, monkeypatch):
    """default_identity_path returns ~/.<app>/identity and creates the dir."""
    monkeypatch.setattr(os.path, "expanduser", lambda p: p.replace("~", str(tmp_path)))

    path = default_identity_path("myapp")

    assert path.endswith(os.path.join(".myapp", "identity"))
    assert os.path.isdir(os.path.dirname(path))


def test_load_transport_identity_missing_returns_none(tmp_path, monkeypatch):
    """When no shared transport identity exists, returns None rather than raising."""
    monkeypatch.setattr(RNS.Reticulum, "storagepath", str(tmp_path), raising=False)

    assert load_transport_identity() is None
