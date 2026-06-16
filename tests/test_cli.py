"""Smoke tests for CLI entry points — argument parsing and error branches.

These exercise the deterministic validation paths (usage, bad input, dispatch)
without establishing real RNS links.
"""

from unittest.mock import MagicMock, patch

import pytest

from rns_icn import cli, cli_fetch, cli_publish
from rns_icn.config import ServerConfig


def test_cli_main_no_subcommand_returns_1(monkeypatch):
    monkeypatch.setattr(cli.sys, "argv", ["prog"])
    assert cli.main() == 1


def test_cli_main_unknown_subcommand_returns_1(monkeypatch):
    monkeypatch.setattr(cli.sys, "argv", ["prog", "bogus"])
    assert cli.main() == 1


@pytest.mark.asyncio
async def test_client_main_missing_fetch_or_peer_returns_1(monkeypatch):
    monkeypatch.setattr(cli.sys, "argv", ["icn-client", "--config", "x.toml"])
    with patch.object(cli, "load_client_config", return_value=MagicMock(max_retries=5)):
        assert await cli.client_main() == 1


@pytest.mark.asyncio
async def test_client_main_invalid_peer_hash_returns_1(monkeypatch):
    monkeypatch.setattr(
        cli.sys, "argv",
        ["icn-client", "--fetch", "/p/x", "--peer", "nothex"],
    )
    with patch.object(cli, "load_client_config", return_value=MagicMock(max_retries=5)):
        assert await cli.client_main() == 1


def test_fetch_main_usage_exits_1(monkeypatch):
    monkeypatch.setattr(cli_fetch.sys, "argv", ["icn-fetch"])
    with pytest.raises(SystemExit) as exc:
        cli_fetch.main()
    assert exc.value.code == 1


def test_publish_main_usage_exits_1(monkeypatch):
    monkeypatch.setattr(cli_publish.sys, "argv", ["icn-publish"])
    with pytest.raises(SystemExit) as exc:
        cli_publish.main()
    assert exc.value.code == 1


def test_publish_main_file_not_found_exits_1(monkeypatch):
    monkeypatch.setattr(
        cli_publish.sys, "argv",
        ["icn-publish", "ab" * 16, "myname", "/no/such/file"],
    )
    with pytest.raises(SystemExit) as exc:
        cli_publish.main()
    assert exc.value.code == 1


def test_publish_main_empty_stdin_exits_1(monkeypatch):
    monkeypatch.setattr(
        cli_publish.sys, "argv",
        ["icn-publish", "ab" * 16, "myname", "-"],
    )
    stdin = MagicMock()
    stdin.buffer.read.return_value = b""
    monkeypatch.setattr(cli_publish.sys, "stdin", stdin)
    with pytest.raises(SystemExit) as exc:
        cli_publish.main()
    assert exc.value.code == 1


def test_ephemeral_config_builders_return_serverconfig():
    for builder in (cli_fetch._ephemeral_config, cli_publish._ephemeral_config):
        cfg = builder()
        assert isinstance(cfg, ServerConfig)
        assert cfg.http_enabled is False
        assert cfg.cs_path.endswith("content_store.db")
