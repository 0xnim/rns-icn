"""Smoke tests for CLI entry points — argument parsing and error branches.

These exercise the deterministic validation paths (usage, bad input, dispatch)
without establishing real RNS links.
"""

from unittest.mock import MagicMock, patch

import pytest

from rns_icn import cli, cli_fetch, cli_publish, cli_subscribe
from rns_icn.config import ServerConfig
from rns_icn.name import Name
from rns_icn.packet import Data


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
    for builder in (
        cli_fetch._ephemeral_config,
        cli_publish._ephemeral_config,
        cli_subscribe._ephemeral_config,
    ):
        cfg = builder()
        assert isinstance(cfg, ServerConfig)
        assert cfg.http_enabled is False
        assert cfg.cs_path.endswith("content_store.db")


def test_subscribe_main_missing_args_exits_2(monkeypatch):
    # argparse exits 2 when required positionals are absent.
    monkeypatch.setattr(cli_subscribe.sys, "argv", ["icn-subscribe"])
    with pytest.raises(SystemExit) as exc:
        cli_subscribe.main()
    assert exc.value.code == 2


def test_subscribe_safe_filename_sanitizes_path():
    name = Name(bytes(16), [b"sensors", b"temp/3"])
    assert cli_subscribe._safe_filename(name) == "sensors_temp_3"


def test_subscribe_safe_filename_root_falls_back_to_addr():
    name = Name(bytes(16), [])
    assert cli_subscribe._safe_filename(name) == bytes(16).hex()


def test_subscribe_emit_writes_to_out_dir(tmp_path):
    name = Name(bytes(16), [b"feed", b"v1"])
    data = Data.new(name=name, content=b"hello").with_sequence(1)
    cli_subscribe._emit(data, index=1, out_dir=str(tmp_path))
    written = list(tmp_path.iterdir())
    assert len(written) == 1
    assert written[0].name == "000001_feed_v1"
    assert written[0].read_bytes() == b"hello"
