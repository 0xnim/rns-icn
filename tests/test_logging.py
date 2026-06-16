"""Tests for icn_logging — JSON formatter and setup."""

import json
import logging

from rns_icn.config import ClientConfig
from rns_icn.icn_logging import JSONFormatter, setup_logging


def _make_record(msg="hello", **extra):
    record = logging.LogRecord(
        name="rns_icn.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )
    for k, v in extra.items():
        setattr(record, k, v)
    return record


def test_json_formatter_emits_valid_json_with_core_fields():
    out = JSONFormatter().format(_make_record("a message"))
    parsed = json.loads(out)

    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "rns_icn.test"
    assert parsed["message"] == "a message"
    assert "timestamp" in parsed


def test_json_formatter_includes_extra_fields_but_not_internal():
    out = JSONFormatter().format(_make_record(peer="abc123", hops=3))
    parsed = json.loads(out)

    assert parsed["peer"] == "abc123"
    assert parsed["hops"] == 3
    # Internal LogRecord attributes must not leak into the payload.
    assert "args" not in parsed
    assert "pathname" not in parsed


def test_setup_logging_json_installs_single_handler():
    setup_logging(ClientConfig(log_json=True, log_level="DEBUG"))
    root = logging.getLogger()

    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JSONFormatter)
    assert root.level == logging.DEBUG


def test_setup_logging_is_idempotent():
    cfg = ClientConfig(log_json=False, log_level="WARNING")
    setup_logging(cfg)
    setup_logging(cfg)
    root = logging.getLogger()

    # Repeated setup replaces handlers rather than accumulating them.
    assert len(root.handlers) == 1
    assert root.level == logging.WARNING
    # Non-DEBUG level should quiet the noisy RNS logger.
    assert logging.getLogger("RNS").level == logging.WARNING
