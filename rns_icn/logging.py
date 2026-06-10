"""Structured logging for ICN — JSON formatter and setup."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict, Union

from .config import ClientConfig, ServerConfig


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "levelname", "levelno",
                "pathname", "filename", "module", "lineno",
                "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process",
                "exc_info", "exc_text", "stack_info", "getMessage",
            }:
                log_entry[key] = value
        return json.dumps(log_entry)


def setup_logging(config: Union[ClientConfig, ServerConfig]) -> None:
    """Configure logging based on config."""
    level = getattr(logging, config.log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    if config.log_json:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        ))

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]

    # Also silence RNS verbose logs unless DEBUG
    if level > logging.DEBUG:
        logging.getLogger("RNS").setLevel(logging.WARNING)