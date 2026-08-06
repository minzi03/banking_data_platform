"""
Tests for code_etl/shared/utils/logger.py

Covers:
  - get_logger: returns logger, correct name, handler setup, idempotency
"""

import importlib.util
import logging
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Direct import via importlib to avoid package name conflicts
_spec = importlib.util.spec_from_file_location(
    "logger_mod",
    str(PROJECT_ROOT / "code_etl" / "shared" / "utils" / "logger.py")
)
_logger_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_logger_mod)

get_logger = _logger_mod.get_logger


class TestGetLogger:
    """Tests for the structured logger factory."""

    def test_returns_logger_instance(self):
        """Should return a logging.Logger instance."""
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)

    def test_logger_has_correct_name(self):
        """Should set the logger name correctly."""
        logger = get_logger("my_etl_job")
        assert logger.name == "my_etl_job"

    def test_logger_has_stdout_handler(self):
        """Should add a StreamHandler writing to stdout."""
        # get_logger adds handler only if logger.hasHandlers() is False.
        # Python's hasHandlers() checks parent loggers too, so test the
        # handler configuration by calling get_logger on a fresh logger.
        logger = logging.getLogger("test_stdout_handler_fresh_9x7")
        logger.handlers.clear()
        logger.parent.handlers.clear()
        result = get_logger("test_stdout_handler_fresh_9x7")
        stream_handlers = [h for h in result.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) >= 1

    def test_logger_level_is_info(self):
        """Should set log level to INFO."""
        logger = logging.getLogger("test_level_info_fresh_9x7")
        logger.handlers.clear()
        logger.parent.handlers.clear()
        result = get_logger("test_level_info_fresh_9x7")
        assert result.level == logging.INFO

    def test_logger_format_contains_expected_parts(self):
        """Should format messages with timestamp, name, level."""
        logger = logging.getLogger("test_format_parts_fresh_9x7")
        logger.handlers.clear()
        logger.parent.handlers.clear()
        result = get_logger("test_format_parts_fresh_9x7")
        handler = result.handlers[0]
        formatter = handler.formatter
        assert formatter is not None
        fmt = formatter._fmt
        assert "%(asctime)s" in fmt
        assert "%(name)s" in fmt
        assert "%(levelname)s" in fmt
        assert "%(message)s" in fmt

    def test_idempotent_returns_same_logger(self):
        """Should return the same logger on repeated calls (idempotent)."""
        logger1 = get_logger("test_idempotent")
        logger2 = get_logger("test_idempotent")
        assert logger1 is logger2

    def test_different_names_return_different_loggers(self):
        """Different names should produce different logger instances."""
        logger_a = get_logger("logger_a")
        logger_b = get_logger("logger_b")
        assert logger_a is not logger_b

    def test_logger_can_log(self):
        """Should not raise when logging a message."""
        logger = get_logger("test_can_log")
        logger.info("Test message %s", "with args")
        logger.warning("Warning message")
        logger.error("Error message")
