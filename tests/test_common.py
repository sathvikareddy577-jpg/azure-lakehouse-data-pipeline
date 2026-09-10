"""Tests for configuration, logging, and Spark lifecycle helpers."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import Mock

from src.common import Config, get_logger, setup_logging, stop_spark_session


def test_ensure_paths_creates_local_layer_directories(tmp_path: Path):
    data = tmp_path / "raw"
    warehouse = tmp_path / "warehouse"
    Config.ensure_paths(str(data), str(warehouse))
    assert data.is_dir()
    for layer in ("bronze", "silver", "gold", "quarantine", "audit"):
        assert (warehouse / layer).is_dir()


def test_ensure_paths_ignores_cloud_uris(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Config.ensure_paths("abfss://raw@account", "abfss://warehouse@account")
    assert list(tmp_path.iterdir()) == []


def test_setup_logging_does_not_duplicate_handlers():
    logger = setup_logging("INFO")
    logger = setup_logging("DEBUG")
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1


def test_setup_logging_writes_optional_file(tmp_path: Path):
    path = tmp_path / "logs" / "pipeline.log"
    logger = setup_logging("INFO", str(path))
    logger.info("verified-message")
    for handler in logger.handlers:
        handler.flush()
    assert "verified-message" in path.read_text(encoding="utf-8")


def test_get_logger_uses_project_namespace():
    assert get_logger("quality").name == "lakehouse.quality"


def test_stop_spark_session_calls_stop():
    session = Mock()
    stop_spark_session(session)
    session.stop.assert_called_once_with()
    stop_spark_session(None)
