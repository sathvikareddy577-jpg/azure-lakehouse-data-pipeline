"""Configuration management for local and Azure pipeline runs."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    """Environment-backed defaults shared by scripts and notebooks."""

    DATA_PATH = os.getenv("DATA_PATH", "data")
    WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "data/warehouse")
    SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "lakehouse-pipeline")
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE")

    ADLS_STORAGE_ACCOUNT = os.getenv("ADLS_STORAGE_ACCOUNT", "")
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")

    MAX_NULL_RATIO = float(os.getenv("MAX_NULL_RATIO", "0.05"))
    DUPLICATE_THRESHOLD = float(os.getenv("DUPLICATE_THRESHOLD", "0.02"))

    @staticmethod
    def ensure_paths(data_path: str, warehouse_path: str) -> None:
        """Create only the local directories required for a pipeline run."""
        if data_path.startswith("abfss://") or warehouse_path.startswith("abfss://"):
            return

        for path in (
            Path(data_path),
            Path(warehouse_path) / "bronze",
            Path(warehouse_path) / "silver",
            Path(warehouse_path) / "gold",
            Path(warehouse_path) / "quarantine",
            Path(warehouse_path) / "audit",
        ):
            path.mkdir(parents=True, exist_ok=True)
