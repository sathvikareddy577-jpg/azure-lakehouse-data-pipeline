# Databricks notebook source
"""Parameterized Databricks notebook entry point for the medallion pipeline."""

# COMMAND ----------

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

repo_root = str(Path.cwd())
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from scripts.run_pipeline import run_bronze_layer, run_gold_layer, run_silver_layer
from src.common import get_logger, setup_logging

runtime: dict[str, Any] = globals()
spark_session = runtime.get("spark")
dbutils_runtime = runtime.get("dbutils")
if spark_session is None or dbutils_runtime is None:
    raise RuntimeError("This entry point must run inside an Azure Databricks notebook")

# COMMAND ----------

defaults = {
    "stage": "full",
    "raw_data_path": "abfss://raw@<storage-account>.dfs.core.windows.net",
    "warehouse_path": "abfss://warehouse@<storage-account>.dfs.core.windows.net",
    "cdc_path": "",
}
for widget_name, default_value in defaults.items():
    dbutils_runtime.widgets.text(widget_name, default_value)

stage = dbutils_runtime.widgets.get("stage").strip().lower()
raw_data_path = dbutils_runtime.widgets.get("raw_data_path").rstrip("/")
warehouse_path = dbutils_runtime.widgets.get("warehouse_path").rstrip("/")
cdc_path = dbutils_runtime.widgets.get("cdc_path").strip() or None

if stage not in {"bronze", "silver", "gold", "full"}:
    raise ValueError(f"Unsupported stage: {stage}")
if "<storage-account>" in raw_data_path or "<storage-account>" in warehouse_path:
    raise ValueError("Replace <storage-account> in the Databricks widget paths")

setup_logging("INFO")
logger = get_logger("databricks")

# COMMAND ----------

if stage in {"bronze", "full"}:
    run_bronze_layer(spark_session, raw_data_path, warehouse_path, logger)

if stage in {"silver", "full"}:
    run_silver_layer(spark_session, warehouse_path, logger, cdc_path)

if stage in {"gold", "full"}:
    run_gold_layer(spark_session, warehouse_path, logger)

logger.info("Databricks stage completed: %s", stage)
dbutils_runtime.notebook.exit(f"SUCCESS: {stage}")
