"""Delta-backed integration test for the entire pipeline."""

from __future__ import annotations

import logging
import random
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from scripts.generate_sample_data import (
    generate_customer_cdc,
    generate_customers,
    generate_orders,
    generate_products,
)
from scripts.run_pipeline import run_bronze_layer, run_gold_layer, run_silver_layer

pytestmark = pytest.mark.integration


def test_full_pipeline_reconciles_and_bronze_replay_is_stable(
    delta_spark: SparkSession,
    tmp_path: Path,
):
    raw = tmp_path / "data" / "raw"
    cdc = tmp_path / "data" / "cdc"
    warehouse = tmp_path / "warehouse"
    raw.mkdir(parents=True)
    cdc.mkdir(parents=True)
    warehouse.mkdir()
    rng = random.Random(42)
    generate_customers(raw, 20, rng)
    generate_products(raw, 10, rng)
    generate_orders(raw, 20, 10, 50, rng)
    cdc_path = generate_customer_cdc(cdc, 20, 6)
    logger = logging.getLogger("integration-test")

    first_bronze = run_bronze_layer(delta_spark, str(raw), str(warehouse), logger)
    second_bronze = run_bronze_layer(delta_spark, str(raw), str(warehouse), logger)
    assert (
        first_bronze
        == second_bronze
        == {
            "customers": 20,
            "products": 10,
            "orders": 50,
        }
    )

    silver = run_silver_layer(
        delta_spark,
        str(warehouse),
        logger,
        str(cdc_path),
    )
    gold = run_gold_layer(delta_spark, str(warehouse), logger)
    assert silver["quarantine_rows"]["customers"] >= 1
    assert silver["quarantine_rows"]["products"] >= 1
    assert silver["quarantine_rows"]["orders"] >= 4
    assert 0 < gold["fact_orders"] < 50
    assert gold["daily_sales_kpi"] > 0
