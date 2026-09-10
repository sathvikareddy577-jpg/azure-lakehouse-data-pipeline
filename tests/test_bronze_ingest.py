"""Delta integration tests for replay-safe Bronze ingestion."""

from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession

from src.bronze import ingest_csv_to_bronze
from src.data_models import BRONZE_CUSTOMERS_SCHEMA
from tests.fixtures.sample_data import create_sample_customers

pytestmark = pytest.mark.integration


def test_bronze_ingest_adds_lineage(
    delta_spark: SparkSession,
    tmp_warehouse: str,
    tmp_data: str,
):
    source = create_sample_customers(delta_spark, 10)
    csv_path = os.path.join(tmp_data, "customers")
    source.write.csv(csv_path, header=True, mode="overwrite")
    result = ingest_csv_to_bronze(
        delta_spark,
        csv_path,
        BRONZE_CUSTOMERS_SCHEMA,
        "customers",
        tmp_warehouse,
        "unit-test",
    )
    assert result.count() == 10
    assert {
        "_source_system",
        "_source_file",
        "_ingested_at",
        "_record_hash",
    }.issubset(result.columns)


def test_bronze_file_replay_is_idempotent(
    delta_spark: SparkSession,
    tmp_warehouse: str,
    tmp_data: str,
):
    source = create_sample_customers(delta_spark, 5)
    csv_path = os.path.join(tmp_data, "customers")
    source.write.csv(csv_path, header=True, mode="overwrite")
    arguments = (
        delta_spark,
        csv_path,
        BRONZE_CUSTOMERS_SCHEMA,
        "customers",
        tmp_warehouse,
        "unit-test",
    )
    ingest_csv_to_bronze(*arguments)
    ingest_csv_to_bronze(*arguments)
    stored = delta_spark.read.format("delta").load(f"{tmp_warehouse}/bronze/customers")
    assert stored.count() == 5
