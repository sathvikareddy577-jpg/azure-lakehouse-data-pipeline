"""Delta integration tests for real customer CDC behavior."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from src.data_models import CUSTOMER_CDC_SCHEMA
from src.silver import merge_customers_cdc, transform_customers
from tests.fixtures.sample_data import create_sample_customers

pytestmark = pytest.mark.integration


def _write_customer_target(
    spark: SparkSession,
    warehouse: str,
    count: int = 2,
) -> str:
    path = f"{warehouse}/silver"
    transform_customers(create_sample_customers(spark, count)).write.format("delta").mode(
        "overwrite"
    ).save(f"{path}/silver_customers")
    return path


def _cdc(spark: SparkSession, rows: list[tuple]):
    return spark.createDataFrame(rows, CUSTOMER_CDC_SCHEMA)


def test_cdc_insert_is_replay_safe(delta_spark: SparkSession, tmp_warehouse: str):
    silver_path = _write_customer_target(delta_spark, tmp_warehouse)
    event_time = datetime(2026, 9, 10, 12, 0)
    events = _cdc(
        delta_spark,
        [("E1", "CUST_NEW", "INSERT", "New User", "new@example.com", 30, "usa", event_time, "crm")],
    )
    assert merge_customers_cdc(delta_spark, silver_path, events) == 3
    assert merge_customers_cdc(delta_spark, silver_path, events) == 3


def test_cdc_update_preserves_missing_attributes(
    delta_spark: SparkSession,
    tmp_warehouse: str,
):
    silver_path = _write_customer_target(delta_spark, tmp_warehouse)
    events = _cdc(
        delta_spark,
        [
            (
                "E2",
                "CUST_00000",
                "UPDATE",
                "Changed Name",
                None,
                None,
                None,
                datetime(2026, 9, 10, 13, 0),
                "crm",
            )
        ],
    )
    merge_customers_cdc(delta_spark, silver_path, events)
    row = (
        delta_spark.read.format("delta")
        .load(f"{silver_path}/silver_customers")
        .filter("customer_id = 'CUST_00000'")
        .first()
    )
    assert row.name == "Changed Name"
    assert row.email == "customer0@example.com"


def test_cdc_delete_is_soft_delete(delta_spark: SparkSession, tmp_warehouse: str):
    silver_path = _write_customer_target(delta_spark, tmp_warehouse)
    events = _cdc(
        delta_spark,
        [
            (
                "E3",
                "CUST_00001",
                "DELETE",
                None,
                None,
                None,
                None,
                datetime(2026, 9, 10, 14, 0),
                "crm",
            )
        ],
    )
    merge_customers_cdc(delta_spark, silver_path, events)
    row = (
        delta_spark.read.format("delta")
        .load(f"{silver_path}/silver_customers")
        .filter("customer_id = 'CUST_00001'")
        .first()
    )
    assert row.is_deleted is True
    assert row.end_date.isoformat() == "2026-09-10"


def test_cdc_uses_latest_event_per_customer(
    delta_spark: SparkSession,
    tmp_warehouse: str,
):
    silver_path = _write_customer_target(delta_spark, tmp_warehouse)
    timestamp = datetime(2026, 9, 10, 15, 0)
    events = _cdc(
        delta_spark,
        [
            ("E4", "CUST_00000", "UPDATE", "Old Name", None, None, None, timestamp, "crm"),
            (
                "E5",
                "CUST_00000",
                "UPDATE",
                "Latest Name",
                None,
                None,
                None,
                timestamp + timedelta(minutes=1),
                "crm",
            ),
        ],
    )
    merge_customers_cdc(delta_spark, silver_path, events)
    row = (
        delta_spark.read.format("delta")
        .load(f"{silver_path}/silver_customers")
        .filter("customer_id = 'CUST_00000'")
        .first()
    )
    assert row.name == "Latest Name"
