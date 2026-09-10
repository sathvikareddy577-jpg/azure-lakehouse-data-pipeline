"""Unit tests for Silver transformations and common quality metrics."""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from src.silver import (
    calculate_quality_metrics,
    remove_duplicates,
    transform_customers,
    transform_orders,
    transform_products,
    validate_nulls,
)
from tests.fixtures.sample_data import create_sample_customers


def test_transform_customers_normalizes_values(spark: SparkSession):
    source = spark.createDataFrame(
        [(" CUST_1 ", " jane DOE ", " JANE@EXAMPLE.COM ", "42", " us ")],
        ["customer_id", "name", "email", "age", "country"],
    )
    row = transform_customers(source).first()
    assert row.customer_id == "CUST_1"
    assert row.name == "Jane Doe"
    assert row.email == "jane@example.com"
    assert row.age == 42
    assert row.country == "US"
    assert row.is_deleted is False
    assert row.effective_date == date.today()


def test_transform_products_casts_price(spark: SparkSession):
    source = spark.createDataFrame(
        [(" P1 ", " Phone ", " electronics ", "499.95")],
        ["product_id", "product_name", "category", "unit_price"],
    )
    row = transform_products(source).first()
    assert row.product_id == "P1"
    assert row.category == "ELECTRONICS"
    assert row.unit_price == pytest.approx(499.95)
    assert row.is_active is True


def test_transform_orders_exposes_bad_casts_as_null(spark: SparkSession):
    source = spark.createDataFrame(
        [("O1", "C1", "P1", "bad-date", "not-money", "x")],
        [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "order_amount",
            "order_quantity",
        ],
    )
    row = transform_orders(source).first()
    assert row.order_date is None
    assert row.order_amount is None
    assert row.order_quantity is None


def test_transform_defaults_missing_source_metadata(spark: SparkSession):
    transformed = transform_customers(create_sample_customers(spark, 1))
    assert transformed.first().source_system == "unknown"


def test_validate_nulls_catches_null_and_blank(spark: SparkSession):
    source = spark.createDataFrame(
        [("1", "ok"), ("2", " "), ("3", None)],
        ["id", "email"],
    )
    valid, quarantine = validate_nulls(source, ["email"])
    assert valid.count() == 1
    assert quarantine.count() == 2
    assert "Missing required value" in quarantine.first().rejection_reason


def test_validate_nulls_rejects_empty_rule_list(spark: SparkSession):
    source = spark.createDataFrame([Row(id="1")])
    with pytest.raises(ValueError, match="at least one"):
        validate_nulls(source, [])


def test_remove_duplicates_reports_count(spark: SparkSession):
    source = spark.createDataFrame([("1",), ("1",), ("2",)], ["id"])
    deduplicated, removed = remove_duplicates(source, ["id"])
    assert deduplicated.count() == 2
    assert removed == 1


def test_quality_metrics_are_record_level(spark: SparkSession):
    source = spark.range(10)
    valid = source.filter(F.col("id") < 8)
    quarantine = source.filter(F.col("id") >= 8)
    metrics = calculate_quality_metrics(source, valid, quarantine)
    assert metrics == {
        "original_records": 10.0,
        "valid_records": 8.0,
        "quarantine_records": 2.0,
        "quality_percentage": 80.0,
    }
