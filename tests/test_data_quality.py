"""Unit tests for dates, numeric rules, and referential integrity."""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import SparkSession

from src.silver import (
    combine_quarantine,
    validate_date_format,
    validate_numeric_ranges,
    validate_references,
)


def test_date_validation_parses_and_quarantines(spark: SparkSession):
    source = spark.createDataFrame(
        [("1", "2026-09-10"), ("2", "invalid"), ("3", None)],
        ["id", "event_date"],
    )
    valid, quarantine = validate_date_format(source, {"event_date": "yyyy-MM-dd"})
    assert valid.count() == 2
    assert quarantine is not None and quarantine.count() == 1
    assert valid.filter("id = '1'").first().event_date == date(2026, 9, 10)


def test_date_validation_honors_requested_format(spark: SparkSession):
    source = spark.createDataFrame([("09/10/2026",)], ["event_date"])
    valid, quarantine = validate_date_format(source, {"event_date": "MM/dd/yyyy"})
    assert quarantine is not None and quarantine.count() == 0
    assert valid.first().event_date == date(2026, 9, 10)


def test_numeric_validation_catches_range_and_type(spark: SparkSession):
    source = spark.createDataFrame(
        [("1", "50"), ("2", "200"), ("3", "not-a-number")],
        ["id", "age"],
    )
    valid, quarantine = validate_numeric_ranges(source, {"age": (0, 150)})
    assert valid.count() == 1
    assert quarantine is not None and quarantine.count() == 2


def test_reference_validation_quarantines_orphan(spark: SparkSession):
    orders = spark.createDataFrame([("O1", "C1"), ("O2", "C9")], ["order_id", "customer_id"])
    customers = spark.createDataFrame([("C1",)], ["customer_id"])
    valid, quarantine = validate_references(
        orders,
        {"customer_id": (customers, "customer_id")},
    )
    assert [row.order_id for row in valid.collect()] == ["O1"]
    assert quarantine is not None
    assert [row.order_id for row in quarantine.collect()] == ["O2"]


def test_combine_quarantine_preserves_missing_columns(spark: SparkSession):
    first = spark.createDataFrame([("1", "reason-a")], ["id", "rejection_reason"])
    second = spark.createDataFrame([("x", "reason-b")], ["code", "rejection_reason"])
    combined = combine_quarantine(first, None, second)
    assert combined is not None
    assert combined.count() == 2
    assert set(combined.columns) == {"id", "code", "rejection_reason"}


def test_reference_validation_requires_local_column(spark: SparkSession):
    source = spark.createDataFrame([("1",)], ["id"])
    reference = spark.createDataFrame([("C1",)], ["customer_id"])
    with pytest.raises(ValueError, match="customer_id"):
        validate_references(source, {"customer_id": (reference, "customer_id")})
