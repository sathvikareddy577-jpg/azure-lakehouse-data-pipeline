"""Test Silver layer transformations"""

import pytest
from pyspark.sql import SparkSession
from tests.fixtures.sample_data import create_sample_customers, create_sample_orders
from src.silver import (
    transform_customers,
    validate_nulls,
    remove_duplicates,
    calculate_quality_metrics,
)


def test_transform_customers(spark: SparkSession):
    """Test customer transformation."""
    customers_df = create_sample_customers(spark, num_records=10)
    transformed = transform_customers(customers_df)
    
    assert transformed.count() == 10
    assert "is_deleted" in transformed.columns
    assert "effective_date" in transformed.columns
    assert "updated_at" in transformed.columns


def test_validate_nulls(spark: SparkSession):
    """Test null value validation."""
    customers_df = create_sample_customers(spark, num_records=10)
    
    # Add some nulls
    from pyspark.sql.functions import when, col
    customers_with_nulls = customers_df.withColumn(
        "email", when(col("age") > 50, None).otherwise(col("email"))
    )
    
    valid_df, quarantine_df = validate_nulls(customers_with_nulls, ["customer_id", "email"])
    
    assert valid_df.count() + quarantine_df.count() == 10
    assert quarantine_df.count() > 0


def test_remove_duplicates(spark: SparkSession):
    """Test duplicate removal."""
    customers_df = create_sample_customers(spark, num_records=5)
    duplicated_df = customers_df.union(customers_df)
    
    deduped_df, removed_count = remove_duplicates(duplicated_df, ["customer_id"])
    
    assert deduped_df.count() == 5
    assert removed_count == 5


def test_calculate_quality_metrics(spark: SparkSession):
    """Test quality metrics calculation."""
    original_df = create_sample_customers(spark, num_records=100)
    
    from pyspark.sql.functions import when, col
    valid_df = original_df.filter(col("age").isNotNull())
    quarantine_df = original_df.filter(col("age").isNull())
    
    metrics = calculate_quality_metrics(original_df, valid_df, quarantine_df)
    
    assert metrics["original_records"] == 100
    assert metrics["valid_records"] + metrics["quarantine_records"] == 100
    assert metrics["quality_percentage"] >= 0
