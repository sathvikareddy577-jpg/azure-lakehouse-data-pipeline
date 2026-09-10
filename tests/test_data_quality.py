"""Test data quality rules"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from tests.fixtures.sample_data import create_sample_customers, create_sample_orders
from src.silver import validate_date_format, validate_numeric_ranges


def test_date_validation(spark: SparkSession):
    """Test date format validation."""
    orders_df = create_sample_orders(spark, num_orders=100)
    
    # Add invalid dates
    from pyspark.sql.functions import when
    invalid_orders = orders_df.withColumn(
        "order_date", when(col("order_id") == "ORD_000001", "invalid-date").otherwise(col("order_date"))
    )
    
    valid_df, quarantine_df = validate_date_format(invalid_orders, {"order_date": "yyyy-MM-dd"})
    
    assert valid_df.count() + quarantine_df.count() == 100
    assert quarantine_df.count() > 0


def test_numeric_range_validation(spark: SparkSession):
    """Test numeric range validation."""
    customers_df = create_sample_customers(spark, num_records=100)
    
    # Add out-of-range ages
    from pyspark.sql.functions import when
    invalid_customers = customers_df.withColumn(
        "age", when(col("customer_id") == "CUST_00000", 200).otherwise(col("age"))
    )
    
    range_rules = {"age": (0, 150)}
    valid_df, quarantine_df = validate_numeric_ranges(invalid_customers, range_rules)
    
    assert valid_df.count() + quarantine_df.count() == 100


def test_business_rule_validation(spark: SparkSession):
    """Test business rule validation (order_amount >= 0)."""
    orders_df = create_sample_orders(spark, num_orders=100)
    
    # All should have positive amounts
    from pyspark.sql.functions import when
    invalid_orders = orders_df.withColumn(
        "order_amount", when(col("order_id") == "ORD_000001", "-100").otherwise(col("order_amount"))
    )
    
    range_rules = {"order_amount": (0, 100000)}
    valid_df, quarantine_df = validate_numeric_ranges(invalid_orders, range_rules)
    
    assert quarantine_df.count() > 0
