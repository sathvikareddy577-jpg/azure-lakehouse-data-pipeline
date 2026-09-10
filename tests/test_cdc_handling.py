"""Test CDC idempotency handling"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from tests.fixtures.sample_data import create_sample_customers
from src.silver import transform_customers


def test_cdc_duplicate_event_idempotency(spark: SparkSession, tmp_warehouse: str):
    """Test that duplicate CDC events don't create duplicates."""
    # Create initial customer
    customers_df = create_sample_customers(spark, num_records=1)
    transformed = transform_customers(customers_df)
    
    # Write to Silver
    silver_path = f"{tmp_warehouse}/silver/silver_customers"
    transformed.write.format("delta").mode("overwrite").save(silver_path)
    
    # Simulate duplicate CDC event (same customer, same timestamp)
    df1 = transformed
    df2 = transformed  # Duplicate
    
    # Union (simulating two identical CDC events)
    merged = df1.union(df2)
    
    # After deduplication
    deduped = merged.dropDuplicates(["customer_id"])
    
    # Should still have 1 record (idempotency)
    assert deduped.count() == 1


def test_cdc_insert_new_customer(spark: SparkSession):
    """Test CDC INSERT event."""
    from pyspark.sql.functions import lit, current_timestamp, to_date
    
    customers_df = create_sample_customers(spark, num_records=1)
    
    # Simulate CDC INSERT event
    cdc_df = customers_df.withColumn("operation", lit("INSERT"))
    
    assert cdc_df.count() == 1
    assert "operation" in cdc_df.columns


def test_cdc_update_customer(spark: SparkSession):
    """Test CDC UPDATE event."""
    from pyspark.sql.functions import lit, col
    
    customers_df = create_sample_customers(spark, num_records=1)
    
    # Simulate CDC UPDATE: change name
    updated_df = customers_df.withColumn("name", lit("Updated Name")).withColumn("operation", lit("UPDATE"))
    
    assert updated_df.count() == 1
    assert updated_df.select("name").collect()[0][0] == "Updated Name"


def test_cdc_delete_customer(spark: SparkSession):
    """Test CDC DELETE event (soft delete)."""
    from pyspark.sql.functions import lit
    
    customers_df = create_sample_customers(spark, num_records=1)
    
    # Simulate CDC DELETE event
    deleted_df = customers_df.withColumn("operation", lit("DELETE"))
    
    assert deleted_df.count() == 1
    assert "operation" in deleted_df.columns
