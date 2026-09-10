"""Test Bronze layer ingestion"""

import pytest
from pyspark.sql import SparkSession
import os
from tests.fixtures.sample_data import create_sample_customers, create_sample_products, create_sample_orders
from src.bronze import ingest_csv_to_bronze
from src.data_models import BRONZE_CUSTOMERS_SCHEMA


def test_bronze_ingest_csv(spark: SparkSession, tmp_warehouse: str, tmp_data: str):
    """Test CSV ingestion to Bronze layer."""
    # Create sample data
    customers_df = create_sample_customers(spark, num_records=10)
    csv_path = os.path.join(tmp_data, "customers.csv")
    customers_df.write.csv(csv_path, header=True, mode="overwrite")
    
    # Ingest to Bronze
    result = ingest_csv_to_bronze(
        spark,
        csv_path,
        BRONZE_CUSTOMERS_SCHEMA,
        "customers",
        tmp_warehouse,
        source_system="test_system"
    )
    
    # Verify
    assert result.count() == 10
    assert "load_timestamp" in result.columns
    assert "source_system" in result.columns
