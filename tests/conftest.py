"""Test configuration and fixtures"""

import pytest
from pyspark.sql import SparkSession
import tempfile
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests."""
    spark_session = SparkSession.builder \
        .appName("test-lakehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .master("local[2]") \
        .getOrCreate()
    
    yield spark_session
    
    spark_session.stop()


@pytest.fixture(scope="session")
def tmp_warehouse():
    """Create temporary warehouse directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        warehouse_path = os.path.join(tmp_dir, "warehouse")
        Path(warehouse_path).mkdir(parents=True, exist_ok=True)
        
        # Create layer subdirectories
        for layer in ["bronze", "silver", "gold", "quarantine"]:
            Path(os.path.join(warehouse_path, layer)).mkdir(exist_ok=True)
        
        yield warehouse_path


@pytest.fixture(scope="session")
def tmp_data():
    """Create temporary data directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        data_path = os.path.join(tmp_dir, "data")
        Path(data_path).mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        for subdir in ["raw", "cdc"]:
            Path(os.path.join(data_path, subdir)).mkdir(exist_ok=True)
        
        yield data_path
