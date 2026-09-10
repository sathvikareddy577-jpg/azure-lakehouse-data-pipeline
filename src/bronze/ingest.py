"""Bronze layer raw data ingestion"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import StructType
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def ingest_csv_to_bronze(
    spark: SparkSession,
    file_path: str,
    schema: StructType,
    table_name: str,
    warehouse_path: str,
    source_system: str = "csv_file",
) -> DataFrame:
    """
    Ingest CSV file to Bronze layer with source tracking.
    
    Args:
        spark: Spark session
        file_path: Path to CSV file
        schema: Spark schema for validation
        table_name: Name of the table
        warehouse_path: Base warehouse path
        source_system: Source system identifier
    
    Returns:
        DataFrame written to Bronze
    """
    try:
        # Read CSV
        df = spark.read.csv(file_path, header=True, schema=schema)
        
        # Add metadata
        df = df.withColumn("load_timestamp", current_timestamp())
        df = df.withColumn("source_system", lit(source_system))
        
        # Write to Delta table (Bronze - append mode)
        bronze_path = f"{warehouse_path}/bronze/{table_name}"
        df.write.format("delta").mode("append").save(bronze_path)
        
        count = df.count()
        logger.info(f"✓ Bronze {table_name}: {count} records ingested")
        
        return df
    except Exception as e:
        logger.error(f"✗ Failed to ingest {table_name}: {e}")
        raise


def ingest_json_to_bronze(
    spark: SparkSession,
    file_path: str,
    table_name: str,
    warehouse_path: str,
    source_system: str = "json_file",
) -> DataFrame:
    """Ingest JSON file to Bronze layer."""
    try:
        # Read JSON (schema will be inferred)
        df = spark.read.json(file_path)
        
        # Add metadata
        df = df.withColumn("load_timestamp", current_timestamp())
        df = df.withColumn("source_system", lit(source_system))
        
        # Write to Delta table
        bronze_path = f"{warehouse_path}/bronze/{table_name}"
        df.write.format("delta").mode("append").save(bronze_path)
        
        count = df.count()
        logger.info(f"✓ Bronze {table_name}: {count} records ingested")
        
        return df
    except Exception as e:
        logger.error(f"✗ Failed to ingest {table_name}: {e}")
        raise


def read_bronze_table(spark: SparkSession, table_name: str, warehouse_path: str) -> DataFrame:
    """Read Bronze Delta table."""
    bronze_path = f"{warehouse_path}/bronze/{table_name}"
    return spark.read.format("delta").load(bronze_path)
