"""Spark session and utilities management"""

from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "lakehouse-pipeline", warehouse_path: str = None) -> SparkSession:
    """Create or get Spark session with Delta Lake support."""
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "10")
    
    if warehouse_path:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_path)
    
    try:
        spark = builder.getOrCreate()
        logger.info(f"✓ Spark session created: {app_name}")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {e}")
        raise


def stop_spark_session(spark: SparkSession) -> None:
    """Stop Spark session gracefully."""
    if spark:
        spark.stop()
        logger.info("✓ Spark session stopped")
