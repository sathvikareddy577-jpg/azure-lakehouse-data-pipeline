"""Databricks Auto Loader configuration for incremental ingestion"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


def setup_autoloader_customers(spark: SparkSession, source_path: str, checkpoint_path: str, target_table: str):
    """
    Setup Auto Loader for incremental customer data ingestion.
    
    Args:
        spark: Spark session
        source_path: ADLS path to customer CSV files (e.g., abfss://raw@lakehouse.dfs.core.windows.net/customers/)
        checkpoint_path: Checkpoint location for Auto Loader state
        target_table: Target Delta table (e.g., bronze.customers)
    
    Example:
        source_path = "abfss://raw@lakehousedata.dfs.core.windows.net/customers/"
        checkpoint = "abfss://warehouse@lakehousedata.dfs.core.windows.net/_checkpoints/customers"
        setup_autoloader_customers(spark, source_path, checkpoint, "bronze.customers")
    """
    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
        .option("header", "true") \
        .load(source_path)
    
    df = df.withColumn("load_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("autoloader"))
    
    query = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint_path}/state") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .table(target_table)
    
    return query


def setup_autoloader_orders(spark: SparkSession, source_path: str, checkpoint_path: str, target_table: str):
    """Setup Auto Loader for incremental order data ingestion."""
    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
        .option("header", "true") \
        .load(source_path)
    
    df = df.withColumn("load_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("autoloader"))
    
    query = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint_path}/state") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .table(target_table)
    
    return query


def setup_autoloader_cdc(spark: SparkSession, source_path: str, checkpoint_path: str):
    """
    Setup Auto Loader for CDC events (JSON format).
    
    CDC events are read as a stream and processed for idempotent merges.
    """
    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
        .load(source_path)
    
    return df
