"""Azure Databricks Auto Loader helpers for incremental cloud-file ingestion."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType


def build_autoloader_stream(
    spark: SparkSession,
    source_path: str,
    checkpoint_path: str,
    source_format: str,
    schema: StructType,
) -> DataFrame:
    """Create a schema-controlled Auto Loader stream with rescued data."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .schema(schema)
        .load(source_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_system", F.lit("databricks_autoloader"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_autoloader_table(
    stream: DataFrame,
    target_table: str,
    checkpoint_path: str,
    available_now: bool = True,
) -> StreamingQuery:
    """Write a Delta stream with an isolated checkpoint per target table."""
    writer = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/state")
        .option("mergeSchema", "true")
        .outputMode("append")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.toTable(target_table)
