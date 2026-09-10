"""Bronze-layer ingestion with source lineage and replay protection."""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def _decorate_with_lineage(df: DataFrame, source_system: str) -> DataFrame:
    """Add deterministic metadata used to make file replays idempotent."""
    business_columns = sorted(df.columns)
    normalized_values = [
        F.coalesce(F.col(name).cast("string"), F.lit("<NULL>")) for name in business_columns
    ]
    return (
        df.withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
        .withColumn("_record_hash", F.sha2(F.concat_ws("||", *normalized_values), 256))
        .dropDuplicates(["_source_system", "_record_hash"])
    )


def _merge_new_records(df: DataFrame, target_path: str) -> int:
    """Insert unseen source records and return the resulting table count."""
    spark = df.sparkSession
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("target")
            .merge(
                df.alias("source"),
                "target._source_system = source._source_system "
                "AND target._record_hash = source._record_hash",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target_path)
        )

    return spark.read.format("delta").load(target_path).count()


def ingest_csv_to_bronze(
    spark: SparkSession,
    file_path: str,
    schema: StructType,
    table_name: str,
    warehouse_path: str,
    source_system: str = "csv_file",
) -> DataFrame:
    """Read a CSV with an explicit schema and merge unseen rows into Bronze."""
    df = spark.read.option("header", True).schema(schema).csv(file_path)
    decorated = _decorate_with_lineage(df, source_system)
    target_path = f"{warehouse_path}/bronze/{table_name}"
    total_count = _merge_new_records(decorated, target_path)
    logger.info(
        "Bronze %s: %s source rows, %s rows after idempotent merge",
        table_name,
        decorated.count(),
        total_count,
    )
    return decorated


def ingest_json_to_bronze(
    spark: SparkSession,
    file_path: str,
    table_name: str,
    warehouse_path: str,
    source_system: str = "json_file",
    schema: StructType | None = None,
) -> DataFrame:
    """Read newline-delimited JSON and merge unseen rows into Bronze."""
    reader = spark.read
    if schema is not None:
        reader = reader.schema(schema)
    df = reader.json(file_path)
    decorated = _decorate_with_lineage(df, source_system)
    target_path = f"{warehouse_path}/bronze/{table_name}"
    total_count = _merge_new_records(decorated, target_path)
    logger.info(
        "Bronze %s: %s source rows, %s rows after idempotent merge",
        table_name,
        decorated.count(),
        total_count,
    )
    return decorated


def read_bronze_table(
    spark: SparkSession,
    table_name: str,
    warehouse_path: str,
) -> DataFrame:
    """Read a Bronze Delta table."""
    return spark.read.format("delta").load(f"{warehouse_path}/bronze/{table_name}")
