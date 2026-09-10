"""Silver-layer transformations, quarantine writes, and customer CDC merge."""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def _column_or_literal(df: DataFrame, name: str, default: str) -> F.Column:
    return F.col(name) if name in df.columns else F.lit(default)


def transform_customers(df: DataFrame) -> DataFrame:
    """Standardize raw customer records for Silver validation."""
    return df.select(
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.initcap(F.trim(F.col("name"))).alias("name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.col("age").cast("int").alias("age"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        F.lit(False).alias("is_deleted"),
        F.current_date().alias("effective_date"),
        F.lit(None).cast("date").alias("end_date"),
        _column_or_literal(df, "_source_system", "unknown").alias("source_system"),
        F.current_timestamp().alias("updated_at"),
    )


def transform_products(df: DataFrame) -> DataFrame:
    """Standardize raw product records for Silver validation."""
    return df.select(
        F.trim(F.col("product_id")).alias("product_id"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.upper(F.trim(F.col("category"))).alias("category"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.lit(True).alias("is_active"),
        F.current_date().alias("effective_date"),
        F.lit(None).cast("date").alias("end_date"),
        _column_or_literal(df, "_source_system", "unknown").alias("source_system"),
        F.current_timestamp().alias("updated_at"),
    )


def transform_orders(df: DataFrame) -> DataFrame:
    """Cast and standardize raw order records for Silver validation."""
    return df.select(
        F.trim(F.col("order_id")).alias("order_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("product_id")).alias("product_id"),
        F.to_date(F.col("order_date"), "yyyy-MM-dd").alias("order_date"),
        F.col("order_amount").cast("double").alias("order_amount"),
        F.col("order_quantity").cast("int").alias("order_quantity"),
        F.lit(False).alias("is_deleted"),
        _column_or_literal(df, "_source_system", "unknown").alias("source_system"),
        F.current_timestamp().alias("updated_at"),
    )


def merge_customers_cdc(
    spark: SparkSession,
    silver_path: str,
    cdc_df: DataFrame,
) -> int:
    """Apply the latest INSERT/UPDATE/DELETE event per customer with Delta MERGE.

    Duplicate event IDs are removed before the merge. Replaying the same input
    therefore produces the same customer state and never creates a second row.
    """
    required = {"event_id", "customer_id", "operation", "event_timestamp"}
    missing = sorted(required - set(cdc_df.columns))
    if missing:
        raise ValueError(f"CDC data is missing columns: {', '.join(missing)}")

    target_path = f"{silver_path}/silver_customers"
    if not DeltaTable.isDeltaTable(spark, target_path):
        raise ValueError(f"Silver customer table does not exist: {target_path}")

    latest_window = Window.partitionBy("customer_id").orderBy(
        F.col("event_timestamp").desc(),
        F.col("event_id").desc(),
    )
    source = (
        cdc_df.withColumn("operation", F.upper(F.trim(F.col("operation"))))
        .filter(F.col("operation").isin("INSERT", "UPDATE", "DELETE"))
        .dropDuplicates(["event_id"])
        .withColumn("__row_number", F.row_number().over(latest_window))
        .filter(F.col("__row_number") == 1)
        .drop("__row_number")
    )

    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("target")
        .merge(source.alias("source"), "target.customer_id = source.customer_id")
        .whenMatchedUpdate(
            condition="source.operation = 'UPDATE'",
            set={
                "name": "coalesce(source.name, target.name)",
                "email": "coalesce(source.email, target.email)",
                "age": "coalesce(source.age, target.age)",
                "country": "coalesce(upper(source.country), target.country)",
                "updated_at": "source.event_timestamp",
            },
        )
        .whenMatchedUpdate(
            condition="source.operation = 'DELETE'",
            set={
                "is_deleted": "true",
                "end_date": "to_date(source.event_timestamp)",
                "updated_at": "source.event_timestamp",
            },
        )
        .whenNotMatchedInsert(
            condition=(
                "source.operation = 'INSERT' AND source.name IS NOT NULL "
                "AND source.email IS NOT NULL AND source.age IS NOT NULL "
                "AND source.country IS NOT NULL"
            ),
            values={
                "customer_id": "source.customer_id",
                "name": "initcap(source.name)",
                "email": "lower(source.email)",
                "age": "source.age",
                "country": "upper(source.country)",
                "is_deleted": "false",
                "effective_date": "to_date(source.event_timestamp)",
                "end_date": "cast(null as date)",
                "source_system": "source.source_system",
                "updated_at": "source.event_timestamp",
            },
        )
        .execute()
    )

    result_count = spark.read.format("delta").load(target_path).count()
    logger.info("Customer CDC merge complete: %s current rows", result_count)
    return result_count


def write_silver_table(
    df: DataFrame,
    table_name: str,
    warehouse_path: str,
    mode: str = "overwrite",
) -> int:
    """Write a curated Silver Delta table and return its row count."""
    target_path = f"{warehouse_path}/silver/{table_name}"
    (df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(target_path))
    row_count = df.count()
    logger.info("Silver %s: %s rows written", table_name, row_count)
    return row_count


def write_quarantine(
    df: DataFrame | None,
    warehouse_path: str,
    entity: str,
    record_id_column: str,
    mode: str = "overwrite",
) -> int:
    """Write rejected rows in a consistent, auditable quarantine schema."""
    if df is None:
        return 0

    payload_columns = [name for name in df.columns if name != "rejection_reason"]
    record_id = (
        F.col(record_id_column).cast("string")
        if record_id_column in df.columns
        else F.lit(None).cast("string")
    )
    normalized = df.select(
        F.lit(entity).alias("entity"),
        record_id.alias("record_id"),
        F.to_json(F.struct(*[F.col(name) for name in payload_columns])).alias("record_json"),
        F.col("rejection_reason"),
        F.current_timestamp().alias("rejected_at"),
    )
    target_path = f"{warehouse_path}/quarantine/{entity}"
    (
        normalized.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(target_path)
    )
    row_count = normalized.count()
    logger.info("Quarantine %s: %s rows written", entity, row_count)
    return row_count
