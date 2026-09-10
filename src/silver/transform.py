"""Silver layer data transformations"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, to_date, cast, current_timestamp, lit, upper, trim
)
from pyspark.sql.types import DoubleType, IntegerType, DateType
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)


def transform_customers(df: DataFrame) -> DataFrame:
    """Transform Bronze customers to Silver."""
    return (
        df
        .select(
            col("customer_id"),
            trim(upper(col("name"))).alias("name"),
            lower(trim(col("email"))).alias("email"),
            cast(col("age"), IntegerType()).alias("age"),
            upper(col("country")).alias("country"),
            lit(False).alias("is_deleted"),
            to_date(lit("1900-01-01")).alias("effective_date"),
            lit(None).cast(DateType()).alias("end_date"),
            col("source_system"),
            current_timestamp().alias("updated_at"),
        )
    )


def transform_products(df: DataFrame) -> DataFrame:
    """Transform Bronze products to Silver."""
    return (
        df
        .select(
            col("product_id"),
            trim(col("product_name")).alias("product_name"),
            upper(col("category")).alias("category"),
            cast(col("unit_price"), DoubleType()).alias("unit_price"),
            lit(True).alias("is_active"),
            to_date(lit("1900-01-01")).alias("effective_date"),
            lit(None).cast(DateType()).alias("end_date"),
            col("source_system"),
            current_timestamp().alias("updated_at"),
        )
    )


def transform_orders(df: DataFrame) -> DataFrame:
    """Transform Bronze orders to Silver."""
    return (
        df
        .select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
            cast(col("order_amount"), DoubleType()).alias("order_amount"),
            cast(col("order_quantity"), IntegerType()).alias("order_quantity"),
            lit(False).alias("is_deleted"),
            col("source_system"),
            current_timestamp().alias("updated_at"),
        )
    )


def merge_customers_cdc(
    spark: SparkSession,
    silver_path: str,
    cdc_df: DataFrame
) -> None:
    """
    Idempotent MERGE for customer CDC events.
    
    Ensures:
    - INSERT: new customers
    - UPDATE: existing customers
    - DELETE: soft delete (is_deleted=true)
    - Duplicate events: idempotently skipped
    """
    target = DeltaTable.forPath(spark, f"{silver_path}/silver_customers")
    
    target.alias("target").merge(
        cdc_df.alias("source"),
        "target.customer_id = source.customer_id"
    ).whenMatchedUpdate(
        condition="source.operation = 'UPDATE'",
        set={
            "name": "source.name",
            "email": "source.email",
            "age": "source.age",
            "country": "source.country",
            "updated_at": "source.updated_at",
        }
    ).whenMatchedUpdate(
        condition="source.operation = 'DELETE'",
        set={"is_deleted": lit(True), "updated_at": "source.updated_at"}
    ).whenNotMatchedInsert(
        condition="source.operation = 'INSERT'",
        values={
            "customer_id": "source.customer_id",
            "name": "source.name",
            "email": "source.email",
            "age": "source.age",
            "country": "source.country",
            "is_deleted": lit(False),
            "effective_date": "source.effective_date",
            "end_date": lit(None),
            "source_system": "source.source_system",
            "updated_at": "source.updated_at",
        }
    ).execute()
    
    logger.info("✓ Customer CDC events merged with idempotency")


def write_silver_table(
    df: DataFrame,
    table_name: str,
    warehouse_path: str,
    mode: str = "overwrite"
) -> None:
    """Write DataFrame to Silver layer as Delta table."""
    silver_path = f"{warehouse_path}/silver/{table_name}"
    df.write.format("delta").mode(mode).save(silver_path)
    logger.info(f"✓ Silver {table_name}: {df.count()} records written")


def write_quarantine(
    df: DataFrame,
    warehouse_path: str
) -> None:
    """Write invalid records to quarantine."""
    if df and df.count() > 0:
        quarantine_path = f"{warehouse_path}/quarantine"
        df.withColumn("rejected_at", current_timestamp()).write \
            .format("delta") \
            .mode("append") \
            .save(quarantine_path)
        logger.info(f"✓ Quarantine: {df.count()} records written")
