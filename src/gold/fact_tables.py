"""Gold layer fact tables"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, to_date, count, sum, avg, current_timestamp, lit, when
)
import logging

logger = logging.getLogger(__name__)


def create_fact_orders(silver_orders: DataFrame) -> DataFrame:
    """
    Create fact table for orders.
    
    Args:
        silver_orders: Silver layer orders
    
    Returns:
        Fact orders table
    """
    fact_orders = (
        silver_orders
        .filter(col("is_deleted") == False)
        .select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("order_date"),
            col("order_amount"),
            col("order_quantity"),
            current_timestamp().alias("updated_at"),
        )
    )
    
    logger.info(f"✓ fact_orders: {fact_orders.count()} records created")
    return fact_orders


def create_daily_sales_kpi(fact_orders: DataFrame) -> DataFrame:
    """
    Create daily KPI aggregations.
    
    Args:
        fact_orders: Fact orders table
    
    Returns:
        Daily sales KPI table
    """
    daily_kpi = (
        fact_orders
        .groupBy(col("order_date").alias("sale_date"))
        .agg(
            sum("order_amount").alias("total_sales_amount"),
            count("order_id").alias("total_orders"),
            sum("order_quantity").alias("total_quantity"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products"),
            avg("order_amount").alias("avg_order_value"),
        )
        .withColumn("updated_at", current_timestamp())
        .orderBy("sale_date")
    )
    
    logger.info(f"✓ daily_sales_kpi: {daily_kpi.count()} records created")
    return daily_kpi


def write_gold_table(
    df: DataFrame,
    table_name: str,
    warehouse_path: str,
    mode: str = "overwrite"
) -> None:
    """Write DataFrame to Gold layer as Delta table."""
    gold_path = f"{warehouse_path}/gold/{table_name}"
    df.write.format("delta").mode(mode).save(gold_path)
    logger.info(f"✓ Gold {table_name}: {df.count()} records written")


def validate_gold_aggregations(
    spark: SparkSession,
    fact_orders: DataFrame,
    daily_kpi: DataFrame
) -> bool:
    """
    Validate Gold layer aggregations match source data.
    
    Returns:
        True if validation passes
    """
    total_orders = fact_orders.count()
    total_kpi_orders = daily_kpi.agg(sum("total_orders")).collect()[0][0]
    
    if total_orders == total_kpi_orders:
        logger.info("✓ Gold aggregation validation passed")
        return True
    else:
        logger.error(
            f"✗ Gold aggregation mismatch: "
            f"fact_orders={total_orders}, daily_kpi={total_kpi_orders}"
        )
        return False
