"""Gold-layer facts, KPIs, and reconciliation checks."""

from __future__ import annotations

import logging
from math import isclose

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def create_fact_orders(silver_orders: DataFrame) -> DataFrame:
    """Create the order fact table from validated, non-deleted rows."""
    return silver_orders.filter(~F.col("is_deleted")).select(
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "order_amount",
        "order_quantity",
        F.current_timestamp().alias("updated_at"),
    )


def create_daily_sales_kpi(fact_orders: DataFrame) -> DataFrame:
    """Aggregate daily sales and customer/product reach metrics."""
    return (
        fact_orders.groupBy(F.col("order_date").alias("sale_date"))
        .agg(
            F.sum("order_amount").alias("total_sales_amount"),
            F.count("order_id").alias("total_orders"),
            F.sum("order_quantity").alias("total_quantity"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("product_id").alias("unique_products"),
            F.avg("order_amount").alias("avg_order_value"),
        )
        .withColumn("updated_at", F.current_timestamp())
        .orderBy("sale_date")
    )


def write_gold_table(
    df: DataFrame,
    table_name: str,
    warehouse_path: str,
    mode: str = "overwrite",
) -> int:
    """Write a Gold Delta table and return its row count."""
    target_path = f"{warehouse_path}/gold/{table_name}"
    (df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(target_path))
    row_count = df.count()
    logger.info("Gold %s: %s rows written", table_name, row_count)
    return row_count


def validate_gold_aggregations(fact_orders: DataFrame, daily_kpi: DataFrame) -> bool:
    """Reconcile both order counts and sales amounts between fact and KPI tables."""
    fact_summary = fact_orders.agg(
        F.count("order_id").alias("orders"),
        F.coalesce(F.sum("order_amount"), F.lit(0.0)).alias("amount"),
    ).first()
    kpi_summary = daily_kpi.agg(
        F.coalesce(F.sum("total_orders"), F.lit(0)).alias("orders"),
        F.coalesce(F.sum("total_sales_amount"), F.lit(0.0)).alias("amount"),
    ).first()

    count_matches = int(fact_summary["orders"]) == int(kpi_summary["orders"])
    amount_matches = isclose(
        float(fact_summary["amount"]),
        float(kpi_summary["amount"]),
        rel_tol=1e-9,
        abs_tol=0.01,
    )
    if not count_matches or not amount_matches:
        logger.error(
            "Gold reconciliation failed: fact=(%s, %.2f), kpi=(%s, %.2f)",
            fact_summary["orders"],
            fact_summary["amount"],
            kpi_summary["orders"],
            kpi_summary["amount"],
        )
    return count_matches and amount_matches
