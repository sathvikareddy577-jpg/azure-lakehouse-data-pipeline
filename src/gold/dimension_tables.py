"""Gold-layer customer and product dimensions."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def create_dim_customer(silver_customers: DataFrame, fact_orders: DataFrame) -> DataFrame:
    """Create one analytics-ready row per active customer."""
    order_aggregates = fact_orders.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("order_amount").alias("total_spend"),
    )
    dimension = (
        silver_customers.filter(~F.col("is_deleted"))
        .select("customer_id", "name", "email", "age", "country")
        .dropDuplicates(["customer_id"])
        .join(order_aggregates, "customer_id", "left")
        .fillna({"total_orders": 0, "total_spend": 0.0})
        .withColumn("is_active", F.col("total_orders") > 0)
        .withColumn("updated_at", F.current_timestamp())
    )
    logger.info("dim_customer created")
    return dimension


def create_dim_product(silver_products: DataFrame, fact_orders: DataFrame) -> DataFrame:
    """Create one analytics-ready row per active product."""
    sales_aggregates = fact_orders.groupBy("product_id").agg(
        F.sum("order_quantity").alias("units_sold"),
        F.sum("order_amount").alias("total_revenue"),
    )
    dimension = (
        silver_products.filter(F.col("is_active"))
        .select("product_id", "product_name", "category", "unit_price")
        .dropDuplicates(["product_id"])
        .join(sales_aggregates, "product_id", "left")
        .fillna({"units_sold": 0, "total_revenue": 0.0})
        .withColumn("is_active", F.col("units_sold") > 0)
        .withColumn("updated_at", F.current_timestamp())
    )
    logger.info("dim_product created")
    return dimension
