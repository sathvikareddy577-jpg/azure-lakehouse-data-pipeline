"""Gold layer dimension tables"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum, current_timestamp, when, lit
)
import logging

logger = logging.getLogger(__name__)


def create_dim_customer(silver_customers: DataFrame, fact_orders: DataFrame) -> DataFrame:
    """
    Create dimension table for customers with aggregated metrics.
    
    Args:
        silver_customers: Silver layer customers
        fact_orders: Fact orders table (for aggregations)
    
    Returns:
        Dimension customer table
    """
    # Aggregate order metrics
    order_agg = (
        fact_orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("order_amount").alias("total_spend"),
        )
    )
    
    # Join with customer dimension
    dim_customer = (
        silver_customers
        .filter(col("is_deleted") == False)
        .select(
            "customer_id",
            "name",
            "email",
            "age",
            "country",
        )
        .join(order_agg, on="customer_id", how="left")
        .fillna({"total_orders": 0, "total_spend": 0.0})
        .withColumn("is_active", col("total_orders") > 0)
        .withColumn("updated_at", current_timestamp())
    )
    
    logger.info(f"✓ dim_customer: {dim_customer.count()} records created")
    return dim_customer


def create_dim_product(silver_products: DataFrame, fact_orders: DataFrame) -> DataFrame:
    """
    Create dimension table for products with aggregated metrics.
    
    Args:
        silver_products: Silver layer products
        fact_orders: Fact orders table
    
    Returns:
        Dimension product table
    """
    # Aggregate sales metrics
    sales_agg = (
        fact_orders
        .groupBy("product_id")
        .agg(
            sum("order_quantity").alias("units_sold"),
            sum("order_amount").alias("total_revenue"),
        )
    )
    
    # Join with product dimension
    dim_product = (
        silver_products
        .filter(col("is_active") == True)
        .select(
            "product_id",
            "product_name",
            "category",
            "unit_price",
        )
        .join(sales_agg, on="product_id", how="left")
        .fillna({"units_sold": 0, "total_revenue": 0.0})
        .withColumn("is_active", col("units_sold") > 0)
        .withColumn("updated_at", current_timestamp())
    )
    
    logger.info(f"✓ dim_product: {dim_product.count()} records created")
    return dim_product
