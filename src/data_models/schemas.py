"""Spark SQL schemas for all layers"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType, BooleanType, ArrayType
)


# Bronze Layer Schemas (raw, minimal transformation)
BRONZE_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("source_system", StringType(), False),
    StructField("source_id", StringType(), False),
    StructField("load_timestamp", TimestampType(), False),
])

BRONZE_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("source_system", StringType(), False),
    StructField("source_id", StringType(), False),
    StructField("load_timestamp", TimestampType(), False),
])

BRONZE_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_amount", StringType(), True),
    StructField("order_quantity", IntegerType(), True),
    StructField("source_system", StringType(), False),
    StructField("source_id", StringType(), False),
    StructField("load_timestamp", TimestampType(), False),
])


# Silver Layer Schemas (cleaned, validated)
SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("country", StringType(), False),
    StructField("is_deleted", BooleanType(), False),
    StructField("effective_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("source_system", StringType(), False),
    StructField("updated_at", TimestampType(), False),
])

SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("effective_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("source_system", StringType(), False),
    StructField("updated_at", TimestampType(), False),
])

SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("order_amount", DoubleType(), False),
    StructField("order_quantity", IntegerType(), False),
    StructField("is_deleted", BooleanType(), False),
    StructField("source_system", StringType(), False),
    StructField("updated_at", TimestampType(), False),
])


# Gold Layer Schemas (analytics-ready)
DIM_CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("country", StringType(), False),
    StructField("total_orders", IntegerType(), False),
    StructField("total_spend", DoubleType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("updated_at", TimestampType(), False),
])

DIM_PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("units_sold", IntegerType(), False),
    StructField("total_revenue", DoubleType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("updated_at", TimestampType(), False),
])

FACT_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("order_amount", DoubleType(), False),
    StructField("order_quantity", IntegerType(), False),
    StructField("updated_at", TimestampType(), False),
])

DAILY_SALES_KPI_SCHEMA = StructType([
    StructField("sale_date", DateType(), False),
    StructField("total_sales_amount", DoubleType(), False),
    StructField("total_orders", IntegerType(), False),
    StructField("total_quantity", IntegerType(), False),
    StructField("unique_customers", IntegerType(), False),
    StructField("unique_products", IntegerType(), False),
    StructField("avg_order_value", DoubleType(), False),
    StructField("updated_at", TimestampType(), False),
])


# Quarantine Schema (invalid records)
QUARANTINE_SCHEMA = StructType([
    StructField("original_data", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("rejection_reason", StringType(), False),
    StructField("rejected_at", TimestampType(), False),
])
