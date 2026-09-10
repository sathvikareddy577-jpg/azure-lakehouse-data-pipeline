"""Explicit Spark schemas for source, curated, and CDC data."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Bronze input is deliberately read as strings so malformed values can be
# quarantined in Silver rather than silently discarded during ingestion.
BRONZE_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

BRONZE_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", StringType(), True),
    ]
)

BRONZE_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_amount", StringType(), True),
        StructField("order_quantity", StringType(), True),
    ]
)

CUSTOMER_CDC_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("operation", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("event_timestamp", TimestampType(), False),
        StructField("source_system", StringType(), False),
    ]
)

SILVER_CUSTOMERS_SCHEMA = StructType(
    [
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
    ]
)

SILVER_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("is_active", BooleanType(), False),
        StructField("effective_date", DateType(), False),
        StructField("end_date", DateType(), True),
        StructField("source_system", StringType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

SILVER_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("order_amount", DoubleType(), False),
        StructField("order_quantity", IntegerType(), False),
        StructField("is_deleted", BooleanType(), False),
        StructField("source_system", StringType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

DIM_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("country", StringType(), False),
        StructField("total_orders", LongType(), False),
        StructField("total_spend", DoubleType(), False),
        StructField("is_active", BooleanType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

DIM_PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("units_sold", LongType(), False),
        StructField("total_revenue", DoubleType(), False),
        StructField("is_active", BooleanType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

FACT_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("order_amount", DoubleType(), False),
        StructField("order_quantity", IntegerType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

DAILY_SALES_KPI_SCHEMA = StructType(
    [
        StructField("sale_date", DateType(), False),
        StructField("total_sales_amount", DoubleType(), False),
        StructField("total_orders", LongType(), False),
        StructField("total_quantity", LongType(), False),
        StructField("unique_customers", LongType(), False),
        StructField("unique_products", LongType(), False),
        StructField("avg_order_value", DoubleType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("entity", StringType(), False),
        StructField("record_id", StringType(), True),
        StructField("record_json", StringType(), False),
        StructField("rejection_reason", StringType(), False),
        StructField("rejected_at", TimestampType(), False),
    ]
)
