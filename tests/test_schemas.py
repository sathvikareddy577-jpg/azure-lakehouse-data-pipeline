"""Contract tests for the public Spark schemas."""

from pyspark.sql.types import DateType, LongType, StringType

from src.data_models import (
    BRONZE_ORDERS_SCHEMA,
    CUSTOMER_CDC_SCHEMA,
    DAILY_SALES_KPI_SCHEMA,
    SILVER_ORDERS_SCHEMA,
)


def test_bronze_preserves_malformed_values_as_strings():
    assert isinstance(BRONZE_ORDERS_SCHEMA["order_amount"].dataType, StringType)
    assert isinstance(BRONZE_ORDERS_SCHEMA["order_date"].dataType, StringType)


def test_silver_contract_uses_typed_date():
    assert isinstance(SILVER_ORDERS_SCHEMA["order_date"].dataType, DateType)


def test_kpi_counts_use_spark_long_type():
    assert isinstance(DAILY_SALES_KPI_SCHEMA["total_orders"].dataType, LongType)


def test_cdc_contract_contains_event_identity():
    assert {"event_id", "event_timestamp", "operation"}.issubset(CUSTOMER_CDC_SCHEMA.fieldNames())
