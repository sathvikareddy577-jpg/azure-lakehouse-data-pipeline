"""Unit tests for dimensional models and KPI reconciliation."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.gold import (
    create_daily_sales_kpi,
    create_dim_customer,
    create_dim_product,
    create_fact_orders,
    validate_gold_aggregations,
)
from src.silver import transform_customers, transform_orders, transform_products
from tests.fixtures.sample_data import (
    create_sample_customers,
    create_sample_orders,
    create_sample_products,
)


def test_fact_orders_filters_soft_deleted_rows(spark: SparkSession):
    orders = transform_orders(create_sample_orders(spark, num_orders=5)).withColumn(
        "is_deleted",
        F.col("order_id") == "ORD_000000",
    )
    facts = create_fact_orders(orders)
    assert facts.count() == 4
    assert facts.filter("order_id = 'ORD_000000'").count() == 0


def test_customer_dimension_includes_customers_without_orders(spark: SparkSession):
    customers = transform_customers(create_sample_customers(spark, 3))
    orders = transform_orders(create_sample_orders(spark, num_customers=1, num_orders=4))
    dimension = create_dim_customer(customers, create_fact_orders(orders))
    assert dimension.count() == 3
    inactive = dimension.filter("customer_id = 'CUST_00002'").first()
    assert inactive.total_orders == 0
    assert inactive.total_spend == pytest.approx(0.0)
    assert inactive.is_active is False


def test_product_dimension_calculates_sales(spark: SparkSession):
    products = transform_products(create_sample_products(spark, 2))
    orders = transform_orders(create_sample_orders(spark, num_products=1, num_orders=4))
    dimension = create_dim_product(products, create_fact_orders(orders))
    sold = dimension.filter("product_id = 'PROD_00000'").first()
    unsold = dimension.filter("product_id = 'PROD_00001'").first()
    assert sold.units_sold > 0
    assert sold.total_revenue > 0
    assert unsold.units_sold == 0
    assert unsold.is_active is False


def test_daily_kpi_has_exact_order_total(spark: SparkSession):
    facts = create_fact_orders(transform_orders(create_sample_orders(spark, num_orders=100)))
    kpi = create_daily_sales_kpi(facts)
    assert kpi.agg(F.sum("total_orders")).first()[0] == 100
    assert "unique_customers" in kpi.columns
    assert "avg_order_value" in kpi.columns


def test_gold_reconciliation_passes(spark: SparkSession):
    facts = create_fact_orders(transform_orders(create_sample_orders(spark, num_orders=20)))
    assert validate_gold_aggregations(facts, create_daily_sales_kpi(facts))


def test_gold_reconciliation_detects_mismatch(spark: SparkSession):
    facts = create_fact_orders(transform_orders(create_sample_orders(spark, num_orders=20)))
    bad_kpi = create_daily_sales_kpi(facts).withColumn(
        "total_orders",
        F.col("total_orders") + 1,
    )
    assert not validate_gold_aggregations(facts, bad_kpi)
