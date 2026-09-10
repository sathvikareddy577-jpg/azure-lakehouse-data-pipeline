"""Test Gold layer aggregations"""

import pytest
from pyspark.sql import SparkSession
from tests.fixtures.sample_data import create_sample_customers, create_sample_products, create_sample_orders
from src.silver import transform_customers, transform_products, transform_orders
from src.gold import create_dim_customer, create_dim_product, create_fact_orders, create_daily_sales_kpi


def test_create_fact_orders(spark: SparkSession):
    """Test fact orders table creation."""
    orders_df = create_sample_orders(spark, num_orders=100)
    transformed_orders = transform_orders(orders_df)
    
    fact_orders = create_fact_orders(transformed_orders)
    
    assert fact_orders.count() == 100
    assert "order_id" in fact_orders.columns
    assert "customer_id" in fact_orders.columns
    assert "product_id" in fact_orders.columns


def test_create_dim_customer(spark: SparkSession):
    """Test customer dimension creation."""
    customers_df = create_sample_customers(spark, num_records=10)
    transformed_customers = transform_customers(customers_df)
    
    orders_df = create_sample_orders(spark, num_customers=10, num_orders=50)
    transformed_orders = transform_orders(orders_df)
    fact_orders = create_fact_orders(transformed_orders)
    
    dim_customer = create_dim_customer(transformed_customers, fact_orders)
    
    assert dim_customer.count() > 0
    assert "total_orders" in dim_customer.columns
    assert "total_spend" in dim_customer.columns
    assert "is_active" in dim_customer.columns


def test_create_dim_product(spark: SparkSession):
    """Test product dimension creation."""
    products_df = create_sample_products(spark, num_records=10)
    transformed_products = transform_products(products_df)
    
    orders_df = create_sample_orders(spark, num_products=10, num_orders=50)
    transformed_orders = transform_orders(orders_df)
    fact_orders = create_fact_orders(transformed_orders)
    
    dim_product = create_dim_product(transformed_products, fact_orders)
    
    assert dim_product.count() > 0
    assert "units_sold" in dim_product.columns
    assert "total_revenue" in dim_product.columns


def test_create_daily_sales_kpi(spark: SparkSession):
    """Test daily KPI aggregation."""
    orders_df = create_sample_orders(spark, num_orders=100)
    transformed_orders = transform_orders(orders_df)
    fact_orders = create_fact_orders(transformed_orders)
    
    daily_kpi = create_daily_sales_kpi(fact_orders)
    
    assert daily_kpi.count() > 0
    assert "total_sales_amount" in daily_kpi.columns
    assert "total_orders" in daily_kpi.columns
    assert "unique_customers" in daily_kpi.columns
    
    # Verify aggregations
    total_orders_kpi = daily_kpi.select("total_orders").rdd.map(lambda x: x[0]).sum()
    total_orders_fact = fact_orders.count()
    assert total_orders_kpi == total_orders_fact
