"""Pytest fixtures for sample data generation"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta


def create_sample_customers(spark: SparkSession, num_records: int = 100) -> DataFrame:
    """Generate sample customer data."""
    countries = ["USA", "UK", "Canada", "Australia", "Germany"]
    
    customers = []
    for i in range(num_records):
        customers.append((
            f"CUST_{i:05d}",
            f"Customer {i}",
            f"customer{i}@example.com",
            random.randint(18, 80),
            random.choice(countries),
        ))
    
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("age", IntegerType()),
        StructField("country", StringType()),
    ])
    
    return spark.createDataFrame(customers, schema=schema)


def create_sample_products(spark: SparkSession, num_records: int = 50) -> DataFrame:
    """Generate sample product data."""
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
    
    products = []
    for i in range(num_records):
        products.append((
            f"PROD_{i:05d}",
            f"Product {i}",
            random.choice(categories),
            round(random.uniform(10, 500), 2),
        ))
    
    schema = StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("unit_price", DoubleType()),
    ])
    
    return spark.createDataFrame(products, schema=schema)


def create_sample_orders(
    spark: SparkSession,
    num_customers: int = 100,
    num_products: int = 50,
    num_orders: int = 500
) -> DataFrame:
    """Generate sample order data."""
    orders = []
    base_date = datetime(2023, 1, 1)
    
    for i in range(num_orders):
        orders.append((
            f"ORD_{i:06d}",
            f"CUST_{random.randint(0, num_customers-1):05d}",
            f"PROD_{random.randint(0, num_products-1):05d}",
            (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            round(random.uniform(50, 5000), 2),
            random.randint(1, 10),
        ))
    
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("order_date", StringType()),
        StructField("order_amount", StringType()),
        StructField("order_quantity", IntegerType()),
    ])
    
    return spark.createDataFrame(orders, schema=schema)
