"""Deterministic Spark DataFrames used by the test suite."""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


def create_sample_customers(
    spark: SparkSession,
    num_records: int = 100,
) -> DataFrame:
    rng = random.Random(101)
    countries = ["USA", "UK", "Canada", "Australia", "Germany"]
    rows = [
        (
            f"CUST_{index:05d}",
            f"Customer {index}",
            f"customer{index}@example.com",
            str(rng.randint(18, 80)),
            rng.choice(countries),
        )
        for index in range(num_records)
    ]
    schema = StructType(
        [
            StructField("customer_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("age", StringType()),
            StructField("country", StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def create_sample_products(
    spark: SparkSession,
    num_records: int = 50,
) -> DataFrame:
    rng = random.Random(202)
    categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
    rows = [
        (
            f"PROD_{index:05d}",
            f"Product {index}",
            rng.choice(categories),
            f"{rng.uniform(10, 500):.2f}",
        )
        for index in range(num_records)
    ]
    schema = StructType(
        [
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("unit_price", StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def create_sample_orders(
    spark: SparkSession,
    num_customers: int = 100,
    num_products: int = 50,
    num_orders: int = 500,
) -> DataFrame:
    rng = random.Random(303)
    base_date = datetime(2024, 1, 1)
    rows = [
        (
            f"ORD_{index:06d}",
            f"CUST_{rng.randint(0, num_customers - 1):05d}",
            f"PROD_{rng.randint(0, num_products - 1):05d}",
            (base_date + timedelta(days=rng.randint(0, 364))).strftime("%Y-%m-%d"),
            f"{rng.uniform(50, 5000):.2f}",
            str(rng.randint(1, 10)),
        )
        for index in range(num_orders)
    ]
    schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("order_date", StringType()),
            StructField("order_amount", StringType()),
            StructField("order_quantity", StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema)
