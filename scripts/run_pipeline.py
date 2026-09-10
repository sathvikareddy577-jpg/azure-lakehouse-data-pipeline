"""Orchestrate the Bronze, Silver, CDC, and Gold pipeline stages."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession

from src.bronze import ingest_csv_to_bronze, read_bronze_table
from src.common import Config, get_logger, get_spark_session, setup_logging
from src.data_models import (
    BRONZE_CUSTOMERS_SCHEMA,
    BRONZE_ORDERS_SCHEMA,
    BRONZE_PRODUCTS_SCHEMA,
    CUSTOMER_CDC_SCHEMA,
)
from src.gold import (
    create_daily_sales_kpi,
    create_dim_customer,
    create_dim_product,
    create_fact_orders,
    validate_gold_aggregations,
    write_gold_table,
)
from src.silver import (
    calculate_quality_metrics,
    combine_quarantine,
    merge_customers_cdc,
    remove_duplicates,
    transform_customers,
    transform_orders,
    transform_products,
    validate_nulls,
    validate_numeric_ranges,
    validate_references,
    write_quarantine,
    write_silver_table,
)


def _table_count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count()


def run_bronze_layer(
    spark: SparkSession,
    raw_data_path: str,
    warehouse_path: str,
    logger,
) -> dict[str, int]:
    """Ingest all three source files with replay-safe Delta merges."""
    logger.info("BRONZE | ingesting immutable source records")
    specifications = (
        ("customers", BRONZE_CUSTOMERS_SCHEMA),
        ("products", BRONZE_PRODUCTS_SCHEMA),
        ("orders", BRONZE_ORDERS_SCHEMA),
    )
    counts: dict[str, int] = {}
    for entity, schema in specifications:
        ingest_csv_to_bronze(
            spark,
            f"{raw_data_path}/{entity}.csv",
            schema,
            f"bronze_{entity}",
            warehouse_path,
            source_system="retail_csv",
        )
        counts[entity] = _table_count(
            spark,
            f"{warehouse_path}/bronze/bronze_{entity}",
        )
    logger.info("BRONZE | complete | %s", counts)
    return counts


def _customer_quality(df: DataFrame):
    required = ["customer_id", "name", "email", "age", "country"]
    valid, missing = validate_nulls(df, required)
    valid, ranges = validate_numeric_ranges(valid, {"age": (18, 120)})
    valid, duplicates = remove_duplicates(valid, ["customer_id"])
    return valid, combine_quarantine(missing, ranges), duplicates


def _product_quality(df: DataFrame):
    required = ["product_id", "product_name", "category", "unit_price"]
    valid, missing = validate_nulls(df, required)
    valid, ranges = validate_numeric_ranges(valid, {"unit_price": (0.01, 1_000_000)})
    valid, duplicates = remove_duplicates(valid, ["product_id"])
    return valid, combine_quarantine(missing, ranges), duplicates


def _order_quality(
    df: DataFrame,
    customers: DataFrame,
    products: DataFrame,
):
    required = [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "order_amount",
        "order_quantity",
    ]
    valid, missing = validate_nulls(df, required)
    valid, ranges = validate_numeric_ranges(
        valid,
        {"order_amount": (0.0, 1_000_000_000), "order_quantity": (1, 1_000_000)},
    )
    valid, references = validate_references(
        valid,
        {
            "customer_id": (
                customers.filter("is_deleted = false"),
                "customer_id",
            ),
            "product_id": (products.filter("is_active = true"), "product_id"),
        },
    )
    valid, duplicates = remove_duplicates(valid, ["order_id"])
    return valid, combine_quarantine(missing, ranges, references), duplicates


def run_silver_layer(
    spark: SparkSession,
    warehouse_path: str,
    logger,
    cdc_path: str | None = None,
) -> dict[str, Any]:
    """Transform, validate, quarantine, deduplicate, and apply optional CDC."""
    logger.info("SILVER | standardizing and enforcing data contracts")

    customer_source = transform_customers(
        read_bronze_table(spark, "bronze_customers", warehouse_path)
    )
    customers, customer_quarantine, customer_duplicates = _customer_quality(customer_source)
    customer_metrics = calculate_quality_metrics(
        customer_source,
        customers,
        customer_quarantine,
    )
    write_silver_table(customers, "silver_customers", warehouse_path)
    customer_rejections = write_quarantine(
        customer_quarantine,
        warehouse_path,
        "customers",
        "customer_id",
    )

    cdc_rows = 0
    if cdc_path:
        cdc = spark.read.schema(CUSTOMER_CDC_SCHEMA).json(cdc_path)
        cdc_rows = cdc.count()
        merge_customers_cdc(spark, f"{warehouse_path}/silver", cdc)
    customers = spark.read.format("delta").load(f"{warehouse_path}/silver/silver_customers")

    product_source = transform_products(read_bronze_table(spark, "bronze_products", warehouse_path))
    products, product_quarantine, product_duplicates = _product_quality(product_source)
    write_silver_table(products, "silver_products", warehouse_path)
    product_rejections = write_quarantine(
        product_quarantine,
        warehouse_path,
        "products",
        "product_id",
    )

    order_source = transform_orders(read_bronze_table(spark, "bronze_orders", warehouse_path))
    orders, order_quarantine, order_duplicates = _order_quality(
        order_source,
        customers,
        products,
    )
    write_silver_table(orders, "silver_orders", warehouse_path)
    order_rejections = write_quarantine(
        order_quarantine,
        warehouse_path,
        "orders",
        "order_id",
    )

    metrics = {
        "customers": customer_metrics,
        "products": calculate_quality_metrics(
            product_source,
            products,
            product_quarantine,
        ),
        "orders": calculate_quality_metrics(order_source, orders, order_quarantine),
        "duplicates_removed": {
            "customers": customer_duplicates,
            "products": product_duplicates,
            "orders": order_duplicates,
        },
        "quarantine_rows": {
            "customers": customer_rejections,
            "products": product_rejections,
            "orders": order_rejections,
        },
        "cdc_input_rows": cdc_rows,
    }
    logger.info("SILVER | complete | %s", json.dumps(metrics, sort_keys=True))
    return metrics


def run_gold_layer(
    spark: SparkSession,
    warehouse_path: str,
    logger,
) -> dict[str, int]:
    """Build star-schema outputs and reconcile KPIs to the order fact."""
    logger.info("GOLD | building dimensions, facts, and daily KPIs")
    customers = spark.read.format("delta").load(f"{warehouse_path}/silver/silver_customers")
    products = spark.read.format("delta").load(f"{warehouse_path}/silver/silver_products")
    orders = spark.read.format("delta").load(f"{warehouse_path}/silver/silver_orders")

    facts = create_fact_orders(orders)
    customer_dimension = create_dim_customer(customers, facts)
    product_dimension = create_dim_product(products, facts)
    daily_kpi = create_daily_sales_kpi(facts)

    if not validate_gold_aggregations(facts, daily_kpi):
        raise RuntimeError("Gold reconciliation failed; refusing to publish outputs")

    counts = {
        "fact_orders": write_gold_table(facts, "fact_orders", warehouse_path),
        "dim_customer": write_gold_table(
            customer_dimension,
            "dim_customer",
            warehouse_path,
        ),
        "dim_product": write_gold_table(
            product_dimension,
            "dim_product",
            warehouse_path,
        ),
        "daily_sales_kpi": write_gold_table(
            daily_kpi,
            "daily_sales_kpi",
            warehouse_path,
        ),
    }
    logger.info("GOLD | complete and reconciled | %s", counts)
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "full"],
        default="full",
    )
    parser.add_argument(
        "--raw-data-path",
        "--data-path",
        dest="raw_data_path",
        default="data/raw",
    )
    parser.add_argument("--warehouse-path", default=Config.WAREHOUSE_PATH)
    parser.add_argument("--cdc-path")
    parser.add_argument("--log-level", default=Config.LOG_LEVEL)
    parser.add_argument("--log-file", default=Config.LOG_FILE)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    Config.ensure_paths(args.raw_data_path, args.warehouse_path)
    setup_logging(args.log_level, args.log_file)
    logger = get_logger("pipeline")
    spark = get_spark_session("azure-lakehouse-pipeline", args.warehouse_path)
    summary: dict[str, Any] = {}

    try:
        if args.layer in {"bronze", "full"}:
            summary["bronze"] = run_bronze_layer(
                spark,
                args.raw_data_path,
                args.warehouse_path,
                logger,
            )
        if args.layer in {"silver", "full"}:
            summary["silver"] = run_silver_layer(
                spark,
                args.warehouse_path,
                logger,
                args.cdc_path,
            )
        if args.layer in {"gold", "full"}:
            summary["gold"] = run_gold_layer(
                spark,
                args.warehouse_path,
                logger,
            )
        logger.info("PIPELINE COMPLETE | %s", json.dumps(summary, sort_keys=True))
        return 0
    except Exception:
        logger.exception("PIPELINE FAILED")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
