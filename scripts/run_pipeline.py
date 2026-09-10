"""Pipeline orchestration - Main entry point for running the lakehouse pipeline"""

import sys
import argparse
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common import get_spark_session, setup_logging, get_logger, Config
from src.bronze import ingest_csv_to_bronze, read_bronze_table
from src.silver import (
    transform_customers, transform_products, transform_orders,
    validate_nulls, validate_date_format, validate_numeric_ranges,
    remove_duplicates, write_silver_table, write_quarantine,
    calculate_quality_metrics
)
from src.gold import (
    create_dim_customer, create_dim_product, create_fact_orders,
    create_daily_sales_kpi, write_gold_table, validate_gold_aggregations
)
from src.data_models import (
    BRONZE_CUSTOMERS_SCHEMA, BRONZE_PRODUCTS_SCHEMA, BRONZE_ORDERS_SCHEMA
)


def run_bronze_layer(spark, data_path: str, warehouse_path: str, logger):
    """Run Bronze layer ingestion."""
    logger.info("=" * 60)
    logger.info("BRONZE LAYER - Raw Data Ingest")
    logger.info("=" * 60)
    
    # Ingest customers
    customers_csv = f"{data_path}/raw/customers.csv"
    bronze_customers = ingest_csv_to_bronze(
        spark, customers_csv, BRONZE_CUSTOMERS_SCHEMA,
        "bronze_customers", warehouse_path, source_system="csv"
    )
    
    # Ingest products
    products_csv = f"{data_path}/raw/products.csv"
    bronze_products = ingest_csv_to_bronze(
        spark, products_csv, BRONZE_PRODUCTS_SCHEMA,
        "bronze_products", warehouse_path, source_system="csv"
    )
    
    # Ingest orders
    orders_csv = f"{data_path}/raw/orders.csv"
    bronze_orders = ingest_csv_to_bronze(
        spark, orders_csv, BRONZE_ORDERS_SCHEMA,
        "bronze_orders", warehouse_path, source_system="csv"
    )
    
    logger.info(f"✓ Bronze layer complete: {bronze_customers.count()} customers, "
                f"{bronze_products.count()} products, {bronze_orders.count()} orders")


def run_silver_layer(spark, warehouse_path: str, logger):
    """Run Silver layer transformations with validation."""
    logger.info("=" * 60)
    logger.info("SILVER LAYER - Cleanse & Validate")
    logger.info("=" * 60)
    
    # Read Bronze
    bronze_customers = read_bronze_table(spark, "bronze_customers", warehouse_path)
    bronze_products = read_bronze_table(spark, "bronze_products", warehouse_path)
    bronze_orders = read_bronze_table(spark, "bronze_orders", warehouse_path)
    
    # Transform customers
    logger.info("Processing customers...")
    silver_customers = transform_customers(bronze_customers)
    valid_customers, quarantine_customers = validate_nulls(silver_customers, ["customer_id"])
    write_silver_table(valid_customers, "silver_customers", warehouse_path)
    write_quarantine(quarantine_customers, warehouse_path)
    
    # Transform products
    logger.info("Processing products...")
    silver_products = transform_products(bronze_products)
    write_silver_table(silver_products, "silver_products", warehouse_path)
    
    # Transform orders
    logger.info("Processing orders...")
    silver_orders = transform_orders(bronze_orders)
    valid_orders, quarantine_orders = validate_nulls(silver_orders, ["order_id", "customer_id"])
    if quarantine_orders:
        write_quarantine(quarantine_orders, warehouse_path)
    
    # Deduplicate orders
    deduped_orders, removed = remove_duplicates(valid_orders, ["order_id", "customer_id", "order_date"])
    write_silver_table(deduped_orders, "silver_orders", warehouse_path)
    
    # Calculate metrics
    metrics = calculate_quality_metrics(silver_orders, deduped_orders, quarantine_orders)
    logger.info(f"Quality metrics: {metrics['quality_percentage']:.2f}% valid records")
    
    logger.info("✓ Silver layer complete")


def run_gold_layer(spark, warehouse_path: str, logger):
    """Run Gold layer aggregations."""
    logger.info("=" * 60)
    logger.info("GOLD LAYER - Analytics Ready")
    logger.info("=" * 60)
    
    # Read Silver
    silver_customers = spark.read.format("delta").load(f"{warehouse_path}/silver/silver_customers")
    silver_products = spark.read.format("delta").load(f"{warehouse_path}/silver/silver_products")
    silver_orders = spark.read.format("delta").load(f"{warehouse_path}/silver/silver_orders")
    
    # Create fact orders
    fact_orders = create_fact_orders(silver_orders)
    write_gold_table(fact_orders, "fact_orders", warehouse_path)
    
    # Create dimensions
    dim_customer = create_dim_customer(silver_customers, fact_orders)
    write_gold_table(dim_customer, "dim_customer", warehouse_path)
    
    dim_product = create_dim_product(silver_products, fact_orders)
    write_gold_table(dim_product, "dim_product", warehouse_path)
    
    # Create KPIs
    daily_kpi = create_daily_sales_kpi(fact_orders)
    write_gold_table(daily_kpi, "daily_sales_kpi", warehouse_path)
    
    # Validate
    if validate_gold_aggregations(spark, fact_orders, daily_kpi):
        logger.info("✓ Gold layer complete - all validations passed")
    else:
        logger.error("✗ Gold layer validation failed")


def main():
    """Main pipeline entry point."""
    parser = argparse.ArgumentParser(description="Azure Lakehouse Data Pipeline")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "full"],
                        default="full", help="Which layer(s) to run")
    parser.add_argument("--data-path", type=str, default="data", help="Data directory")
    parser.add_argument("--warehouse-path", type=str, default="data/warehouse", help="Warehouse directory")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Setup
    Config.ensure_paths()
    setup_logging(log_level=args.log_level)
    logger = get_logger("pipeline")
    
    spark = get_spark_session("lakehouse-pipeline", args.warehouse_path)
    
    try:
        if args.layer in ["bronze", "full"]:
            run_bronze_layer(spark, args.data_path, args.warehouse_path, logger)
        
        if args.layer in ["silver", "full"]:
            run_silver_layer(spark, args.warehouse_path, logger)
        
        if args.layer in ["gold", "full"]:
            run_gold_layer(spark, args.warehouse_path, logger)
        
        logger.info("=" * 60)
        logger.info("✓ PIPELINE COMPLETE")
        logger.info("=" * 60)
    
    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
