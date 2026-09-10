"""
Databricks notebook main for Azure Lakehouse Pipeline
This notebook is meant to run in Databricks and use ADLS Gen2 for storage.

To run:
1. Import this notebook into your Databricks workspace
2. Attach to a cluster with PySpark 3.3+
3. Configure ADLS Gen2 credentials
4. Run all cells

Parameters:
- warehouse_path: abfss://warehouse@lakehousedata.dfs.core.windows.net
- raw_data_path: abfss://raw@lakehousedata.dfs.core.windows.net
"""

# COMMAND ----------

# Import required libraries
import sys
sys.path.append('/Workspace/Repos/lakehouse-pipeline')

from src.common import get_spark_session, setup_logging, get_logger
from src.bronze import ingest_csv_to_bronze
from src.silver import transform_customers, transform_products, validate_nulls, write_silver_table
from src.gold import create_dim_customer, create_fact_orders

setup_logging()
logger = get_logger("databricks-notebook")

# COMMAND ----------

# Configuration
warehouse_path = "abfss://warehouse@lakehousedata.dfs.core.windows.net"
raw_data_path = "abfss://raw@lakehousedata.dfs.core.windows.net"

logger.info(f"Warehouse Path: {warehouse_path}")
logger.info(f"Raw Data Path: {raw_data_path}")

# COMMAND ----------

# Bronze Layer: Ingest data
logger.info("Starting Bronze layer ingest...")

# Customers
customers_path = f"{raw_data_path}/customers.csv"
bronze_customers = spark.read.csv(customers_path, header=True, inferSchema=True)
bronze_customers.write.format("delta").mode("overwrite").save(f"{warehouse_path}/bronze/customers")

# Products
products_path = f"{raw_data_path}/products.csv"
bronze_products = spark.read.csv(products_path, header=True, inferSchema=True)
bronze_products.write.format("delta").mode("overwrite").save(f"{warehouse_path}/bronze/products")

# Orders
orders_path = f"{raw_data_path}/orders.csv"
bronze_orders = spark.read.csv(orders_path, header=True, inferSchema=True)
bronze_orders.write.format("delta").mode("overwrite").save(f"{warehouse_path}/bronze/orders")

logger.info("✓ Bronze layer complete")

# COMMAND ----------

# Silver Layer: Transform and validate
logger.info("Starting Silver layer transforms...")

bronze_customers = spark.read.format("delta").load(f"{warehouse_path}/bronze/customers")
silver_customers = transform_customers(bronze_customers)
valid_customers, quarantine = validate_nulls(silver_customers, ["customer_id"])

write_silver_table(valid_customers, "customers", warehouse_path)
logger.info("✓ Silver layer complete")

# COMMAND ----------

# Gold Layer: Aggregate for analytics
logger.info("Starting Gold layer aggregations...")

silver_customers = spark.read.format("delta").load(f"{warehouse_path}/silver/customers")
silver_orders = spark.read.format("delta").load(f"{warehouse_path}/silver/orders")

# Create dimension
dim_customer = create_dim_customer(silver_customers, silver_orders)
dim_customer.write.format("delta").mode("overwrite").save(f"{warehouse_path}/gold/dim_customer")

logger.info("✓ Gold layer complete")

# COMMAND ----------

# Show sample data from Gold layer
gold_customers = spark.read.format("delta").load(f"{warehouse_path}/gold/dim_customer")
display(gold_customers.limit(10))

# COMMAND ----------

logger.info("✓ Databricks notebook pipeline complete!")
