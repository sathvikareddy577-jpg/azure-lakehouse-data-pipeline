"""Spark session helpers with working Delta Lake configuration."""

from __future__ import annotations

import logging
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from .config import Config

logger = logging.getLogger(__name__)


def get_spark_session(
    app_name: str = "lakehouse-pipeline",
    warehouse_path: str | None = None,
    master: str | None = None,
    with_delta: bool = True,
) -> SparkSession:
    """Create Spark locally; attach matching Delta JARs when requested."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or Config.SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    if warehouse_path:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_path)

    if with_delta:
        builder = configure_spark_with_delta_pip(builder)
    else:
        # Unit tests that exercise DataFrame logic do not need a network JAR
        # download. Delta integration tests run in a separate process.
        builder = (
            SparkSession.builder.appName(app_name)
            .master(master or Config.SPARK_MASTER)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.ui.enabled", "false")
        )
        if warehouse_path:
            builder = builder.config("spark.sql.warehouse.dir", warehouse_path)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created: %s", app_name)
    return spark


def stop_spark_session(spark: SparkSession | None) -> None:
    """Stop a Spark session gracefully."""
    if spark is not None:
        spark.stop()
        logger.info("Spark session stopped")
