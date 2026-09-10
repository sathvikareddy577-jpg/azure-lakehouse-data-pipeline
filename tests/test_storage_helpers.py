"""Fast tests for lineage decoration and Delta writer control flow."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameWriter

from src.bronze import ingest as bronze_ingest
from src.data_models import BRONZE_CUSTOMERS_SCHEMA
from src.gold import write_gold_table
from src.silver import write_quarantine, write_silver_table


def test_lineage_hash_is_stable_and_deduplicates(spark: SparkSession):
    source = spark.createDataFrame([("1", "A"), ("1", "A")], ["id", "value"])
    decorated = bronze_ingest._decorate_with_lineage(source, "test")
    assert decorated.count() == 1
    row = decorated.first()
    assert row._source_system == "test"
    assert len(row._record_hash) == 64


def test_csv_ingest_uses_schema_and_merge_helper(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch,
):
    csv_path = tmp_path / "customers.csv"
    csv_path.write_text(
        "customer_id,name,email,age,country\nC1,Jane,jane@example.com,30,USA\n",
        encoding="utf-8",
    )
    called = {}

    def fake_merge(frame, target_path):
        called["path"] = target_path
        return frame.count()

    monkeypatch.setattr(bronze_ingest, "_merge_new_records", fake_merge)
    result = bronze_ingest.ingest_csv_to_bronze(
        spark,
        str(csv_path),
        BRONZE_CUSTOMERS_SCHEMA,
        "customers",
        str(tmp_path / "warehouse"),
        "test",
    )
    assert result.count() == 1
    assert called["path"].endswith("/bronze/customers")


def test_delta_write_helpers_return_counts(spark: SparkSession, monkeypatch):
    saved_paths = []

    def fake_save(_writer, path):
        saved_paths.append(path)

    monkeypatch.setattr(DataFrameWriter, "save", fake_save)
    frame = spark.createDataFrame([("1",)], ["id"])
    assert write_silver_table(frame, "sample", "/warehouse") == 1
    assert write_gold_table(frame, "sample", "/warehouse") == 1
    assert saved_paths == ["/warehouse/silver/sample", "/warehouse/gold/sample"]


def test_quarantine_writer_normalizes_rejected_record(spark: SparkSession, monkeypatch):
    monkeypatch.setattr(DataFrameWriter, "save", lambda _writer, _path: None)
    rejected = spark.createDataFrame(
        [("O1", "bad amount")],
        ["order_id", "rejection_reason"],
    )
    assert write_quarantine(rejected, "/warehouse", "orders", "order_id") == 1
    assert write_quarantine(None, "/warehouse", "orders", "order_id") == 0
