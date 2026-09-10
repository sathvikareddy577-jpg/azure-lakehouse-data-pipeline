"""Reusable Silver-layer data-quality rules."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def _require_columns(df: DataFrame, columns: Iterable[str]) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"DataFrame is missing required columns: {', '.join(missing)}")


def combine_quarantine(*frames: DataFrame | None) -> DataFrame | None:
    """Union quarantine frames while preserving entity-specific columns."""
    available = [frame for frame in frames if frame is not None]
    if not available:
        return None
    return reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        available,
    )


def validate_nulls(
    df: DataFrame,
    key_columns: Sequence[str],
) -> tuple[DataFrame, DataFrame]:
    """Split rows containing null or blank required values."""
    if not key_columns:
        raise ValueError("key_columns must contain at least one column")
    _require_columns(df, key_columns)

    invalid_condition = F.lit(False)
    for name in key_columns:
        invalid_condition = (
            invalid_condition | F.col(name).isNull() | (F.trim(F.col(name).cast("string")) == "")
        )

    valid = df.filter(~invalid_condition)
    quarantine = df.filter(invalid_condition).withColumn(
        "rejection_reason",
        F.lit(f"Missing required value: {', '.join(key_columns)}"),
    )
    return valid, quarantine


def validate_date_format(
    df: DataFrame,
    date_columns: Mapping[str, str],
) -> tuple[DataFrame, DataFrame | None]:
    """Parse date columns and quarantine non-null values that cannot be parsed."""
    _require_columns(df, date_columns)
    valid = df
    quarantines = []

    for name, date_format in date_columns.items():
        parsed_name = f"__parsed_{name}"
        working = valid.withColumn(parsed_name, F.to_date(F.col(name), date_format))
        invalid_condition = F.col(name).isNotNull() & F.col(parsed_name).isNull()
        quarantines.append(
            working.filter(invalid_condition)
            .drop(parsed_name)
            .withColumn("rejection_reason", F.lit(f"Invalid date: {name}"))
        )
        valid = (
            working.filter(~invalid_condition)
            .withColumn(name, F.col(parsed_name))
            .drop(parsed_name)
        )

    return valid, combine_quarantine(*quarantines)


def validate_numeric_ranges(
    df: DataFrame,
    range_rules: Mapping[str, tuple[float, float]],
) -> tuple[DataFrame, DataFrame | None]:
    """Quarantine non-null values that are non-numeric or outside a range."""
    _require_columns(df, range_rules)
    valid = df
    quarantines = []

    for name, (minimum, maximum) in range_rules.items():
        numeric_name = f"__numeric_{name}"
        working = valid.withColumn(numeric_name, F.col(name).cast("double"))
        invalid_condition = F.col(name).isNotNull() & (
            F.col(numeric_name).isNull()
            | ~F.col(numeric_name).between(float(minimum), float(maximum))
        )
        quarantines.append(
            working.filter(invalid_condition)
            .drop(numeric_name)
            .withColumn(
                "rejection_reason",
                F.lit(f"Out of range: {name} must be [{minimum}, {maximum}]"),
            )
        )
        valid = working.filter(~invalid_condition).drop(numeric_name)

    return valid, combine_quarantine(*quarantines)


def validate_references(
    df: DataFrame,
    references: Mapping[str, tuple[DataFrame, str]],
) -> tuple[DataFrame, DataFrame | None]:
    """Quarantine facts whose foreign keys are missing from a dimension source."""
    _require_columns(df, references)
    valid = df
    quarantines = []

    for local_column, (reference_df, reference_column) in references.items():
        _require_columns(reference_df, [reference_column])
        key_name = f"__valid_{local_column}"
        valid_keys = reference_df.select(F.col(reference_column).alias(key_name)).distinct()
        join_condition = valid[local_column] == valid_keys[key_name]
        quarantines.append(
            valid.join(valid_keys, join_condition, "left_anti").withColumn(
                "rejection_reason",
                F.lit(f"Missing reference: {local_column}"),
            )
        )
        valid = valid.join(valid_keys, join_condition, "left_semi")

    return valid, combine_quarantine(*quarantines)


def remove_duplicates(
    df: DataFrame,
    key_columns: Sequence[str],
) -> tuple[DataFrame, int]:
    """Deduplicate on a business key and report how many rows were removed."""
    if not key_columns:
        raise ValueError("key_columns must contain at least one column")
    _require_columns(df, key_columns)
    original_count = df.count()
    deduplicated = df.dropDuplicates(list(key_columns))
    removed = original_count - deduplicated.count()
    logger.info("Removed %s duplicate rows using %s", removed, list(key_columns))
    return deduplicated, removed


def calculate_quality_metrics(
    original_df: DataFrame,
    valid_df: DataFrame,
    quarantine_df: DataFrame | None = None,
) -> dict[str, float]:
    """Calculate record-level quality metrics for one entity."""
    original_count = original_df.count()
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count() if quarantine_df is not None else 0
    quality_percentage = round(valid_count / original_count * 100, 2) if original_count else 100.0
    return {
        "original_records": float(original_count),
        "valid_records": float(valid_count),
        "quarantine_records": float(quarantine_count),
        "quality_percentage": quality_percentage,
    }
