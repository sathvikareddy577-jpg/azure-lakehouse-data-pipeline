"""Silver layer data quality validation"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, current_timestamp, count
import logging
from typing import Tuple, Dict

logger = logging.getLogger(__name__)


def validate_nulls(df: DataFrame, key_columns: list) -> Tuple[DataFrame, DataFrame]:
    """
    Separate records with null key columns to quarantine.
    
    Args:
        df: Input DataFrame
        key_columns: Column names that should not be null
    
    Returns:
        (valid_df, quarantine_df): Valid records and records with nulls
    """
    # Identify records with nulls in key columns
    null_condition = None
    for col_name in key_columns:
        if null_condition is None:
            null_condition = col(col_name).isNull()
        else:
            null_condition = null_condition | col(col_name).isNull()
    
    valid_df = df.filter(~null_condition)
    quarantine_df = df.filter(null_condition).withColumn(
        "rejection_reason", lit("NULL in key columns")
    )
    
    return valid_df, quarantine_df


def validate_date_format(df: DataFrame, date_columns: Dict[str, str]) -> Tuple[DataFrame, DataFrame]:
    """
    Validate date columns. Remove rows with invalid dates.
    
    Args:
        df: Input DataFrame
        date_columns: Dict of {column_name: expected_format}
    
    Returns:
        (valid_df, quarantine_df)
    """
    from pyspark.sql.functions import to_date
    
    valid_df = df
    quarantine_df = None
    
    for col_name in date_columns:
        # Try to parse date
        valid_df = valid_df.withColumn(
            f"{col_name}_parsed",
            to_date(col(col_name), "yyyy-MM-dd")
        )
        
        # Identify invalid dates
        invalid_mask = col(f"{col_name}_parsed").isNull() & col(col_name).isNotNull()
        
        if quarantine_df is None:
            quarantine_df = valid_df.filter(invalid_mask).withColumn(
                "rejection_reason", lit(f"Invalid date in {col_name}")
            )
        else:
            quarantine_df = quarantine_df.union(
                valid_df.filter(invalid_mask).withColumn(
                    "rejection_reason", lit(f"Invalid date in {col_name}")
                )
            )
        
        valid_df = valid_df.filter(~invalid_mask).drop(f"{col_name}_parsed")
    
    return valid_df, quarantine_df


def validate_numeric_ranges(
    df: DataFrame,
    range_rules: Dict[str, Tuple[float, float]]
) -> Tuple[DataFrame, DataFrame]:
    """
    Validate numeric columns are within acceptable ranges.
    
    Args:
        df: Input DataFrame
        range_rules: Dict of {column_name: (min_value, max_value)}
    
    Returns:
        (valid_df, quarantine_df)
    """
    valid_df = df
    quarantine_records = []
    
    for col_name, (min_val, max_val) in range_rules.items():
        out_of_range = col(col_name).between(min_val, max_val).cast("boolean") == False
        invalid_df = valid_df.filter(out_of_range).withColumn(
            "rejection_reason", 
            lit(f"{col_name} outside range [{min_val}, {max_val}]")
        )
        quarantine_records.append(invalid_df)
        valid_df = valid_df.filter(~out_of_range)
    
    quarantine_df = None
    if quarantine_records:
        quarantine_df = quarantine_records[0]
        for qdf in quarantine_records[1:]:
            quarantine_df = quarantine_df.union(qdf)
    
    return valid_df, quarantine_df


def remove_duplicates(df: DataFrame, key_columns: list) -> Tuple[DataFrame, int]:
    """
    Remove duplicate records based on key columns.
    
    Args:
        df: Input DataFrame
        key_columns: Columns to check for duplicates
    
    Returns:
        (deduplicated_df, num_removed)
    """
    original_count = df.count()
    deduped_df = df.dropDuplicates(key_columns)
    final_count = deduped_df.count()
    removed = original_count - final_count
    
    logger.info(f"Removed {removed} duplicates (idempotency verification)")
    
    return deduped_df, removed


def calculate_quality_metrics(
    original_df: DataFrame,
    valid_df: DataFrame,
    quarantine_df: DataFrame = None
) -> Dict[str, float]:
    """Calculate data quality metrics."""
    original_count = original_df.count()
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count() if quarantine_df else 0
    
    quality_pct = (valid_count / original_count * 100) if original_count > 0 else 0
    
    return {
        "original_records": original_count,
        "valid_records": valid_count,
        "quarantine_records": quarantine_count,
        "quality_percentage": quality_pct,
    }
