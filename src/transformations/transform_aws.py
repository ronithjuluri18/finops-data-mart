"""
transform_aws.py
----------------
PySpark job that reads raw AWS billing CSV and produces a normalized
DataFrame matching the NORMALIZED_SCHEMA.

Key transformations:
  1. Cast types (cost_usd → Double, bill_date → DateType)
  2. Drop rows with null cost or date (they'd corrupt aggregations)
  3. Standardize region codes (strip AZ suffix like us-east-1a → us-east-1)
  4. Add cloud_provider = 'AWS' column
  5. Repartition by date for efficient partition pruning downstream

Performance notes:
  - We read only the columns we need (projection pushdown on CSV)
  - Repartition to 4 partitions before write — right-sizes output files
    for the ~5K row local dataset; increase for production volumes
"""

import sys
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Allow running this file directly from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from src.utils.schema import AWS_RAW_SCHEMA


def transform_aws(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read raw AWS billing CSV and return a normalized DataFrame.

    Args:
        spark: Active SparkSession
        input_path: Path to the AWS billing CSV file

    Returns:
        Normalized DataFrame with columns matching NORMALIZED_SCHEMA
    """
    print(f"[AWS Transform] Reading from {input_path}")

    # Read with explicit schema — never infer from CSV (cost_usd becomes string)
    raw = (
        spark.read.option("header", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")   # keep bad rows as nulls, don't crash
        .schema(AWS_RAW_SCHEMA)
        .csv(input_path)
    )

    raw_count = raw.count()
    print(f"[AWS Transform] Raw rows: {raw_count:,}")

    # -------------------------------------------------------------------------
    # Step 1: Drop rows with missing critical fields
    # Null cost or date makes a fact row meaningless and would corrupt totals
    # -------------------------------------------------------------------------
    cleaned = raw.dropna(subset=["bill_date", "cost_usd", "service_name", "account_id"])

    dropped = raw_count - cleaned.count()
    if dropped > 0:
        print(f"[AWS Transform] Dropped {dropped:,} rows with null critical fields")

    # -------------------------------------------------------------------------
    # Step 2: Standardize region codes
    # AWS sometimes appends AZ letter (us-east-1a) — strip it to get the region
    # Regex: match standard region format, capture without AZ suffix
    # -------------------------------------------------------------------------
    normalized = cleaned.withColumn(
        "region",
        F.regexp_replace(F.col("region"), r"([a-z]{2}-[a-z]+-\d)[a-z]$", "$1"),
    )

    # -------------------------------------------------------------------------
    # Step 3: Fill defaults for optional fields
    # -------------------------------------------------------------------------
    normalized = (
        normalized.fillna({"environment": "unknown", "team": "unknown", "region": "global"})
        .withColumn("account_name", F.coalesce(F.col("account_name"), F.col("account_id")))
    )

    # -------------------------------------------------------------------------
    # Step 4: Map to canonical schema
    # Rename GCP-style field names so both sources share one schema
    # -------------------------------------------------------------------------
    canonical = normalized.select(
        F.col("bill_date").alias("date_key"),
        F.lit("AWS").alias("cloud_provider"),
        F.col("account_id"),
        F.col("account_name"),
        F.col("service_name"),
        F.col("region").alias("region_code"),
        F.col("usage_quantity"),
        F.col("usage_unit"),
        F.col("cost_usd"),
        F.col("environment"),
        F.col("team"),
        F.col("resource_id"),
    )

    # -------------------------------------------------------------------------
    # Step 5: Repartition by date for downstream partition pruning
    # This means queries like WHERE date_key = '2024-03-01' read 1 partition
    # instead of scanning the full dataset
    # -------------------------------------------------------------------------
    result = canonical.repartition(4, "date_key")

    print(f"[AWS Transform] Output rows: {result.count():,}")
    return result
