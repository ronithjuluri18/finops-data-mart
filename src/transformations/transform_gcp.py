"""
transform_gcp.py
----------------
PySpark job that reads raw GCP billing CSV and produces a normalized
DataFrame matching the same NORMALIZED_SCHEMA as transform_aws.py.

GCP-specific handling:
  - Maps "project_id/project_name" → "account_id/account_name"
  - Maps "usage_date" → "date_key"
  - Maps "cost" → "cost_usd"
  - Handles negative costs (GCP emits credits as negative rows — we keep them
    so the mart correctly shows net spend, not gross spend)
  - Maps "location_region" → "region_code"
"""

import sys
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from src.utils.schema import GCP_RAW_SCHEMA


def transform_gcp(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read raw GCP billing CSV and return a normalized DataFrame.

    Args:
        spark: Active SparkSession
        input_path: Path to the GCP billing CSV file

    Returns:
        Normalized DataFrame with columns matching NORMALIZED_SCHEMA
    """
    print(f"[GCP Transform] Reading from {input_path}")

    raw = (
        spark.read.option("header", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .schema(GCP_RAW_SCHEMA)
        .csv(input_path)
    )

    raw_count = raw.count()
    print(f"[GCP Transform] Raw rows: {raw_count:,}")

    # -------------------------------------------------------------------------
    # Step 1: Drop rows missing critical fields
    # -------------------------------------------------------------------------
    cleaned = raw.dropna(
        subset=["usage_date", "cost", "service_description", "project_id"]
    )

    dropped = raw_count - cleaned.count()
    if dropped > 0:
        print(f"[GCP Transform] Dropped {dropped:,} rows with null critical fields")

    # -------------------------------------------------------------------------
    # Step 2: Standardize region codes
    # GCP region codes are already clean (us-central1) but may include zone
    # suffixes like us-central1-a — strip the zone letter
    # -------------------------------------------------------------------------
    normalized = cleaned.withColumn(
        "location_region",
        F.regexp_replace(F.col("location_region"), r"-[a-z]$", ""),
    )

    # -------------------------------------------------------------------------
    # Step 3: Defaults for optional fields
    # -------------------------------------------------------------------------
    normalized = normalized.fillna(
        {"environment": "unknown", "team": "unknown", "location_region": "global"}
    ).withColumn(
        "project_name",
        F.coalesce(F.col("project_name"), F.col("project_id")),
    )

    # -------------------------------------------------------------------------
    # Step 4: Map to canonical schema
    # GCP "service_description" becomes "service_name"
    # GCP "cost" becomes "cost_usd"
    # GCP "project_id" becomes "account_id" for unified treatment
    # -------------------------------------------------------------------------
    canonical = normalized.select(
        F.col("usage_date").alias("date_key"),
        F.lit("GCP").alias("cloud_provider"),
        F.col("project_id").alias("account_id"),
        F.col("project_name").alias("account_name"),
        F.col("service_description").alias("service_name"),
        F.col("location_region").alias("region_code"),
        F.col("usage_amount").alias("usage_quantity"),
        F.col("usage_unit"),
        F.col("cost").alias("cost_usd"),
        F.col("environment"),
        F.col("team"),
        F.col("resource_name").alias("resource_id"),
    )

    result = canonical.repartition(4, "date_key")

    print(f"[GCP Transform] Output rows: {result.count():,}")
    return result
