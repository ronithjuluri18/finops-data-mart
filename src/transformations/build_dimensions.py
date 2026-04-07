"""
build_dimensions.py
-------------------
Derives all four dimension tables from the combined normalized DataFrame.

Why derive dimensions from the data (not hardcode them)?
  - dim_date: auto-covers whatever date range exists in the billing data
  - dim_service: captures every service that actually appeared, not just ones
    we anticipated — new AWS/GCP services show up automatically
  - dim_account: same principle — new teams/projects auto-register
  - dim_region: same

Each dimension uses a deterministic surrogate key (MD5 hash of natural key)
so the same service always gets the same key across pipeline re-runs.
This matters for incremental loads — we never create duplicate dimension rows.

Dimension tables are small (~100s of rows) and written as Delta Lake tables
without partitioning. The fact table's broadcast joins rely on this smallness.
"""

import sys
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


# ---------------------------------------------------------------------------
# Service category mapping — enriches service names with a category label
# ---------------------------------------------------------------------------
SERVICE_CATEGORY_MAP = {
    # AWS
    "Amazon EC2": "Compute",
    "Amazon EKS": "Compute",
    "AWS Lambda": "Compute",
    "Amazon RDS": "Database",
    "Amazon DynamoDB": "Database",
    "Amazon Redshift": "Analytics",
    "AWS Glue": "Analytics",
    "Amazon S3": "Storage",
    "Amazon CloudFront": "Network",
    "Amazon VPC": "Network",
    "Amazon Route 53": "Network",
    "AWS Data Transfer": "Network",
    "Amazon SageMaker": "AI/ML",
    "Amazon Bedrock": "AI/ML",
    # GCP
    "Compute Engine": "Compute",
    "Google Kubernetes Engine": "Compute",
    "Cloud Run": "Compute",
    "Cloud SQL": "Database",
    "Cloud Spanner": "Database",
    "BigQuery": "Analytics",
    "Cloud Dataflow": "Analytics",
    "Cloud Storage": "Storage",
    "Networking": "Network",
    "Cloud CDN": "Network",
    "Cloud Load Balancing": "Network",
    "Vertex AI": "AI/ML",
    "Cloud Pub/Sub": "Messaging",
    "Cloud Logging": "Operations",
}

# ---------------------------------------------------------------------------
# Region name + geography mapping
# ---------------------------------------------------------------------------
REGION_META = {
    # AWS
    "us-east-1":      ("US East (N. Virginia)", "US"),
    "us-east-2":      ("US East (Ohio)", "US"),
    "us-west-1":      ("US West (N. California)", "US"),
    "us-west-2":      ("US West (Oregon)", "US"),
    "eu-west-1":      ("EU (Ireland)", "EU"),
    "eu-central-1":   ("EU (Frankfurt)", "EU"),
    "ap-southeast-1": ("Asia Pacific (Singapore)", "APAC"),
    "ap-northeast-1": ("Asia Pacific (Tokyo)", "APAC"),
    # GCP
    "us-central1":    ("US Central (Iowa)", "US"),
    "us-east1":       ("US East (South Carolina)", "US"),
    "us-west1":       ("US West (Oregon)", "US"),
    "us-west2":       ("US West (Los Angeles)", "US"),
    "europe-west1":   ("EU West (Belgium)", "EU"),
    "europe-west2":   ("EU West (London)", "EU"),
    "asia-east1":     ("Asia East (Taiwan)", "APAC"),
    "asia-southeast1": ("Asia Southeast (Singapore)", "APAC"),
    "global":         ("Global / Multi-region", "Global"),
}


def build_dim_date(spark: SparkSession, combined: DataFrame) -> DataFrame:
    """
    Build dim_date from distinct date_key values in the combined dataset.
    Derives year, month, quarter, day-of-week from the date string.
    """
    print("[Dimensions] Building dim_date...")

    dates = combined.select("date_key").distinct()

    dim_date = (
        dates.withColumn("date_ts", F.to_date(F.col("date_key"), "yyyy-MM-dd"))
        .withColumn("year",        F.year("date_ts").cast("string"))
        .withColumn("month",       F.lpad(F.month("date_ts").cast("string"), 2, "0"))
        .withColumn("day",         F.lpad(F.dayofmonth("date_ts").cast("string"), 2, "0"))
        .withColumn("quarter",     F.concat(F.lit("Q"), F.quarter("date_ts").cast("string")))
        .withColumn("month_name",  F.date_format("date_ts", "MMMM"))
        .withColumn("day_of_week", F.date_format("date_ts", "EEEE"))
        .withColumn("is_weekend",
            F.when(F.dayofweek("date_ts").isin([1, 7]), F.lit("true"))
             .otherwise(F.lit("false"))
        )
        .drop("date_ts")
    )

    print(f"[Dimensions] dim_date: {dim_date.count():,} rows")
    return dim_date


def build_dim_service(spark: SparkSession, combined: DataFrame) -> DataFrame:
    """
    Build dim_service from distinct (cloud_provider, service_name) pairs.
    Enriches with a service_category via a broadcast join on a small lookup map.
    """
    print("[Dimensions] Building dim_service...")

    # Build a small lookup DataFrame for service → category
    category_rows = [
        (svc, cat) for svc, cat in SERVICE_CATEGORY_MAP.items()
    ]
    category_df = spark.createDataFrame(category_rows, ["service_name", "service_category"])

    dim_service = (
        combined.select("cloud_provider", "service_name").distinct()
        # Broadcast join: category_df is tiny (~20 rows), no shuffle needed
        .join(F.broadcast(category_df), on="service_name", how="left")
        .fillna({"service_category": "Other"})
        .withColumn(
            "service_key",
            F.md5(F.concat_ws("|", F.col("cloud_provider"), F.col("service_name")))
        )
        .select("service_key", "cloud_provider", "service_name", "service_category")
    )

    print(f"[Dimensions] dim_service: {dim_service.count():,} rows")
    return dim_service


def build_dim_account(spark: SparkSession, combined: DataFrame) -> DataFrame:
    """
    Build dim_account from distinct (cloud_provider, account_id) pairs.
    Includes team and environment from the billing tags.
    """
    print("[Dimensions] Building dim_account...")

    dim_account = (
        combined.select(
            "cloud_provider", "account_id", "account_name", "team", "environment"
        )
        .distinct()
        .withColumn(
            "account_key",
            F.md5(F.concat_ws("|", F.col("cloud_provider"), F.col("account_id")))
        )
        .select("account_key", "cloud_provider", "account_id", "account_name", "team", "environment")
    )

    print(f"[Dimensions] dim_account: {dim_account.count():,} rows")
    return dim_account


def build_dim_region(spark: SparkSession, combined: DataFrame) -> DataFrame:
    """
    Build dim_region from distinct (cloud_provider, region_code) pairs.
    Enriches with human-readable region name and geography bucket (US/EU/APAC).
    """
    print("[Dimensions] Building dim_region...")

    # Build region metadata lookup
    region_rows = [
        (code, name, geo) for code, (name, geo) in REGION_META.items()
    ]
    region_meta_df = spark.createDataFrame(
        region_rows, ["region_code", "region_name", "geography"]
    )

    dim_region = (
        combined.select("cloud_provider", "region_code").distinct()
        .join(F.broadcast(region_meta_df), on="region_code", how="left")
        .fillna({"region_name": "Unknown", "geography": "Other"})
        .withColumn(
            "region_key",
            F.md5(F.concat_ws("|", F.col("cloud_provider"), F.col("region_code")))
        )
        .select("region_key", "cloud_provider", "region_code", "region_name", "geography")
    )

    print(f"[Dimensions] dim_region: {dim_region.count():,} rows")
    return dim_region
