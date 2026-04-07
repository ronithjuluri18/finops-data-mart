"""
schema.py
---------
Canonical StructType definitions for all tables in the data mart.
Centralizing schemas here means transformations never infer types from raw CSV
(which would make cost_usd a StringType), and schema changes are made in one place.
"""

from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Raw source schemas
# ---------------------------------------------------------------------------

AWS_RAW_SCHEMA = StructType(
    [
        StructField("bill_date", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("service_name", StringType(), False),
        StructField("region", StringType(), True),
        StructField("usage_type", StringType(), True),
        StructField("usage_quantity", DoubleType(), True),
        StructField("usage_unit", StringType(), True),
        StructField("cost_usd", DoubleType(), False),
        StructField("environment", StringType(), True),
        StructField("team", StringType(), True),
        StructField("resource_id", StringType(), True),
    ]
)

GCP_RAW_SCHEMA = StructType(
    [
        StructField("usage_date", StringType(), False),
        StructField("project_id", StringType(), False),
        StructField("project_name", StringType(), True),
        StructField("service_description", StringType(), False),
        StructField("sku_description", StringType(), True),
        StructField("location_region", StringType(), True),
        StructField("usage_amount", DoubleType(), True),
        StructField("usage_unit", StringType(), True),
        StructField("cost", DoubleType(), False),
        StructField("environment", StringType(), True),
        StructField("team", StringType(), True),
        StructField("resource_name", StringType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Canonical normalized schema (output of transform_aws / transform_gcp)
# Both sources get mapped to this before hitting the star schema build
# ---------------------------------------------------------------------------

NORMALIZED_SCHEMA = StructType(
    [
        StructField("date_key", StringType(), False),        # YYYY-MM-DD
        StructField("cloud_provider", StringType(), False),  # AWS | GCP
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("service_name", StringType(), False),
        StructField("region_code", StringType(), True),
        StructField("usage_quantity", DoubleType(), True),
        StructField("usage_unit", StringType(), True),
        StructField("cost_usd", DoubleType(), False),
        StructField("environment", StringType(), True),
        StructField("team", StringType(), True),
        StructField("resource_id", StringType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Dimension & fact table schemas
# ---------------------------------------------------------------------------

DIM_DATE_SCHEMA = StructType(
    [
        StructField("date_key", StringType(), False),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
        StructField("quarter", StringType(), True),
        StructField("month_name", StringType(), True),
        StructField("day_of_week", StringType(), True),
        StructField("is_weekend", StringType(), True),
    ]
)

DIM_SERVICE_SCHEMA = StructType(
    [
        StructField("service_key", StringType(), False),
        StructField("cloud_provider", StringType(), False),
        StructField("service_name", StringType(), False),
        StructField("service_category", StringType(), True),
    ]
)

DIM_ACCOUNT_SCHEMA = StructType(
    [
        StructField("account_key", StringType(), False),
        StructField("cloud_provider", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("team", StringType(), True),
        StructField("environment", StringType(), True),
    ]
)

DIM_REGION_SCHEMA = StructType(
    [
        StructField("region_key", StringType(), False),
        StructField("cloud_provider", StringType(), False),
        StructField("region_code", StringType(), False),
        StructField("region_name", StringType(), True),
        StructField("geography", StringType(), True),
    ]
)

FACT_SCHEMA = StructType(
    [
        StructField("cost_id", StringType(), False),
        StructField("date_key", StringType(), False),
        StructField("service_key", StringType(), True),
        StructField("account_key", StringType(), True),
        StructField("region_key", StringType(), True),
        StructField("cloud_provider", StringType(), False),
        StructField("cost_usd", DoubleType(), False),
        StructField("usage_quantity", DoubleType(), True),
        StructField("usage_unit", StringType(), True),
        StructField("tags", MapType(StringType(), StringType()), True),
    ]
)
