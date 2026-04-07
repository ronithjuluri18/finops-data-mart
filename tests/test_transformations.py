"""
test_transformations.py
-----------------------
Unit tests for the transformation layer.
Uses a lightweight local SparkSession — no cloud connection needed.

Run:
    pytest tests/ -v
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


# ---------------------------------------------------------------------------
# Shared SparkSession fixture (created once, reused across all tests)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("FinOps-Tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ---------------------------------------------------------------------------
# Helpers — build minimal test DataFrames
# ---------------------------------------------------------------------------
def make_aws_raw(spark):
    from src.utils.schema import AWS_RAW_SCHEMA
    rows = [
        ("2024-03-15", "111", "prod-account", "Amazon EC2", "us-east-1", "BoxUsage", 100.0, "Hrs", 42.50, "prod", "platform", "i-abc"),
        ("2024-03-16", "111", "prod-account", "Amazon S3",  "us-west-2", "Storage",   50.0, "GB",   3.20, "prod", "platform", "s3-xyz"),
        ("2024-03-17", "222", None,            "Amazon RDS", None,        "DBUsage",   20.0, "Hrs", 18.00, "dev",  None,       None),
        # Bad row: null cost — should be dropped
        ("2024-03-18", "333", "dev-account",  "Amazon EC2", "us-east-1", "BoxUsage",  10.0, "Hrs",  None, "dev",  "infra",    "i-bad"),
        # Bad row: null date — should be dropped
        (None,         "444", "test-account", "AWS Lambda", "eu-west-1", "Lambda",     5.0, "GB-s",  1.00, "dev",  "ml",       "fn-x"),
    ]
    return spark.createDataFrame(rows, schema=AWS_RAW_SCHEMA)


def make_gcp_raw(spark):
    from src.utils.schema import GCP_RAW_SCHEMA
    rows = [
        ("2024-03-15", "proj-1", "prod-proj", "Compute Engine", "N2 Core",    "us-central1", 80.0, "core hrs", 55.00, "prod", "platform", "proj-1/vm-a"),
        ("2024-03-16", "proj-1", "prod-proj", "Cloud Storage",  "Standard",   "europe-west1",30.0, "GiB",       2.10, "prod", "platform", "proj-1/bucket-a"),
        ("2024-03-17", "proj-2", None,         "BigQuery",       "Analysis",   "us-east1",    10.0, "TiB",       8.50, "dev",  "data",     "proj-2/bq"),
        # GCP credit (negative cost) — must be kept
        ("2024-03-18", "proj-1", "prod-proj", "Compute Engine", "Sustained",  "us-central1",  5.0, "core hrs", -2.50, "prod", "platform", "proj-1/vm-b"),
    ]
    return spark.createDataFrame(rows, schema=GCP_RAW_SCHEMA)


# ---------------------------------------------------------------------------
# Tests: transform_aws
# ---------------------------------------------------------------------------
class TestTransformAws:
    def test_drops_null_cost_rows(self, spark, tmp_path):
        """Rows with null cost_usd must be excluded from output."""
        from src.utils.schema import AWS_RAW_SCHEMA
        raw = make_aws_raw(spark)
        csv_path = str(tmp_path / "aws.csv")
        raw.write.option("header", "true").csv(csv_path)

        from src.transformations.transform_aws import transform_aws
        result = transform_aws(spark, csv_path)
        # 5 raw rows - 1 null cost - 1 null date = 3 valid rows
        assert result.count() == 3

    def test_adds_cloud_provider_column(self, spark, tmp_path):
        from src.utils.schema import AWS_RAW_SCHEMA
        raw = make_aws_raw(spark)
        csv_path = str(tmp_path / "aws.csv")
        raw.write.option("header", "true").csv(csv_path)

        from src.transformations.transform_aws import transform_aws
        result = transform_aws(spark, csv_path)
        providers = [r.cloud_provider for r in result.select("cloud_provider").distinct().collect()]
        assert providers == ["AWS"]

    def test_normalizes_az_region(self, spark, tmp_path):
        """AZ suffix like us-east-1a should be stripped to us-east-1."""
        from src.utils.schema import AWS_RAW_SCHEMA
        rows = [("2024-01-01", "111", "acct", "Amazon EC2", "us-east-1a", "BoxUsage", 1.0, "Hrs", 5.00, "prod", "team", "i-x")]
        df = spark.createDataFrame(rows, schema=AWS_RAW_SCHEMA)
        csv_path = str(tmp_path / "aws_az.csv")
        df.write.option("header", "true").csv(csv_path)

        from src.transformations.transform_aws import transform_aws
        result = transform_aws(spark, csv_path)
        region = result.select("region_code").first()[0]
        assert region == "us-east-1", f"Expected 'us-east-1', got '{region}'"

    def test_output_has_canonical_columns(self, spark, tmp_path):
        raw = make_aws_raw(spark)
        csv_path = str(tmp_path / "aws.csv")
        raw.write.option("header", "true").csv(csv_path)

        from src.transformations.transform_aws import transform_aws
        result = transform_aws(spark, csv_path)
        expected_cols = {"date_key", "cloud_provider", "account_id", "service_name",
                         "region_code", "cost_usd", "usage_quantity", "usage_unit"}
        assert expected_cols.issubset(set(result.columns))


# ---------------------------------------------------------------------------
# Tests: transform_gcp
# ---------------------------------------------------------------------------
class TestTransformGcp:
    def test_keeps_negative_costs(self, spark, tmp_path):
        """GCP credits (negative costs) must be preserved for net spend accuracy."""
        raw = make_gcp_raw(spark)
        csv_path = str(tmp_path / "gcp.csv")
        raw.write.option("header", "true").csv(csv_path)

        from src.transformations.transform_gcp import transform_gcp
        result = transform_gcp(spark, csv_path)
        # All 4 GCP rows are valid (negative cost is OK)
        assert result.count() == 4

    def test_adds_gcp_provider_tag(self, spark, tmp_path):
        raw = make_gcp_raw(spark)
        csv_path = str(tmp_path / "gcp.csv")
        raw.write.option("header", "true").csv(csv_path)

        from src.transformations.transform_gcp import transform_gcp
        result = transform_gcp(spark, csv_path)
        providers = [r.cloud_provider for r in result.select("cloud_provider").distinct().collect()]
        assert providers == ["GCP"]

    def test_maps_project_to_account(self, spark, tmp_path):
        """GCP project_id should land in the account_id column."""
        raw = make_gcp_raw(spark)
        csv_path = str(tmp_path / "gcp.csv")
        raw.write.option("header", "true").csv(csv_path)

        from src.transformations.transform_gcp import transform_gcp
        result = transform_gcp(spark, csv_path)
        account_ids = {r.account_id for r in result.select("account_id").distinct().collect()}
        assert "proj-1" in account_ids


# ---------------------------------------------------------------------------
# Tests: dimension builders
# ---------------------------------------------------------------------------
class TestBuildDimensions:
    def _make_combined(self, spark):
        """Minimal combined DataFrame for dimension building tests."""
        schema = StructType([
            StructField("date_key",       StringType(), False),
            StructField("cloud_provider", StringType(), False),
            StructField("account_id",     StringType(), False),
            StructField("account_name",   StringType(), True),
            StructField("service_name",   StringType(), False),
            StructField("region_code",    StringType(), True),
            StructField("usage_quantity", DoubleType(), True),
            StructField("usage_unit",     StringType(), True),
            StructField("cost_usd",       DoubleType(), False),
            StructField("environment",    StringType(), True),
            StructField("team",           StringType(), True),
            StructField("resource_id",    StringType(), True),
        ])
        rows = [
            ("2024-01-15", "AWS", "111", "prod", "Amazon EC2",     "us-east-1",  100.0, "Hrs",  42.0, "prod", "platform", "i-a"),
            ("2024-02-10", "GCP", "p-1", "proj", "Compute Engine", "us-central1", 80.0, "Hrs",  55.0, "prod", "data",     "vm-a"),
            ("2024-03-20", "AWS", "222", "dev",  "Amazon S3",      "us-west-2",   50.0, "GB",    3.0, "dev",  "platform", "s3-b"),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_dim_date_derives_year_month(self, spark):
        from src.transformations.build_dimensions import build_dim_date
        combined = self._make_combined(spark)
        dim = build_dim_date(spark, combined)
        row = dim.filter(dim.date_key == "2024-01-15").first()
        assert row.year == "2024"
        assert row.month == "01"
        assert row.month_name == "January"

    def test_dim_date_weekend_flag(self, spark):
        from src.transformations.build_dimensions import build_dim_date
        combined = self._make_combined(spark)
        dim = build_dim_date(spark, combined)
        # 2024-01-15 is a Monday (not weekend)
        row = dim.filter(dim.date_key == "2024-01-15").first()
        assert row.is_weekend == "false"

    def test_dim_service_has_category(self, spark):
        from src.transformations.build_dimensions import build_dim_service
        combined = self._make_combined(spark)
        dim = build_dim_service(spark, combined)
        ec2_row = dim.filter(dim.service_name == "Amazon EC2").first()
        assert ec2_row.service_category == "Compute"

    def test_dim_service_key_is_deterministic(self, spark):
        """Same service must always get the same key (idempotent pipeline)."""
        from pyspark.sql import functions as F
        expected_key = spark.sql(
            "SELECT md5('AWS|Amazon EC2') AS k"
        ).first().k

        from src.transformations.build_dimensions import build_dim_service
        combined = self._make_combined(spark)
        dim = build_dim_service(spark, combined)
        actual_key = dim.filter(dim.service_name == "Amazon EC2").first().service_key
        assert actual_key == expected_key

    def test_dim_region_enriches_geography(self, spark):
        from src.transformations.build_dimensions import build_dim_region
        combined = self._make_combined(spark)
        dim = build_dim_region(spark, combined)
        us_row = dim.filter(dim.region_code == "us-east-1").first()
        assert us_row.geography == "US"


# ---------------------------------------------------------------------------
# Tests: fact table builder
# ---------------------------------------------------------------------------
class TestBuildFact:
    def test_fact_row_count_matches_combined(self, spark):
        """Every row in combined must produce exactly one fact row."""
        from src.transformations.build_dimensions import (
            build_dim_service, build_dim_account, build_dim_region
        )
        from src.transformations.build_fact import build_fact

        schema = StructType([
            StructField("date_key",       StringType(), False),
            StructField("cloud_provider", StringType(), False),
            StructField("account_id",     StringType(), False),
            StructField("account_name",   StringType(), True),
            StructField("service_name",   StringType(), False),
            StructField("region_code",    StringType(), True),
            StructField("usage_quantity", DoubleType(), True),
            StructField("usage_unit",     StringType(), True),
            StructField("cost_usd",       DoubleType(), False),
            StructField("environment",    StringType(), True),
            StructField("team",           StringType(), True),
            StructField("resource_id",    StringType(), True),
        ])
        rows = [
            ("2024-03-01", "AWS", "111", "prod", "Amazon EC2", "us-east-1",  10.0, "Hrs", 5.0, "prod", "eng",  "i-a"),
            ("2024-03-02", "GCP", "p-1", "proj", "Compute Engine", "us-central1", 5.0, "Hrs", 3.0, "prod", "data", "vm-b"),
        ]
        combined = spark.createDataFrame(rows, schema=schema)

        dim_service = build_dim_service(spark, combined)
        dim_account = build_dim_account(spark, combined)
        dim_region  = build_dim_region(spark, combined)
        fact        = build_fact(spark, combined, dim_service, dim_account, dim_region)

        assert fact.count() == len(rows)

    def test_fact_has_no_null_cost(self, spark):
        """cost_usd must never be null in the fact table."""
        from src.transformations.build_dimensions import (
            build_dim_service, build_dim_account, build_dim_region
        )
        from src.transformations.build_fact import build_fact

        schema = StructType([
            StructField("date_key",       StringType(), False),
            StructField("cloud_provider", StringType(), False),
            StructField("account_id",     StringType(), False),
            StructField("account_name",   StringType(), True),
            StructField("service_name",   StringType(), False),
            StructField("region_code",    StringType(), True),
            StructField("usage_quantity", DoubleType(), True),
            StructField("usage_unit",     StringType(), True),
            StructField("cost_usd",       DoubleType(), False),
            StructField("environment",    StringType(), True),
            StructField("team",           StringType(), True),
            StructField("resource_id",    StringType(), True),
        ])
        rows = [("2024-03-01", "AWS", "111", "prod", "Amazon EC2", "us-east-1", 10.0, "Hrs", 5.0, "prod", "eng", "i-a")]
        combined = spark.createDataFrame(rows, schema=schema)

        dim_service = build_dim_service(spark, combined)
        dim_account = build_dim_account(spark, combined)
        dim_region  = build_dim_region(spark, combined)
        fact        = build_fact(spark, combined, dim_service, dim_account, dim_region)

        null_count = fact.filter(fact.cost_usd.isNull()).count()
        assert null_count == 0
