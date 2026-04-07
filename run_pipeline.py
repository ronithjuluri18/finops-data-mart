"""
run_pipeline.py
---------------
Runs the complete FinOps data mart pipeline locally — no Airflow, no cloud.
Equivalent to what the Airflow DAG does, but as a single Python script.

Usage:
    python run_pipeline.py

What it does:
    1. Generate synthetic AWS + GCP billing CSVs (~10K rows total)
    2. Read + normalize both datasets with PySpark
    3. Union them into a single combined DataFrame
    4. Build 4 dimension tables (dim_date, dim_service, dim_account, dim_region)
    5. Build fact_cloud_costs with broadcast joins + caching
    6. Write all 5 tables as Delta Lake to data/processed/
    7. Run 5 sample FinOps SQL queries and print results
    8. Print a summary with row counts and timing
"""

import os
import sys
import time

# Ensure src/ is importable from the project root
sys.path.insert(0, os.path.dirname(__file__))

from src.ingestion.generate_aws_data import generate_aws_data
from src.ingestion.generate_gcp_data import generate_gcp_data
from src.utils.spark_session import get_spark
from src.transformations.transform_aws import transform_aws
from src.transformations.transform_gcp import transform_gcp
from src.transformations.build_dimensions import (
    build_dim_date,
    build_dim_service,
    build_dim_account,
    build_dim_region,
)
from src.transformations.build_fact import build_fact, write_delta_tables


# ---------------------------------------------------------------------------
# Sample FinOps SQL queries — these simulate what a Tableau dashboard runs
# ---------------------------------------------------------------------------
QUERIES = [
    (
        "Total spend by cloud provider",
        """
        SELECT cloud_provider,
               ROUND(SUM(cost_usd), 2)           AS total_cost_usd,
               COUNT(*)                           AS num_line_items
        FROM   fact_cloud_costs
        GROUP  BY cloud_provider
        ORDER  BY total_cost_usd DESC
        """,
    ),
    (
        "Top 10 most expensive services (all clouds)",
        """
        SELECT s.cloud_provider,
               s.service_name,
               s.service_category,
               ROUND(SUM(f.cost_usd), 2)           AS total_cost_usd
        FROM   fact_cloud_costs f
        JOIN   dim_service s ON f.service_key = s.service_key
        GROUP  BY s.cloud_provider, s.service_name, s.service_category
        ORDER  BY total_cost_usd DESC
        LIMIT  10
        """,
    ),
    (
        "Monthly cost trend (last 6 months)",
        """
        SELECT d.year,
               d.month,
               d.month_name,
               f.cloud_provider,
               ROUND(SUM(f.cost_usd), 2)  AS monthly_cost_usd
        FROM   fact_cloud_costs f
        JOIN   dim_date d ON f.date_key = d.date_key
        WHERE  d.year = '2024'
        GROUP  BY d.year, d.month, d.month_name, f.cloud_provider
        ORDER  BY d.month, f.cloud_provider
        LIMIT  24
        """,
    ),
    (
        "Cost by team and environment",
        """
        SELECT a.team,
               a.environment,
               a.cloud_provider,
               ROUND(SUM(f.cost_usd), 2)  AS total_cost_usd
        FROM   fact_cloud_costs f
        JOIN   dim_account a ON f.account_key = a.account_key
        GROUP  BY a.team, a.environment, a.cloud_provider
        ORDER  BY total_cost_usd DESC
        """,
    ),
    (
        "Cost by geography (US vs EU vs APAC)",
        """
        SELECT r.geography,
               f.cloud_provider,
               ROUND(SUM(f.cost_usd), 2)  AS total_cost_usd,
               ROUND(AVG(f.cost_usd), 4)  AS avg_cost_per_line_item
        FROM   fact_cloud_costs f
        JOIN   dim_region r ON f.region_key = r.region_key
        GROUP  BY r.geography, f.cloud_provider
        ORDER  BY total_cost_usd DESC
        """,
    ),
]


def run_queries(spark, base_path: str):
    """Register Delta tables as Spark SQL temp views and run sample queries."""
    print("\n" + "=" * 60)
    print("  SAMPLE FINOPS QUERY RESULTS")
    print("=" * 60)

    tables = ["fact_cloud_costs", "dim_date", "dim_service", "dim_account", "dim_region"]
    for table in tables:
        df = spark.read.format("delta").load(f"{base_path}/{table}")
        df.createOrReplaceTempView(table)

    for title, sql in QUERIES:
        print(f"\n{'─' * 60}")
        print(f"  {title}")
        print(f"{'─' * 60}")
        spark.sql(sql).show(truncate=False)


def main():
    total_start = time.time()

    print("=" * 60)
    print("  FINOPS MULTI-CLOUD COST DATA MART — LOCAL PIPELINE RUN")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: Generate synthetic data
    # ------------------------------------------------------------------
    print("\n[Step 1/5] Generating synthetic billing data...")
    t0 = time.time()
    aws_path = generate_aws_data()
    gcp_path = generate_gcp_data()
    print(f"  Done in {time.time() - t0:.1f}s")

    # ------------------------------------------------------------------
    # Step 2: Start Spark and transform both sources
    # ------------------------------------------------------------------
    print("\n[Step 2/5] Starting PySpark and normalizing billing data...")
    print("  (First run downloads Delta Lake JARs — may take 1-2 min)")
    t0 = time.time()
    spark = get_spark("FinOps-LocalPipeline")

    aws_normalized  = transform_aws(spark, aws_path)
    gcp_normalized  = transform_gcp(spark, gcp_path)

    # Union both sources — they share the same canonical schema
    combined = aws_normalized.unionByName(gcp_normalized)
    combined.cache()
    total_rows = combined.count()
    print(f"  Combined rows: {total_rows:,}  |  Done in {time.time() - t0:.1f}s")

    # ------------------------------------------------------------------
    # Step 3: Build dimension tables
    # ------------------------------------------------------------------
    print("\n[Step 3/5] Building dimension tables...")
    t0 = time.time()
    dim_date    = build_dim_date(spark, combined)
    dim_service = build_dim_service(spark, combined)
    dim_account = build_dim_account(spark, combined)
    dim_region  = build_dim_region(spark, combined)
    print(f"  Done in {time.time() - t0:.1f}s")

    # ------------------------------------------------------------------
    # Step 4: Build fact table (broadcast joins + caching)
    # ------------------------------------------------------------------
    print("\n[Step 4/5] Building fact_cloud_costs with broadcast joins + caching...")
    t0 = time.time()
    fact = build_fact(spark, combined, dim_service, dim_account, dim_region)
    print(f"  Done in {time.time() - t0:.1f}s")

    # ------------------------------------------------------------------
    # Step 5: Write Delta Lake tables
    # ------------------------------------------------------------------
    print("\n[Step 5/5] Writing Delta Lake tables to data/processed/...")
    t0 = time.time()
    write_delta_tables(fact, dim_date, dim_service, dim_account, dim_region)
    print(f"  Done in {time.time() - t0:.1f}s")

    # ------------------------------------------------------------------
    # Run sample queries
    # ------------------------------------------------------------------
    run_queries(spark, "data/processed")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    elapsed = time.time() - total_start
    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Total run time     : {elapsed:.1f}s")
    print(f"  Combined input rows: {total_rows:,}")
    print(f"  Delta tables written:")
    for table in ["fact_cloud_costs", "dim_date", "dim_service", "dim_account", "dim_region"]:
        df = spark.read.format("delta").load(f"data/processed/{table}")
        print(f"    {table:<25} {df.count():>6,} rows")
    print("\n  data/processed/  ← your Delta Lake data mart is here")
    print("  Run: pytest tests/ -v  ← to run unit tests")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
