"""
finops_pipeline_dag.py
----------------------
Airflow DAG that orchestrates the full FinOps data mart pipeline.

DAG structure:
    generate_data >> [transform_aws, transform_gcp] >> build_star_schema >> validate >> notify_success
                                                                          ↓ (on failure)
                                                                     alert_on_failure

Schedule: Daily at 06:00 UTC (billing exports from both clouds are
available by this time for the previous day).

Failure alerting:
  - Every task has on_failure_callback pointing to alert_on_failure()
  - In production, replace the print() with Slack/PagerDuty/email webhook

Tableau refresh:
  - notify_success task calls refresh_tableau_extract() after a successful run
  - In production, replace with a real Tableau REST API call
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Default arguments applied to every task unless overridden
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,           # don't wait for yesterday's run
    "start_date": days_ago(1),
    "email_on_failure": False,          # we use the callback instead
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Alert callback — fires whenever ANY task in the DAG fails
# ---------------------------------------------------------------------------
def alert_on_failure(context):
    """
    Called automatically when a task fails.
    In production: POST to Slack webhook, trigger PagerDuty, send email, etc.
    """
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    exception = context.get("exception", "Unknown error")

    message = (
        f":red_circle: *FinOps Pipeline FAILED*\n"
        f"  DAG: `{dag_id}`\n"
        f"  Task: `{task_id}`\n"
        f"  Run ID: `{run_id}`\n"
        f"  Error: `{exception}`\n"
        f"  Time: {datetime.utcnow().isoformat()}Z"
    )
    print(f"[ALERT] {message}")
    # production: requests.post(SLACK_WEBHOOK_URL, json={"text": message})


# ---------------------------------------------------------------------------
# Task functions — each wraps the corresponding src/ module
# ---------------------------------------------------------------------------
def task_generate_data(**context):
    """Generate synthetic AWS + GCP billing CSVs."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from src.ingestion.generate_aws_data import generate_aws_data
    from src.ingestion.generate_gcp_data import generate_gcp_data

    aws_path = generate_aws_data()
    gcp_path = generate_gcp_data()

    # Push paths to XCom so downstream tasks can read them
    context["ti"].xcom_push(key="aws_path", value=aws_path)
    context["ti"].xcom_push(key="gcp_path", value=gcp_path)


def task_transform_aws(**context):
    """Normalize AWS billing data → canonical schema."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from src.utils.spark_session import get_spark
    from src.transformations.transform_aws import transform_aws

    aws_path = context["ti"].xcom_pull(key="aws_path", task_ids="generate_data")
    spark = get_spark("FinOps-AWS-Transform")
    df = transform_aws(spark, aws_path)

    # Write intermediate parquet for cross-task handoff
    out = "data/processed/tmp_aws_normalized"
    df.write.mode("overwrite").parquet(out)
    context["ti"].xcom_push(key="aws_normalized_path", value=out)
    spark.stop()


def task_transform_gcp(**context):
    """Normalize GCP billing data → canonical schema."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from src.utils.spark_session import get_spark
    from src.transformations.transform_gcp import transform_gcp

    gcp_path = context["ti"].xcom_pull(key="gcp_path", task_ids="generate_data")
    spark = get_spark("FinOps-GCP-Transform")
    df = transform_gcp(spark, gcp_path)

    out = "data/processed/tmp_gcp_normalized"
    df.write.mode("overwrite").parquet(out)
    context["ti"].xcom_push(key="gcp_normalized_path", value=out)
    spark.stop()


def task_build_star_schema(**context):
    """
    Union AWS + GCP normalized data, build dimensions and fact table,
    write everything as Delta Lake.
    """
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from src.utils.spark_session import get_spark
    from src.transformations.build_dimensions import (
        build_dim_date, build_dim_service, build_dim_account, build_dim_region,
    )
    from src.transformations.build_fact import build_fact, write_delta_tables

    ti = context["ti"]
    aws_path = ti.xcom_pull(key="aws_normalized_path", task_ids="transform_aws")
    gcp_path = ti.xcom_pull(key="gcp_normalized_path", task_ids="transform_gcp")

    spark = get_spark("FinOps-StarSchema")

    aws_df = spark.read.parquet(aws_path)
    gcp_df = spark.read.parquet(gcp_path)
    combined = aws_df.unionByName(gcp_df)

    dim_date    = build_dim_date(spark, combined)
    dim_service = build_dim_service(spark, combined)
    dim_account = build_dim_account(spark, combined)
    dim_region  = build_dim_region(spark, combined)
    fact        = build_fact(spark, combined, dim_service, dim_account, dim_region)

    write_delta_tables(fact, dim_date, dim_service, dim_account, dim_region)
    spark.stop()


def task_validate(**context):
    """
    Basic data quality checks on the written Delta tables.
    Fails the DAG if any check fails — preventing bad data from reaching BI.
    """
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from src.utils.spark_session import get_spark

    spark = get_spark("FinOps-Validate")
    base = "data/processed"

    checks = {
        "fact_cloud_costs": "data/processed/fact_cloud_costs",
        "dim_date":         "data/processed/dim_date",
        "dim_service":      "data/processed/dim_service",
        "dim_account":      "data/processed/dim_account",
        "dim_region":       "data/processed/dim_region",
    }

    for table_name, path in checks.items():
        df = spark.read.format("delta").load(path)
        count = df.count()
        null_cost = 0

        if table_name == "fact_cloud_costs":
            null_cost = df.filter(df.cost_usd.isNull()).count()

        print(f"[Validate] {table_name}: {count:,} rows, {null_cost} null costs")
        assert count > 0, f"VALIDATION FAILED: {table_name} is empty!"
        assert null_cost == 0, f"VALIDATION FAILED: {table_name} has {null_cost} null cost rows!"

    print("[Validate] All checks passed ✓")
    spark.stop()


def task_notify_success(**context):
    """
    Fires after a successful run.
    In production: trigger Tableau extract refresh via REST API.
    """
    run_id = context["run_id"]
    execution_date = context["execution_date"]

    print(f"[Notify] Pipeline completed successfully!")
    print(f"  Run ID: {run_id}")
    print(f"  Execution date: {execution_date}")
    print(f"  Tableau dashboard refresh triggered (simulated)")

    # Production implementation:
    # import tableauserverclient as TSC
    # tableau_auth = TSC.TableauAuth(username, password, site_id)
    # server = TSC.Server(tableau_server_url)
    # with server.auth.sign_in(tableau_auth):
    #     server.datasources.refresh(finops_datasource_id)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="finops_pipeline",
    description="FinOps multi-cloud cost data mart — daily refresh",
    schedule_interval="0 6 * * *",   # 06:00 UTC daily
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["finops", "data-mart", "delta-lake"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=task_generate_data,
        on_failure_callback=alert_on_failure,
    )

    transform_aws = PythonOperator(
        task_id="transform_aws",
        python_callable=task_transform_aws,
        on_failure_callback=alert_on_failure,
    )

    transform_gcp = PythonOperator(
        task_id="transform_gcp",
        python_callable=task_transform_gcp,
        on_failure_callback=alert_on_failure,
    )

    build_star_schema = PythonOperator(
        task_id="build_star_schema",
        python_callable=task_build_star_schema,
        on_failure_callback=alert_on_failure,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
        on_failure_callback=alert_on_failure,
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=task_notify_success,
    )

    # DAG dependency graph:
    # generate_data runs first, then AWS and GCP transforms run IN PARALLEL,
    # then star schema build, validation, and notification run in sequence
    generate_data >> [transform_aws, transform_gcp] >> build_star_schema >> validate >> notify_success
