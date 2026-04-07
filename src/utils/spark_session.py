"""
spark_session.py
----------------
Shared SparkSession factory for local development.
On Databricks, replace this with: spark = SparkSession.builder.getOrCreate()
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "FinOps-DataMart") -> SparkSession:
    """
    Creates (or reuses) a local SparkSession with Delta Lake support.

    Delta Lake is configured via the spark.jars.packages setting, which
    automatically downloads the Delta JAR on first run. Subsequent runs
    use the cached JAR from ~/.ivy2/cache.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # use all available CPU cores locally
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0",
        )
        # Performance tuning: use all cores for shuffle partitions
        .config("spark.sql.shuffle.partitions", "8")
        # Disable noisy Spark UI for local runs
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Suppress verbose INFO logs — only show WARN and above
    spark.sparkContext.setLogLevel("WARN")
    return spark
