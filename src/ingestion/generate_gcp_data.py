"""
generate_gcp_data.py
--------------------
Generates synthetic GCP Billing export CSV (mirrors BigQuery billing export format).

GCP billing has a different schema from AWS:
  - "project" instead of "account"
  - "service_description" + "sku_description" instead of just "service_name"
  - "location_region" instead of "region"
  - "cost" instead of "cost_usd"

The transform_gcp.py step maps these to the canonical normalized schema.

Run standalone:
    python src/ingestion/generate_gcp_data.py
"""

import csv
import os
import random
from datetime import date, timedelta

from faker import Faker

fake = Faker()
random.seed(99)
Faker.seed(99)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NUM_ROWS = 5_000
START_DATE = date(2024, 1, 1)
END_DATE = date(2024, 12, 31)
OUTPUT_PATH = "data/raw/gcp/gcp_billing.csv"

# ---------------------------------------------------------------------------
# Reference data matching real GCP service naming
# ---------------------------------------------------------------------------
GCP_SERVICES = [
    ("Compute Engine", "N2 instance Core running", "Compute"),
    ("Cloud Storage", "Standard Storage US", "Storage"),
    ("BigQuery", "Analysis", "Analytics"),
    ("Cloud SQL", "Cloud SQL for PostgreSQL", "Database"),
    ("Google Kubernetes Engine", "Cluster Management Fee", "Compute"),
    ("Cloud Run", "CPU Allocation Time", "Compute"),
    ("Vertex AI", "Custom Training", "AI/ML"),
    ("Cloud Pub/Sub", "Message Delivery Basic", "Messaging"),
    ("Cloud Dataflow", "Batch Processing", "Analytics"),
    ("Cloud Spanner", "Processing Units", "Database"),
    ("Cloud Logging", "Log Volume", "Operations"),
    ("Cloud CDN", "Cache Egress", "Network"),
    ("Cloud Load Balancing", "Forwarding Rule", "Network"),
    ("Networking", "Premium Tier Egress", "Network"),
]

GCP_REGIONS = [
    "us-central1", "us-east1", "us-west1", "us-west2",
    "europe-west1", "europe-west2", "asia-east1", "asia-southeast1",
]

PROJECTS = [
    ("proj-prod-001", "production-platform", "platform", "prod"),
    ("proj-data-001", "data-engineering-prod", "data-engineering", "prod"),
    ("proj-dev-001", "development-sandbox", "platform", "dev"),
    ("proj-ml-001", "ml-platform-prod", "ml-platform", "prod"),
    ("proj-stg-001", "staging-env", "data-engineering", "staging"),
]

USAGE_UNITS = {
    "Compute Engine": ("core hours", "core hours"),
    "Cloud Storage": ("gibibyte month", "GiB-Mo"),
    "BigQuery": ("tebibyte", "TiB"),
    "Cloud SQL": ("core hours", "core hours"),
    "Google Kubernetes Engine": ("hours", "Hrs"),
    "Cloud Run": ("vCPU-seconds", "vCPU-s"),
    "Vertex AI": ("node hours", "node Hrs"),
    "Cloud Pub/Sub": ("gibibytes", "GiB"),
    "Cloud Dataflow": ("DCU hours", "DCU-Hrs"),
    "Cloud Spanner": ("processing unit hours", "PU-Hrs"),
    "Cloud Logging": ("gibibytes", "GiB"),
    "Cloud CDN": ("gibibytes", "GiB"),
    "Cloud Load Balancing": ("hours", "Hrs"),
    "Networking": ("gibibytes", "GiB"),
}

COST_RANGES = {
    "Compute Engine": (0.5, 200.0),
    "Cloud Storage": (0.01, 25.0),
    "BigQuery": (0.1, 80.0),
    "Cloud SQL": (0.3, 60.0),
    "Google Kubernetes Engine": (0.5, 150.0),
    "Cloud Run": (0.001, 10.0),
    "Vertex AI": (0.5, 100.0),
    "Cloud Pub/Sub": (0.01, 15.0),
    "Cloud Dataflow": (0.1, 40.0),
    "Cloud Spanner": (1.0, 80.0),
    "Cloud Logging": (0.01, 10.0),
    "Cloud CDN": (0.01, 20.0),
    "Cloud Load Balancing": (0.05, 15.0),
    "Networking": (0.01, 50.0),
}


def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


def generate_gcp_data(output_path: str = OUTPUT_PATH, num_rows: int = NUM_ROWS) -> str:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fieldnames = [
        "usage_date", "project_id", "project_name", "service_description",
        "sku_description", "location_region", "usage_amount", "usage_unit",
        "cost", "environment", "team", "resource_name",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_rows):
            service_name, sku, _ = random.choice(GCP_SERVICES)
            project_id, project_name, team, environment = random.choice(PROJECTS)
            usage_type, usage_unit = USAGE_UNITS.get(service_name, ("units", "Units"))
            cost_min, cost_max = COST_RANGES.get(service_name, (0.01, 10.0))

            # GCP occasionally emits negative costs (credits/refunds)
            cost = round(random.uniform(cost_min, cost_max), 6)
            if random.random() < 0.02:   # 2% chance of credit
                cost = -round(random.uniform(0.01, 5.0), 6)

            writer.writerow({
                "usage_date": random_date(START_DATE, END_DATE),
                "project_id": project_id,
                "project_name": project_name,
                "service_description": service_name,
                "sku_description": sku,
                "location_region": random.choice(GCP_REGIONS),
                "usage_amount": round(random.uniform(0.001, 500.0), 4),
                "usage_unit": usage_unit,
                "cost": cost,
                "environment": environment,
                "team": team,
                "resource_name": f"projects/{project_id}/instances/{fake.slug()}",
            })

    print(f"[GCP] Generated {num_rows} rows → {output_path}")
    return output_path


if __name__ == "__main__":
    generate_gcp_data()
