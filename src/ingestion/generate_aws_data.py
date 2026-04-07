"""
generate_aws_data.py
--------------------
Generates a realistic synthetic AWS Cost & Usage Report CSV.

Why fake data?
  - Lets any recruiter or engineer run the full pipeline without an AWS account
  - Covers edge cases: missing regions, nulls, multi-team tagging, varied services
  - Deterministic seed means tests are reproducible

Run standalone:
    python src/ingestion/generate_aws_data.py
"""

import csv
import os
import random
from datetime import date, timedelta

from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ---------------------------------------------------------------------------
# Configuration — tweak these to change data volume / date range
# ---------------------------------------------------------------------------
NUM_ROWS = 5_000
START_DATE = date(2024, 1, 1)
END_DATE = date(2024, 12, 31)
OUTPUT_PATH = "data/raw/aws/aws_billing.csv"

# ---------------------------------------------------------------------------
# Reference data matching real AWS naming conventions
# ---------------------------------------------------------------------------
AWS_SERVICES = [
    ("Amazon EC2", "Compute"),
    ("Amazon S3", "Storage"),
    ("Amazon RDS", "Database"),
    ("AWS Lambda", "Compute"),
    ("Amazon CloudFront", "Network"),
    ("Amazon EKS", "Compute"),
    ("Amazon Redshift", "Analytics"),
    ("Amazon DynamoDB", "Database"),
    ("AWS Glue", "Analytics"),
    ("Amazon SageMaker", "AI/ML"),
    ("Amazon Bedrock", "AI/ML"),
    ("Amazon VPC", "Network"),
    ("Amazon Route 53", "Network"),
    ("AWS Data Transfer", "Network"),
]

AWS_REGIONS = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-northeast-1",
]

ACCOUNTS = [
    ("123456789001", "prod-account", "platform", "prod"),
    ("123456789002", "data-account", "data-engineering", "prod"),
    ("123456789003", "dev-account", "platform", "dev"),
    ("123456789004", "ml-account", "ml-platform", "prod"),
    ("123456789005", "staging-account", "data-engineering", "staging"),
]

USAGE_TYPES = {
    "Amazon EC2": ("BoxUsage:t3.medium", "Hrs"),
    "Amazon S3": ("TimedStorage-ByteHrs", "GB-Mo"),
    "Amazon RDS": ("InstanceUsage:db.t3.medium", "Hrs"),
    "AWS Lambda": ("Lambda-GB-Second", "GB-Second"),
    "Amazon CloudFront": ("DataTransfer-Out-Bytes", "GB"),
    "Amazon EKS": ("AmazonEKS-Hours:t3.large", "Hrs"),
    "Amazon Redshift": ("Node:dc2.large", "Hrs"),
    "Amazon DynamoDB": ("WriteCapacityUnit-Hrs", "WCU-Hrs"),
    "AWS Glue": ("Crawler-DPU-Hour", "DPU-Hour"),
    "Amazon SageMaker": ("ml.m5.xlarge", "Hrs"),
    "Amazon Bedrock": ("claude-3-sonnet-tokens", "1K-tokens"),
    "Amazon VPC": ("NatGateway-Hours", "Hrs"),
    "Amazon Route 53": ("DNS-Queries", "1M-queries"),
    "AWS Data Transfer": ("DataTransfer-Regional-Bytes", "GB"),
}

# Cost ranges (USD) per service — loosely realistic
COST_RANGES = {
    "Amazon EC2": (0.5, 150.0),
    "Amazon S3": (0.01, 30.0),
    "Amazon RDS": (0.3, 80.0),
    "AWS Lambda": (0.0001, 5.0),
    "Amazon CloudFront": (0.01, 20.0),
    "Amazon EKS": (0.5, 200.0),
    "Amazon Redshift": (1.0, 100.0),
    "Amazon DynamoDB": (0.01, 40.0),
    "AWS Glue": (0.1, 25.0),
    "Amazon SageMaker": (0.5, 120.0),
    "Amazon Bedrock": (0.01, 15.0),
    "Amazon VPC": (0.05, 10.0),
    "Amazon Route 53": (0.001, 2.0),
    "AWS Data Transfer": (0.01, 50.0),
}


def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


def generate_aws_data(output_path: str = OUTPUT_PATH, num_rows: int = NUM_ROWS) -> str:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fieldnames = [
        "bill_date", "account_id", "account_name", "service_name",
        "region", "usage_type", "usage_quantity", "usage_unit",
        "cost_usd", "environment", "team", "resource_id",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_rows):
            service_name, _ = random.choice(AWS_SERVICES)
            account_id, account_name, team, environment = random.choice(ACCOUNTS)
            usage_type, usage_unit = USAGE_TYPES.get(service_name, ("Generic-Usage", "Units"))
            cost_min, cost_max = COST_RANGES.get(service_name, (0.01, 10.0))

            writer.writerow({
                "bill_date": random_date(START_DATE, END_DATE),
                "account_id": account_id,
                "account_name": account_name,
                "service_name": service_name,
                "region": random.choice(AWS_REGIONS),
                "usage_type": usage_type,
                "usage_quantity": round(random.uniform(0.1, 1000.0), 4),
                "usage_unit": usage_unit,
                "cost_usd": round(random.uniform(cost_min, cost_max), 6),
                "environment": environment,
                "team": team,
                "resource_id": f"i-{fake.hexify(text='^^^^^^^^^^^^^^^^')}",
            })

    print(f"[AWS] Generated {num_rows} rows → {output_path}")
    return output_path


if __name__ == "__main__":
    generate_aws_data()
