# Data Dictionary — FinOps Multi-Cloud Cost Data Mart

## Overview

The data mart uses a **star schema** with one fact table and four dimension tables.
All tables are stored as **Delta Lake** format under `data/processed/`.

---

## fact_cloud_costs

The central fact table. One row = one billing line item from either AWS or GCP.

| Column | Type | Nullable | Description |
|---|---|---|---|
| cost_id | STRING | No | MD5 surrogate key (unique per row) |
| date_key | STRING | No | Foreign key → dim_date. Format: YYYY-MM-DD |
| service_key | STRING | Yes | Foreign key → dim_service (null if service not in dim) |
| account_key | STRING | Yes | Foreign key → dim_account |
| region_key | STRING | Yes | Foreign key → dim_region |
| cloud_provider | STRING | No | `AWS` or `GCP` |
| cost_usd | DOUBLE | No | Billed amount in USD. Can be negative (GCP credits) |
| usage_quantity | DOUBLE | Yes | Amount consumed. Units defined by usage_unit |
| usage_unit | STRING | Yes | e.g. `Hrs`, `GB`, `Requests`, `core hours` |
| tags | MAP<STRING,STRING> | Yes | Resource metadata. Always contains `environment` and `team` keys |

**Partitioned by:** `date_key` (4 partitions locally; use `YYYY-MM` in production)
**Z-ordered by:** `date_key`, `cloud_provider`

---

## dim_date

One row per calendar date that appears in the billing data.

| Column | Type | Description |
|---|---|---|
| date_key | STRING | PK. Format: YYYY-MM-DD |
| year | STRING | e.g. `2024` |
| month | STRING | Zero-padded: `01`–`12` |
| day | STRING | Zero-padded: `01`–`31` |
| quarter | STRING | `Q1`, `Q2`, `Q3`, `Q4` |
| month_name | STRING | `January`, `February`, … |
| day_of_week | STRING | `Monday`, `Tuesday`, … |
| is_weekend | STRING | `true` if Saturday or Sunday, else `false` |

---

## dim_service

One row per unique (cloud_provider, service_name) combination.

| Column | Type | Description |
|---|---|---|
| service_key | STRING | PK. MD5 of `cloud_provider|service_name` |
| cloud_provider | STRING | `AWS` or `GCP` |
| service_name | STRING | e.g. `Amazon EC2`, `Compute Engine` |
| service_category | STRING | `Compute`, `Storage`, `Database`, `Analytics`, `Network`, `AI/ML`, `Messaging`, `Operations`, `Other` |

---

## dim_account

One row per unique (cloud_provider, account_id) combination.

| Column | Type | Description |
|---|---|---|
| account_key | STRING | PK. MD5 of `cloud_provider|account_id` |
| cloud_provider | STRING | `AWS` or `GCP` |
| account_id | STRING | AWS account ID or GCP project ID |
| account_name | STRING | Human-readable name |
| team | STRING | Engineering team that owns the account |
| environment | STRING | `prod`, `dev`, `staging`, or `unknown` |

---

## dim_region

One row per unique (cloud_provider, region_code) combination.

| Column | Type | Description |
|---|---|---|
| region_key | STRING | PK. MD5 of `cloud_provider|region_code` |
| cloud_provider | STRING | `AWS` or `GCP` |
| region_code | STRING | e.g. `us-east-1`, `us-central1` |
| region_name | STRING | Human-readable: `US East (N. Virginia)` |
| geography | STRING | `US`, `EU`, `APAC`, `Global`, or `Other` |

---

## Relationships

```
dim_date     ─┐
dim_service  ─┤─── fact_cloud_costs
dim_account  ─┤
dim_region   ─┘
```

All joins are `LEFT` joins from fact to dimensions.
The `*_key` columns in the fact table are MD5 hashes — identical inputs always
produce identical keys, ensuring referential consistency across incremental loads.

---

## Source-to-Target Mapping

### AWS → Canonical

| AWS Column | Canonical Column | Notes |
|---|---|---|
| bill_date | date_key | — |
| account_id | account_id | — |
| account_name | account_name | — |
| service_name | service_name | — |
| region | region_code | AZ suffix stripped |
| usage_quantity | usage_quantity | — |
| usage_unit | usage_unit | — |
| cost_usd | cost_usd | — |
| — | cloud_provider | Hardcoded `AWS` |

### GCP → Canonical

| GCP Column | Canonical Column | Notes |
|---|---|---|
| usage_date | date_key | — |
| project_id | account_id | GCP project = AWS account |
| project_name | account_name | — |
| service_description | service_name | — |
| location_region | region_code | Zone suffix stripped |
| usage_amount | usage_quantity | — |
| usage_unit | usage_unit | — |
| cost | cost_usd | Negative = credit |
| — | cloud_provider | Hardcoded `GCP` |
