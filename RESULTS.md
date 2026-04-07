# Pipeline Run Results

## Environment
- Python 3.12 (Anaconda)
- PySpark 3.5.1 + Delta Lake 3.2.0
- Run time: 13 seconds
- Platform: macOS (local)

## Data Generated
| Source | Raw Rows |
|---|---|
| AWS Billing (simulated) | 5,000 |
| GCP Billing (simulated) | 5,000 |
| **Combined** | **10,000** |

## Delta Lake Tables Written
| Table | Rows |
|---|---|
| fact_cloud_costs | 10,000 |
| dim_date | 366 |
| dim_service | 28 |
| dim_account | 10 |
| dim_region | 16 |

## Sample Query Results

### Total Spend by Cloud Provider
| cloud_provider | total_cost_usd | num_line_items |
|---|---|---|
| AWS | 150,830.38 | 5,000 |
| GCP | 149,785.51 | 5,000 |

### Top 10 Most Expensive Services
| cloud_provider | service_name | service_category | total_cost_usd |
|---|---|---|---|
| GCP | Compute Engine | Compute | 36,654.58 |
| AWS | Amazon EKS | Compute | 34,766.30 |
| AWS | Amazon EC2 | Compute | 25,832.96 |
| GCP | Google Kubernetes Engine | Compute | 24,106.93 |
| AWS | Amazon SageMaker | AI/ML | 20,968.57 |
| AWS | Amazon Redshift | Analytics | 18,683.60 |
| GCP | Vertex AI | AI/ML | 17,886.27 |
| AWS | Amazon RDS | Database | 15,320.36 |
| GCP | Cloud Spanner | Database | 14,253.34 |
| GCP | BigQuery | Analytics | 14,237.14 |

### Monthly Cost Trend (Jan–Oct 2024)
| Month | AWS | GCP |
|---|---|---|
| January | 12,963.91 | 10,998.66 |
| February | 9,899.09 | 12,336.16 |
| March | 14,702.20 | 14,247.43 |
| April | 11,484.97 | 12,433.18 |
| May | 12,960.37 | 12,646.05 |
| June | 12,861.65 | 13,040.08 |
| July | 12,221.67 | 12,349.81 |
| August | 11,847.89 | 14,062.47 |
| September | 11,262.38 | 10,796.69 |
| October | 15,045.23 | 10,814.06 |

### Cost by Team and Environment
| team | environment | cloud_provider | total_cost_usd |
|---|---|---|---|
| platform | dev | GCP | 33,363.49 |
| platform | dev | AWS | 31,704.62 |
| data-engineering | prod | AWS | 30,296.84 |
| platform | prod | AWS | 30,232.04 |
| data-engineering | prod | GCP | 30,212.19 |

### Cost by Geography
| geography | cloud_provider | total_cost_usd | avg_cost_per_line_item |
|---|---|---|---|
| US | GCP | 76,396.92 | 30.18 |
| US | AWS | 75,151.63 | 30.27 |
| APAC | AWS | 38,766.50 | 31.75 |
| APAC | GCP | 37,266.75 | 29.74 |
| EU | AWS | 36,912.25 | 28.48 |
| EU | GCP | 36,121.85 | 29.71 |

## Performance Optimizations Applied
| Technique | Result |
|---|---|
| Broadcast joins (dim tables <1MB) | No shuffle on 10K fact rows |
| DataFrame caching | Single compute pass for all queries |
| Partition pruning (date_key) | Reads only relevant date partitions |
| Repartition before write (4 partitions) | Even file sizes in Delta |
| Spark workers pinned to Python 3.12 | Version mismatch eliminated |
