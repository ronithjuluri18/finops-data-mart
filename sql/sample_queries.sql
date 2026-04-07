-- ============================================================
-- FinOps Data Mart — Sample Reporting Queries
-- These queries simulate what Tableau or a BI tool would run
-- against the Delta Lake star schema.
-- ============================================================

-- 1. Total spend by cloud provider
SELECT cloud_provider,
       ROUND(SUM(cost_usd), 2)  AS total_cost_usd,
       COUNT(*)                  AS num_line_items
FROM   fact_cloud_costs
GROUP  BY cloud_provider
ORDER  BY total_cost_usd DESC;


-- 2. Top 10 most expensive services
SELECT s.cloud_provider,
       s.service_name,
       s.service_category,
       ROUND(SUM(f.cost_usd), 2)  AS total_cost_usd
FROM   fact_cloud_costs f
JOIN   dim_service s ON f.service_key = s.service_key
GROUP  BY s.cloud_provider, s.service_name, s.service_category
ORDER  BY total_cost_usd DESC
LIMIT  10;


-- 3. Monthly cost trend
SELECT d.year,
       d.month,
       d.month_name,
       f.cloud_provider,
       ROUND(SUM(f.cost_usd), 2)  AS monthly_cost_usd
FROM   fact_cloud_costs f
JOIN   dim_date d ON f.date_key = d.date_key
WHERE  d.year = '2024'
GROUP  BY d.year, d.month, d.month_name, f.cloud_provider
ORDER  BY d.month, f.cloud_provider;


-- 4. Cost breakdown by team and environment
SELECT a.team,
       a.environment,
       a.cloud_provider,
       ROUND(SUM(f.cost_usd), 2)  AS total_cost_usd
FROM   fact_cloud_costs f
JOIN   dim_account a ON f.account_key = a.account_key
GROUP  BY a.team, a.environment, a.cloud_provider
ORDER  BY total_cost_usd DESC;


-- 5. Regional cost distribution
SELECT r.geography,
       r.region_name,
       f.cloud_provider,
       ROUND(SUM(f.cost_usd), 2)  AS total_cost_usd
FROM   fact_cloud_costs f
JOIN   dim_region r ON f.region_key = r.region_key
GROUP  BY r.geography, r.region_name, f.cloud_provider
ORDER  BY total_cost_usd DESC;


-- 6. Weekend vs weekday spend (spot waste detection)
SELECT d.is_weekend,
       d.day_of_week,
       f.cloud_provider,
       ROUND(SUM(f.cost_usd), 2)       AS total_cost_usd,
       ROUND(AVG(f.cost_usd), 4)       AS avg_line_item_cost,
       COUNT(*)                         AS num_line_items
FROM   fact_cloud_costs f
JOIN   dim_date d ON f.date_key = d.date_key
GROUP  BY d.is_weekend, d.day_of_week, f.cloud_provider
ORDER  BY d.is_weekend DESC, total_cost_usd DESC;


-- 7. Compute vs Storage vs other category split
SELECT s.service_category,
       f.cloud_provider,
       ROUND(SUM(f.cost_usd), 2)   AS total_cost_usd,
       ROUND(
         100.0 * SUM(f.cost_usd)
         / SUM(SUM(f.cost_usd)) OVER (PARTITION BY f.cloud_provider),
         1
       )                            AS pct_of_provider_spend
FROM   fact_cloud_costs f
JOIN   dim_service s ON f.service_key = s.service_key
GROUP  BY s.service_category, f.cloud_provider
ORDER  BY f.cloud_provider, total_cost_usd DESC;


-- 8. AI/ML spend specifically (for GenAI cost tracking)
SELECT d.year,
       d.month,
       d.month_name,
       s.service_name,
       f.cloud_provider,
       ROUND(SUM(f.cost_usd), 2)  AS ai_spend_usd
FROM   fact_cloud_costs f
JOIN   dim_service s ON f.service_key = s.service_key
JOIN   dim_date    d ON f.date_key    = d.date_key
WHERE  s.service_category = 'AI/ML'
GROUP  BY d.year, d.month, d.month_name, s.service_name, f.cloud_provider
ORDER  BY d.month, ai_spend_usd DESC;
