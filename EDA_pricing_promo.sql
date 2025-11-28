/*----- SQL bootstrap: indexing and data quality -----*/
USE olist_analytics;

-- performance indexes
CREATE INDEX idx_orders_ts_customer ON olist_orders (order_purchase_timestamp, customer_id);
CREATE INDEX idx_items_prod_seller_ship ON olist_order_items (product_id, seller_id, shipping_limit_date);
CREATE INDEX idx_payments_type ON olist_order_payments (payment_type);
CREATE INDEX idx_products_category ON olist_products (product_category_name);
CREATE INDEX idx_customers_unique_state ON olist_customers (customer_unique_id, customer_state);
CREATE INDEX idx_sellers_state ON olist_sellers (seller_state);
CREATE INDEX idx_reviews_order_score_date ON olist_order_reviews (order_id, review_score, review_creation_date);

-- row counts
SELECT 'orders' t, COUNT(*) c FROM olist_orders UNION ALL
SELECT 'items', COUNT(*) FROM olist_order_items UNION ALL
SELECT 'payments', COUNT(*) FROM olist_order_payments UNION ALL
SELECT 'products', COUNT(*) FROM olist_products UNION ALL
SELECT 'customers', COUNT(*) FROM olist_customers UNION ALL
SELECT 'sellers', COUNT(*) FROM olist_sellers UNION ALL
SELECT 'reviews', COUNT(*) FROM olist_order_reviews;

-- data quality checks
SELECT 'neg_price_items' t, COUNT(*) c FROM olist_order_items WHERE price < 0 OR freight_value < 0;
SELECT 'orders_missing_ts' t, COUNT(*) c FROM olist_orders WHERE order_purchase_timestamp IS NULL;
SELECT 'items_without_order' t, COUNT(*) c
FROM olist_order_items oi LEFT JOIN olist_orders o USING(order_id) WHERE o.order_id IS NULL;

-- order and shipping date range
SELECT MIN(order_purchase_timestamp) min_ts, MAX(order_purchase_timestamp) max_ts FROM olist_orders;
SELECT MIN(shipping_limit_date) min_ship, MAX(shipping_limit_date) max_ship FROM olist_order_items;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- product-month median pricing and QA -----*/

-- product-month median pricing with sample size
DROP VIEW IF EXISTS v_product_month_price_median;
CREATE OR REPLACE VIEW v_product_month_price_median AS
WITH priced AS (
  SELECT oi.product_id, DATE_FORMAT(o.order_purchase_timestamp,'%Y-%m') AS ym, oi.price
  FROM olist_order_items oi JOIN olist_orders o USING(order_id)
  WHERE oi.price IS NOT NULL
),
ranked AS (
  SELECT product_id, ym, price,
         ROW_NUMBER() OVER (PARTITION BY product_id, ym ORDER BY price) AS rn,
         COUNT(*)    OVER (PARTITION BY product_id, ym) AS cnt
  FROM priced
)
SELECT product_id, ym,
       AVG(price) AS median_price,
       MAX(cnt)   AS baseline_n
FROM ranked
WHERE rn IN (FLOOR((cnt+1)/2), CEIL((cnt+1)/2))
GROUP BY product_id, ym;

-- median view row count
SELECT COUNT(*) FROM v_product_month_price_median;

-- median view sample (10 rows)
SELECT * FROM v_product_month_price_median LIMIT 10;

-- median coverage vs distinct product-month groups
SELECT a.med_groups, b.distinct_groups, a.med_groups = b.distinct_groups AS exact_match
FROM (SELECT COUNT(*) AS med_groups FROM v_product_month_price_median) a
CROSS JOIN (
  SELECT COUNT(*) AS distinct_groups
  FROM (
    SELECT DISTINCT oi.product_id, DATE_FORMAT(o.order_purchase_timestamp,'%Y-%m') ym
    FROM olist_order_items oi JOIN olist_orders o USING(order_id)
    WHERE oi.price IS NOT NULL
  ) x
) b;

-- duplicate median check by product-month
SELECT product_id, ym, COUNT(*) c
FROM v_product_month_price_median
GROUP BY 1,2
HAVING c > 1
LIMIT 10;

-- median price distribution (min/avg/max)
SELECT MIN(median_price) min_med, AVG(median_price) avg_med, MAX(median_price) max_med
FROM v_product_month_price_median;

-- invalid median counts (null/zero/negative)
SELECT
  SUM(median_price IS NULL) AS null_med,
  SUM(median_price = 0)     AS zero_med,
  SUM(median_price < 0)     AS neg_med
FROM v_product_month_price_median;

-- distribution of item counts per product-month
SELECT n AS items_in_group, COUNT(*) AS product_month_groups
FROM (
  SELECT oi.product_id, DATE_FORMAT(o.order_purchase_timestamp,'%Y-%m') ym, COUNT(*) n
  FROM olist_order_items oi JOIN olist_orders o USING(order_id)
  WHERE oi.price IS NOT NULL
  GROUP BY 1,2
) g
GROUP BY n
ORDER BY n
LIMIT 10;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- item enrichment: join median and discount flags -----*/

-- item-level enrichment with discount flags
DROP VIEW IF EXISTS v_item_enriched;
CREATE OR REPLACE VIEW v_item_enriched AS
SELECT
  oi.order_id, oi.order_item_id, oi.product_id, oi.seller_id,
  oi.price, oi.freight_value, o.order_purchase_timestamp,
  DATE_FORMAT(o.order_purchase_timestamp,'%Y-%m') AS ym,
  pm.median_price, pm.baseline_n,
  CASE WHEN pm.median_price IS NOT NULL AND oi.price < 0.95*pm.median_price THEN 1 ELSE 0 END AS promo_discount_flag,
  CASE WHEN pm.baseline_n < 3 THEN 1 ELSE 0 END AS low_confidence_flag,
  CASE WHEN pm.median_price IS NOT NULL AND pm.baseline_n >= 3 AND oi.price < 0.95*pm.median_price THEN 1 ELSE 0 END AS trusted_discount_flag
FROM olist_order_items oi
JOIN olist_orders o USING(order_id)
LEFT JOIN v_product_month_price_median pm
  ON pm.product_id = oi.product_id
 AND pm.ym = DATE_FORMAT(o.order_purchase_timestamp,'%Y-%m');

-- Overall discount rate
SELECT COUNT(*) total_lines, SUM(promo_discount_flag) discounted_lines,
       ROUND(AVG(promo_discount_flag)*100,2) AS discount_rate_pct
FROM v_item_enriched;

-- median join coverage
SELECT
  SUM(median_price IS NULL) AS missing_median,
  ROUND(100*AVG(median_price IS NULL),2) AS pct_missing
FROM v_item_enriched;

-- monthly discount rate
SELECT ym,
       ROUND(AVG(promo_discount_flag)*100,2) AS discount_rate_pct
FROM v_item_enriched
GROUP BY ym ORDER BY ym;

-- category discount rate (top 10)
SELECT
  COALESCE(p.product_category_name,'(unknown)') AS category,
  ROUND(AVG(ie.promo_discount_flag)*100,2) AS discount_rate_pct,
  COUNT(*) AS line_count
FROM v_item_enriched ie
LEFT JOIN olist_products p ON p.product_id = ie.product_id
GROUP BY category
HAVING COUNT(*) >= 100
ORDER BY discount_rate_pct DESC, line_count DESC
LIMIT 10;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- item enrichment validation -----*/

-- row-count parity: items vs enriched
SELECT
  a.raw_item_rows,
  b.enriched_rows,
  (a.raw_item_rows = b.enriched_rows) AS counts_match
FROM (SELECT COUNT(*) AS raw_item_rows FROM olist_order_items) a
CROSS JOIN (SELECT COUNT(*) AS enriched_rows FROM v_item_enriched) b;

-- missing median counts
SELECT
  SUM(median_price IS NULL) AS missing_median,
  ROUND(100 * AVG(median_price IS NULL), 2) AS pct_missing
FROM v_item_enriched;

-- Low-confidence share (baseline_n < 3)
SELECT
  SUM(baseline_n < 3) AS low_conf_lines,
  ROUND(100 * AVG(baseline_n < 3), 2) AS pct_low_conf
FROM v_item_enriched
WHERE median_price IS NOT NULL;

-- discount rates (overall and trusted)
SELECT
  COUNT(*) AS total_lines,
  SUM(promo_discount_flag) AS discounted_lines,
  ROUND(AVG(promo_discount_flag) * 100, 2) AS discount_rate_pct,
  SUM(trusted_discount_flag) AS trusted_discounted_lines,
  ROUND(AVG(trusted_discount_flag) * 100, 2) AS trusted_discount_rate_pct
FROM v_item_enriched
WHERE median_price IS NOT NULL;

-- discount depth distribution (price/median)
SELECT
  bucket,
  COUNT(*) AS line,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM (
  SELECT
    CASE
      WHEN price / median_price < 0.80 THEN '<0.80'
      WHEN price / median_price < 0.90 THEN '0.80-0.90'
      WHEN price / median_price < 0.95 THEN '0.90-0.95'
      WHEN price / median_price < 1.00 THEN '0.95-1.00'
      WHEN price / median_price < 1.05 THEN '1.00-1.05'
      WHEN price / median_price < 1.10 THEN '1.05-1.10'
      ELSE '>1.10'
    END AS bucket
  FROM v_item_enriched
  WHERE median_price > 0 AND baseline_n >= 3
) x
GROUP BY bucket
ORDER BY
  CASE bucket
    WHEN '<0.80' THEN 1
    WHEN '0.80-0.90' THEN 2
    WHEN '0.90-0.95' THEN 3
    WHEN '0.95-1.00' THEN 4
    WHEN '1.00-1.05' THEN 5
    WHEN '1.05-1.10' THEN 6
    WHEN '>1.10' THEN 7
  END;

-- deep discount outliers (top 20)
SELECT
  order_id,
  order_item_id,
  product_id,
  ym,
  price,
  median_price,
  baseline_n,
  ROUND(price / median_price, 3) AS depth_ratio
FROM v_item_enriched
WHERE median_price > 0 AND baseline_n >= 3
ORDER BY depth_ratio ASC, baseline_n DESC
LIMIT 20;

-- monthly discount rates (overall vs trusted)
SELECT
  ym,
  ROUND(AVG(promo_discount_flag) * 100, 2) AS discount_rate_pct,
  ROUND(AVG(trusted_discount_flag) * 100, 2) AS trusted_discount_rate_pct,
  COUNT(*) AS line_count
FROM v_item_enriched
WHERE median_price IS NOT NULL
GROUP BY ym
ORDER BY ym;

-- category discount rates (top 10, volume threshold)
SELECT
  COALESCE(p.product_category_name, '(unknown)') AS category,
  ROUND(AVG(ie.promo_discount_flag) * 100, 2) AS discount_rate_pct,
  ROUND(AVG(ie.trusted_discount_flag) * 100, 2) AS trusted_discount_rate_pct,
  COUNT(*) AS line_count
FROM v_item_enriched ie
LEFT JOIN olist_products p ON p.product_id = ie.product_id
WHERE ie.median_price IS NOT NULL
GROUP BY category
HAVING COUNT(*) >= 100
ORDER BY trusted_discount_rate_pct DESC, line_count DESC
LIMIT 10;

-- duplicate check: (order_id, order_item_id) uniqueness
SELECT
  a.total_rows,
  b.distinct_pairs,
  (a.total_rows = b.distinct_pairs) AS no_duplicates
FROM (SELECT COUNT(*) AS total_rows FROM v_item_enriched) a
CROSS JOIN (SELECT COUNT(DISTINCT order_id, order_item_id) AS distinct_pairs FROM v_item_enriched) b;

-- duplicate sample
SELECT
  order_id,
  order_item_id,
  COUNT(*) AS dup_count
FROM v_item_enriched
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 10;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- order KPIs: revenue, freight, flags, margin proxy -----*/

-- order-level KPI view
DROP VIEW IF EXISTS v_order_kpis;
CREATE OR REPLACE VIEW v_order_kpis AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_purchase_timestamp,
  SUM(ie.price) AS items_revenue,
  SUM(ie.freight_value) AS freight_total,
  MAX(ie.promo_discount_flag)   AS any_discount_flag,
  MAX(ie.trusted_discount_flag) AS any_trusted_discount_flag,
  CASE WHEN SUM(ie.freight_value) = 0 THEN 1 ELSE 0 END AS free_shipping_flag,
  COALESCE(p.payments_total, 0) AS payment_value,
  (SUM(ie.price) - SUM(ie.freight_value)) AS margin_proxy
FROM olist_orders o
JOIN v_item_enriched ie USING(order_id)
LEFT JOIN (
  SELECT order_id, SUM(payment_value) AS payments_total
  FROM olist_order_payments
  GROUP BY order_id
) p USING(order_id)
GROUP BY o.order_id, o.customer_id, o.order_purchase_timestamp, p.payments_total;

-- AOV and margin by trusted promo flag
SELECT
  any_trusted_discount_flag AS trusted_discount_flag,
  COUNT(*) AS orders,
  ROUND(AVG(items_revenue),2) AS aov_items,
  ROUND(AVG(margin_proxy),2)  AS margin_per_order
FROM v_order_kpis
GROUP BY any_trusted_discount_flag
ORDER BY trusted_discount_flag DESC;

-- AOV and margin by free-shipping flag
SELECT
  free_shipping_flag,
  COUNT(*) AS orders,
  ROUND(AVG(items_revenue),2) AS aov_items,
  ROUND(AVG(margin_proxy),2)  AS margin_per_order
FROM v_order_kpis
GROUP BY free_shipping_flag
ORDER BY free_shipping_flag DESC;

-- cross-tab: free shipping by trusted promo
SELECT
  free_shipping_flag,
  any_trusted_discount_flag AS trusted_discount_flag,
  COUNT(*) AS orders,
  ROUND(AVG(items_revenue),2) AS aov_items,
  ROUND(AVG(margin_proxy),2)  AS margin_per_order
FROM v_order_kpis
GROUP BY free_shipping_flag, any_trusted_discount_flag
ORDER BY free_shipping_flag DESC, trusted_discount_flag DESC;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- order KPI validation -----*/

-- one-row-per-order validation
SELECT COUNT(*) AS kpi_rows FROM v_order_kpis;
SELECT COUNT(DISTINCT order_id) AS distinct_orders_in_items FROM v_item_enriched;
SELECT order_id, COUNT(*) AS c
FROM v_order_kpis
GROUP BY order_id
HAVING c > 1
LIMIT 10;

-- coverage: items vs KPI rows
SELECT
  (SELECT COUNT(DISTINCT order_id) FROM v_item_enriched) AS orders_in_items,
  (SELECT COUNT(*) FROM v_order_kpis)                    AS rows_in_kpi;

-- null checks (items_revenue, freight_total, payment_value)
SELECT
  SUM(items_revenue IS NULL) AS null_items_revenue,
  SUM(freight_total IS NULL) AS null_freight_total,
  SUM(payment_value IS NULL) AS null_payment_value
FROM v_order_kpis;

-- negative margin share
SELECT
  SUM(margin_proxy < 0) AS neg_margin_orders,
  ROUND(100 * AVG(margin_proxy < 0), 2) AS neg_margin_pct
FROM v_order_kpis;

-- payment reconciliation distribution (exact/small/big)
SELECT
  SUM(ABS(payment_value - (items_revenue + freight_total)) <= 0.01) AS exact_match,
  SUM(ABS(payment_value - (items_revenue + freight_total)) > 0.01
      AND ABS(payment_value - (items_revenue + freight_total)) <= 5) AS small_gap,
  SUM(ABS(payment_value - (items_revenue + freight_total)) > 5)      AS big_gap
FROM v_order_kpis;

-- free-shipping share
SELECT
  SUM(free_shipping_flag) AS free_ship_orders,
  COUNT(*) AS total_orders,
  ROUND(100 * AVG(free_shipping_flag), 2) AS free_ship_pct
FROM v_order_kpis;

-- stacking analysis: promo × free shipping
SELECT
  free_shipping_flag,
  any_trusted_discount_flag AS trusted_discount_flag,
  COUNT(*) AS orders
FROM v_order_kpis
GROUP BY free_shipping_flag, any_trusted_discount_flag
ORDER BY free_shipping_flag DESC, trusted_discount_flag DESC;

-- order timestamp coverage (min/max)
SELECT
  MIN(order_purchase_timestamp) AS min_kpi_ts,
  MAX(order_purchase_timestamp) AS max_kpi_ts
FROM v_order_kpis;
SELECT
  MIN(order_purchase_timestamp) AS min_orders_ts,
  MAX(order_purchase_timestamp) AS max_orders_ts
FROM olist_orders;

-- orders missing in KPI view
SELECT COUNT(*) AS orders_missing_in_kpi
FROM olist_orders o
LEFT JOIN v_order_kpis k USING(order_id)
WHERE k.order_id IS NULL;
SELECT o.order_id
FROM olist_orders o
LEFT JOIN v_order_kpis k USING(order_id)
WHERE k.order_id IS NULL
LIMIT 10;

-- clean sample size (exactly reconciled, non-negative margin)
SELECT
  SUM(clean_flag) AS clean_orders,
  COUNT(*) AS total_orders,
  ROUND(100 * SUM(clean_flag) / COUNT(*), 2) AS clean_pct
FROM (
  SELECT
    order_id,
    CASE
      WHEN margin_proxy >= 0
       AND ABS(payment_value - (items_revenue + freight_total)) <= 0.01
      THEN 1 ELSE 0
    END AS clean_flag
  FROM v_order_kpis
) t;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- Tableau views: monthly, promo split, category/SKU rollups -----*/

-- base BI source (v_order_kpis_clean)
DROP VIEW IF EXISTS v_order_kpis_clean;
CREATE OR REPLACE VIEW v_order_kpis_clean AS
SELECT
  k.order_id,
  k.customer_id,
  k.order_purchase_timestamp,
  DATE_FORMAT(k.order_purchase_timestamp, '%Y-%m') AS ym,
  k.items_revenue,
  k.freight_total,
  k.payment_value,
  k.margin_proxy,
  k.free_shipping_flag,
  k.any_discount_flag,
  k.any_trusted_discount_flag
FROM v_order_kpis k
WHERE
  COALESCE(k.margin_proxy, 0) >= 0
  AND ABS(COALESCE(k.payment_value, 0) - (COALESCE(k.items_revenue, 0) + COALESCE(k.freight_total, 0))) <= 0.01;

-- monthly overview (orders, revenue, AOV, margin)
DROP VIEW IF EXISTS v_monthly_overview;
CREATE OR REPLACE VIEW v_monthly_overview AS
SELECT
  ym,
  COUNT(*)                                     AS orders,
  ROUND(SUM(items_revenue), 2)                 AS items_revenue,
  ROUND(SUM(freight_total), 2)                 AS freight_total,
  ROUND(SUM(payment_value), 2)                 AS payment_value,
  ROUND(SUM(items_revenue) / COUNT(*), 2)      AS aov_items,
  ROUND(SUM(margin_proxy) / COUNT(*), 2)       AS margin_per_order
FROM v_order_kpis_clean
GROUP BY ym;

-- monthly promo split (trusted vs non-promo)
DROP VIEW IF EXISTS v_monthly_promo_split;
CREATE OR REPLACE VIEW v_monthly_promo_split AS
WITH base AS (
  SELECT ym,
         any_trusted_discount_flag AS trusted_flag,
         items_revenue,
         margin_proxy
  FROM v_order_kpis_clean
)
SELECT
  ym,
  trusted_flag,
  COUNT(*)                                                   AS orders,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ym), 2) AS order_share_pct,
  ROUND(SUM(items_revenue) / COUNT(*), 2)                    AS aov_items,
  ROUND(SUM(margin_proxy) / COUNT(*), 2)                     AS margin_per_order
FROM base
GROUP BY ym, trusted_flag;

-- category and SKU rollups (with volume thresholds)
DROP VIEW IF EXISTS v_category_sku_rollup;
CREATE OR REPLACE VIEW v_category_sku_rollup AS
SELECT
  'category' AS level,
  COALESCE(p.product_category_name, '(unknown)') AS category,
  NULL AS product_id,
  COUNT(*) AS line_count,
  COUNT(DISTINCT ie.order_id) AS order_count,
  ROUND(SUM(ie.price), 2) AS items_revenue,
  ROUND(SUM(ie.price - ie.freight_value), 2) AS margin_proxy
FROM v_item_enriched ie
JOIN v_order_kpis_clean k ON k.order_id = ie.order_id
LEFT JOIN olist_products p ON p.product_id = ie.product_id
GROUP BY COALESCE(p.product_category_name, '(unknown)')
HAVING COUNT(*) >= 100

UNION ALL

SELECT
  'sku' AS level,
  COALESCE(p.product_category_name, '(unknown)') AS category,
  ie.product_id AS product_id,
  COUNT(*) AS line_count,
  COUNT(DISTINCT ie.order_id) AS order_count,
  ROUND(SUM(ie.price), 2) AS items_revenue,
  ROUND(SUM(ie.price - ie.freight_value), 2) AS margin_proxy
FROM v_item_enriched ie
JOIN v_order_kpis_clean k ON k.order_id = ie.order_id
LEFT JOIN olist_products p ON p.product_id = ie.product_id
GROUP BY COALESCE(p.product_category_name, '(unknown)'), ie.product_id
HAVING COUNT(*) >= 50;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- cohorts: acquisition and activity -----*/

-- cohort base (first purchase month per unique customer)
DROP VIEW IF EXISTS v_cohort_base;
CREATE OR REPLACE VIEW v_cohort_base AS
SELECT
  c.customer_unique_id AS customer_id,
  MIN(DATE_FORMAT(k.order_purchase_timestamp, '%Y-%m')) AS cohort_month
FROM v_order_kpis k
JOIN olist_customers c USING (customer_id)
GROUP BY c.customer_unique_id;

-- customer-month activity with month_offset
DROP VIEW IF EXISTS v_customer_month_activity;
CREATE OR REPLACE VIEW v_customer_month_activity AS
SELECT
  cu.customer_unique_id AS customer_id,
  b.cohort_month,
  DATE_FORMAT(k.order_purchase_timestamp, '%Y-%m') AS order_month,
  TIMESTAMPDIFF(
    MONTH,
    STR_TO_DATE(CONCAT(b.cohort_month, '-01'), '%Y-%m-%d'),
    STR_TO_DATE(CONCAT(DATE_FORMAT(k.order_purchase_timestamp, '%Y-%m'), '-01'), '%Y-%m-%d')
  ) AS month_offset,
  k.items_revenue,
  k.margin_proxy
FROM v_order_kpis k
JOIN olist_customers cu USING (customer_id)
JOIN v_cohort_base b
  ON b.customer_id = cu.customer_unique_id;

-- retention curve (% active by offset)
DROP VIEW IF EXISTS v_retention_curve;
CREATE OR REPLACE VIEW v_retention_curve AS
WITH agg AS (
  SELECT
    cohort_month,
    month_offset,
    COUNT(DISTINCT customer_id) AS active_customers
  FROM v_customer_month_activity
  GROUP BY cohort_month, month_offset
),
sizes AS (
  SELECT cohort_month, active_customers AS cohort_size
  FROM agg
  WHERE month_offset = 0
)
SELECT
  a.cohort_month,
  a.month_offset,
  a.active_customers,
  ROUND(100 * a.active_customers / NULLIF(s.cohort_size, 0), 2) AS retention_pct
FROM agg a
JOIN sizes s USING (cohort_month);

-- revenue per active customer by offset
DROP VIEW IF EXISTS v_revenue_retention;
CREATE OR REPLACE VIEW v_revenue_retention AS
WITH base AS (
  SELECT
    cohort_month,
    month_offset,
    SUM(items_revenue) AS total_revenue,
    COUNT(DISTINCT customer_id) AS active_customers
  FROM v_customer_month_activity
  GROUP BY cohort_month, month_offset
)
SELECT
  cohort_month,
  month_offset,
  ROUND(total_revenue, 2) AS total_revenue,
  active_customers,
  ROUND(total_revenue / NULLIF(active_customers, 0), 2) AS revenue_per_active_customer
FROM base;

-- acquisition channel: promo vs non-promo (first order)
DROP VIEW IF EXISTS v_cohort_promo_split;
CREATE OR REPLACE VIEW v_cohort_promo_split AS
WITH first_order AS (
  SELECT
    cu.customer_unique_id AS customer_id,
    k.order_purchase_timestamp,
    k.any_trusted_discount_flag,
    ROW_NUMBER() OVER (
      PARTITION BY cu.customer_unique_id
      ORDER BY k.order_purchase_timestamp
    ) AS rn
  FROM v_order_kpis k
  JOIN olist_customers cu USING (customer_id)
)
SELECT
  b.customer_id,
  b.cohort_month,
  CASE WHEN f.any_trusted_discount_flag = 1 THEN 1 ELSE 0 END AS promo_acquired_flag
FROM v_cohort_base b
LEFT JOIN first_order f
  ON f.customer_id = b.customer_id AND f.rn = 1;

-- time to second order (months)
DROP VIEW IF EXISTS v_time_to_second_order;
CREATE OR REPLACE VIEW v_time_to_second_order AS
WITH ordered AS (
  SELECT
    cu.customer_unique_id AS customer_id,
    k.order_purchase_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY cu.customer_unique_id
      ORDER BY k.order_purchase_timestamp
    ) AS rn
  FROM v_order_kpis k
  JOIN olist_customers cu USING (customer_id)
),
pairs AS (
  SELECT
    customer_id,
    MAX(CASE WHEN rn = 1 THEN order_purchase_timestamp END) AS first_ts,
    MAX(CASE WHEN rn = 2 THEN order_purchase_timestamp END) AS second_ts
  FROM ordered
  WHERE rn IN (1, 2)
  GROUP BY customer_id
  HAVING COUNT(*) = 2
)
SELECT
  customer_id,
  DATE_FORMAT(first_ts, '%Y-%m') AS first_order_month,
  TIMESTAMPDIFF(MONTH, DATE(first_ts), DATE(second_ts)) AS months_to_second_order
FROM pairs;

-- cohort size filter (offset 0 baseline)
DROP VIEW IF EXISTS v_retention_curve_filtered;
CREATE OR REPLACE VIEW v_retention_curve_filtered AS
WITH sizes AS (
  SELECT cohort_month, active_customers AS cohort_size
  FROM v_retention_curve
  WHERE month_offset = 0
)
SELECT
  r.cohort_month,
  r.month_offset,
  r.active_customers,
  r.retention_pct
FROM v_retention_curve r
JOIN sizes s USING (cohort_month)
WHERE s.cohort_size >= 30;

-- offset bounds and date coverage
SELECT MIN(month_offset) AS min_offset, MAX(month_offset) AS max_offset
FROM v_customer_month_activity;

-- cohort and activity date coverage
SELECT
  MIN(cohort_month) AS first_cohort,
  MAX(cohort_month) AS last_cohort,
  MIN(order_month)  AS first_activity_month,
  MAX(order_month)  AS last_activity_month
FROM v_customer_month_activity;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- parameters and guards -----*/

-- parameter store (single row)
DROP TABLE IF EXISTS cfg_params;
CREATE TABLE IF NOT EXISTS cfg_params (
  id TINYINT PRIMARY KEY,
  discount_threshold_pct DECIMAL(6,4) NOT NULL DEFAULT 0.0500,
  min_baseline_n INT NOT NULL DEFAULT 3,
  window_start_ym CHAR(7) NOT NULL DEFAULT '2016-09',
  window_end_ym   CHAR(7) NOT NULL DEFAULT '2018-09',
  min_price   DECIMAL(12,2) NOT NULL DEFAULT 0.01,
  min_freight DECIMAL(12,2) NOT NULL DEFAULT 0.00
);

-- upsert parameter row (non-deprecated syntax)
INSERT INTO cfg_params (id) VALUES (1) AS t ON DUPLICATE KEY UPDATE id = t.id;

DROP VIEW IF EXISTS v_params_current;
CREATE OR REPLACE VIEW v_params_current AS
SELECT
  discount_threshold_pct,
  min_baseline_n,
  window_start_ym,
  window_end_ym,
  min_price,
  min_freight
FROM cfg_params
WHERE id = 1;

-- discount-threshold sensitivity (3%, 5%, 10%)
DROP VIEW IF EXISTS v_discount_sensitivity;
CREATE OR REPLACE VIEW v_discount_sensitivity AS
WITH thresholds AS (
  SELECT 0.03 AS thr UNION ALL
  SELECT 0.05 UNION ALL
  SELECT 0.10
),
base AS (
  SELECT
    oi.product_id,
    oi.order_id,
    oi.order_item_id,
    oi.price,
    oi.freight_value,
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS ym,
    pm.median_price,
    pm.baseline_n
  FROM olist_order_items oi
  JOIN olist_orders o USING (order_id)
  LEFT JOIN v_product_month_price_median pm
    ON pm.product_id = oi.product_id
   AND pm.ym = DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')
),
x AS (
  SELECT
    b.*,
    CASE WHEN b.median_price IS NOT NULL AND b.median_price > 0 THEN 1 ELSE 0 END AS pm_ok
  FROM base b
)
SELECT
  t.thr AS discount_threshold,
  COUNT(*) AS line_count,
  SUM(pm_ok) AS with_median,
  ROUND(100 * AVG(CASE WHEN pm_ok = 1 AND price < (1 - t.thr) * median_price THEN 1 ELSE 0 END), 2) AS discount_rate_pct,
  ROUND(100 * AVG(CASE WHEN pm_ok = 1 AND baseline_n >= p.min_baseline_n AND price < (1 - t.thr) * median_price THEN 1 ELSE 0 END), 2) AS trusted_discount_rate_pct
FROM x
CROSS JOIN v_params_current p
CROSS JOIN thresholds t
GROUP BY t.thr;

-- enrichment with guards (discount flags, confidence)
DROP view IF EXISTS v_item_enriched_guarded;
CREATE OR REPLACE view v_item_enriched_guarded
AS
SELECT
  oi.order_id,
  oi.order_item_id,
  oi.product_id,
  oi.seller_id,
  oi.price,
  oi.freight_value,
  o.order_purchase_timestamp,
  DATE_FORMAT(o.order_purchase_timestamp,'%Y-%m') AS ym,
  pm.median_price,
  pm.baseline_n,
  CASE
        WHEN pm.median_price IS NOT NULL
         AND oi.price < (1 - p.discount_threshold_pct) * pm.median_price
        THEN 1
        ELSE 0
    END AS promo_discount_flag,
    CASE
        WHEN pm.median_price IS NOT NULL
         AND pm.baseline_n < p.min_baseline_n
        THEN 1
        ELSE 0
    END AS low_confidence_flag,
    CASE
        WHEN pm.median_price IS NOT NULL
         AND pm.baseline_n >= p.min_baseline_n
         AND oi.price < (1 - p.discount_threshold_pct) * pm.median_price
        THEN 1
        ELSE 0
    END AS trusted_discount_flag
FROM olist_order_items oi
JOIN olist_orders o USING (order_id)
LEFT JOIN v_product_month_price_median pm
    ON pm.product_id = oi.product_id
   AND pm.ym = DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')
CROSS JOIN v_params_current p
WHERE DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') BETWEEN p.window_start_ym AND p.window_end_ym
  AND oi.price > p.min_price
  AND oi.freight_value >= p.min_freight;

-- order date window filter
DROP VIEW IF EXISTS v_orders_in_window;
CREATE OR REPLACE VIEW v_orders_in_window AS
SELECT k.*
FROM v_order_kpis k
CROSS JOIN v_params_current p
WHERE DATE_FORMAT(k.order_purchase_timestamp, '%Y-%m') BETWEEN p.window_start_ym AND p.window_end_ym;

-- item price and freight filters
DROP VIEW IF EXISTS v_items_sane;
CREATE OR REPLACE VIEW v_items_sane AS
SELECT oi.*
FROM olist_order_items oi
CROSS JOIN v_params_current p
WHERE oi.price > p.min_price
  AND oi.freight_value >= p.min_freight;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- performance and persistence -----*/

-- materialize heavy views
DROP TABLE IF EXISTS m_item_enriched_guarded;
CREATE TABLE m_item_enriched_guarded AS
SELECT *
FROM v_item_enriched_guarded;

DROP TABLE IF EXISTS m_order_kpis_clean;
CREATE TABLE m_order_kpis_clean AS
SELECT *
FROM v_order_kpis_clean;

-- Index join and filter keys
CREATE INDEX idx_m_item_order            ON m_item_enriched_guarded (order_id, order_item_id);
CREATE INDEX idx_m_item_prod_ym          ON m_item_enriched_guarded (product_id, ym);
CREATE INDEX idx_m_item_seller           ON m_item_enriched_guarded (seller_id);
CREATE INDEX idx_m_item_ym               ON m_item_enriched_guarded (ym);
CREATE INDEX idx_m_item_trusted_lowconf  ON m_item_enriched_guarded (trusted_discount_flag, low_confidence_flag);
CREATE INDEX idx_m_kpis_ts               ON m_order_kpis_clean (order_purchase_timestamp);
CREATE INDEX idx_m_kpis_ym               ON m_order_kpis_clean (ym);
CREATE INDEX idx_m_kpis_flags            ON m_order_kpis_clean (any_trusted_discount_flag, free_shipping_flag);
CREATE INDEX idx_m_kpis_customer         ON m_order_kpis_clean (customer_id);

-- pre-aggregate monthly rollups
DROP TABLE IF EXISTS agg_monthly_overview;
CREATE TABLE agg_monthly_overview AS
SELECT
  ym,
  COUNT(*)                                        AS orders,
  ROUND(SUM(items_revenue), 2)                    AS items_revenue,
  ROUND(SUM(freight_total), 2)                    AS freight_total,
  ROUND(SUM(payment_value), 2)                    AS payment_value,
  ROUND(SUM(items_revenue) / COUNT(*), 2)         AS aov_items,
  ROUND(SUM(margin_proxy) / COUNT(*), 2)          AS margin_per_order
FROM m_order_kpis_clean
GROUP BY ym;

CREATE INDEX idx_agg_monthly_overview_ym ON agg_monthly_overview (ym);

DROP TABLE IF EXISTS agg_monthly_promo_split;
CREATE TABLE agg_monthly_promo_split AS
SELECT
  ym,
  any_trusted_discount_flag AS trusted_flag,
  COUNT(*) AS orders,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ym), 2) AS order_share_pct,
  ROUND(SUM(items_revenue) / COUNT(*), 2) AS aov_items,
  ROUND(SUM(margin_proxy) / COUNT(*), 2)  AS margin_per_order
FROM m_order_kpis_clean
GROUP BY ym, any_trusted_discount_flag;

CREATE INDEX idx_agg_monthly_promo_split ON agg_monthly_promo_split (ym, trusted_flag);

-- category and SKU rollup (volume thresholds; clean orders only)
DROP TABLE IF EXISTS agg_category_sku_rollup;
CREATE TABLE agg_category_sku_rollup AS
SELECT
  'category' AS level,
  COALESCE(p.product_category_name, '(unknown)') AS category,
  NULL AS product_id,
  COUNT(*) AS line_count,
  COUNT(DISTINCT ie.order_id) AS order_count,
  ROUND(SUM(ie.price), 2) AS items_revenue,
  ROUND(SUM(ie.price - ie.freight_value), 2) AS margin_proxy
FROM m_item_enriched_guarded ie
JOIN m_order_kpis_clean k      ON k.order_id = ie.order_id
LEFT JOIN olist_products p     ON p.product_id = ie.product_id
GROUP BY COALESCE(p.product_category_name, '(unknown)')
HAVING COUNT(*) >= 100

UNION ALL

SELECT
  'sku' AS level,
  COALESCE(p.product_category_name, '(unknown)') AS category,
  ie.product_id AS product_id,
  COUNT(*) AS line_count,
  COUNT(DISTINCT ie.order_id) AS order_count,
  ROUND(SUM(ie.price), 2) AS items_revenue,
  ROUND(SUM(ie.price - ie.freight_value), 2) AS margin_proxy
FROM m_item_enriched_guarded ie
JOIN m_order_kpis_clean k      ON k.order_id = ie.order_id
LEFT JOIN olist_products p     ON p.product_id = ie.product_id
GROUP BY COALESCE(p.product_category_name, '(unknown)'), ie.product_id
HAVING COUNT(*) >= 50;

CREATE INDEX idx_agg_cat_level_cat ON agg_category_sku_rollup (level, category);
CREATE INDEX idx_agg_cat_sku       ON agg_category_sku_rollup (product_id);

-- BI view alias (fast path to materialized table)
DROP VIEW IF EXISTS v_order_kpis_clean_bi;
CREATE OR REPLACE VIEW v_order_kpis_clean_bi AS
SELECT * FROM m_order_kpis_clean;

-- Refresh: m_item_enriched_guarded
TRUNCATE TABLE m_item_enriched_guarded;
INSERT INTO m_item_enriched_guarded
SELECT * FROM v_item_enriched_guarded;

-- refresh m_order_kpis_clean
TRUNCATE TABLE m_order_kpis_clean;
INSERT INTO m_order_kpis_clean
SELECT * FROM v_order_kpis_clean;

-- refresh: agg_monthly_overview
TRUNCATE TABLE agg_monthly_overview;
INSERT INTO agg_monthly_overview (ym, orders, items_revenue, freight_total, payment_value, aov_items, margin_per_order)
SELECT
  ym,
  COUNT(*)                                        AS orders,
  ROUND(SUM(items_revenue), 2)                    AS items_revenue,
  ROUND(SUM(freight_total), 2)                    AS freight_total,
  ROUND(SUM(payment_value), 2)                    AS payment_value,
  ROUND(SUM(items_revenue) / COUNT(*), 2)         AS aov_items,
  ROUND(SUM(margin_proxy) / COUNT(*), 2)          AS margin_per_order
FROM m_order_kpis_clean
GROUP BY ym;

-- refresh: agg_monthly_promo_split
TRUNCATE TABLE agg_monthly_promo_split;
INSERT INTO agg_monthly_promo_split (ym, trusted_flag, orders, order_share_pct, aov_items, margin_per_order)
SELECT
  ym,
  any_trusted_discount_flag AS trusted_flag,
  COUNT(*) AS orders,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ym), 2) AS order_share_pct,
  ROUND(SUM(items_revenue) / COUNT(*), 2) AS aov_items,
  ROUND(SUM(margin_proxy) / COUNT(*), 2)  AS margin_per_order
FROM m_order_kpis_clean
GROUP BY ym, any_trusted_discount_flag;

-- refresh: agg_category_sku_rollup
TRUNCATE TABLE agg_category_sku_rollup;
INSERT INTO agg_category_sku_rollup (level, category, product_id, line_count, order_count, items_revenue, margin_proxy)
SELECT
  'category' AS level,
  COALESCE(p.product_category_name, '(unknown)') AS category,
  NULL AS product_id,
  COUNT(*) AS line_count,
  COUNT(DISTINCT ie.order_id) AS order_count,
  ROUND(SUM(ie.price), 2) AS items_revenue,
  ROUND(SUM(ie.price - ie.freight_value), 2) AS margin_proxy
FROM m_item_enriched_guarded ie
JOIN m_order_kpis_clean k      ON k.order_id = ie.order_id
LEFT JOIN olist_products p     ON p.product_id = ie.product_id
GROUP BY COALESCE(p.product_category_name, '(unknown)')
HAVING COUNT(*) >= 100
UNION ALL
SELECT
  'sku' AS level,
  COALESCE(p.product_category_name, '(unknown)') AS category,
  ie.product_id AS product_id,
  COUNT(*) AS line_count,
  COUNT(DISTINCT ie.order_id) AS order_count,
  ROUND(SUM(ie.price), 2) AS items_revenue,
  ROUND(SUM(ie.price - ie.freight_value), 2) AS margin_proxy
FROM m_item_enriched_guarded ie
JOIN m_order_kpis_clean k      ON k.order_id = ie.order_id
LEFT JOIN olist_products p     ON p.product_id = ie.product_id
GROUP BY COALESCE(p.product_category_name, '(unknown)'), ie.product_id
HAVING COUNT(*) >= 50;

-- ------------------------------------------------------------------------------------------------------------------------------------------------
/*----- final validation and export points -----*/

-- row-count parity and unique keys
SELECT
  (SELECT COUNT(*) FROM m_item_enriched_guarded)                                   AS item_rows,
  (SELECT COUNT(DISTINCT order_id, order_item_id) FROM m_item_enriched_guarded)    AS item_distinct_pairs,
  (SELECT COUNT(*) FROM m_order_kpis_clean)                                         AS order_rows,
  (SELECT COUNT(DISTINCT order_id) FROM m_order_kpis_clean)                         AS order_distinct_ids,
  (SELECT COUNT(DISTINCT order_id) FROM m_item_enriched_guarded)                    AS orders_seen_in_items,
  (SELECT COUNT(*) FROM (
      SELECT k.order_id
      FROM m_order_kpis_clean k
      LEFT JOIN m_item_enriched_guarded ie ON ie.order_id = k.order_id
      WHERE ie.order_id IS NULL
  ) x)                                                                              AS orders_missing_items;

-- null and outlier scan (KPIs)
SELECT
  SUM(items_revenue IS NULL) AS null_items_revenue,
  SUM(freight_total IS NULL) AS null_freight_total,
  SUM(payment_value IS NULL) AS null_payment_value,
  SUM(margin_proxy  IS NULL) AS null_margin_proxy,
  SUM(items_revenue <= 0)    AS nonpositive_items_revenue,
  SUM(freight_total  < 0)    AS negative_freight,
  SUM(margin_proxy   < 0)    AS negative_margin,
  MIN(items_revenue)         AS min_items_revenue,
  MAX(items_revenue)         AS max_items_revenue,
  MIN(freight_total)         AS min_freight_total,
  MAX(freight_total)         AS max_freight_total,
  MIN(payment_value)         AS min_payment_value,
  MAX(payment_value)         AS max_payment_value,
  MIN(margin_proxy)          AS min_margin_proxy,
  MAX(margin_proxy)          AS max_margin_proxy
FROM m_order_kpis_clean;

-- payment reconciliation summary
SELECT
  SUM(ABS(payment_value - (items_revenue + freight_total)) <= 0.01)                                                 AS exact_match,
  SUM(ABS(payment_value - (items_revenue + freight_total)) > 0.01
   AND ABS(payment_value - (items_revenue + freight_total)) <= 5)                                                   AS small_gap,
  SUM(ABS(payment_value - (items_revenue + freight_total)) > 5)                                                     AS big_gap,
  COUNT(*)                                                                                                          AS total_orders
FROM m_order_kpis_clean;

-- date coverage vs parameter window
SELECT
  p.window_start_ym,
  DATE_FORMAT(MIN(k.order_purchase_timestamp), '%Y-%m') AS first_ym_in_data,
  DATE_FORMAT(MAX(k.order_purchase_timestamp), '%Y-%m') AS last_ym_in_data,
  p.window_end_ym,
  (DATE_FORMAT(MIN(k.order_purchase_timestamp), '%Y-%m') >= p.window_start_ym) AS starts_within_window,
  (DATE_FORMAT(MAX(k.order_purchase_timestamp), '%Y-%m') <= p.window_end_ym)   AS ends_within_window
FROM m_order_kpis_clean k
CROSS JOIN v_params_current p;

-- export points for BI
SELECT 'm_order_kpis_clean'       AS bi_source, COUNT(*) AS row_count FROM m_order_kpis_clean
UNION ALL
SELECT 'agg_monthly_overview',            COUNT(*)                     FROM agg_monthly_overview
UNION ALL
SELECT 'agg_monthly_promo_split',         COUNT(*)                     FROM agg_monthly_promo_split
UNION ALL
SELECT 'agg_category_sku_rollup',         COUNT(*)                     FROM agg_category_sku_rollup
UNION ALL
SELECT 'v_order_kpis_clean_bi',           COUNT(*)                     FROM v_order_kpis_clean_bi;

-- grant BI read access (optional)
CREATE USER IF NOT EXISTS 'bi_reader'@'%' IDENTIFIED BY 'strong_password_here';
GRANT SELECT ON olist_analytics.m_order_kpis_clean       TO 'bi_reader'@'%';
GRANT SELECT ON olist_analytics.agg_monthly_overview     TO 'bi_reader'@'%';
GRANT SELECT ON olist_analytics.agg_monthly_promo_split  TO 'bi_reader'@'%';
GRANT SELECT ON olist_analytics.agg_category_sku_rollup  TO 'bi_reader'@'%';
GRANT SELECT ON olist_analytics.v_order_kpis_clean_bi    TO 'bi_reader'@'%';
FLUSH PRIVILEGES;

-- ------------------------------------------------------------------------------------------------------------------------------------------------