-- =============================================================================
-- GOLD LAYER - STAR SCHEMA  (week6.gold_test)
-- File: transformations/gold.sql
-- Owner: Kinah (dashboard) - design by Jemma, column names aligned to the
--        real week6.silver_test.*_clean tables so the pipeline actually runs.
-- =============================================================================
-- Grain of the fact table: one row per (order_id, product_id) - one order line
-- item. This is the natural grain of order_products and supports all four
-- business questions without a second fact table.
--
-- There is no real DimDate. Instacart's public dataset has no calendar dates,
-- only order_dow (0-6) and order_hour_of_day (0-23) per order. Instacart never
-- documented which integer is which weekday, so order_day_name is a LABELING
-- ASSUMPTION (0 = Sunday). The pattern (which days are high or low vs each
-- other) is valid either way. Only the name is an assumption.
--
-- DimCustomer is intentionally omitted: the dataset has nothing about a user
-- beyond user_id (no name, no demographics). user_id is kept on the fact table
-- as a degenerate dimension instead of a near-empty DimCustomer.
--
-- What changed vs the first draft (so the pipeline runs):
--   * All names are fully qualified: week6.gold_test.* reads week6.silver_test.*
--   * silver has NO order_items_clean. It has order_products_prior_clean and
--     order_products_train_clean. The fact table UNION ALLs both and keeps a
--     source_set column ('prior' / 'train') so we can slice by it.
--   * silver has one quality flag column per table (product_quality_flag,
--     order_quality_flag, line_item_quality_flag), not the flag_* booleans in
--     the draft. The dims and fact carry that single flag through.
--   * is_first_order is derived here (order_number = 1). silver does not have it.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- dim_product : one row per product_id, aisle and department names denormalized
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.dim_product
COMMENT 'Product dimension: one row per product_id with aisle and department names flattened in. Unknown aisle/department are labeled, not dropped, so every fact row still joins.'
AS
SELECT
  p.product_id                                           AS product_key,
  p.product_id,
  COALESCE(p.product_name, 'Unknown Product')            AS product_name,
  p.aisle_id,
  COALESCE(a.aisle, 'Unknown Aisle')                     AS aisle_name,
  p.department_id,
  COALESCE(d.department, 'Unknown Department')           AS department_name,
  p.product_quality_flag,                                 -- NULL = clean row
  p.product_quality_flag IS NULL                          AS is_clean_product
FROM week6.silver_test.products_clean p
LEFT JOIN week6.silver_test.aisles_clean      a ON p.aisle_id      = a.aisle_id
LEFT JOIN week6.silver_test.departments_clean d ON p.department_id = d.department_id;


-- -----------------------------------------------------------------------------
-- dim_order : one row per order_id, with day name, weekend flag, daypart
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.dim_order
COMMENT 'Order dimension: one row per order_id. order_day_name assumes order_dow 0 = Sunday (undocumented by Instacart). daypart buckets order_hour_of_day. eval_set = test was already removed in silver.'
AS
SELECT
  order_id                                               AS order_key,
  order_id,
  user_id,
  eval_set                                               AS source_set,   -- 'prior' or 'train'
  order_number,
  order_number = 1                                       AS is_first_order,
  order_dow,
  CASE order_dow                                         -- ASSUMPTION: 0 = Sunday, see header
    WHEN 0 THEN 'Sunday'    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday'   WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday'  WHEN 5 THEN 'Friday'
    WHEN 6 THEN 'Saturday'
    ELSE 'Unknown'
  END                                                    AS order_day_name,
  order_dow IN (0, 6)                                    AS is_weekend,
  order_hour_of_day,
  CASE
    WHEN order_hour_of_day BETWEEN 5  AND 11 THEN 'Morning'
    WHEN order_hour_of_day BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN order_hour_of_day BETWEEN 17 AND 20 THEN 'Evening'
    WHEN order_hour_of_day IS NULL           THEN 'Unknown'
    ELSE 'Night'
  END                                                    AS daypart,
  days_since_prior_order,
  order_quality_flag,                                     -- NULL = clean row
  order_quality_flag IS NULL                              AS is_clean_order
FROM week6.silver_test.orders_clean;


-- -----------------------------------------------------------------------------
-- fact_order_items : one row per (order_id, product_id)
-- prior + train are unioned. Orphans were already dropped in silver by the
-- LEFT SEMI JOINs, so the INNER JOIN to orders_clean here is a safety net,
-- not a filter that should ever remove rows. (Validation view checks that.)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.fact_order_items (
  CONSTRAINT fact_keys_present
    EXPECT (order_key IS NOT NULL AND product_key IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Fact table at order-line grain: one row per (order_id, product_id) from order_products_prior_clean UNION ALL order_products_train_clean. user_id kept as a degenerate dimension. reordered is 0/1 or NULL when the source value was invalid.'
AS
WITH line_items AS (
  SELECT order_id, product_id, add_to_cart_order, reordered,
         line_item_quality_flag, 'prior' AS source_set
  FROM week6.silver_test.order_products_prior_clean
  UNION ALL
  SELECT order_id, product_id, add_to_cart_order, reordered,
         line_item_quality_flag, 'train' AS source_set
  FROM week6.silver_test.order_products_train_clean
)
SELECT
  li.order_id                                            AS order_key,
  li.product_id                                          AS product_key,
  o.user_id,
  li.source_set,
  li.add_to_cart_order,
  li.reordered,
  COALESCE(li.reordered, 0)                              AS reordered_flag, -- safe for SUM / rate math
  li.line_item_quality_flag,                              -- NULL = clean row
  li.line_item_quality_flag IS NULL                       AS is_clean_line
FROM line_items li
INNER JOIN week6.silver_test.orders_clean o ON li.order_id = o.order_id;


-- -----------------------------------------------------------------------------
-- validation_gold : evidence the gold layer is correct (goes in the docs and
-- on the dashboard as a small table). One row per check.
--   grain_is_unique     : COUNT(*) = COUNT(DISTINCT order_id, product_id)
--   fact_matches_silver : fact rows = prior_clean rows + train_clean rows
--                         (nothing silently dropped by the join)
--   every_fact_has_dim  : no fact row without a product / order dimension row
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.validation_gold
COMMENT 'Gold layer checks: grain uniqueness, fact vs silver reconciliation, and dimension coverage. status = PASS or FAIL.'
AS
WITH fact_counts AS (
  SELECT
    COUNT(*)                                        AS fact_rows,
    COUNT(DISTINCT order_key, product_key)          AS fact_distinct_keys
  FROM week6.gold_test.fact_order_items
),
silver_counts AS (
  SELECT
    (SELECT COUNT(*) FROM week6.silver_test.order_products_prior_clean)
  + (SELECT COUNT(*) FROM week6.silver_test.order_products_train_clean) AS silver_line_rows
),
dim_coverage AS (
  SELECT
    SUM(CASE WHEN dp.product_key IS NULL THEN 1 ELSE 0 END) AS missing_product_dim,
    SUM(CASE WHEN do.order_key   IS NULL THEN 1 ELSE 0 END) AS missing_order_dim
  FROM week6.gold_test.fact_order_items f
  LEFT JOIN week6.gold_test.dim_product dp ON f.product_key = dp.product_key
  LEFT JOIN week6.gold_test.dim_order   do ON f.order_key   = do.order_key
)
SELECT 'grain_is_unique' AS check_name,
       fact_rows          AS expected_value,
       fact_distinct_keys AS actual_value,
       CASE WHEN fact_rows = fact_distinct_keys THEN 'PASS' ELSE 'FAIL' END AS status
FROM fact_counts
UNION ALL
SELECT 'fact_matches_silver',
       s.silver_line_rows,
       fc.fact_rows,
       CASE WHEN s.silver_line_rows = fc.fact_rows THEN 'PASS' ELSE 'FAIL' END
FROM fact_counts fc CROSS JOIN silver_counts s
UNION ALL
SELECT 'every_fact_has_product_dim', 0, missing_product_dim,
       CASE WHEN missing_product_dim = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_coverage
UNION ALL
SELECT 'every_fact_has_order_dim', 0, missing_order_dim,
       CASE WHEN missing_order_dim = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_coverage;


-- =============================================================================
-- GOLD LAYER - BUSINESS QUESTION VIEWS (dashboard-ready)  (week6.gold_test)
-- (continued) transformations/gold.sql
-- Owner: Kinah (dashboard) - design by Jemma, names aligned to week6.gold_test.*
-- =============================================================================
-- Every view below reads ONLY from the star schema (fact + dims). No view
-- reaches back into silver. That is the point of the gold layer: engineer the
-- joins once, then every dashboard tile is a simple SELECT.
--
-- reordered_flag (0/1, never NULL) is used for all reorder math so a NULL
-- reordered value (invalid source) counts as "not reordered" instead of
-- silently shrinking the denominator.
-- =============================================================================


-- Q1a: Which products are purchased most frequently?
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.vw_product_popularity
COMMENT 'Q1a. One row per product: how many order lines it appears in, how many were reorders, and its popularity rank.'
AS
SELECT
  dp.product_id,
  dp.product_name,
  dp.department_name,
  dp.aisle_name,
  COUNT(*)                                     AS times_ordered,
  SUM(f.reordered_flag)                        AS times_reordered,
  RANK() OVER (ORDER BY COUNT(*) DESC)         AS popularity_rank
FROM week6.gold_test.fact_order_items f
JOIN week6.gold_test.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.product_id, dp.product_name, dp.department_name, dp.aisle_name;


-- Q1b: Which departments are purchased most frequently?
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.vw_department_popularity
COMMENT 'Q1b. One row per department: order lines and share of all order lines.'
AS
SELECT
  dp.department_name,
  COUNT(*)                                                    AS times_ordered,
  COUNT(DISTINCT f.order_key)                                 AS orders_containing,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)          AS pct_of_total_items
FROM week6.gold_test.fact_order_items f
JOIN week6.gold_test.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.department_name;


-- Q1c: Which aisles are purchased most frequently? (extra drill-down for the
-- dashboard, same pattern as Q1b)
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.vw_aisle_popularity
COMMENT 'Q1c. One row per aisle: order lines and share of all order lines. Drill-down under departments.'
AS
SELECT
  dp.department_name,
  dp.aisle_name,
  COUNT(*)                                                    AS times_ordered,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)          AS pct_of_total_items
FROM week6.gold_test.fact_order_items f
JOIN week6.gold_test.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.department_name, dp.aisle_name;


-- Q2: How does purchasing behavior change by day of week and hour of day?
-- Counted at the ORDER level (distinct orders) so one big basket does not
-- inflate a time slot. item_count is kept for basket-size math.
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.vw_orders_by_dow_hour
COMMENT 'Q2. One row per (day of week, hour of day): distinct orders and order lines. Day name assumes 0 = Sunday.'
AS
SELECT
  do.order_dow,
  do.order_day_name,
  do.is_weekend,
  do.order_hour_of_day,
  do.daypart,
  COUNT(DISTINCT f.order_key)    AS order_count,
  COUNT(*)                       AS item_count,
  ROUND(COUNT(*) / COUNT(DISTINCT f.order_key), 2) AS avg_basket_size
FROM week6.gold_test.fact_order_items f
JOIN week6.gold_test.dim_order do ON f.order_key = do.order_key
GROUP BY do.order_dow, do.order_day_name, do.is_weekend, do.order_hour_of_day, do.daypart;


-- Q3: Which products have the highest reorder behavior?
-- HAVING threshold keeps low-volume products (ordered twice, reordered once
-- = 50% "rate") from dominating a naive top-N by reorder_rate.
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.vw_product_reorder_rate
COMMENT 'Q3. One row per product with at least 50 order lines: reorder count and reorder rate (0 to 1).'
AS
SELECT
  dp.product_id,
  dp.product_name,
  dp.department_name,
  COUNT(*)                                             AS total_orders,
  SUM(f.reordered_flag)                                AS reorder_count,
  ROUND(SUM(f.reordered_flag) / COUNT(*), 4)           AS reorder_rate
FROM week6.gold_test.fact_order_items f
JOIN week6.gold_test.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.product_id, dp.product_name, dp.department_name
HAVING COUNT(*) >= 50;


-- Q3b: Reorder rate by department (supports Q3 on the dashboard: which
-- departments are "habit" purchases vs one-off purchases)
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.vw_department_reorder_rate
COMMENT 'Q3b. One row per department: reorder rate across all its order lines.'
AS
SELECT
  dp.department_name,
  COUNT(*)                                             AS total_orders,
  SUM(f.reordered_flag)                                AS reorder_count,
  ROUND(SUM(f.reordered_flag) / COUNT(*), 4)           AS reorder_rate
FROM week6.gold_test.fact_order_items f
JOIN week6.gold_test.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.department_name;


-- Q4: Team's own question - candidate: does basket size vary by day / daypart?
-- Swap this view if the team lands on a different Q4. Same fact + dim pattern.
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.vw_basket_size_by_daypart
COMMENT 'Q4 (candidate). One row per (day of week, daypart): average items per order.'
AS
SELECT
  do.order_dow,
  do.order_day_name,
  do.daypart,
  COUNT(DISTINCT f.order_key)                          AS order_count,
  ROUND(COUNT(*) / COUNT(DISTINCT f.order_key), 2)     AS avg_basket_size
FROM week6.gold_test.fact_order_items f
JOIN week6.gold_test.dim_order do ON f.order_key = do.order_key
GROUP BY do.order_dow, do.order_day_name, do.daypart;


-- Supporting KPI view - feeds the dashboard's top-row stat tiles.
CREATE OR REFRESH MATERIALIZED VIEW week6.gold_test.vw_kpi_summary
COMMENT 'One-row KPI summary for the dashboard header tiles.'
AS
SELECT
  COUNT(DISTINCT f.order_key)                                     AS total_orders,
  COUNT(*)                                                        AS total_order_items,
  COUNT(DISTINCT f.product_key)                                   AS distinct_products_ordered,
  COUNT(DISTINCT f.user_id)                                       AS distinct_users,
  ROUND(SUM(f.reordered_flag) / COUNT(*), 4)                      AS overall_reorder_rate,
  ROUND(COUNT(*) / COUNT(DISTINCT f.order_key), 2)                AS avg_basket_size,
  ROUND(100.0 * SUM(CASE WHEN f.is_clean_line THEN 0 ELSE 1 END) / COUNT(*), 2)
                                                                  AS pct_lines_with_quality_flag
FROM week6.gold_test.fact_order_items f;
