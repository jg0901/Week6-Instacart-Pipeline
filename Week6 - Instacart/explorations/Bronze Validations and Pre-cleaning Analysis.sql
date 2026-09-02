-- Databricks notebook source
--[Jemma]

-- =============================================================================
-- BRONZE INGESTION AUDIT — volume + quality, in one historical log
-- =============================================================================
--- Run this after each pipeline update (a Job task downstream of the
-- pipeline task, or a cell you run manually after each test batch).
-- null_key_rows / duplicate_key_rows / domain_check_fail_rows are computed
-- with the SAME conditions as each table's CONSTRAINT block in bronze layer
-- If you change a constraint there, update
-- the matching COUNT_IF here too — they're two separate definitions of the
-- same rule, kept in sync by hand, not by the engine.
-- =============================================================================
 
CREATE TABLE IF NOT EXISTS week6.bronze_test.ingestion_audit_log (
  run_ts                 TIMESTAMP,
  table_name              STRING,
  cumulative_rows          BIGINT,  -- total rows in the table as of this run
  newly_processed          BIGINT,  -- rows added since the last logged run
  null_key_rows            BIGINT,  -- rows failing the PK/composite-key presence check
  duplicate_key_rows       BIGINT,  -- COUNT(*) - COUNT(DISTINCT key) -- this is what test3's 1k seeded duplicates should show up as
  rescued_rows             BIGINT,  -- rows where _rescued_data IS NOT NULL
  domain_check_fail_rows   BIGINT   -- rows failing a table-specific range/domain check (0 where none apply)
) USING DELTA;
 
-- run this after each pipeline update
INSERT INTO week6.bronze_test.ingestion_audit_log
SELECT
  CURRENT_TIMESTAMP() AS run_ts,
  t.table_name,
  t.cumulative_rows,
  t.cumulative_rows - COALESCE(prev.last_cumulative, 0) AS newly_processed,
  t.null_key_rows,
  t.duplicate_key_rows,
  t.rescued_rows,
  t.domain_check_fail_rows
FROM (
 
  SELECT
    'aisles'                                            AS table_name,
    COUNT(*)                                             AS cumulative_rows,
    COUNT_IF(TRY_CAST(aisle_id AS BIGINT) IS NULL OR TRY_CAST(aisle_id AS BIGINT) <= 0) AS null_key_rows,
    COUNT(*) - COUNT(DISTINCT aisle_id)                   AS duplicate_key_rows,
    COUNT_IF(_rescued_data IS NOT NULL)                   AS rescued_rows,
    0                                                      AS domain_check_fail_rows
  FROM week6.bronze_test.aisles
 
  UNION ALL
 
  SELECT
    'departments',
    COUNT(*),
    COUNT_IF(TRY_CAST(department_id AS BIGINT) IS NULL OR TRY_CAST(department_id AS BIGINT) <= 0),
    COUNT(*) - COUNT(DISTINCT department_id),
    COUNT_IF(_rescued_data IS NOT NULL),
    0
  FROM week6.bronze_test.departments
 
  UNION ALL
 
  SELECT
    'orders',
    COUNT(*),
    COUNT_IF(TRY_CAST(order_id AS BIGINT) IS NULL OR TRY_CAST(order_id AS BIGINT) <= 0),
    COUNT(*) - COUNT(DISTINCT order_id),
    COUNT_IF(_rescued_data IS NOT NULL),
    COUNT_IF(
      TRY_CAST(user_id AS BIGINT) IS NULL OR TRY_CAST(user_id AS BIGINT) <= 0
      OR TRY_CAST(order_number AS INT) IS NULL OR TRY_CAST(order_number AS INT) <= 0
      OR TRY_CAST(order_dow AS INT) NOT BETWEEN 0 AND 6
      OR TRY_CAST(order_hour_of_day AS INT) NOT BETWEEN 0 AND 23
      OR (days_since_prior_order IS NOT NULL AND TRY_CAST(days_since_prior_order AS DOUBLE) < 0)
    )
  FROM week6.bronze_test.orders
 
  UNION ALL
 
  SELECT
    'order_products_prior',
    COUNT(*),
    COUNT_IF(
      TRY_CAST(order_id AS BIGINT) IS NULL OR TRY_CAST(order_id AS BIGINT) <= 0
      OR TRY_CAST(product_id AS BIGINT) IS NULL OR TRY_CAST(product_id AS BIGINT) <= 0
    ),
    COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '-', product_id)),
    COUNT_IF(_rescued_data IS NOT NULL),
    COUNT_IF(
      (add_to_cart_order IS NOT NULL AND TRY_CAST(add_to_cart_order AS INT) <= 0)
      OR (reordered IS NOT NULL AND reordered NOT IN ('0', '1'))
    )
  FROM week6.bronze_test.order_products_prior
 
  UNION ALL
 
  SELECT
    'order_products_train',
    COUNT(*),
    COUNT_IF(
      TRY_CAST(order_id AS BIGINT) IS NULL OR TRY_CAST(order_id AS BIGINT) <= 0
      OR TRY_CAST(product_id AS BIGINT) IS NULL OR TRY_CAST(product_id AS BIGINT) <= 0
    ),
    COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '-', product_id)),
    COUNT_IF(_rescued_data IS NOT NULL),
    COUNT_IF(
      (add_to_cart_order IS NOT NULL AND TRY_CAST(add_to_cart_order AS INT) <= 0)
      OR (reordered IS NOT NULL AND reordered NOT IN ('0', '1'))
    )
  FROM week6.bronze_test.order_products_train
 
  UNION ALL
 
  SELECT
    'products',
    COUNT(*),
    COUNT_IF(TRY_CAST(product_id AS BIGINT) IS NULL OR TRY_CAST(product_id AS BIGINT) <= 0),
    COUNT(*) - COUNT(DISTINCT product_id),
    COUNT_IF(_rescued_data IS NOT NULL),
    COUNT_IF(
      TRY_CAST(aisle_id AS BIGINT) IS NULL OR TRY_CAST(aisle_id AS BIGINT) <= 0
      OR TRY_CAST(department_id AS BIGINT) IS NULL OR TRY_CAST(department_id AS BIGINT) <= 0
    )
  FROM week6.bronze_test.products
 
) t
LEFT JOIN (
  SELECT table_name, MAX(cumulative_rows) AS last_cumulative
  FROM week6.bronze_test.ingestion_audit_log
  GROUP BY table_name
) prev
ON t.table_name = prev.table_name;
 
 
-- =============================================================================
-- TRACE: duplicates introduced per batch (read-only, run anytime after the
-- INSERT above has logged at least two runs for a table)
-- =============================================================================
-- duplicate_key_rows above is CUMULATIVE -- COUNT(*) over the whole table as
-- it stands at that run, not "how many duplicates did the batch I just
-- loaded add." To see which run introduced (or worsened) duplication, diff
-- consecutive runs per table with LAG(). A positive new_duplicates value on
-- the run right after loading a test batch means that batch is the culprit.
-- =============================================================================
SELECT
  table_name,
  run_ts,
  cumulative_rows,
  duplicate_key_rows,
  duplicate_key_rows - LAG(duplicate_key_rows) OVER (
    PARTITION BY table_name ORDER BY run_ts
  ) AS new_duplicates_this_batch
FROM week6.bronze_test.ingestion_audit_log
ORDER BY table_name, run_ts;
 
 
-- =============================================================================
-- DRILL-DOWN: once a batch is implicated above, find the actual duplicate
-- rows (not just the count) by grouping on the same key each table's
-- duplicate_key_rows uses, then optionally scoping by _ingested_at to the
-- suspect batch's load window. Run one at a time, table_name swapped as needed.
-- =============================================================================
 
-- aisles / departments / products: single-column key
SELECT aisle_id, COUNT(*) AS n
FROM week6.bronze_test.aisles
-- WHERE _ingested_at >= '<suspect batch load time>'
GROUP BY aisle_id
HAVING COUNT(*) > 1;
 
-- orders: single-column key
SELECT order_id, COUNT(*) AS n
FROM week6.bronze_test.orders
-- WHERE _ingested_at >= '<suspect batch load time>'
GROUP BY order_id
HAVING COUNT(*) > 1;
 
-- order_products_prior / order_products_train: composite key
SELECT order_id, product_id, COUNT(*) AS n
FROM week6.bronze_test.order_products_prior
-- WHERE _ingested_at >= '<suspect batch load time>'
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;

SELECT  * FROM week6.bronze_test.ingestion_audit_log;


-- COMMAND ----------

-- Null Checkers and Uniqueness Checker in Aisle and Department Bronze tables
Select count(*) AS row_count, count(distinct aisle_id) AS distinct_aisle_id_count, count(distinct aisle) as distinct_aisle_count from week6.bronze_test.aisles; 
Select count(*) AS row_count, count(distinct department_id)as distinct_dep_id_count, count(distinct department) as distinct_dep_count from week6.bronze_test.departments; 

-- COMMAND ----------

--Products Table
select count(*) AS row_count, count(distinct product_id) AS product_id, count(distinct product_name) as product_name_count, 
count(distinct aisle_id) aisle_count, count(distinct department_id) as dept_id_count 
from week6.bronze_test.products; 
select * from week6.bronze_test.products 
where aisle_id IS NULL OR department_id IS NULL;
select distinct aisle_id from week6.bronze_test.products;
select distinct department_id from week6.bronze_test.products; 
--This shows the rows where aisle_id is not in the aisles table
select * from week6.bronze_test.products where aisle_id NOT IN (SELECT distinct aisle_id FROM week6.bronze_test.aisles);
--This shows the rows where department_id is not in the departments table
select * from week6.bronze_test.products where department_id NOT IN (SELECT distinct department_id FROM week6.bronze_test.departments);

-- COMMAND ----------

--Kinah
-- ============================================================
-- Order_Products_Train : pre-cleaning analysis  [Kinah]
-- ============================================================
-- Profiled vs raw CSV (Sep 1): 1,384,617 rows | 131,209 orders | 39,123 products
-- avg_reorder_rate 0.5986 | 0 nulls | 0 duplicate (order_id, product_id) keys
-- add_to_cart_order 1..80, unbroken 1..N per order | 1,158 rows (0.084%) > 50 items
-- Full findings + treatments: Data Quality Issues doc > Order_products_train [Kinah]
-- 1) Volume, uniqueness, reorder ratio
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT order_id) AS unique_orders,
  COUNT(DISTINCT product_id) AS unique_products,
  ROUND(AVG(CAST(reordered AS INT)), 4) AS avg_reorder_rate
FROM
  week6.bronze_test.order_products_train;

-- 2) Completeness: nulls / blanks per column + parse health  [Completeness]
SELECT
  COUNT_IF(
    order_id IS NULL
    OR TRIM(order_id) = ''
  ) AS missing_order_id,
  COUNT_IF(
    product_id IS NULL
    OR TRIM(product_id) = ''
  ) AS missing_product_id,
  COUNT_IF(
    add_to_cart_order IS NULL
    OR TRIM(add_to_cart_order) = ''
  ) AS missing_sequence,
  COUNT_IF(
    reordered IS NULL
    OR TRIM(reordered) = ''
  ) AS missing_reordered,
  COUNT_IF(_rescued_data IS NOT NULL) AS unparsed_rows
FROM
  week6.bronze_test.order_products_train;

-- 3) Grain / duplicate primary keys: exactly one row per (order_id, product_id)  [Uniqueness]
SELECT
  COUNT(*) - COUNT(DISTINCT order_id, product_id) AS duplicate_keys
FROM
  week6.bronze_test.order_products_train;

-- 4) Validity: non-numeric or out-of-range values  [Validity]
SELECT
  COUNT_IF(TRY_CAST(order_id AS INT) IS NULL) AS non_numeric_order_id,
  COUNT_IF(TRY_CAST(product_id AS INT) IS NULL) AS non_numeric_product_id,
  COUNT_IF(TRY_CAST(order_id AS INT) <= 0) AS invalid_order_ids,
  COUNT_IF(TRY_CAST(product_id AS INT) <= 0) AS invalid_product_ids,
  COUNT_IF(TRY_CAST(add_to_cart_order AS INT) <= 0) AS invalid_sequences,
  COUNT_IF(reordered NOT IN ('0', '1')) AS invalid_reordered_flags
FROM
  week6.bronze_test.order_products_train;

-- 5) Outliers: extreme cart sizes distort market-basket analysis  [Outliers]
--    Treatment: WARN, flag is_large_cart (> 50 items) at silver; keep the rows
SELECT
  COUNT_IF(CAST(add_to_cart_order AS INT) > 50) AS large_cart_rows,
  ROUND(COUNT_IF(CAST(add_to_cart_order AS INT) > 50) * 100.0 / COUNT(*), 4) AS pct_of_total
FROM
  week6.bronze_test.order_products_train;

-- 6) Consistency: add_to_cart_order must be an unbroken 1..N run per order
WITH per_order AS (
  SELECT
    order_id,
    COUNT(*) AS n_items,
    MIN(CAST(add_to_cart_order AS INT)) AS min_seq,
    SUM(CAST(add_to_cart_order AS INT)) AS sum_seq
  FROM
    week6.bronze_test.order_products_train
  GROUP BY
    order_id
)
SELECT
  COUNT(*) AS orders_with_broken_sequence
FROM
  per_order
WHERE
  min_seq != 1
  OR sum_seq != (n_items * (n_items + 1)) / 2;

-- 7) Referential integrity (run once orders + products are ingested)  [Referential]
--    Treatment: REJECT orphans at gold - a broken key silently drops the row from
--    every downstream total, so keys get no WARN tier (same rule as week 5 facts)
SELECT
  COUNT(*) AS orphan_order_ids
FROM
  week6.bronze_test.order_products_train op
WHERE
  NOT EXISTS (
    SELECT
      1
    FROM
      week6.bronze_test.orders o
    WHERE
      o.order_id = op.order_id
      AND o.eval_set = 'train'
  );

SELECT
  COUNT(*) AS orphan_product_ids
FROM
  week6.bronze_test.order_products_train op
WHERE
  NOT EXISTS (
    SELECT
      1
    FROM
      week6.bronze_test.products p
    WHERE
      p.product_id = op.product_id
  );
-- N/A for this table: invalid dates / inconsistent text formats (all 4 business
-- columns are numeric codes; dates live in orders). Silver guard for reordered:
-- CHECK (reordered IN (0, 1))

-- COMMAND ----------

--Shiena

-- COMMAND ----------

--Vee