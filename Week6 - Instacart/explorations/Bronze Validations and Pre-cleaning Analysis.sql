-- Databricks notebook source
--Jemma
-- Null Checkers and Uniqueness Checker in Aisle and Department Bronze tables
Select count(*) AS row_count, count(distinct aisle_id) AS distinct_aisle_id_count, count(distinct aisle) as distinct_aisle_count from week6.bronze_test.aisles; 
Select count(*) AS row_count, count(distinct department_id)as distinct_dep_id_count, count(distinct department) as distinct_dep_count from week6.bronze_test.departments; 

-- COMMAND ----------

--Jemma
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
--Vee

%sql
--Data Profilling
WITH raw_orders AS (
  SELECT * FROM read_files(
    '/Volumes/week6/bronze/raw_files/orders.csv',
    format => 'csv',
    header => 'true',
    inferSchema => 'true'
  )
)
SELECT
  -- 1. Volume & Rescue Integrity
  -- corrupt_rescued_rows are the corrupted or misformatted data during the import
  COUNT(*) AS total_records,
  COUNT(DISTINCT order_id) AS unique_orders,
  COUNT(DISTINCT user_id) AS unique_users,
  COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_order_ids,
  COUNT_IF(_rescued_data IS NOT NULL) AS corrupt_rescued_rows,

  -- 2. Domain & Boundary Checks
  -- Dow as day of the week
  -- 0 as Sunday, 6 as Saturday
  -- 0 as Midnight, 23 as 11pm
  MIN(order_dow) AS min_dow,                 -- Expecting 0
  MAX(order_dow) AS max_dow,                 -- Expecting 6
  MIN(order_hour_of_day) AS min_hour,       -- Expecting 0
  MAX(order_hour_of_day) AS max_hour,       -- Expecting 23

  -- 3. Business Logic Anomalies
  COUNT_IF(order_number = 1 AND days_since_prior_order IS NOT NULL) AS invalid_first_order_days,
  COUNT_IF(order_number > 1 AND days_since_prior_order IS NULL) AS missing_subsequent_days
FROM raw_orders;

--Data Partition and Reorder Analysis
WITH raw_orders AS (
  SELECT * FROM read_files(
    '/Volumes/week6/bronze/raw_files/orders.csv',
    format => 'csv',
    header => 'true',
    inferSchema => 'true'
  )
)
SELECT 
  eval_set,
  COUNT(*) AS row_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_of_total,
  MIN(days_since_prior_order) AS min_days_prior,
  MAX(days_since_prior_order) AS max_days_prior,
  AVG(days_since_prior_order) AS avg_days_prior
FROM raw_orders
GROUP BY eval_set
ORDER BY row_count DESC;

-- Data Sample Inspection
-- Preview the top 1000 rows and automatically infer headers and column types
SELECT *
FROM read_files(
 '/Volumes/week6/bronze/raw_files/orders.csv',
 format => 'csv',
 header => 'true',
 inferSchema => 'true'
)
LIMIT 1000;

--support Data Sample Inspection for days_since_prior_order
SELECT 
  COUNT_IF(order_number = 1 AND days_since_prior_order IS NULL) AS expected_first_order_nulls,
  COUNT_IF(order_number > 1 AND days_since_prior_order IS NULL) AS unexpected_subsequent_nulls,
  ROUND(COUNT_IF(order_number > 1 AND days_since_prior_order IS NULL) * 100.0 / COUNT(*), 2) AS error_rate_pct
FROM week6.silver.orders;

--support Data Sample Inspection

WITH sample_users AS (
  SELECT DISTINCT user_id
  FROM week6.silver.orders
  LIMIT 5
)
SELECT 
  user_id,
  order_id,
  order_number,
  days_since_prior_order
FROM week6.silver.orders
WHERE user_id IN (SELECT user_id FROM sample_users)
ORDER BY user_id, order_number;

--Support Data Sample Inspection

CREATE OR REPLACE TEMP VIEW raw_orders_with_timestamps AS
SELECT 
  order_id,
  user_id,
  order_number,
  days_since_prior_order,
  -- Sum cumulative days elapsed per user to project forward from baseline date
  TIMESTAMP(
    CONCAT(
      DATE_ADD(
        '2026-01-01', 
        CAST(SUM(COALESCE(days_since_prior_order, 0)) OVER (PARTITION BY user_id ORDER BY order_number) AS INT)
      ), 
      ' ', 
      LPAD(order_hour_of_day, 2, '0'), 
      ':00:00'
    )
  ) AS order_timestamp
FROM week6.silver.orders;

-- Inspect the schema (column names and data types)
DESCRIBE SELECT * 
FROM read_files(
  '/Volumes/week6/bronze/raw_files/orders.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'true'
);

-- Raw string-level profiling: nulls, distincts, and untrimmed whitespace per column

WITH raw_csv AS (
  SELECT * FROM read_files(
    '/Volumes/week6/bronze/raw_files/orders.csv',
    format => 'csv',
    header => 'true',
    inferSchema => 'false' -- Disables auto-parsing so raw whitespace can be accurately flagged
  )
),
metrics AS (
  SELECT
    -- order_id
    COUNT_IF(order_id IS NULL OR order_id = '') AS order_id_nulls,
    COUNT(DISTINCT order_id) AS order_id_distinct,
    COUNT_IF(order_id != TRIM(order_id)) AS order_id_trims,

    -- user_id
    COUNT_IF(user_id IS NULL OR user_id = '') AS user_id_nulls,
    COUNT(DISTINCT user_id) AS user_id_distinct,
    COUNT_IF(user_id != TRIM(user_id)) AS user_id_trims,

    -- eval_set
    COUNT_IF(eval_set IS NULL OR eval_set = '') AS eval_set_nulls,
    COUNT(DISTINCT eval_set) AS eval_set_distinct,
    COUNT_IF(eval_set != TRIM(eval_set)) AS eval_set_trims,

    -- order_number
    COUNT_IF(order_number IS NULL OR order_number = '') AS order_number_nulls,
    COUNT(DISTINCT order_number) AS order_number_distinct,
    COUNT_IF(order_number != TRIM(order_number)) AS order_number_trims,

    -- order_dow
    COUNT_IF(order_dow IS NULL OR order_dow = '') AS order_dow_nulls,
    COUNT(DISTINCT order_dow) AS order_dow_distinct,
    COUNT_IF(order_dow != TRIM(order_dow)) AS order_dow_trims,

    -- order_hour_of_day
    COUNT_IF(order_hour_of_day IS NULL OR order_hour_of_day = '') AS order_hour_of_day_nulls,
    COUNT(DISTINCT order_hour_of_day) AS order_hour_of_day_distinct,
    COUNT_IF(order_hour_of_day != TRIM(order_hour_of_day)) AS order_hour_of_day_trims,

    -- days_since_prior_order
    COUNT_IF(days_since_prior_order IS NULL OR days_since_prior_order = '') AS days_since_prior_order_nulls,
    COUNT(DISTINCT days_since_prior_order) AS days_since_prior_order_distinct,
    COUNT_IF(days_since_prior_order != TRIM(days_since_prior_order)) AS days_since_prior_order_trims,

    -- _rescued_data
    COUNT_IF(_rescued_data IS NULL OR _rescued_data = '') AS rescued_data_nulls,
    COUNT(DISTINCT _rescued_data) AS rescued_data_distinct,
    COUNT_IF(_rescued_data != TRIM(_rescued_data)) AS rescued_data_trims
  FROM raw_csv
)
SELECT 
  stack(8,
    'order_id', order_id_nulls, order_id_distinct, order_id_trims,
    'user_id', user_id_nulls, user_id_distinct, user_id_trims,
    'eval_set', eval_set_nulls, eval_set_distinct, eval_set_trims,
    'order_number', order_number_nulls, order_number_distinct, order_number_trims,
    'order_dow', order_dow_nulls, order_dow_distinct, order_dow_trims,
    'order_hour_of_day', order_hour_of_day_nulls, order_hour_of_day_distinct, order_hour_of_day_trims,
    'days_since_prior_order', days_since_prior_order_nulls, days_since_prior_order_distinct, days_since_prior_order_trims,
    '_rescued_data', rescued_data_nulls, rescued_data_distinct, rescued_data_trims
  ) AS (column_name, null_count, distinct_count, trim_needed_count)
FROM metrics;

-- Sanity Check: Must return 0 rows
WITH bronze_orders AS (
  SELECT * FROM read_files(
    '/Volumes/week6/bronze/raw_files/orders.csv',
    format => 'csv',
    header => 'true',
    inferSchema => 'true'
  )
)
SELECT COUNT(*) 
FROM bronze_orders 
WHERE (order_number = 1 AND days_since_prior_order IS NOT NULL)
   OR (order_number > 1 AND days_since_prior_order IS NULL);


-- Define Silver table structure, enforcing primary key and NOT NULL constraints
CREATE TABLE IF NOT EXISTS week6.silver.orders (
  order_id INT NOT NULL,
  user_id INT NOT NULL,
  eval_set STRING NOT NULL,
  order_number INT NOT NULL,
  order_dow INT NOT NULL,
  order_hour_of_day INT NOT NULL,
  days_since_prior_order DOUBLE,
  CONSTRAINT pk_silver_orders PRIMARY KEY (order_id)
) USING DELTA;

-- Validation: Flag rows that violate order_dow constraint (0-6)
SELECT 
  'check_valid_dow' AS constraint_name,
  COUNT_IF(order_dow NOT BETWEEN 0 AND 6) AS violation_count,
  CASE 
    WHEN COUNT_IF(order_dow NOT BETWEEN 0 AND 6) = 0 THEN 'PASS' 
    ELSE 'FLAGGED' 
  END AS status
FROM week6.silver.orders;

-- Validation: Flag rows that violate order_hour_of_day constraint (0-23)
SELECT 
  'check_valid_hour' AS constraint_name,
  COUNT_IF(order_hour_of_day NOT BETWEEN 0 AND 23) AS violation_count,
  CASE 
    WHEN COUNT_IF(order_hour_of_day NOT BETWEEN 0 AND 23) = 0 THEN 'PASS' 
    ELSE 'FLAGGED' 
  END AS status
FROM week6.silver.orders;

-- Validation: Flag rows that violate order_number constraint (>= 1)
SELECT 
  'check_positive_order_number' AS constraint_name,
  COUNT_IF(order_number < 1) AS violation_count,
  CASE 
    WHEN COUNT_IF(order_number < 1) = 0 THEN 'PASS' 
    ELSE 'FLAGGED' 
  END AS status
FROM week6.silver.orders;

-- Validation: Flag rows that violate eval_set constraint (prior, train, test)
SELECT 
  'check_valid_eval_set' AS constraint_name,
  COUNT_IF(eval_set NOT IN ('prior', 'train', 'test')) AS violation_count,
  CASE 
    WHEN COUNT_IF(eval_set NOT IN ('prior', 'train', 'test')) = 0 THEN 'PASS' 
    ELSE 'FLAGGED' 
  END AS status
FROM week6.silver.orders;

-- ============================================================
-- STEP 1b: QUARANTINE TABLE
-- Holds rows dropped from Silver: bad casts, missing required
-- fields, or rows the source flagged via _rescued_data.
-- ============================================================

CREATE TABLE IF NOT EXISTS week6.silver.orders_quarantine (
  order_id STRING,
  user_id STRING,
  eval_set STRING,
  order_number STRING,
  order_dow STRING,
  order_hour_of_day STRING,
  days_since_prior_order STRING,
  _rescued_data STRING,
  quarantine_reason STRING,
  quarantined_at TIMESTAMP
) USING DELTA;


-- ============================================================
-- STEP 2: LOAD SILVER FROM BRONZE (fixed)
--   - TRY_CAST instead of CAST (bad values -> NULL, no crash)
--   - Explicit filter on required-field NULLs and _rescued_data
--   - ROW_NUMBER() dedup on order_id (PK is not self-enforcing)
--   - Bad/duplicate rows routed to quarantine, not silently lost
-- ============================================================

CREATE OR REPLACE TEMP VIEW typed_orders AS
SELECT
  TRY_CAST(order_id AS INT)                   AS order_id,
  TRY_CAST(user_id AS INT)                     AS user_id,
  eval_set,
  TRY_CAST(order_number AS INT)                AS order_number,
  TRY_CAST(order_dow AS INT)                   AS order_dow,
  TRY_CAST(order_hour_of_day AS INT)           AS order_hour_of_day,
  TRY_CAST(days_since_prior_order AS DOUBLE)   AS days_since_prior_order,
  _rescued_data,
  order_id   AS raw_order_id,
  user_id    AS raw_user_id,
  order_number AS raw_order_number,
  order_dow  AS raw_order_dow,
  order_hour_of_day AS raw_order_hour_of_day,
  days_since_prior_order AS raw_days_since_prior_order
FROM read_files(
  '/Volumes/week6/bronze/raw_files/orders.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'true'
);


-- 2a. Create ranked_orders view with deduplication
CREATE OR REPLACE TEMP VIEW ranked_orders AS
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS rn
FROM typed_orders;

-- 2b. Clean load into Silver
INSERT OVERWRITE week6.silver.orders (
  order_id,
  user_id,
  eval_set,
  order_number,
  order_dow,
  order_hour_of_day,
  days_since_prior_order
)
SELECT
  order_id,
  user_id,
  eval_set,
  order_number,
  order_dow,
  order_hour_of_day,
  days_since_prior_order
FROM ranked_orders
WHERE rn = 1
  AND _rescued_data IS NULL
  AND order_id IS NOT NULL
  AND user_id IS NOT NULL
  AND eval_set IS NOT NULL AND eval_set != ''
  AND order_number IS NOT NULL
  AND order_dow IS NOT NULL
  AND order_hour_of_day IS NOT NULL;


-- ============================================================
-- STEP 3: POST-LOAD VALIDATION & PROFILING (unchanged)
-- ============================================================
 
SELECT
  MIN(order_id) AS min_order_id,
  MAX(order_id) AS max_order_id,
  MIN(user_id) AS min_user_id,
  MAX(user_id) AS max_user_id,
  MIN(order_number) AS min_order_number,
  MAX(order_number) AS max_order_number,
  MIN(order_dow) AS min_order_dow,
  MAX(order_dow) AS max_order_dow,
  MIN(order_hour_of_day) AS min_hour_of_day,
  MAX(order_hour_of_day) AS max_hour_of_day,
  MIN(days_since_prior_order) AS min_days_prior,
  MAX(days_since_prior_order) AS max_days_prior
FROM week6.silver.orders;

SELECT
  eval_set,
  COUNT(*) AS category_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_share,
  MIN(LENGTH(eval_set)) AS min_string_length,
  MAX(LENGTH(eval_set)) AS max_string_length
FROM week6.silver.orders
GROUP BY eval_set
ORDER BY category_count DESC;

WITH user_metrics AS (
  SELECT
    user_id,
    COUNT(*) AS total_orders,
    MAX(order_number) AS max_order_num,
    MIN(order_number) AS min_order_num,
    COUNT(DISTINCT order_number) AS distinct_order_nums
  FROM week6.silver.orders
  GROUP BY user_id
)
SELECT
  COUNT_IF(min_order_num != 1) AS users_not_starting_at_1,
  COUNT_IF(total_orders != max_order_num) AS users_with_sequence_gaps,
  COUNT_IF(total_orders != distinct_order_nums) AS users_with_duplicate_order_nums
FROM user_metrics;

WITH user_order_ranks AS (
  SELECT
    user_id,
    order_id,
    eval_set,
    order_number,
    MAX(order_number) OVER (PARTITION BY user_id) AS user_max_order
  FROM week6.silver.orders
)
SELECT
  COUNT_IF(eval_set IN ('train', 'test') AND order_number != user_max_order) AS misplaced_eval_orders,
  COUNT_IF(eval_set = 'prior' AND order_number = user_max_order) AS prior_marked_as_final
FROM user_order_ranks;

SELECT
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY days_since_prior_order) AS p25_days,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY days_since_prior_order) AS median_days,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_since_prior_order) AS p75_days,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY days_since_prior_order) AS p95_days,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY days_since_prior_order) AS p99_days
FROM week6.silver.orders
WHERE days_since_prior_order IS NOT NULL;

SELECT
  order_dow,
  order_hour_of_day,
  COUNT(*) AS total_orders,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM week6.silver.orders
GROUP BY order_dow, order_hour_of_day
ORDER BY total_orders DESC
LIMIT 1000;

SELECT
  user_id,
  COUNT(DISTINCT eval_set) AS split_count
FROM week6.silver.orders
WHERE eval_set IN ('train', 'test')
GROUP BY user_id
HAVING split_count > 1;


-- ============================================================
-- STEP 4: FINAL VALIDATION / AUDIT
-- (unchanged — should now reliably return all PASS, since
--  dedup + filtering + CHECK constraints are enforced at load time)
-- ============================================================
 

SELECT
  'valid_order_id' AS check_name,
  COUNT_IF(order_id IS NULL) AS violation_count,
  CASE WHEN COUNT_IF(order_id IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM week6.silver.orders
UNION ALL
SELECT
  'valid_dow',
  COUNT_IF(order_dow NOT BETWEEN 0 AND 6),
  CASE WHEN COUNT_IF(order_dow NOT BETWEEN 0 AND 6) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM week6.silver.orders
UNION ALL
SELECT
  'valid_days_prior',
  COUNT_IF(days_since_prior_order IS NOT NULL AND days_since_prior_order < 0),
  CASE WHEN COUNT_IF(days_since_prior_order IS NOT NULL AND days_since_prior_order < 0) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM week6.silver.orders;
 
WITH checks AS (
  SELECT
    'PK Uniqueness' AS test_name,
    CASE WHEN COUNT(*) = COUNT(DISTINCT order_id) THEN 'PASS' ELSE 'FAIL' END AS status
  FROM week6.silver.orders
  UNION ALL
  SELECT
    'Sequence Integrity',
    CASE WHEN COUNT_IF(order_number = 1 AND days_since_prior_order IS NOT NULL) = 0 THEN 'PASS' ELSE 'FAIL' END
  FROM week6.silver.orders
  UNION ALL
  SELECT
    'Categorical Boundaries',
    CASE WHEN COUNT_IF(eval_set NOT IN ('prior', 'train', 'test')) = 0 THEN 'PASS' ELSE 'FAIL' END
  FROM week6.silver.orders
)
SELECT * FROM checks;


-- ============================================================
-- STEP 5 (NEW): QUARANTINE SUMMARY
-- Quick sanity read on what got dropped and why — replaces the
-- "silent data loss" behavior of the original pipeline.
-- ============================================================
 
SELECT
  quarantine_reason,
  COUNT(*) AS row_count
FROM week6.silver.orders_quarantine
GROUP BY quarantine_reason
ORDER BY row_count DESC;


-- Batch load alternative for serverless compute
-- For streaming ingestion, use a Lakeflow Spark Declarative Pipeline instead
CREATE OR REPLACE TABLE week6.bronze.orders
TBLPROPERTIES (
  'delta.feature.timestampNtz' = 'supported',
  'delta.columnMapping.mode' = 'name')
AS
SELECT * FROM read_files(
  '/Volumes/week6/bronze/raw_files/orders.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);