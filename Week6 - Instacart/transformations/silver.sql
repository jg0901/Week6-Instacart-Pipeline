-- =====================================================
--       Silver Layer: Cleaning + Flagging + Validation
-- =====================================================

-- =====================================================
--               Aisles
-- =====================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.aisles_clean (
  CONSTRAINT aisle_id_valid EXPECT (
    aisle_id IS NOT NULL AND aisle_id > 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned aisles: one row per aisle_id (latest ingested wins on a name conflict), typed+validated key (not null, > 0), trimmed name, quality-flags array'
AS
WITH typed AS (
  SELECT
    TRY_CAST(aisle_id AS BIGINT)  AS aisle_id,   -- null aisle_id or a value that fails to cast both become NULL here; non-positive values pass through untouched and are caught by the aisle_id_valid constraint below
    NULLIF(TRIM(aisle), '')       AS aisle,
    _ingested_at,
    _source_file,
    _source_file_modified_at
  FROM week6.bronze_test.aisles),

name_counts AS (
  SELECT aisle_id, COUNT(DISTINCT aisle) AS distinct_name_count
  FROM typed
  WHERE aisle_id IS NOT NULL
  GROUP BY aisle_id),

ranked AS (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY t.aisle_id
      ORDER BY t._source_file_modified_at DESC, t._ingested_at DESC, t._source_file DESC
    )  AS rn,
    COALESCE(nc.distinct_name_count, 0) > 1  AS is_conflicting_name
  FROM typed t
  LEFT JOIN name_counts nc ON nc.aisle_id = t.aisle_id),
deduped AS (
  SELECT aisle_id, aisle, is_conflicting_name
  FROM ranked
  WHERE rn = 1                -- keeps 1 row per aisle_id: exact duplicates collapse silently,
),                             -- conflicting names keep the most recently ingested version
flagged AS (
  SELECT
    aisle_id,
    aisle,
    is_conflicting_name,
    aisle IS NOT NULL AND COUNT(*) OVER (PARTITION BY aisle) > 1 AS is_duplicate_name
  FROM deduped)
SELECT  aisle_id, aisle,
  FILTER(
    ARRAY(
      CASE WHEN aisle IS NULL       THEN 'missing_name' END,
      CASE WHEN is_conflicting_name THEN 'conflicting_name' END,
      CASE WHEN is_duplicate_name   THEN 'duplicate_name' END ),
    x -> x IS NOT NULL
  ) AS aisle_quality_flags
FROM flagged;

-- =====================================================
--               Departments
-- =====================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.departments_clean (
  CONSTRAINT department_id_valid EXPECT (
    department_id IS NOT NULL AND department_id > 0
  ) ON VIOLATION DROP ROW)
COMMENT 'Cleaned departments: one row per department_id (latest ingested wins on a name conflict), typed+validated key (not null, > 0), trimmed name, quality-flags array'
AS
WITH typed AS (
  SELECT
    TRY_CAST(department_id AS BIGINT)  AS department_id,   -- null/uncastable become NULL here; non-positive values pass through and are caught by the department_id_valid constraint below
    NULLIF(TRIM(department), '')  AS department,
    _ingested_at,
    _source_file,
    _source_file_modified_at
  FROM week6.bronze_test.departments),
-- Same DISTINCT_WINDOW_FUNCTION_UNSUPPORTED fix as aisles_clean: distinct-
-- name count computed as a plain grouped aggregate, joined back per row.
name_counts AS (
  SELECT department_id, COUNT(DISTINCT department) AS distinct_name_count
  FROM typed
  WHERE department_id IS NOT NULL
  GROUP BY department_id),

ranked AS ( SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY t.department_id
      ORDER BY t._source_file_modified_at DESC, t._ingested_at DESC, t._source_file DESC
    ) AS rn,
    COALESCE(nc.distinct_name_count, 0) > 1  AS is_conflicting_name
  FROM typed t
  LEFT JOIN name_counts nc ON nc.department_id = t.department_id),
deduped AS (
  SELECT department_id, department, is_conflicting_name
  FROM ranked
  WHERE rn = 1),
flagged AS (
  SELECT
    department_id,
    department,
    is_conflicting_name,
    department IS NOT NULL AND COUNT(*) OVER (PARTITION BY department) > 1 AS is_duplicate_name
  FROM deduped)
SELECT department_id, department,
  FILTER(
    ARRAY(
      CASE WHEN department IS NULL  THEN 'missing_name' END,
      CASE WHEN is_conflicting_name THEN 'conflicting_name' END,
      CASE WHEN is_duplicate_name   THEN 'duplicate_name' END ),
    x -> x IS NOT NULL
  ) AS department_quality_flags
FROM flagged;



-- =====================================================
--               Products
-- =====================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.products_clean (
  CONSTRAINT product_id_valid EXPECT (
    product_id IS NOT NULL AND product_id > 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned products: one row per product_id (latest ingested wins on a name conflict), typed+validated key (not null, > 0), invalid FKs nulled (not dropped), quality-flags array'
AS
WITH typed AS (
  SELECT
    TRY_CAST(product_id AS BIGINT)     AS product_id,  
    NULLIF(TRIM(product_name), '')     AS product_name,
    TRY_CAST(aisle_id AS BIGINT)       AS aisle_id,
    TRY_CAST(department_id AS BIGINT)  AS department_id,
    _ingested_at,
    _source_file,
    _source_file_modified_at
  FROM week6.bronze_test.products),

name_counts AS (
  SELECT product_id, COUNT(DISTINCT product_name) AS distinct_name_count
  FROM typed
  WHERE product_id IS NOT NULL
  GROUP BY product_id),

ranked AS (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY t.product_id
      ORDER BY t._source_file_modified_at DESC, t._ingested_at DESC, t._source_file DESC
    ) AS rn,
    COALESCE(nc.distinct_name_count, 0) > 1 AS is_conflicting_name
  FROM typed t
  LEFT JOIN name_counts nc ON nc.product_id = t.product_id),
deduped AS (
  SELECT product_id, product_name, aisle_id, department_id, is_conflicting_name
  FROM ranked
  WHERE rn = 1),
flagged AS (
  SELECT
    product_id,
    product_name,
    aisle_id,
    department_id,
    is_conflicting_name,
    product_name IS NOT NULL AND COUNT(*) OVER (PARTITION BY product_name) > 1 AS is_duplicate_name,
    product_name IS NOT NULL AND NOT product_name RLIKE '[A-Za-z]' AS is_gibberish_name
  FROM deduped)
SELECT product_id, product_name,
  aisle_id,
  department_id,
  FILTER(
    ARRAY(
      CASE WHEN product_name IS NULL  THEN 'missing_name' END,
      CASE WHEN is_gibberish_name     THEN 'gibberish_name' END,
      CASE WHEN is_conflicting_name   THEN 'conflicting_name' END,
      CASE WHEN is_duplicate_name     THEN 'duplicate_name' END,
      CASE WHEN aisle_id IS NULL      THEN 'missing_aisle' END,
      CASE WHEN department_id IS NULL THEN 'missing_department' END
    ),
    x -> x IS NOT NULL
  ) AS product_quality_flags
FROM flagged;


-- =====================================================================
--   orders / order_products_prior / order_products_train
-- =====================================================================


-- =====================================================
--               Orders
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.silver_test.orders_clean (
  CONSTRAINT order_id_valid EXPECT (
    order_id IS NOT NULL AND order_id > 0
  ) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned orders: PK cast+validated (drop on failure), eval_set=test excluded, descriptive columns typed/nulled-on-invalid, quality-flags array. Duplicate order_id across batches is not expected -- caught by the Bronze audit log, not resolved here.'
AS
SELECT
  TRY_CAST(order_id AS BIGINT) AS order_id,
  CASE WHEN TRY_CAST(user_id AS BIGINT) > 0
       THEN TRY_CAST(user_id AS BIGINT) END          AS user_id,
  eval_set,
  CASE WHEN TRY_CAST(order_number AS INT) > 0
       THEN TRY_CAST(order_number AS INT) END AS order_number,
  CASE WHEN TRY_CAST(order_dow AS INT) BETWEEN 0 AND 6
       THEN TRY_CAST(order_dow AS INT) END AS order_dow,
  CASE WHEN TRY_CAST(order_hour_of_day AS INT) BETWEEN 0 AND 23
       THEN TRY_CAST(order_hour_of_day AS INT) END   AS order_hour_of_day,
  CASE WHEN TRY_CAST(days_since_prior_order AS DOUBLE) >= 0
       THEN TRY_CAST(days_since_prior_order AS DOUBLE) END AS days_since_prior_order,
  FILTER(
    ARRAY(
      CASE WHEN TRY_CAST(user_id AS BIGINT) IS NULL OR TRY_CAST(user_id AS BIGINT) <= 0
        THEN 'missing_user_id' END,
      CASE WHEN TRY_CAST(order_number AS INT) IS NULL OR TRY_CAST(order_number AS INT) <= 0
        THEN 'missing_order_number' END,
      CASE WHEN TRY_CAST(order_dow AS INT) IS NULL OR TRY_CAST(order_dow AS INT) NOT BETWEEN 0 AND 6
        THEN 'invalid_dow' END,
      CASE WHEN TRY_CAST(order_hour_of_day AS INT) IS NULL OR TRY_CAST(order_hour_of_day AS INT) NOT BETWEEN 0 AND 23
        THEN 'invalid_hour' END,
      CASE WHEN TRY_CAST(order_number AS INT) = 1 AND TRY_CAST(days_since_prior_order AS DOUBLE) IS NOT NULL
        THEN 'unexpected_days_prior_on_first_order' END,
      CASE WHEN TRY_CAST(order_number AS INT) <> 1 AND TRY_CAST(days_since_prior_order AS DOUBLE) IS NULL
        THEN 'missing_days_prior_on_repeat_order' END,
      CASE WHEN eval_set NOT IN ('prior', 'train')
        THEN 'unexpected_eval_set' END
    ),
    x -> x IS NOT NULL
  ) AS order_quality_flags,
  _ingested_at,
  _source_file
FROM STREAM(week6.bronze_test.orders)
WHERE eval_set != 'test';

-- =====================================================
--               Order_Products_Prior
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.silver_test.order_products_prior_clean (
  CONSTRAINT keys_valid EXPECT (
    order_id IS NOT NULL AND order_id > 0
    AND product_id IS NOT NULL AND product_id > 0
  ) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned order_products_prior: composite key cast+validated+orphan-checked (drop on failure), parent order confirmed eval_set=prior (not just any matching order_id), add_to_cart_order/reordered typed/nulled-on-invalid, quality-flags array. Duplicate (order_id, product_id) across batches is not expected -- caught by the Bronze audit log, not resolved here. Rows excluded here may include pending_order_reference cases, not only genuine rejects.'
AS
SELECT
  TRY_CAST(op.order_id AS BIGINT)   AS order_id,
  TRY_CAST(op.product_id AS BIGINT) AS product_id,
  CASE WHEN TRY_CAST(op.add_to_cart_order AS INT) > 0
       THEN TRY_CAST(op.add_to_cart_order AS INT) END AS add_to_cart_order,
  CASE WHEN op.reordered IN ('0', '1')
       THEN TRY_CAST(op.reordered AS INT) END         AS reordered,
  FILTER(
    ARRAY(
      CASE WHEN TRY_CAST(op.add_to_cart_order AS INT) IS NULL OR TRY_CAST(op.add_to_cart_order AS INT) <= 0
        THEN 'invalid_cart_order' END,
      CASE WHEN op.reordered IS NOT NULL AND op.reordered NOT IN ('0', '1')
        THEN 'invalid_reordered' END
    ),
    x -> x IS NOT NULL
  ) AS line_item_quality_flags,
  op._ingested_at,
  op._source_file
FROM STREAM(week6.bronze_test.order_products_prior) op
LEFT SEMI JOIN week6.silver_test.orders_clean o
  ON o.order_id = TRY_CAST(op.order_id AS BIGINT)
  AND o.eval_set = 'prior'
LEFT SEMI JOIN week6.silver_test.products_clean p
  ON p.product_id = TRY_CAST(op.product_id AS BIGINT);


-- =====================================================
--               Order_Products_Train
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.silver_test.order_products_train_clean (
  CONSTRAINT keys_valid EXPECT (
    order_id IS NOT NULL AND order_id > 0
    AND product_id IS NOT NULL AND product_id > 0
  ) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned order_products_train: composite key cast+validated+orphan-checked (drop on failure), parent order confirmed eval_set=train (not just any matching order_id), add_to_cart_order/reordered typed/nulled-on-invalid, quality-flags array. Duplicate (order_id, product_id) across batches is not expected -- caught by the Bronze audit log, not resolved here. Rows excluded here may include pending_order_reference cases, not only genuine rejects.'
AS
SELECT
  TRY_CAST(op.order_id AS BIGINT)   AS order_id,
  TRY_CAST(op.product_id AS BIGINT) AS product_id,
  CASE WHEN TRY_CAST(op.add_to_cart_order AS INT) > 0
       THEN TRY_CAST(op.add_to_cart_order AS INT) END AS add_to_cart_order,
  CASE WHEN op.reordered IN ('0', '1')
       THEN TRY_CAST(op.reordered AS INT) END         AS reordered,
  FILTER(
    ARRAY(
      CASE WHEN TRY_CAST(op.add_to_cart_order AS INT) IS NULL OR TRY_CAST(op.add_to_cart_order AS INT) <= 0
        THEN 'invalid_cart_order' END,
      CASE WHEN op.reordered IS NOT NULL AND op.reordered NOT IN ('0', '1')
        THEN 'invalid_reordered' END
    ),
    x -> x IS NOT NULL
  ) AS line_item_quality_flags,
  op._ingested_at,
  op._source_file
FROM STREAM(week6.bronze_test.order_products_train) op
LEFT SEMI JOIN week6.silver_test.orders_clean o
  ON o.order_id = TRY_CAST(op.order_id AS BIGINT)
  AND o.eval_set = 'train'
LEFT SEMI JOIN week6.silver_test.products_clean p
  ON p.product_id = TRY_CAST(op.product_id AS BIGINT);

-- =============================================================================
-- SILVER LAYER — WARN-FLAG SUMMARY (rows kept, but with an issue flagged)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.dq_warnings_summary AS
SELECT 'aisles' AS table_name, flag, COUNT(*) AS flagged_row_count
FROM week6.silver_test.aisles_clean LATERAL VIEW EXPLODE(aisle_quality_flags) t AS flag
GROUP BY flag
UNION ALL
SELECT 'departments', flag, COUNT(*)
FROM week6.silver_test.departments_clean LATERAL VIEW EXPLODE(department_quality_flags) t AS flag
GROUP BY flag
UNION ALL
SELECT 'products', flag, COUNT(*)
FROM week6.silver_test.products_clean LATERAL VIEW EXPLODE(product_quality_flags) t AS flag
GROUP BY flag
UNION ALL
SELECT 'orders', flag, COUNT(*)
FROM week6.silver_test.orders_clean LATERAL VIEW EXPLODE(order_quality_flags) t AS flag
GROUP BY flag
UNION ALL
SELECT 'order_products_prior', flag, COUNT(*)
FROM week6.silver_test.order_products_prior_clean LATERAL VIEW EXPLODE(line_item_quality_flags) t AS flag
GROUP BY flag
UNION ALL
SELECT 'order_products_train', flag, COUNT(*)
FROM week6.silver_test.order_products_train_clean LATERAL VIEW EXPLODE(line_item_quality_flags) t AS flag
GROUP BY flag
ORDER BY table_name, flagged_row_count DESC;
 
 
-- =============================================================================
-- SUPPLEMENT — total rows and % with at least one flag, per table
-- =============================================================================
-- dq_warnings_summary above answers "how many rows have flag X" (multi-count
-- per row). This answers the coarser "how much of this table has ANY issue
-- at all" -- one row counted once regardless of how many flags it carries.
-- Useful as a single top-line data-quality number per table alongside the
-- drop-rate gates' percentages.
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.dq_warnings_coverage AS
SELECT
  'aisles' AS table_name,
  COUNT(*)                                   AS total_rows,
  COUNT_IF(SIZE(aisle_quality_flags) > 0)    AS rows_with_any_flag,
  ROUND(100.0 * COUNT_IF(SIZE(aisle_quality_flags) > 0) / COUNT(*), 2) AS pct_rows_flagged
FROM week6.silver_test.aisles_clean
UNION ALL
SELECT
  'departments',
  COUNT(*),
  COUNT_IF(SIZE(department_quality_flags) > 0),
  ROUND(100.0 * COUNT_IF(SIZE(department_quality_flags) > 0) / COUNT(*), 2)
FROM week6.silver_test.departments_clean
UNION ALL
SELECT
  'products',
  COUNT(*),
  COUNT_IF(SIZE(product_quality_flags) > 0),
  ROUND(100.0 * COUNT_IF(SIZE(product_quality_flags) > 0) / COUNT(*), 2)
FROM week6.silver_test.products_clean
UNION ALL
SELECT
  'orders',
  COUNT(*),
  COUNT_IF(SIZE(order_quality_flags) > 0),
  ROUND(100.0 * COUNT_IF(SIZE(order_quality_flags) > 0) / COUNT(*), 2)
FROM week6.silver_test.orders_clean
 
UNION ALL
SELECT
  'order_products_prior',
  COUNT(*),
  COUNT_IF(SIZE(line_item_quality_flags) > 0),
  ROUND(100.0 * COUNT_IF(SIZE(line_item_quality_flags) > 0) / COUNT(*), 2)
FROM week6.silver_test.order_products_prior_clean
UNION ALL
SELECT
  'order_products_train',
  COUNT(*),
  COUNT_IF(SIZE(line_item_quality_flags) > 0),
  ROUND(100.0 * COUNT_IF(SIZE(line_item_quality_flags) > 0) / COUNT(*), 2)
FROM week6.silver_test.order_products_train_clean
 
ORDER BY table_name;