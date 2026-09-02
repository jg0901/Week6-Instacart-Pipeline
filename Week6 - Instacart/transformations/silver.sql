-- =====================================================
--       Silver Layer: Cleaning + Flagging + Validation
-- =====================================================

-- =====================================================
--               Aisles
-- =====================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.aisles_clean (
  CONSTRAINT aisle_id_valid EXPECT (
    aisle_id IS NOT NULL
  ) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned aisles: one row per aisle_id (latest ingested wins on a name conflict), typed key, trimmed name, single quality-flag column'
AS
WITH typed AS (
  SELECT
    TRY_CAST(aisle_id AS BIGINT)  AS aisle_id,   -- null aisle_id or a value that fails to cast both become NULL here
    NULLIF(TRIM(aisle), '')       AS aisle,
    _ingested_at
  FROM week6.bronze_test.aisles),
name_counts AS (
  SELECT aisle_id, COUNT(DISTINCT aisle) AS distinct_name_count
  FROM typed
  WHERE aisle_id IS NOT NULL
  GROUP BY aisle_id
),
ranked AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY t.aisle_id ORDER BY t._ingested_at DESC
    )                                                AS rn,
    COALESCE(nc.distinct_name_count, 0) > 1           AS is_conflicting_name
  FROM typed t
  LEFT JOIN name_counts nc ON nc.aisle_id = t.aisle_id
),

deduped AS (
  SELECT aisle_id, aisle, is_conflicting_name
  FROM ranked
  WHERE rn = 1),               -- keeps 1 row per aisle_id: exact duplicates collapse silently,
                            -- conflicting names keep the most recently ingested version
flagged AS (
  SELECT
    aisle_id,
    aisle,
    is_conflicting_name,
    aisle IS NOT NULL AND COUNT(*) OVER (PARTITION BY aisle) > 1 AS is_duplicate_name
  FROM deduped)

SELECT aisle_id, aisle,
  CASE WHEN aisle IS NULL THEN 'missing_name'
    WHEN is_conflicting_name THEN 'conflicting_name'
    WHEN is_duplicate_name   THEN 'duplicate_name'
    WHEN aisle RLIKE '^[^a-zA-Z]*$' THEN 'gibberish_name'
    ELSE NULL
  END AS aisle_quality_flag
FROM flagged;


-- =====================================================
--               Departments
-- =====================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.departments_clean (
  CONSTRAINT department_id_valid EXPECT (
    department_id IS NOT NULL
  ) ON VIOLATION DROP ROW)
COMMENT 'Cleaned departments: one row per department_id (latest ingested wins on a name conflict), typed key, trimmed name, single quality-flag column'
AS WITH typed AS (
  SELECT TRY_CAST(department_id AS BIGINT) AS department_id,
    NULLIF(TRIM(department), '') AS department,
    _ingested_at
  FROM week6.bronze_test.departments),

name_counts AS (
  SELECT department_id, COUNT(DISTINCT department) AS distinct_name_count
  FROM typed
  WHERE department_id IS NOT NULL
  GROUP BY department_id
),
ranked AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY t.department_id ORDER BY t._ingested_at DESC
    )                                                AS rn,
    COALESCE(nc.distinct_name_count, 0) > 1           AS is_conflicting_name
  FROM typed t
  LEFT JOIN name_counts nc ON nc.department_id = t.department_id),

deduped AS (
  SELECT department_id, department, is_conflicting_name
  FROM ranked
  WHERE rn = 1),

flagged AS (
  SELECT department_id, department, is_conflicting_name,
    department IS NOT NULL AND COUNT(*) OVER (PARTITION BY department) > 1 AS is_duplicate_name
  FROM deduped)
SELECT department_id,department,
  CASE
    WHEN department IS NULL  THEN 'missing_name'
    WHEN is_conflicting_name THEN 'conflicting_name'
    WHEN is_duplicate_name   THEN 'duplicate_name'
    ELSE NULL
  END AS department_quality_flag
FROM flagged;


-- =====================================================
--               Products
-- =====================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.products_clean (
  CONSTRAINT product_id_valid EXPECT (
    product_id IS NOT NULL) 
    ON VIOLATION DROP ROW)
COMMENT 'Cleaned products: one row per product_id (latest ingested wins on a name conflict), typed keys, invalid FKs nulled (not dropped), single quality-flag column'
AS
WITH typed AS (
  SELECT
    TRY_CAST(product_id AS BIGINT)     AS product_id,
    NULLIF(TRIM(product_name), '')     AS product_name,
    TRY_CAST(aisle_id AS BIGINT)       AS aisle_id,
    TRY_CAST(department_id AS BIGINT)  AS department_id,
    _ingested_at
  FROM week6.bronze_test.products),
name_counts AS (
  SELECT product_id, COUNT(DISTINCT product_name) AS distinct_name_count
  FROM typed
  WHERE product_id IS NOT NULL
  GROUP BY product_id),
ranked AS (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY t.product_id ORDER BY t._ingested_at DESC) AS rn,
    COALESCE(nc.distinct_name_count, 0) > 1 AS is_conflicting_name
  FROM typed t
  LEFT JOIN name_counts nc ON nc.product_id = t.product_id ),

deduped AS (
  SELECT product_id, product_name, aisle_id, department_id, is_conflicting_name
  FROM ranked
  WHERE rn = 1),

flagged AS ( SELECT
    product_id,
    product_name,
    aisle_id,
    department_id,
    is_conflicting_name,
    product_name IS NOT NULL AND COUNT(*) OVER (PARTITION BY product_name) > 1 AS is_duplicate_name,
    product_name IS NOT NULL AND NOT product_name RLIKE '[A-Za-z]' AS is_gibberish_name
  FROM deduped)
SELECT  product_id, product_name,
  aisle_id, department_id,
  CASE
    WHEN product_name IS NULL  THEN 'missing_name'
    WHEN is_gibberish_name     THEN 'gibberish_name'
    WHEN is_conflicting_name   THEN 'conflicting_name'
    WHEN is_duplicate_name     THEN 'duplicate_name'
    WHEN aisle_id IS NULL      THEN 'missing_aisle'
    WHEN department_id IS NULL THEN 'missing_department'
    ELSE NULL
  END AS product_quality_flag
FROM flagged;

-- =====================================================================
--   orders / order_products_prior / order_products_train
-- =====================================================================
-- Plain STREAMING TABLE per table -- same simple monitor-and-filter
-- pattern as the rest of the pipeline. No dedup mechanism built in
-- (no ROW_NUMBER/MATERIALIZED VIEW, no APPLY CHANGES INTO).
--
-- Why not dedup here: a duplicate (order_id, product_id) or duplicate
-- order_id across batches isn't a legitimate occurrence for this data --
-- unlike aisles.aisle_id getting a corrected name in a later batch, an
-- order_products row is an immutable historical fact. If the same key
-- shows up twice, it means a batch was built wrong (overlapping row
-- ranges, or the same file re-dropped), not that two different real
-- versions of the same row need reconciling. That's a process bug to
-- catch, not a data condition to silently resolve.
--
-- Detection, not resolution: duplicate keys are caught by the Bronze
-- audit log's null_key_rows/duplicate-count queries (02_bronze_audit_
-- processed_vs_new.sql), which runs as an automatic chained step after
-- every pipeline update -- not a manual check, just a different kind of
-- automation than a live streaming constraint. A per-row EXPECT can't do
-- this kind of check anyway: "is this key duplicated anywhere in the
-- table" needs an unbounded aggregation across the whole table, which
-- streaming constraints can't evaluate per-row regardless of mechanism.
-- If the audit log ever shows a nonzero duplicate count here, that's the
-- signal to go check how the batch was built -- not something the
-- pipeline should try to paper over on its own.
-- =====================================================================


-- =====================================================
--               Orders
-- =====================================================
-- order_id (PK): TRY_CAST + > 0 required, drop on failure -- same
-- identity-vs-descriptive framework as every other table. user_id/
-- order_number/order_dow/order_hour_of_day/days_since_prior_order are
-- descriptive: invalid -> nulled via CASE WHEN, not dropped, row stays.
--
-- eval_set = 'test' is filtered out entirely here -- a business-scope
-- exclusion (Kaggle competition holdout, no order_products data exists
-- for these orders by design), not a data-quality drop. Any eval_set
-- value that isn't 'prior'/'train' after that filter is flagged, not
-- expected to ever fire.
--
-- days_since_prior_order consistency checks: flagged (not dropped) if
-- non-null on a user's first order (order_number = 1) or null on a
-- repeat order (order_number != 1) -- both indicate something's off
-- without invalidating the row.
--
-- Quality signals collapse into one order_quality_flag via CASE WHEN,
-- same convention as aisles/departments/products -- only the first
-- matching issue is reported per row if several apply at once.
-- ---------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE week6.silver_test.orders_clean (
  CONSTRAINT order_id_valid EXPECT (
    order_id IS NOT NULL AND order_id > 0
  ) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned orders: PK cast+validated (drop on failure), eval_set=test excluded, descriptive columns typed/nulled-on-invalid, single quality-flag column. Duplicate order_id across batches is not expected -- caught by the Bronze audit log, not resolved here.'
AS
SELECT
  TRY_CAST(order_id AS BIGINT) AS order_id,
  CASE WHEN TRY_CAST(user_id AS BIGINT) > 0
       THEN TRY_CAST(user_id AS BIGINT) END          AS user_id,
  eval_set,
  CASE WHEN TRY_CAST(order_number AS INT) > 0
       THEN TRY_CAST(order_number AS INT) END        AS order_number,
  CASE WHEN TRY_CAST(order_dow AS INT) BETWEEN 0 AND 6
       THEN TRY_CAST(order_dow AS INT) END           AS order_dow,
  CASE WHEN TRY_CAST(order_hour_of_day AS INT) BETWEEN 0 AND 23
       THEN TRY_CAST(order_hour_of_day AS INT) END   AS order_hour_of_day,
  CASE WHEN TRY_CAST(days_since_prior_order AS DOUBLE) >= 0
       THEN TRY_CAST(days_since_prior_order AS DOUBLE) END AS days_since_prior_order,
  CASE
    WHEN TRY_CAST(user_id AS BIGINT) IS NULL OR TRY_CAST(user_id AS BIGINT) <= 0
      THEN 'missing_user_id'
    WHEN TRY_CAST(order_number AS INT) IS NULL OR TRY_CAST(order_number AS INT) <= 0
      THEN 'missing_order_number'
    WHEN TRY_CAST(order_dow AS INT) IS NULL OR TRY_CAST(order_dow AS INT) NOT BETWEEN 0 AND 6
      THEN 'invalid_dow'
    WHEN TRY_CAST(order_hour_of_day AS INT) IS NULL OR TRY_CAST(order_hour_of_day AS INT) NOT BETWEEN 0 AND 23
      THEN 'invalid_hour'
    WHEN TRY_CAST(order_number AS INT) = 1 AND TRY_CAST(days_since_prior_order AS DOUBLE) IS NOT NULL
      THEN 'unexpected_days_prior_on_first_order'
    WHEN TRY_CAST(order_number AS INT) <> 1 AND TRY_CAST(days_since_prior_order AS DOUBLE) IS NULL
      THEN 'missing_days_prior_on_repeat_order'
    WHEN eval_set NOT IN ('prior', 'train')
      THEN 'unexpected_eval_set'
    ELSE NULL
  END AS order_quality_flag,
  _ingested_at
FROM STREAM(week6.bronze_test.orders)
WHERE eval_set != 'test';


-- =====================================================
--               Order_Products_Prior
-- =====================================================
-- order_id/product_id here are grain-defining, not descriptive -- unlike
-- products.aisle_id/department_id, a row with no valid order_id or
-- product_id has nothing salvageable (the whole row's reason to exist is
-- "this product was in this order"). So both a null/invalid key AND an
-- orphaned-but-syntactically-valid key (doesn't exist in orders_clean/
-- products_clean) get dropped here, via the CONSTRAINT plus the two
-- LEFT SEMI JOINs below -- different treatment from products' FK warn
-- rule, same reasoning we walked through for that distinction.
--
-- add_to_cart_order/reordered are descriptive: invalid -> nulled, not
-- dropped, same as every other non-identity column in this pipeline.
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE week6.silver_test.order_products_prior_clean (
  CONSTRAINT keys_valid EXPECT (
    order_id IS NOT NULL AND order_id > 0
    AND product_id IS NOT NULL AND product_id > 0
  ) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned order_products_prior: composite key cast+validated+orphan-checked (drop on failure), add_to_cart_order/reordered typed/nulled-on-invalid, single quality-flag column. Duplicate (order_id, product_id) across batches is not expected -- caught by the Bronze audit log, not resolved here.'
AS
SELECT
  TRY_CAST(op.order_id AS BIGINT)   AS order_id,
  TRY_CAST(op.product_id AS BIGINT) AS product_id,
  CASE WHEN TRY_CAST(op.add_to_cart_order AS INT) > 0
       THEN TRY_CAST(op.add_to_cart_order AS INT) END AS add_to_cart_order,
  CASE WHEN op.reordered IN ('0', '1')
       THEN TRY_CAST(op.reordered AS INT) END         AS reordered,
  CASE
    WHEN TRY_CAST(op.add_to_cart_order AS INT) IS NULL OR TRY_CAST(op.add_to_cart_order AS INT) <= 0
      THEN 'invalid_cart_order'
    WHEN op.reordered IS NOT NULL AND op.reordered NOT IN ('0', '1')
      THEN 'invalid_reordered'
    ELSE NULL
  END AS line_item_quality_flag,
  op._ingested_at
FROM STREAM(week6.bronze_test.order_products_prior) op
LEFT SEMI JOIN week6.silver_test.orders_clean o
  ON o.order_id = TRY_CAST(op.order_id AS BIGINT)
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
COMMENT 'Cleaned order_products_train: composite key cast+validated+orphan-checked (drop on failure), add_to_cart_order/reordered typed/nulled-on-invalid, single quality-flag column. Duplicate (order_id, product_id) across batches is not expected -- caught by the Bronze audit log, not resolved here.'
AS
SELECT
  TRY_CAST(op.order_id AS BIGINT)   AS order_id,
  TRY_CAST(op.product_id AS BIGINT) AS product_id,
  CASE WHEN TRY_CAST(op.add_to_cart_order AS INT) > 0
       THEN TRY_CAST(op.add_to_cart_order AS INT) END AS add_to_cart_order,
  CASE WHEN op.reordered IN ('0', '1')
       THEN TRY_CAST(op.reordered AS INT) END         AS reordered,
  CASE
    WHEN TRY_CAST(op.add_to_cart_order AS INT) IS NULL OR TRY_CAST(op.add_to_cart_order AS INT) <= 0
      THEN 'invalid_cart_order'
    WHEN op.reordered IS NOT NULL AND op.reordered NOT IN ('0', '1')
      THEN 'invalid_reordered'
    ELSE NULL
  END AS line_item_quality_flag,
  op._ingested_at
FROM STREAM(week6.bronze_test.order_products_train) op
LEFT SEMI JOIN week6.silver_test.orders_clean o
  ON o.order_id = TRY_CAST(op.order_id AS BIGINT)
LEFT SEMI JOIN week6.silver_test.products_clean p
  ON p.product_id = TRY_CAST(op.product_id AS BIGINT);
