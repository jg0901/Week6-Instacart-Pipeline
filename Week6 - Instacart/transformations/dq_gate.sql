-- =============================================================================
-- SILVER LAYER — AGGREGATE DROP-RATE GATE (fail the pipeline at >10% drop)
--Aisles, Department, Products
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.dq_gate (
  CONSTRAINT drop_rate_within_threshold
    EXPECT (drop_pct IS NOT NULL AND drop_pct <= 0.10)
    ON VIOLATION FAIL UPDATE
)
AS
SELECT
  'aisles' AS table_name,
  b.bronze_rows,
  s.silver_rows,
  ROUND(1 - (s.silver_rows / NULLIF(b.bronze_rows, 0)), 4) AS drop_pct
FROM (SELECT COUNT(*) AS bronze_rows FROM week6.bronze_test.aisles) b
CROSS JOIN (SELECT COUNT(*) AS silver_rows FROM week6.silver_test.aisles_clean) s
UNION ALL
SELECT
  'departments',
  b.bronze_rows,
  s.silver_rows,
  ROUND(1 - (s.silver_rows / NULLIF(b.bronze_rows, 0)), 4)
FROM (SELECT COUNT(*) AS bronze_rows FROM week6.bronze_test.departments) b
CROSS JOIN (SELECT COUNT(*) AS silver_rows FROM week6.silver_test.departments_clean) s
UNION ALL
SELECT
  'products',
  b.bronze_rows,
  s.silver_rows,
  ROUND(1 - (s.silver_rows / NULLIF(b.bronze_rows, 0)), 4)
FROM (SELECT COUNT(*) AS bronze_rows FROM week6.bronze_test.products) b
CROSS JOIN (SELECT COUNT(*) AS silver_rows FROM week6.silver_test.products_clean) s;

-- =============================================================================
-- SILVER LAYER — PER-BATCH DROP-RATE GATE (fail the pipeline if THIS batch drops > 10%)
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.batch_dq_gate (
  CONSTRAINT batch_drop_rate_within_threshold
    EXPECT (
      new_bronze_rows <= 0
      OR (batch_drop_pct IS NOT NULL AND batch_drop_pct <= 0.10)
    )
    --ON VIOLATION FAIL UPDATE
)
AS
WITH latest_baseline AS (
  SELECT table_name, bronze_rows_in_scope, silver_rows
  FROM (
    SELECT
      table_name,
      bronze_rows_in_scope,
      silver_rows,
      ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY run_ts DESC) AS rn
    FROM week6.silver_test.silver_ingestion_audit_log
  )
  WHERE rn = 1
),
live_counts AS (
  SELECT
    'orders' AS table_name,
    (SELECT COUNT(*) FROM week6.bronze_test.orders WHERE eval_set != 'test') AS bronze_rows_now,
    (SELECT COUNT(*) FROM week6.silver_test.orders_clean)                    AS silver_rows_now
  UNION ALL
  SELECT
    'order_products_prior',
    (SELECT COUNT(*) FROM week6.bronze_test.order_products_prior),
    (SELECT COUNT(*) FROM week6.silver_test.order_products_prior_clean)
 
  UNION ALL
 
  SELECT
    'order_products_train',
    (SELECT COUNT(*) FROM week6.bronze_test.order_products_train),
    (SELECT COUNT(*) FROM week6.silver_test.order_products_train_clean)
),
batches AS (
  SELECT
    lc.table_name,
    lc.bronze_rows_now,
    COALESCE(lb.bronze_rows_in_scope, 0)                       AS bronze_baseline,
    lc.bronze_rows_now - COALESCE(lb.bronze_rows_in_scope, 0)  AS new_bronze_rows,
    lc.silver_rows_now,
    COALESCE(lb.silver_rows, 0)                                AS silver_baseline,
    lc.silver_rows_now - COALESCE(lb.silver_rows, 0)           AS new_silver_rows
  FROM live_counts lc
  LEFT JOIN latest_baseline lb ON lb.table_name = lc.table_name
)
SELECT
  table_name,
  bronze_baseline,
  bronze_rows_now,
  new_bronze_rows,
  silver_baseline,
  silver_rows_now,
  new_silver_rows,
  ROUND(1 - (new_silver_rows / NULLIF(new_bronze_rows, 0)), 4) AS batch_drop_pct
FROM batches;
-- =============================================================================
-- SILVER LAYER — REJECT / QUARANTINE LOG
-- =============================================================================
-- ---------------------------------------------------------------------
--               Aisles
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE week6.silver_test.aisles_rejects
COMMENT 'Rows from bronze.aisles dropped by aisles_clean''s aisle_id_valid constraint (null, non-numeric, or non-positive aisle_id), with reason. Excludes exact-duplicate rows aisles_clean collapses via dedup -- no data is lost there, so it is not a rejection.'
AS
SELECT
  aisle_id                      AS raw_aisle_id,
  TRY_CAST(aisle_id AS BIGINT)  AS aisle_id,
  aisle,
  ARRAY('invalid_aisle_id')     AS rejection_reasons,
  _ingested_at,
  _source_file
FROM STREAM(week6.bronze_test.aisles)
WHERE TRY_CAST(aisle_id AS BIGINT) IS NULL OR TRY_CAST(aisle_id AS BIGINT) <= 0;
 
 
-- ---------------------------------------------------------------------
--               Departments
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE week6.silver_test.departments_rejects
COMMENT 'Rows from bronze.departments dropped by departments_clean''s department_id_valid constraint (null, non-numeric, or non-positive department_id), with reason. Excludes exact-duplicate rows departments_clean collapses via dedup -- no data is lost there, so it is not a rejection.'
AS
SELECT
  department_id                      AS raw_department_id,
  TRY_CAST(department_id AS BIGINT)  AS department_id,
  department,
  ARRAY('invalid_department_id')     AS rejection_reasons,
  _ingested_at,
  _source_file
FROM STREAM(week6.bronze_test.departments)
WHERE TRY_CAST(department_id AS BIGINT) IS NULL OR TRY_CAST(department_id AS BIGINT) <= 0;
 
 
-- ---------------------------------------------------------------------
--               Products
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE week6.silver_test.products_rejects
COMMENT 'Rows from bronze.products dropped by products_clean''s product_id_valid constraint (null, non-numeric, or non-positive product_id), with reason. aisle_id/department_id issues are WARN-bucket in products_clean (nulled, not dropped) and intentionally do not appear here.'
AS
SELECT
  product_id                      AS raw_product_id,
  TRY_CAST(product_id AS BIGINT)  AS product_id,
  product_name,
  ARRAY('invalid_product_id')     AS rejection_reasons,
  _ingested_at,
  _source_file
FROM STREAM(week6.bronze_test.products)
WHERE TRY_CAST(product_id AS BIGINT) IS NULL OR TRY_CAST(product_id AS BIGINT) <= 0;
 
 
-- ---------------------------------------------------------------------
--               Orders
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE week6.silver_test.orders_rejects
COMMENT 'Rows from bronze.orders excluded from orders_clean: invalid/missing order_id (order_id_valid constraint) tagged invalid_order_id, or eval_set=test (intentional business-scope exclusion, not a quality defect) tagged excluded_scope_test_eval_set -- kept as separate reasons so a scope exclusion is never mistaken for a genuine reject.'
AS
SELECT
  order_id                      AS raw_order_id,
  TRY_CAST(order_id AS BIGINT)  AS order_id,
  eval_set,
  FILTER(
    ARRAY(
      CASE WHEN TRY_CAST(order_id AS BIGINT) IS NULL OR TRY_CAST(order_id AS BIGINT) <= 0
        THEN 'invalid_order_id' END,
      CASE WHEN eval_set = 'test'
        THEN 'excluded_scope_test_eval_set' END
    ),
    x -> x IS NOT NULL
  ) AS rejection_reasons,
  _ingested_at,
  _source_file
FROM STREAM(week6.bronze_test.orders)
WHERE TRY_CAST(order_id AS BIGINT) IS NULL OR TRY_CAST(order_id AS BIGINT) <= 0
   OR eval_set = 'test';
 

-- ---------------------------------------------------------------------
--               Order_Products_Prior
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE week6.silver_test.order_products_prior_rejects
COMMENT 'Rows from bronze.order_products_prior excluded from order_products_prior_clean, with reason(s): invalid_key (composite PK), orphan_order_reference (order_id matches no order in orders_clean at all -- see PENDING vs REJECTED caveat in 03_silver_test_cleaning.sql), order_eval_set_mismatch (order_id exists but under a different eval_set -- a structural inconsistency, not a pending-load situation), orphan_product_reference.'
AS
SELECT
  op.order_id                       AS raw_order_id,
  op.product_id                     AS raw_product_id,
  TRY_CAST(op.order_id AS BIGINT)   AS order_id,
  TRY_CAST(op.product_id AS BIGINT) AS product_id,
  FILTER(
    ARRAY(
      CASE WHEN TRY_CAST(op.order_id AS BIGINT) IS NULL OR TRY_CAST(op.order_id AS BIGINT) <= 0
            OR TRY_CAST(op.product_id AS BIGINT) IS NULL OR TRY_CAST(op.product_id AS BIGINT) <= 0
        THEN 'invalid_key' END,
      CASE WHEN TRY_CAST(op.order_id AS BIGINT) > 0 AND o.order_id IS NULL AND o_any.order_id IS NULL
        THEN 'orphan_order_reference' END,
      CASE WHEN TRY_CAST(op.order_id AS BIGINT) > 0 AND o.order_id IS NULL AND o_any.order_id IS NOT NULL
        THEN 'order_eval_set_mismatch' END,
      CASE WHEN TRY_CAST(op.product_id AS BIGINT) > 0 AND p.product_id IS NULL
        THEN 'orphan_product_reference' END
    ),
    x -> x IS NOT NULL
  ) AS rejection_reasons,
  op._ingested_at,
  op._source_file
FROM STREAM(week6.bronze_test.order_products_prior) op
LEFT JOIN week6.silver_test.orders_clean o
  ON o.order_id = TRY_CAST(op.order_id AS BIGINT) AND o.eval_set = 'prior'
LEFT JOIN week6.silver_test.orders_clean o_any
  ON o_any.order_id = TRY_CAST(op.order_id AS BIGINT)
LEFT JOIN week6.silver_test.products_clean p
  ON p.product_id = TRY_CAST(op.product_id AS BIGINT)
WHERE
  TRY_CAST(op.order_id AS BIGINT) IS NULL OR TRY_CAST(op.order_id AS BIGINT) <= 0
  OR TRY_CAST(op.product_id AS BIGINT) IS NULL OR TRY_CAST(op.product_id AS BIGINT) <= 0
  OR o.order_id IS NULL
  OR p.product_id IS NULL;
 
 
-- ---------------------------------------------------------------------
--               Order_Products_Train
-- ---------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE week6.silver_test.order_products_train_rejects
COMMENT 'Rows from bronze.order_products_train excluded from order_products_train_clean, with reason(s): invalid_key (composite PK), orphan_order_reference (order_id matches no order in orders_clean at all -- see PENDING vs REJECTED caveat in 03_silver_test_cleaning.sql), order_eval_set_mismatch (order_id exists but under a different eval_set -- a structural inconsistency, not a pending-load situation), orphan_product_reference.'
AS
SELECT
  op.order_id                       AS raw_order_id,
  op.product_id                     AS raw_product_id,
  TRY_CAST(op.order_id AS BIGINT)   AS order_id,
  TRY_CAST(op.product_id AS BIGINT) AS product_id,
  FILTER(
    ARRAY(
      CASE WHEN TRY_CAST(op.order_id AS BIGINT) IS NULL OR TRY_CAST(op.order_id AS BIGINT) <= 0
            OR TRY_CAST(op.product_id AS BIGINT) IS NULL OR TRY_CAST(op.product_id AS BIGINT) <= 0
        THEN 'invalid_key' END,
      CASE WHEN TRY_CAST(op.order_id AS BIGINT) > 0 AND o.order_id IS NULL AND o_any.order_id IS NULL
        THEN 'orphan_order_reference' END,
      CASE WHEN TRY_CAST(op.order_id AS BIGINT) > 0 AND o.order_id IS NULL AND o_any.order_id IS NOT NULL
        THEN 'order_eval_set_mismatch' END,
      CASE WHEN TRY_CAST(op.product_id AS BIGINT) > 0 AND p.product_id IS NULL
        THEN 'orphan_product_reference' END
    ),
    x -> x IS NOT NULL
  ) AS rejection_reasons,
  op._ingested_at,
  op._source_file
FROM STREAM(week6.bronze_test.order_products_train) op
LEFT JOIN week6.silver_test.orders_clean o
  ON o.order_id = TRY_CAST(op.order_id AS BIGINT) AND o.eval_set = 'train'
LEFT JOIN week6.silver_test.orders_clean o_any
  ON o_any.order_id = TRY_CAST(op.order_id AS BIGINT)
LEFT JOIN week6.silver_test.products_clean p
  ON p.product_id = TRY_CAST(op.product_id AS BIGINT)
WHERE
  TRY_CAST(op.order_id AS BIGINT) IS NULL OR TRY_CAST(op.order_id AS BIGINT) <= 0
  OR TRY_CAST(op.product_id AS BIGINT) IS NULL OR TRY_CAST(op.product_id AS BIGINT) <= 0
  OR o.order_id IS NULL
  OR p.product_id IS NULL;
 
 
-- =============================================================================
-- SUMMARY VIEW — reject counts by table and reason
-- =============================================================================
-- One place to see all six *_rejects tables at a glance instead of querying
-- each individually. EXPLODE fans rejection_reasons out to one row per
-- (row, reason), so a row with two reasons counts once toward each --
-- reject_count is "how many rows had this reason," not "how many rows total."
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW week6.silver_test.dq_rejects_summary AS
SELECT 'aisles' AS table_name, reason, COUNT(*) AS reject_count
FROM week6.silver_test.aisles_rejects LATERAL VIEW EXPLODE(rejection_reasons) t AS reason
GROUP BY reason
UNION ALL
SELECT 'departments', reason, COUNT(*)
FROM week6.silver_test.departments_rejects LATERAL VIEW EXPLODE(rejection_reasons) t AS reason
GROUP BY reason
UNION ALL
SELECT 'products', reason, COUNT(*)
FROM week6.silver_test.products_rejects LATERAL VIEW EXPLODE(rejection_reasons) t AS reason
GROUP BY reason
UNION ALL
SELECT 'orders', reason, COUNT(*)
FROM week6.silver_test.orders_rejects LATERAL VIEW EXPLODE(rejection_reasons) t AS reason
GROUP BY reason
UNION ALL
SELECT 'order_products_prior', reason, COUNT(*)
FROM week6.silver_test.order_products_prior_rejects LATERAL VIEW EXPLODE(rejection_reasons) t AS reason
GROUP BY reason
UNION ALL
SELECT 'order_products_train', reason, COUNT(*)
FROM week6.silver_test.order_products_train_rejects LATERAL VIEW EXPLODE(rejection_reasons) t AS reason
GROUP BY reason
ORDER BY table_name, reject_count DESC;
 



