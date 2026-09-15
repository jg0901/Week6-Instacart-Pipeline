-- =============================================================================
-- SILVER BATCH DEDUP — collapse duplicates WITHIN a batch only, never across
-- =============================================================================
-- STATUS: NOT part of the current pipeline. Same "designed, kept for later"
-- treatment as future_silver_canonical_dedup.sql. This is a SEPARATE,
-- independent design, not a replacement for it -- that file stays exactly
-- as it is. Do not wire both of these in against the same output table at
-- the same time; they answer different questions and are meant to be
-- alternatives to choose between, not layered together.
--
-- Targets week6.silver / week6.bronze (the current pipeline). If you want
-- this against week6.silver_test instead, to match
-- future_silver_canonical_dedup.sql's schema, swap every
-- week6.silver./week6.bronze. reference below.
--
-- WHAT THIS DOES DIFFERENTLY FROM future_silver_canonical_dedup.sql
-- That file compares every new batch against everything already written to
-- *_canonical -- a duplicate is caught no matter how many batches apart the
-- two copies landed. This file deliberately does NOT do that: it only
-- collapses duplicate keys that arrive together in the SAME batch (the same
-- run of this Job task). A duplicate spread across two different batches --
-- the exact scenario you get from re-uploading the same file under a new
-- name in a later run -- is NOT caught here on purpose. If that's the kind
-- of duplicate you're trying to stop, future_silver_canonical_dedup.sql is
-- the file that does it, not this one.
--
-- WHY THIS IS STILL POSSIBLE IN PLAIN SQL, NO WATERMARK/PYTHON NEEDED
-- orders_clean / order_products_prior_clean / order_products_train_clean
-- are read here as plain batch SELECTs (no STREAM()), against their
-- already-committed Delta contents -- not as a live stream. A batch read is
-- a fixed, bounded snapshot, so ROW_NUMBER()/CTEs work exactly like any
-- ordinary SQL query. The "can't rank/aggregate a stream" restriction only
-- applies to STREAM(...) sources inside a STREAMING TABLE declaration
-- itself, which is a completely different thing from reading that same
-- table's committed output afterward, from an external Job task.
--
-- WHY THIS STILL CAN'T WRITE BACK INTO *_clean DIRECTLY
-- Same platform rule as future_silver_canonical_dedup.sql: orders_clean /
-- order_products_*_clean are STREAMING TABLE objects, and Databricks
-- rejects any DML against a pipeline-managed streaming table from outside
-- the pipeline (STREAMING_TABLE_OPERATION_NOT_ALLOWED). That rule blocks
-- ANY external write, scoped to one batch or not -- narrowing the scope to
-- "just this batch" doesn't get around it. So this still has to write to a
-- separate, plain Delta table that Gold would read from instead of
-- *_clean, exactly like the canonical design.
--
-- HOW DUPLICATES ARE CLASSIFIED (identical reasoning to
-- future_silver_canonical_dedup.sql, just never compared against history)
-- Two rows sharing a business key, within the SAME batch, are compared by
-- content fingerprint (XXHASH64):
--   * same key, same fingerprint  -> EXACT duplicate: the later arrival
--     (by _ingested_at, then _source_file) is dropped, one copy proceeds.
--   * same key, different fingerprint -> CONFLICTING duplicate: the batch
--     disagrees with itself about this key. No basis to pick a winner, so
--     neither version is inserted -- logged for manual investigation.
-- A key that also happens to exist in an earlier batch's output is NOT
-- checked here at all -- this file has no memory of anything outside the
-- current batch, deliberately.
--
-- WHY A PLAIN INSERT, NOT A MERGE
-- future_silver_canonical_dedup.sql uses MERGE ... WHEN NOT MATCHED because
-- it's actively comparing against existing canonical rows. This file has
-- nothing to compare against by design, so a plain INSERT INTO ... SELECT
-- is the more honest expression of what it actually does. That means the
-- SAME watermark-retry caveat applies as everywhere else in this pipeline:
-- if a Job retry re-runs this file after a mid-script failure, it will
-- reprocess the same batch and insert a second copy of everything, since
-- there's no MERGE here to catch that. Set this Job task's retry count to
-- 0, same reasoning as 02_ingestion_audit_log.sql.
--
-- WHY A SEPARATE watermark AND log TABLE FROM THE CANONICAL FILE
-- Kept fully independent on purpose, so running one of these two designs
-- never affects the other's state, whichever one you end up actually
-- wiring in.
-- =============================================================================

-- =============================================================================
-- ONE-TIME SETUP
-- =============================================================================
CREATE TABLE IF NOT EXISTS week6.silver.orders_batch_deduped (
  order_id                BIGINT,
  user_id                  BIGINT,
  eval_set                 STRING,
  order_number             INT,
  order_dow                INT,
  order_hour_of_day        INT,
  days_since_prior_order   DOUBLE,
  order_quality_flags      ARRAY<STRING>,
  content_hash             BIGINT,
  _source_file             STRING,
  _ingested_at             TIMESTAMP,
  _deduped_at              TIMESTAMP  -- when this row was written here, not when it was ingested at Bronze
) USING DELTA;

CREATE TABLE IF NOT EXISTS week6.silver.order_products_prior_batch_deduped (
  order_id                 BIGINT,
  product_id                BIGINT,
  add_to_cart_order          INT,
  reordered                  INT,
  line_item_quality_flags    ARRAY<STRING>,
  content_hash                BIGINT,
  _source_file                 STRING,
  _ingested_at                 TIMESTAMP,
  _deduped_at                   TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS week6.silver.order_products_train_batch_deduped (
  order_id                 BIGINT,
  product_id                BIGINT,
  add_to_cart_order          INT,
  reordered                  INT,
  line_item_quality_flags    ARRAY<STRING>,
  content_hash                BIGINT,
  _source_file                 STRING,
  _ingested_at                 TIMESTAMP,
  _deduped_at                   TIMESTAMP
) USING DELTA;

-- One row per duplicate KEY occurrence found within a single batch --
-- product_id is NULL for the single-key orders table, same reasoning as
-- future_silver_canonical_dedup.sql's duplicate_key_log.
CREATE TABLE IF NOT EXISTS week6.silver.batch_duplicate_log (
  run_ts                   TIMESTAMP,
  table_name                STRING,
  order_id                   BIGINT,
  product_id                  BIGINT,   -- NULL for 'orders'
  duplicate_type                STRING, -- 'EXACT' or 'CONFLICTING'
  first_source_file               STRING,
  duplicate_source_file            STRING,
  first_ingested_at                  TIMESTAMP,
  duplicate_ingested_at               TIMESTAMP,
  action_taken                          STRING  -- 'EXACT_DUPLICATE_SUPPRESSED' or 'CONFLICTING_DUPLICATE_QUARANTINED'
) USING DELTA;

CREATE TABLE IF NOT EXISTS week6.silver.batch_dedup_watermark (
  table_name                  STRING,
  last_processed_ingested_at   TIMESTAMP
) USING DELTA;


-- =============================================================================
--               Orders
-- =============================================================================
-- Run these three statements in order: (1) log every within-batch duplicate,
-- (2) insert the single-agreed-version rows from this batch, (3) advance
-- the watermark. No comparison against anything from a prior run, anywhere
-- in this section.
-- =============================================================================

-- (1) classify and log duplicates found within this batch
INSERT INTO week6.silver.batch_duplicate_log
WITH new_batch AS (
  SELECT *
  FROM week6.silver.orders_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark
    WHERE table_name = 'orders'
  )
),
hashed AS (
  SELECT
    *,
    XXHASH64(user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order) AS content_hash
  FROM new_batch
),
batch_ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id, content_hash ORDER BY _ingested_at, _source_file) AS version_rn
  FROM hashed
),
batch_exact_dupes_log AS (
  SELECT
    CURRENT_TIMESTAMP()            AS run_ts,
    'orders'                       AS table_name,
    dup_row.order_id               AS order_id,
    CAST(NULL AS BIGINT)           AS product_id,
    'EXACT'                        AS duplicate_type,
    first_row._source_file         AS first_source_file,
    dup_row._source_file           AS duplicate_source_file,
    first_row._ingested_at         AS first_ingested_at,
    dup_row._ingested_at           AS duplicate_ingested_at,
    'EXACT_DUPLICATE_SUPPRESSED'   AS action_taken
  FROM batch_ranked dup_row
  JOIN batch_ranked first_row
    ON first_row.order_id = dup_row.order_id
    AND first_row.content_hash = dup_row.content_hash
    AND first_row.version_rn = 1
  WHERE dup_row.version_rn > 1
),
batch_versions AS (
  SELECT * EXCEPT (version_rn) FROM batch_ranked WHERE version_rn = 1
),
batch_key_variety AS (
  SELECT order_id, COUNT(*) AS distinct_versions
  FROM batch_versions
  GROUP BY order_id
),
batch_versions_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at, _source_file) AS overall_version_rn
  FROM batch_versions
),
batch_conflicting_log AS (
  SELECT
    CURRENT_TIMESTAMP()                    AS run_ts,
    'orders'                               AS table_name,
    v.order_id                             AS order_id,
    CAST(NULL AS BIGINT)                   AS product_id,
    'CONFLICTING'                          AS duplicate_type,
    f._source_file                         AS first_source_file,
    v._source_file                         AS duplicate_source_file,
    f._ingested_at                         AS first_ingested_at,
    v._ingested_at                         AS duplicate_ingested_at,
    'CONFLICTING_DUPLICATE_QUARANTINED'    AS action_taken
  FROM batch_versions_ranked v
  JOIN batch_versions_ranked f
    ON f.order_id = v.order_id AND f.overall_version_rn = 1
  JOIN batch_key_variety kv ON kv.order_id = v.order_id AND kv.distinct_versions > 1
  WHERE v.overall_version_rn > 1
)
SELECT * FROM batch_exact_dupes_log
UNION ALL
SELECT * FROM batch_conflicting_log;

-- (2) insert the rows this batch agreed on a single version for -- no
-- MERGE, no check against any previous batch's output
INSERT INTO week6.silver.orders_batch_deduped (
  order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day,
  days_since_prior_order, order_quality_flags, content_hash, _source_file,
  _ingested_at, _deduped_at
)
WITH new_batch AS (
  SELECT *
  FROM week6.silver.orders_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark
    WHERE table_name = 'orders'
  )
),
hashed AS (
  SELECT *, XXHASH64(user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order) AS content_hash
  FROM new_batch
),
batch_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, content_hash ORDER BY _ingested_at, _source_file) AS version_rn
  FROM hashed
),
batch_versions AS (
  SELECT * EXCEPT (version_rn) FROM batch_ranked WHERE version_rn = 1
),
batch_key_variety AS (
  SELECT order_id, COUNT(*) AS distinct_versions FROM batch_versions GROUP BY order_id
)
SELECT
  bv.order_id, bv.user_id, bv.eval_set, bv.order_number, bv.order_dow, bv.order_hour_of_day,
  bv.days_since_prior_order, bv.order_quality_flags, bv.content_hash, bv._source_file,
  bv._ingested_at, CURRENT_TIMESTAMP()
FROM batch_versions bv
JOIN batch_key_variety kv ON kv.order_id = bv.order_id AND kv.distinct_versions = 1;

-- (3) advance the watermark to the newest row this run considered
MERGE INTO week6.silver.batch_dedup_watermark AS w
USING (
  SELECT 'orders' AS table_name, MAX(_ingested_at) AS new_watermark
  FROM week6.silver.orders_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark WHERE table_name = 'orders'
  )
) AS new_wm
ON w.table_name = new_wm.table_name
WHEN MATCHED AND new_wm.new_watermark IS NOT NULL THEN
  UPDATE SET w.last_processed_ingested_at = new_wm.new_watermark
WHEN NOT MATCHED AND new_wm.new_watermark IS NOT NULL THEN
  INSERT (table_name, last_processed_ingested_at) VALUES (new_wm.table_name, new_wm.new_watermark);


-- =============================================================================
--               Order_Products_Prior
-- =============================================================================
-- Identical structure to Orders above, composite key (order_id,
-- product_id), content fingerprint over (add_to_cart_order, reordered).
-- =============================================================================

-- (1) classify and log
INSERT INTO week6.silver.batch_duplicate_log
WITH new_batch AS (
  SELECT *
  FROM week6.silver.order_products_prior_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark
    WHERE table_name = 'order_products_prior'
  )
),
hashed AS (
  SELECT *, XXHASH64(add_to_cart_order, reordered) AS content_hash
  FROM new_batch
),
batch_ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id, product_id, content_hash ORDER BY _ingested_at, _source_file) AS version_rn
  FROM hashed
),
batch_exact_dupes_log AS (
  SELECT
    CURRENT_TIMESTAMP()            AS run_ts,
    'order_products_prior'         AS table_name,
    dup_row.order_id               AS order_id,
    dup_row.product_id             AS product_id,
    'EXACT'                        AS duplicate_type,
    first_row._source_file         AS first_source_file,
    dup_row._source_file           AS duplicate_source_file,
    first_row._ingested_at         AS first_ingested_at,
    dup_row._ingested_at           AS duplicate_ingested_at,
    'EXACT_DUPLICATE_SUPPRESSED'   AS action_taken
  FROM batch_ranked dup_row
  JOIN batch_ranked first_row
    ON first_row.order_id = dup_row.order_id
    AND first_row.product_id = dup_row.product_id
    AND first_row.content_hash = dup_row.content_hash
    AND first_row.version_rn = 1
  WHERE dup_row.version_rn > 1
),
batch_versions AS (
  SELECT * EXCEPT (version_rn) FROM batch_ranked WHERE version_rn = 1
),
batch_key_variety AS (
  SELECT order_id, product_id, COUNT(*) AS distinct_versions
  FROM batch_versions
  GROUP BY order_id, product_id
),
batch_versions_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY _ingested_at, _source_file) AS overall_version_rn
  FROM batch_versions
),
batch_conflicting_log AS (
  SELECT
    CURRENT_TIMESTAMP()                    AS run_ts,
    'order_products_prior'                 AS table_name,
    v.order_id                             AS order_id,
    v.product_id                           AS product_id,
    'CONFLICTING'                          AS duplicate_type,
    f._source_file                         AS first_source_file,
    v._source_file                         AS duplicate_source_file,
    f._ingested_at                         AS first_ingested_at,
    v._ingested_at                         AS duplicate_ingested_at,
    'CONFLICTING_DUPLICATE_QUARANTINED'    AS action_taken
  FROM batch_versions_ranked v
  JOIN batch_versions_ranked f
    ON f.order_id = v.order_id AND f.product_id = v.product_id AND f.overall_version_rn = 1
  JOIN batch_key_variety kv ON kv.order_id = v.order_id AND kv.product_id = v.product_id AND kv.distinct_versions > 1
  WHERE v.overall_version_rn > 1
)
SELECT * FROM batch_exact_dupes_log
UNION ALL
SELECT * FROM batch_conflicting_log;

-- (2) insert this batch's single-agreed-version rows
INSERT INTO week6.silver.order_products_prior_batch_deduped (
  order_id, product_id, add_to_cart_order, reordered, line_item_quality_flags,
  content_hash, _source_file, _ingested_at, _deduped_at
)
WITH new_batch AS (
  SELECT *
  FROM week6.silver.order_products_prior_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark
    WHERE table_name = 'order_products_prior'
  )
),
hashed AS (
  SELECT *, XXHASH64(add_to_cart_order, reordered) AS content_hash
  FROM new_batch
),
batch_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id, content_hash ORDER BY _ingested_at, _source_file) AS version_rn
  FROM hashed
),
batch_versions AS (
  SELECT * EXCEPT (version_rn) FROM batch_ranked WHERE version_rn = 1
),
batch_key_variety AS (
  SELECT order_id, product_id, COUNT(*) AS distinct_versions FROM batch_versions GROUP BY order_id, product_id
)
SELECT
  bv.order_id, bv.product_id, bv.add_to_cart_order, bv.reordered, bv.line_item_quality_flags,
  bv.content_hash, bv._source_file, bv._ingested_at, CURRENT_TIMESTAMP()
FROM batch_versions bv
JOIN batch_key_variety kv ON kv.order_id = bv.order_id AND kv.product_id = bv.product_id AND kv.distinct_versions = 1;

-- (3) advance the watermark
MERGE INTO week6.silver.batch_dedup_watermark AS w
USING (
  SELECT 'order_products_prior' AS table_name, MAX(_ingested_at) AS new_watermark
  FROM week6.silver.order_products_prior_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark WHERE table_name = 'order_products_prior'
  )
) AS new_wm
ON w.table_name = new_wm.table_name
WHEN MATCHED AND new_wm.new_watermark IS NOT NULL THEN
  UPDATE SET w.last_processed_ingested_at = new_wm.new_watermark
WHEN NOT MATCHED AND new_wm.new_watermark IS NOT NULL THEN
  INSERT (table_name, last_processed_ingested_at) VALUES (new_wm.table_name, new_wm.new_watermark);


-- =============================================================================
--               Order_Products_Train
-- =============================================================================
-- Identical structure to Order_Products_Prior above, source/target/
-- watermark-key swapped from '..._prior...' to '..._train...'.
-- =============================================================================

-- (1) classify and log
INSERT INTO week6.silver.batch_duplicate_log
WITH new_batch AS (
  SELECT *
  FROM week6.silver.order_products_train_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark
    WHERE table_name = 'order_products_train'
  )
),
hashed AS (
  SELECT *, XXHASH64(add_to_cart_order, reordered) AS content_hash
  FROM new_batch
),
batch_ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id, product_id, content_hash ORDER BY _ingested_at, _source_file) AS version_rn
  FROM hashed
),
batch_exact_dupes_log AS (
  SELECT
    CURRENT_TIMESTAMP()            AS run_ts,
    'order_products_train'         AS table_name,
    dup_row.order_id               AS order_id,
    dup_row.product_id             AS product_id,
    'EXACT'                        AS duplicate_type,
    first_row._source_file         AS first_source_file,
    dup_row._source_file           AS duplicate_source_file,
    first_row._ingested_at         AS first_ingested_at,
    dup_row._ingested_at           AS duplicate_ingested_at,
    'EXACT_DUPLICATE_SUPPRESSED'   AS action_taken
  FROM batch_ranked dup_row
  JOIN batch_ranked first_row
    ON first_row.order_id = dup_row.order_id
    AND first_row.product_id = dup_row.product_id
    AND first_row.content_hash = dup_row.content_hash
    AND first_row.version_rn = 1
  WHERE dup_row.version_rn > 1
),
batch_versions AS (
  SELECT * EXCEPT (version_rn) FROM batch_ranked WHERE version_rn = 1
),
batch_key_variety AS (
  SELECT order_id, product_id, COUNT(*) AS distinct_versions
  FROM batch_versions
  GROUP BY order_id, product_id
),
batch_versions_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY _ingested_at, _source_file) AS overall_version_rn
  FROM batch_versions
),
batch_conflicting_log AS (
  SELECT
    CURRENT_TIMESTAMP()                    AS run_ts,
    'order_products_train'                 AS table_name,
    v.order_id                             AS order_id,
    v.product_id                           AS product_id,
    'CONFLICTING'                          AS duplicate_type,
    f._source_file                         AS first_source_file,
    v._source_file                         AS duplicate_source_file,
    f._ingested_at                         AS first_ingested_at,
    v._ingested_at                         AS duplicate_ingested_at,
    'CONFLICTING_DUPLICATE_QUARANTINED'    AS action_taken
  FROM batch_versions_ranked v
  JOIN batch_versions_ranked f
    ON f.order_id = v.order_id AND f.product_id = v.product_id AND f.overall_version_rn = 1
  JOIN batch_key_variety kv ON kv.order_id = v.order_id AND kv.product_id = v.product_id AND kv.distinct_versions > 1
  WHERE v.overall_version_rn > 1
)
SELECT * FROM batch_exact_dupes_log
UNION ALL
SELECT * FROM batch_conflicting_log;

-- (2) insert this batch's single-agreed-version rows
INSERT INTO week6.silver.order_products_train_batch_deduped (
  order_id, product_id, add_to_cart_order, reordered, line_item_quality_flags,
  content_hash, _source_file, _ingested_at, _deduped_at
)
WITH new_batch AS (
  SELECT *
  FROM week6.silver.order_products_train_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark
    WHERE table_name = 'order_products_train'
  )
),
hashed AS (
  SELECT *, XXHASH64(add_to_cart_order, reordered) AS content_hash
  FROM new_batch
),
batch_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id, content_hash ORDER BY _ingested_at, _source_file) AS version_rn
  FROM hashed
),
batch_versions AS (
  SELECT * EXCEPT (version_rn) FROM batch_ranked WHERE version_rn = 1
),
batch_key_variety AS (
  SELECT order_id, product_id, COUNT(*) AS distinct_versions FROM batch_versions GROUP BY order_id, product_id
)
SELECT
  bv.order_id, bv.product_id, bv.add_to_cart_order, bv.reordered, bv.line_item_quality_flags,
  bv.content_hash, bv._source_file, bv._ingested_at, CURRENT_TIMESTAMP()
FROM batch_versions bv
JOIN batch_key_variety kv ON kv.order_id = bv.order_id AND kv.product_id = bv.product_id AND kv.distinct_versions = 1;

-- (3) advance the watermark
MERGE INTO week6.silver.batch_dedup_watermark AS w
USING (
  SELECT 'order_products_train' AS table_name, MAX(_ingested_at) AS new_watermark
  FROM week6.silver.order_products_train_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver.batch_dedup_watermark WHERE table_name = 'order_products_train'
  )
) AS new_wm
ON w.table_name = new_wm.table_name
WHEN MATCHED AND new_wm.new_watermark IS NOT NULL THEN
  UPDATE SET w.last_processed_ingested_at = new_wm.new_watermark
WHEN NOT MATCHED AND new_wm.new_watermark IS NOT NULL THEN
  INSERT (table_name, last_processed_ingested_at) VALUES (new_wm.table_name, new_wm.new_watermark);
