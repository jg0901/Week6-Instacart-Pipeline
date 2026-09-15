-- =============================================================================
-- SILVER CANONICALIZATION — insert-only dedup for orders / order_products
-- =============================================================================
-- STATUS: NOT part of the current pipeline. Kept here
-- as a designed-and-written reference for later -- the plan is to wire this
-- in when this pipeline gets revised into a portfolio project, not now.
-- Renamed from 06_silver_canonical_dedup.sql to future_silver_canonical_dedup.sql
-- so it doesn't collide with 06_gold_business_views.sql's numbering and so
-- it reads as clearly out-of-sequence with the active, numbered pipeline
-- files. Same "kept for later, not currently wired in" treatment as
-- 03b_silver_apply_changes_alternative.sql. For the homework version,
-- duplicate handling stops at detection-only: the Bronze audit log
--  plus its trace/drill-down queries.
-- orders_clean / order_products_prior_clean / order_products_train_clean
-- are the final Silver tables for the homework build -- there is no
-- *_canonical layer in play, and nothing downstream should read from one.
--

--
-- WHY THIS FILE EXISTS
-- orders_clean / order_products_prior_clean / order_products_train_clean
-- (03_silver_test_cleaning.sql) validate and warn, but never dedup a
-- repeated business key -- duplicates only get counted by the Bronze
-- audit log, not acted on. That's fine for catching a pipeline bug, but
-- it means a true duplicate delivery still flows through to those tables
-- unresolved and would get double-counted by anything Gold builds on
-- top. This file is the automated action that was missing: an
-- insert-only canonical layer that Gold should read from instead of the
-- *_clean tables directly.
--
-- WHY NOT AUTO CDC (APPLY CHANGES INTO)
-- Already decided and documented in 03b_silver_apply_changes_alternative.sql:
-- these rows are immutable events, not change events, so "latest wins"
-- upsert semantics don't have a business basis here. This file
-- deliberately does INSERT-only merges -- no WHEN MATCHED THEN UPDATE
-- anywhere below -- which matches that decision exactly.
--
-- WHY NOT MERGE DIRECTLY INTO orders_clean / order_products_*_clean
-- Checked against Databricks' own docs before building this: those three
-- tables are declared as STREAMING TABLE inside the DLT pipeline, and
-- Databricks explicitly rejects DML against a pipeline-managed streaming
-- table from outside the pipeline (STREAMING_TABLE_OPERATION_NOT_ALLOWED /
-- UNSUPPORTED_OPERATION -- "The operation is not supported on streaming
-- tables."). A MERGE INTO week6.silver_test.orders_clean run from a
-- separate job would simply fail. So canonicalization has to write to a
-- genuinely separate table that the pipeline doesn't own -- the
-- *_canonical tables created below are plain Delta tables, not DLT
-- objects, so ordinary MERGE/INSERT against them is fully supported.
--
-- HOW DUPLICATES ARE CLASSIFIED
-- Two rows sharing a business key are compared by a content fingerprint
-- (XXHASH64 over every non-key, non-metadata column -- 64-bit, chosen
-- over the 32-bit HASH() for lower collision risk at order_products_prior's
-- ~32M-row scale):
--   * same key, same fingerprint  -> EXACT duplicate: a redundant
--     re-delivery of the identical fact. The later arrival is suppressed
--     (not inserted); the first arrival is untouched.
--   * same key, different fingerprint -> CONFLICTING duplicate: two
--     different claims about the same immutable event. There's no
--     business basis to decide which is right, so NEITHER version is
--     inserted or updated -- the key is left out of canonical entirely
--     and logged for manual investigation. (If the key was already
--     canonical from an earlier run, that earlier version stays
--     canonical untouched -- this file never overwrites an existing
--     canonical row, conflicting or not.)
-- Every classification, in both directions (duplicates within one new
-- batch, and a new batch's row against what's already canonical), is
-- written to duplicate_key_log before the actual insert-only MERGE runs,
-- so the "why" is always recorded, not just the outcome.
--
-- WHY A WATERMARK INSTEAD OF RE-SCANNING *_clean EVERY RUN
-- *_clean is cumulative (a STREAMING TABLE keeps every row ever
-- validated), so comparing "all of *_clean" against canonical on every
-- run would mean re-doing the same comparison for old rows repeatedly --
-- exactly the full-table-recompute cost this design exists to avoid.
-- canonicalization_watermark tracks the highest _ingested_at already
-- considered (matched, inserted, or logged either way) per table, so
-- each run only looks at rows newer than that -- the same "only look at
-- what's new" idea as newly_processed in the Bronze audit log, just
-- applied one layer down.
--
-- WHAT GOLD SHOULD READ FROM
-- *_canonical, not *_clean. *_clean is the validated-but-not-yet-
-- deduped layer; *_canonical is the actual "one row per real event"
-- layer this whole file exists to produce.
-- =============================================================================

-- =============================================================================
-- ONE-TIME SETUP
-- =============================================================================
CREATE TABLE IF NOT EXISTS week6.silver_test.orders_canonical (
  order_id                BIGINT,
  user_id                  BIGINT,
  eval_set                 STRING,
  order_number             INT,
  order_dow                INT,
  order_hour_of_day        INT,
  days_since_prior_order   DOUBLE,
  order_quality_flags      ARRAY<STRING>,
  content_hash             BIGINT,    -- XXHASH64 fingerprint, kept so future runs can compare without re-deriving it
  _source_file             STRING,
  _ingested_at             TIMESTAMP,
  _canonicalized_at        TIMESTAMP  -- when this row was written here, not when it was ingested at Bronze
) USING DELTA;

CREATE TABLE IF NOT EXISTS week6.silver_test.order_products_prior_canonical (
  order_id                 BIGINT,
  product_id                BIGINT,
  add_to_cart_order          INT,
  reordered                  INT,
  line_item_quality_flags    ARRAY<STRING>,
  content_hash                BIGINT,
  _source_file                 STRING,
  _ingested_at                 TIMESTAMP,
  _canonicalized_at            TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS week6.silver_test.order_products_train_canonical (
  order_id                 BIGINT,
  product_id                BIGINT,
  add_to_cart_order          INT,
  reordered                  INT,
  line_item_quality_flags    ARRAY<STRING>,
  content_hash                BIGINT,
  _source_file                 STRING,
  _ingested_at                 TIMESTAMP,
  _canonicalized_at            TIMESTAMP
) USING DELTA;

-- One row per duplicate KEY occurrence detected (not one row per raw
-- input row) -- product_id is NULL for the single-key orders table, so
-- one schema covers both single- and composite-key cases rather than
-- forking into two audit schemas.
CREATE TABLE IF NOT EXISTS week6.silver_test.duplicate_key_log (
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

CREATE TABLE IF NOT EXISTS week6.silver_test.canonicalization_watermark (
  table_name                  STRING,
  last_processed_ingested_at   TIMESTAMP
) USING DELTA;


-- =============================================================================
--               Orders
-- =============================================================================
-- Run these three statements in order: (1) log every duplicate this
-- batch surfaces, (2) merge the single-agreed-version rows into
-- canonical, (3) advance the watermark. The logging query and the
-- MERGE's source subquery repeat the same CTEs rather than sharing a
-- temp view -- more text, but each statement stays independently
-- runnable/rerunnable, which matters more for a Job task than DRY-ness.
-- =============================================================================

-- (1) classify and log every duplicate this batch surfaces
INSERT INTO week6.silver_test.duplicate_key_log
WITH new_batch AS (
  SELECT *
  FROM week6.silver_test.orders_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver_test.canonicalization_watermark
    WHERE table_name = 'orders'
  )
),
hashed AS (
  SELECT
    *,
    XXHASH64(user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order) AS content_hash
  FROM new_batch
),
-- within-batch exact-duplicate collapse: same order_id AND same
-- content_hash -> one representative row (earliest by _ingested_at,
-- source file as final tiebreak), the rest logged as suppressed
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
-- one row per (order_id, distinct content_hash) after the intra-batch
-- exact collapse above -- the genuinely distinct versions this batch is
-- offering for each key
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
-- keys where the batch disagrees with itself: >= 2 distinct versions of
-- the same order_id within one batch. No basis to pick a winner -- every
-- version gets logged, none of them proceed to the MERGE below.
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
),
-- keys with exactly one agreed-upon version within this batch -- the
-- only rows eligible to be compared against what's already canonical
batch_single_version AS (
  SELECT bv.*
  FROM batch_versions bv
  JOIN batch_key_variety kv ON kv.order_id = bv.order_id AND kv.distinct_versions = 1
),
-- compare each single-version batch row against canonical, purely to
-- classify + log -- the MERGE below achieves "don't touch an existing
-- canonical row" on its own regardless; this exists so the reason gets
-- recorded either way.
cross_batch_log AS (
  SELECT
    CURRENT_TIMESTAMP()                                                                 AS run_ts,
    'orders'                                                                            AS table_name,
    bsv.order_id                                                                        AS order_id,
    CAST(NULL AS BIGINT)                                                                AS product_id,
    CASE WHEN c.content_hash = bsv.content_hash THEN 'EXACT' ELSE 'CONFLICTING' END     AS duplicate_type,
    c._source_file                                                                      AS first_source_file,
    bsv._source_file                                                                    AS duplicate_source_file,
    c._ingested_at                                                                      AS first_ingested_at,
    bsv._ingested_at                                                                    AS duplicate_ingested_at,
    CASE WHEN c.content_hash = bsv.content_hash
         THEN 'EXACT_DUPLICATE_SUPPRESSED'
         ELSE 'CONFLICTING_DUPLICATE_QUARANTINED' END                                  AS action_taken
  FROM batch_single_version bsv
  JOIN week6.silver_test.orders_canonical c ON c.order_id = bsv.order_id
)
SELECT * FROM batch_exact_dupes_log
UNION ALL
SELECT * FROM batch_conflicting_log
UNION ALL
SELECT * FROM cross_batch_log;

-- (2) insert-only merge: only keys that are brand new to canonical AND
-- had exactly one agreed version within this batch get inserted. No
-- WHEN MATCHED clause anywhere -- an existing canonical row is never
-- touched, exact or conflicting.
MERGE INTO week6.silver_test.orders_canonical AS target
USING (
  WITH new_batch AS (
    SELECT *
    FROM week6.silver_test.orders_clean
    WHERE _ingested_at > (
      SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
      FROM week6.silver_test.canonicalization_watermark
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
  SELECT bv.*
  FROM batch_versions bv
  JOIN batch_key_variety kv ON kv.order_id = bv.order_id AND kv.distinct_versions = 1
) AS source
ON target.order_id = source.order_id
WHEN NOT MATCHED THEN INSERT (
  order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day,
  days_since_prior_order, order_quality_flags, content_hash, _source_file,
  _ingested_at, _canonicalized_at
) VALUES (
  source.order_id, source.user_id, source.eval_set, source.order_number,
  source.order_dow, source.order_hour_of_day, source.days_since_prior_order,
  source.order_quality_flags, source.content_hash, source._source_file,
  source._ingested_at, CURRENT_TIMESTAMP()
);

-- (3) advance the watermark to the newest row this run actually
-- considered (whether it got inserted, suppressed, or quarantined)
MERGE INTO week6.silver_test.canonicalization_watermark AS w
USING (
  SELECT 'orders' AS table_name, MAX(_ingested_at) AS new_watermark
  FROM week6.silver_test.orders_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver_test.canonicalization_watermark WHERE table_name = 'orders'
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
-- product_id), content fingerprint over (add_to_cart_order, reordered)
-- instead of the orders columns.
-- =============================================================================

-- (1) classify and log
INSERT INTO week6.silver_test.duplicate_key_log
WITH new_batch AS (
  SELECT *
  FROM week6.silver_test.order_products_prior_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver_test.canonicalization_watermark
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
),
batch_single_version AS (
  SELECT bv.*
  FROM batch_versions bv
  JOIN batch_key_variety kv
    ON kv.order_id = bv.order_id AND kv.product_id = bv.product_id AND kv.distinct_versions = 1
),
cross_batch_log AS (
  SELECT
    CURRENT_TIMESTAMP()                                                                 AS run_ts,
    'order_products_prior'                                                              AS table_name,
    bsv.order_id                                                                        AS order_id,
    bsv.product_id                                                                      AS product_id,
    CASE WHEN c.content_hash = bsv.content_hash THEN 'EXACT' ELSE 'CONFLICTING' END     AS duplicate_type,
    c._source_file                                                                      AS first_source_file,
    bsv._source_file                                                                    AS duplicate_source_file,
    c._ingested_at                                                                      AS first_ingested_at,
    bsv._ingested_at                                                                    AS duplicate_ingested_at,
    CASE WHEN c.content_hash = bsv.content_hash
         THEN 'EXACT_DUPLICATE_SUPPRESSED'
         ELSE 'CONFLICTING_DUPLICATE_QUARANTINED' END                                  AS action_taken
  FROM batch_single_version bsv
  JOIN week6.silver_test.order_products_prior_canonical c
    ON c.order_id = bsv.order_id AND c.product_id = bsv.product_id
)
SELECT * FROM batch_exact_dupes_log
UNION ALL
SELECT * FROM batch_conflicting_log
UNION ALL
SELECT * FROM cross_batch_log;

-- (2) insert-only merge
MERGE INTO week6.silver_test.order_products_prior_canonical AS target
USING (
  WITH new_batch AS (
    SELECT *
    FROM week6.silver_test.order_products_prior_clean
    WHERE _ingested_at > (
      SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
      FROM week6.silver_test.canonicalization_watermark
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
  SELECT bv.*
  FROM batch_versions bv
  JOIN batch_key_variety kv ON kv.order_id = bv.order_id AND kv.product_id = bv.product_id AND kv.distinct_versions = 1
) AS source
ON target.order_id = source.order_id AND target.product_id = source.product_id
WHEN NOT MATCHED THEN INSERT (
  order_id, product_id, add_to_cart_order, reordered, line_item_quality_flags,
  content_hash, _source_file, _ingested_at, _canonicalized_at
) VALUES (
  source.order_id, source.product_id, source.add_to_cart_order, source.reordered,
  source.line_item_quality_flags, source.content_hash, source._source_file,
  source._ingested_at, CURRENT_TIMESTAMP()
);

-- (3) advance the watermark
MERGE INTO week6.silver_test.canonicalization_watermark AS w
USING (
  SELECT 'order_products_prior' AS table_name, MAX(_ingested_at) AS new_watermark
  FROM week6.silver_test.order_products_prior_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver_test.canonicalization_watermark WHERE table_name = 'order_products_prior'
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
INSERT INTO week6.silver_test.duplicate_key_log
WITH new_batch AS (
  SELECT *
  FROM week6.silver_test.order_products_train_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver_test.canonicalization_watermark
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
),
batch_single_version AS (
  SELECT bv.*
  FROM batch_versions bv
  JOIN batch_key_variety kv
    ON kv.order_id = bv.order_id AND kv.product_id = bv.product_id AND kv.distinct_versions = 1
),
cross_batch_log AS (
  SELECT
    CURRENT_TIMESTAMP()                                                                 AS run_ts,
    'order_products_train'                                                              AS table_name,
    bsv.order_id                                                                        AS order_id,
    bsv.product_id                                                                      AS product_id,
    CASE WHEN c.content_hash = bsv.content_hash THEN 'EXACT' ELSE 'CONFLICTING' END     AS duplicate_type,
    c._source_file                                                                      AS first_source_file,
    bsv._source_file                                                                    AS duplicate_source_file,
    c._ingested_at                                                                      AS first_ingested_at,
    bsv._ingested_at                                                                    AS duplicate_ingested_at,
    CASE WHEN c.content_hash = bsv.content_hash
         THEN 'EXACT_DUPLICATE_SUPPRESSED'
         ELSE 'CONFLICTING_DUPLICATE_QUARANTINED' END                                  AS action_taken
  FROM batch_single_version bsv
  JOIN week6.silver_test.order_products_train_canonical c
    ON c.order_id = bsv.order_id AND c.product_id = bsv.product_id
)
SELECT * FROM batch_exact_dupes_log
UNION ALL
SELECT * FROM batch_conflicting_log
UNION ALL
SELECT * FROM cross_batch_log;

-- (2) insert-only merge
MERGE INTO week6.silver_test.order_products_train_canonical AS target
USING (
  WITH new_batch AS (
    SELECT *
    FROM week6.silver_test.order_products_train_clean
    WHERE _ingested_at > (
      SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
      FROM week6.silver_test.canonicalization_watermark
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
  SELECT bv.*
  FROM batch_versions bv
  JOIN batch_key_variety kv ON kv.order_id = bv.order_id AND kv.product_id = bv.product_id AND kv.distinct_versions = 1
) AS source
ON target.order_id = source.order_id AND target.product_id = source.product_id
WHEN NOT MATCHED THEN INSERT (
  order_id, product_id, add_to_cart_order, reordered, line_item_quality_flags,
  content_hash, _source_file, _ingested_at, _canonicalized_at
) VALUES (
  source.order_id, source.product_id, source.add_to_cart_order, source.reordered,
  source.line_item_quality_flags, source.content_hash, source._source_file,
  source._ingested_at, CURRENT_TIMESTAMP()
);

-- (3) advance the watermark
MERGE INTO week6.silver_test.canonicalization_watermark AS w
USING (
  SELECT 'order_products_train' AS table_name, MAX(_ingested_at) AS new_watermark
  FROM week6.silver_test.order_products_train_clean
  WHERE _ingested_at > (
    SELECT COALESCE(MAX(last_processed_ingested_at), TIMESTAMP('1900-01-01'))
    FROM week6.silver_test.canonicalization_watermark WHERE table_name = 'order_products_train'
  )
) AS new_wm
ON w.table_name = new_wm.table_name
WHEN MATCHED AND new_wm.new_watermark IS NOT NULL THEN
  UPDATE SET w.last_processed_ingested_at = new_wm.new_watermark
WHEN NOT MATCHED AND new_wm.new_watermark IS NOT NULL THEN
  INSERT (table_name, last_processed_ingested_at) VALUES (new_wm.table_name, new_wm.new_watermark);
