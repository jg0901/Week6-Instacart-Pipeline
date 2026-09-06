# Instacart Pipeline — Project Documentation

---

## Objective

Build an end-to-end pipeline for the Instacart Market Basket Analysis dataset using the Bronze–Silver–Gold medallion architecture in Databricks.

All available source files were ingested in a single initial load after completing several test runs. Streaming tables were used so the same pipeline can be reused to process future batches incrementally without reloading previously processed files.
The solution uses Lakeflow Spark Declarative Pipelines to:

* Ingest the initial dataset and any future CSV batches
* Preserve raw source data
* Apply row-level and batch-level data-quality checks
* Separate rejected rows from clean rows or rows with warning-level issues
* Transform the data into a star schema
* Produce dashboard-ready business views

<!-- ─────────── New %md cell ─────────── -->

## 1. Data flow

The project has two related but separate paths:

1. A one-time schema-discovery process using the complete original dataset
2. The main incremental pipeline using test batches and Auto Loader

The discovery tables do not directly feed the production pipeline. Instead, profiling results from the complete files are used to define the appropriate schemas, constraints, and relationships for the incremental pipeline.

```text
Complete original CSV files
        │
        ▼
┌────────────────────┐
│ week6.raw          │  One-time full load
│                    │  Explicit schema and complete source data
└─────────┬──────────┘
          ▼
┌───────────────────────────────┐
│ Schema-discovery checks       │
│                               │
│ • Row counts                  │
│ • Data types                  │
│ • Rescued-data checks         │
│ • Key uniqueness              │
│ • Referential integrity       │
│ • eval_set distribution       │
└─────────┬─────────────────────┘
          │
          └──── informs the schemas and validation rules
                used by the incremental pipeline


Incremental CSV batches
        │
        ▼
┌────────────────────┐
│ week6.bronze_test  │  Auto Loader streaming ingestion
│                    │  No rows dropped
└─────────┬──────────┘
          ▼
┌────────────────────┐
│ week6.silver_test  │  Type conversion, validation,
│                    │  rejection, and warning flags
└─────────┬──────────┘
          ▼
┌────────────────────┐
│ week6.gold_test    │  Star schema
│                    │  Two dimensions and one fact
└─────────┬──────────┘
          ▼
   Business views
          │
          ▼
      Dashboard
```

### Layer responsibilities

| Layer               | Purpose                                                           | Main objects                                         |
| ------------------- | ----------------------------------------------------------------- | ---------------------------------------------------- |
| `week6.raw`         | One-time schema discovery using the complete source files         | 6 tables                                             |
| `week6.bronze_test` | Lossless and incremental ingestion through Auto Loader            | 6 streaming tables                                   |
| `week6.silver_test` | Type conversion, rejection rules, warning flags, and DQ summaries | 3 clean streaming tables, 3 clean materialized views, plus reject, warning, and gate tables |
| `week6.gold_test`   | Dimensional model and business-ready aggregations                 | 2 dimensions, 1 fact table, and business views       |

The large event tables (`orders`, `order_products_prior`, and `order_products_train`) are streaming tables by design (built to absorb future batches incrementally), even though this project's actual data arrived as a single load.

The smaller reference datasets (`aisles`, `departments`, and `products`) use materialized views in Silver because their transformations require grouping and ranking logic. Their sizes are small enough that either incremental maintenance or full recomputation is acceptable.

<!-- ─────────── New %md cell ─────────── -->

## 2. How to run the project

### Prerequisites

The following are required:

* A Databricks workspace with Unity Catalog enabled
* A SQL warehouse or cluster
* Lakeflow pipeline compute
* The six Instacart CSV files stored in a Unity Catalog volume
* Permission to create schemas and tables in the `week6` catalog
* A Databricks Job configured with the tasks listed below

### Run order

| Step | Task                          | Output                                                             | Dependency         |
| ---- | ----------------------------- | ------------------------------------------------------------------ | ------------------ |
| 0    | `01_raw_ingestion.sql`        | Creates the six `week6.raw` discovery tables                       | Original CSV files |
| 1    | `00a_ensure_audit_tables.sql` | Creates the two audit tables if they do not already exist          | None               |
| 2    | Lakeflow pipeline             | Builds Bronze, Silver, Gold, DQ support tables, and business views | Step 1             |
| 3    | `02_ingestion_audit_log.sql`  | Appends ingestion metrics and runs duplicate-key checks            | Step 2             |

Step 0 is a one-time discovery process and is not part of the recurring Job.

The recurring pipeline should be triggered through the Databricks Job rather than directly from the pipeline interface. The Job ensures that the required audit tables exist before the pipeline evaluates its drop-rate gates.

### Incremental execution

This pipeline is incremental and checkpointed; it is not designed as a full rebuild on every run.

When the Job is rerun:

* Previously processed files are not ingested again under the same path.
* New files are processed.
* A run with no new files is a safe no-op.
* The existing output tables remain available.

Concurrent updates should be avoided. A manually triggered pipeline update should not overlap with a Job-triggered update because both may attempt to modify the same pipeline-managed tables.

<!-- ─────────── New %md cell ─────────── -->

## 3. Modelling journey

### Business event

The central business event is:

> A particular product appears in a particular customer order.

This event comes from combining the `order_products_prior` and `order_products_train` datasets with the corresponding order information.

### Fact-table grain

The grain of `fact_order_items` is:

> One unique product appearing in one order.

The business key is:

```text
(order_id, product_id)
```

This is the finest level of detail supported by the source dataset. Every measure or indicator stored in the fact table must therefore be valid at this grain.

### Star schema

```text
                    dim_product
                         │
                         │ product_key
                         ▼
                  fact_order_items
                         ▲
                         │ order_key
                         │
                     dim_order
```

| Object             | Grain                              | Key and contents                                                                             |
| ------------------ | ---------------------------------- | -------------------------------------------------------------------------------------------- |
| `fact_order_items` | One product appearing in one order | `order_key`, `product_key`, cart position, reordered indicator, and quality flags            |
| `dim_product`      | One row per product                | `product_key`, product name, aisle, department, and quality indicators                       |
| `dim_order`        | One row per order                  | `order_key`, user, order sequence, weekday, hour, daypart, and days since the previous order |

`fact_order_items` does not contain a traditional monetary measure because the source has no price or revenue data. Instead, each row represents an implicit item count of one:

```text
item_count = 1
```

Therefore:

```text
COUNT(*)                     = number of product appearances
COUNT(DISTINCT order_key)    = number of orders
COUNT(DISTINCT product_key)  = number of distinct products
SUM(reordered)               = number of reordered product appearances
```

### Dimensions intentionally omitted

A conventional `dim_date` is not created because the dataset contains no calendar date. It only provides:

```text
order_dow
order_hour_of_day
days_since_prior_order
```

A separate `dim_customer` is also omitted because the dataset contains no descriptive customer attributes beyond `user_id`. Creating a customer dimension containing only an identifier would add another join without adding analytical context.

The natural `user_id` is therefore retained directly in the analytical model.

### Design decisions

#### Test orders are excluded from product-level analysis

Orders with:

```text
eval_set = 'test'
```

are excluded intentionally rather than treated as data-quality failures.

The source dataset does not provide product-line records for test orders because they form the competition holdout set. As a result, these orders cannot contribute to product popularity, reorder, department, or basket-size calculations.

#### Prior and train order items are combined

`fact_order_items` is built using:

```text
order_products_prior
UNION ALL
order_products_train
```

Both datasets represent the same business event and have the same grain. Combining them preserves the available order-product history.

`eval_set` is retained so downstream analysis can still distinguish between prior and train records.

#### Invalid classification keys do not remove valid products

An invalid `aisle_id` or `department_id` does not invalidate the product itself.

In Silver, unusable classification values are converted to null and flagged. In Gold, unmatched dimension values fall back to:

```text
Unknown Aisle
Unknown Department
```

This allows valid product purchases to remain in the fact table even when product classification is incomplete.

#### Weekday labels are an explicit assumption

The mapping:

```text
0 = Sunday
1 = Monday
...
6 = Saturday
```

is treated as a documented labeling assumption.

Even if the starting weekday were different, the ordinal patterns in the data would remain valid. Only the human-readable weekday labels would change.

### Deliberate non-decisions

The following alternatives were considered but not implemented in the active pipeline.

| Decision not implemented                                                                    | Reason                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Resolve duplicate orders or order-product keys inside the streaming transformation          | These records represent immutable events. A repeated business key is treated as an ingestion error rather than an expected update.                                                                    |
| Use Auto CDC or `APPLY CHANGES INTO`                                                        | The source does not contain legitimate insert, update, and delete events or a business sequencing field that defines a latest version.                                                                |
| Modify pipeline-managed streaming tables through an external `MERGE`, `UPDATE`, or `DELETE` | Pipeline-managed tables should not be modified outside their owning pipeline. A canonical insert-only target would need to be implemented as a separate downstream process.                           |
| Automatically reevaluate an order-product row after a missing parent order arrives          | A stream-static join does not retroactively reprocess previously consumed rows. Supporting late parent records would require reprocessing, a full refresh, or a different architecture.               |
| Cast `days_since_prior_order` to `INT`                                                      | Non-null values are represented as decimal-formatted strings in the generated CSV files. Casting directly to `INT` would fail for values such as `7.0`; `DOUBLE` preserves the source representation. |
| Hardcode a correction for `product_id = 6816`                                               | A record-specific correction would not generalize. The row is retained with its parsing and classification issues flagged instead.                                                                    |

The guiding principle is:

> Transform a value only when there is a documented reason, but continue measuring data quality even when no correction is applied.

<!-- ─────────── New %md cell ─────────── -->

## 4. Data-quality framework

### Principle: gate on structural usability and measure everything else

The pipeline uses two complementary mechanisms.

| Mechanism         | Coverage                                                  | Output                    | Purpose                                                          |
| ----------------- | --------------------------------------------------------- | ------------------------- | ---------------------------------------------------------------- |
| Validation gates  | Grain-defining IDs and confirmed mandatory relationships  | Keep or reject the row    | Prevent structurally unusable records from entering clean Silver |
| Quality profiling | Descriptive, categorical, temporal, and analytical fields | Flags and summary metrics | Measure degradation without unnecessarily removing useful events |

### REJECT versus WARN

A row is rejected when its business identity cannot be established.

Examples include:

```text
missing order_id
nonnumeric product_id
zero or negative primary key
missing component of an order-product business key
confirmed missing mandatory parent record
```

Rejected rows do not enter the corresponding clean Silver table, but they remain available in Bronze and in the Silver reject datasets.

A warning is used when the business event still exists but one of its attributes is unavailable or unreliable.

Examples include:

```text
missing product name
invalid weekday
invalid order hour
invalid cart position
invalid reordered indicator
inconsistent days_since_prior_order
```

The invalid attribute is converted to null when appropriate, while the rest of the row remains available for analyses that do not depend on that field.

### Flag rather than silently clean

Every accepted row can contain multiple warning conditions. These are stored in an array such as:

```text
order_quality_flags
product_quality_flags
line_item_quality_flags
```

This allows one row to report all applicable problems rather than only the first condition encountered.

Two types of supporting outputs make the decisions observable:

* Rejected rows are written to source-specific `*_rejects` streaming tables and summarized by `dq_rejects_summary`.
* Accepted rows with warning flags are summarized by `dq_warnings_summary` and `dq_warnings_coverage`.

The Silver DQ implementation is contained in:

```text
03c_silver_dq_rejects.sql
03d_silver_dq_warnings_summary.sql
```

### Drop-rate gates

The project uses a 10% rejection threshold as a batch-level safety rule.

For the incremental event tables:

```text
orders
order_products_prior
order_products_train
```

the rejection rate is calculated per incoming batch. A cumulative lifetime rate could hide a badly corrupted new batch inside millions of valid historical records.

For the current project, the small reference datasets are loaded once in full:

```text
aisles
departments
products
```

Their current-state rejection rate is therefore equivalent to their initial batch-level rate.

If these reference datasets later become incremental, their rejection rates should also be calculated per incoming file or batch rather than only against cumulative history.

The gate definitions are implemented in:

```text
04_silver_dq_gate.sql
04b_silver_batch_dq_gate.sql
```

<!-- ─────────── New %md cell ─────────── -->

## 5. Challenges and lessons learned

### 5.1 Initial stream-static join returned no rows

#### Observation

During the first pipeline run, both:

```text
order_products_prior_clean
order_products_train_clean
```

returned zero rows. All product-line records appeared to fail the product-reference check, even though Bronze completed successfully and `products_clean` contained approximately 50,000 products.

#### Investigation

The following checks were performed:

| Check                      | Result                                         |
| -------------------------- | ---------------------------------------------- |
| Bronze pipeline status     | Completed without ingestion errors             |
| `products_clean` row count | Approximately 50,000 rows                      |
| `product_id` data type     | `BIGINT` on both sides                         |
| Sample product IDs         | Matching values were present                   |
| Equivalent standalone join | Returned the expected 32,434,489 matching rows |

The standalone join demonstrated that the SQL condition, data types, and source values were correct.

#### Resolution

A subsequent clean pipeline run completed successfully without changing the join logic.

The evidence suggests that the issue was related to the initial pipeline state or dependency timing rather than the join condition itself. However, the exact internal cause was not independently confirmed.

#### Lesson

A zero-row result during an initial pipeline run does not automatically prove that the join logic is wrong.

Before rewriting the transformation:

1. Verify source and target row counts.
2. Compare join-key types.
3. Inspect sample matching values.
4. Run the equivalent join independently.
5. Review pipeline state and update history.

### 5.2 Missing internal staging table

#### Observation

`orders_clean` failed with an error similar to:

```text
STREAM_FAILED
TABLE_DOES_NOT_EXIST:
Staging Table '<uuid>' does not exist
```

The UUID referred to an internal Databricks staging object rather than a user-created table.

#### Investigation and resolution

The pipeline update history was checked to confirm that no earlier run was still active. The pipeline was then retried without changing the SQL, and the next run succeeded.

The failure was consistent with a transient internal-state or overlapping-update issue. Concurrent updates were considered a possible contributor, although the exact cause was not conclusively proven.

#### Lesson

When an error references an internal UUID:

* Check whether another Job or manual update is still running.
* Review retry settings and update history.
* Avoid starting the pipeline manually while the Job is active.
* Retry cleanly before changing compute size or rewriting transformations.

Large datasets may increase run time and therefore increase the opportunity for overlapping runs, but data volume alone does not prove that it caused the failure.

### 5.3 Auto Loader does not detect duplicate content

#### Observation

Uploading the same CSV content under a new filename caused the records to be ingested again.

#### Explanation

Auto Loader tracks discovered files using file identity and path information. It does not determine whether the contents of two differently named files are identical.

Therefore:

```text
batch_001.csv
batch_001_copy.csv
```

are treated as two input files even when their contents are byte-identical.

#### Current handling

Duplicate business keys are detected through:

```text
ingestion_audit_log.duplicate_key_rows
```

The following proposed designs also exist but are not yet connected to the active pipeline:

```text
future_silver_canonical_dedup.sql
future_silver_batch_dedup.sql
```

A future production implementation could introduce an automated pre-ingestion gate:

```text
incoming directory
→ file-hash and business-key validation
→ accepted files moved to the Auto Loader directory
→ rejected files retained separately
```

#### Lesson

File identity and business-event identity are different controls:

* Auto Loader prevents accidental reprocessing of the same discovered file path.
* Business-key validation detects repeated orders or order-product events.
* A file-hash check detects byte-identical files arriving under different names.

Solving one does not automatically solve the others.

<!-- ─────────── New %md cell ─────────── -->

## 6. Validation coverage

| Validation                         | Layer             | What it demonstrates                                                               |
| ---------------------------------- | ----------------- | ---------------------------------------------------------------------------------- |
| Source row counts                  | Raw and Bronze    | Expected files and records were loaded                                             |
| Rescued-data count                 | Raw and Bronze    | Unexpected schema or parsing problems remain visible                               |
| Key and composite-key uniqueness   | Raw discovery     | Original source keys satisfy the expected grain                                    |
| Referential-integrity checks       | Raw discovery     | Source foreign-key relationships are valid in the complete dataset                 |
| `eval_set` distribution            | Raw discovery     | Prior, train, and test populations match the source design                         |
| Warning-only Bronze expectations   | Bronze            | Invalid records remain observable without being removed                            |
| REJECT/WARN classification         | Silver            | Structurally unusable rows are separated from usable rows with degraded attributes |
| Order and product reference checks | Silver            | Order-product rows reference the expected order type and an existing product       |
| Source-specific reject tables      | Silver            | Every rejected row remains traceable to its rejection reason                       |
| `dq_rejects_summary`               | Silver            | Rejections are summarized by table and reason                                      |
| `dq_warnings_summary`              | Silver            | Each warning type is counted independently                                         |
| `dq_warnings_coverage`             | Silver            | Rows with at least one warning are counted once                                    |
| Current-state reference-data gate  | Silver            | The initial reference datasets remain within the 10% rejection threshold           |
| Per-batch event-data gate          | Silver            | A corrupted incremental batch cannot hide inside historical totals                 |
| Duplicate-key audit                | Bronze audit      | Repeated immutable-event keys are visible and traceable to their source files      |
| Standalone join reconciliation     | Silver validation | Join correctness was verified independently of pipeline execution state            |

### Interpretation notes

#### Orphan references during partial testing

An orphan reference does not always mean that the source data is invalid.

The test pipeline loads the complete `order_products_train` file against only a subset of `orders`. Therefore, an `orphan_order_reference` may mean:

```text
The referenced order is outside the current test subset.
```

It should not automatically be interpreted as:

```text
The referenced order does not exist in the complete source.
```

This is documented as a testing limitation. Confirmed orphan checks should be evaluated against aligned batches or the complete orders dataset.

#### Duplicate keys require a dedicated check

Null checks and row-level expectations cannot detect a repeated business key.

For example, two rows can both contain valid values while sharing the same:

```text
order_id
```

or:

```text
(order_id, product_id)
```

Both rows would pass ordinary row-level constraints.

Duplicate detection therefore requires a grouped comparison across records, which is why `duplicate_key_rows` is calculated separately in the ingestion audit.

The active pipeline currently detects and reports duplicate keys but does not resolve them automatically. Until an automated pre-ingestion or canonical-deduplication process is implemented, any detected duplicate must block Gold publication or be resolved before analytical outputs are considered valid.
