-- =====================================================
--           Bronze Layer: Data Ingestion
-- =====================================================

-- =====================================================
--               Aisles
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.aisles (
  CONSTRAINT aisle_id_valid_number EXPECT (
    TRY_CAST(aisle_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(aisle_id AS BIGINT) > 0),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart aisles'
AS SELECT  *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze_test/raw_files/aisles/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data');

-- =====================================================
--               Departments
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.departments (
  CONSTRAINT department_id_valid_number EXPECT (
    TRY_CAST(department_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(department_id AS BIGINT) > 0),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL)
)
COMMENT 'Lossless raw ingestion of Instacart departments'
AS SELECT *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze_test/raw_files/departments/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data');


-- =====================================================
--               Orders
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.orders (
  CONSTRAINT order_id_valid_number EXPECT (
    TRY_CAST(order_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(order_id AS BIGINT) > 0),
  CONSTRAINT user_id_valid_number EXPECT (
    TRY_CAST(user_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(user_id AS BIGINT) > 0),
  CONSTRAINT order_number_valid_number EXPECT (
    TRY_CAST(order_number AS INT) IS NOT NULL
    AND TRY_CAST(order_number AS INT) > 0),
  CONSTRAINT dow_in_range EXPECT (
    TRY_CAST(order_dow AS INT) BETWEEN 0 AND 6),
  CONSTRAINT hour_in_range EXPECT (
    TRY_CAST(order_hour_of_day AS INT) BETWEEN 0 AND 23),
  CONSTRAINT days_prior_valid EXPECT (
    days_since_prior_order IS NULL
    OR TRY_CAST(days_since_prior_order AS DOUBLE) >= 0),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL )
)
COMMENT 'Lossless raw ingestion of Instacart orders'
AS SELECT *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze_test/raw_files/orders/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data');


-- =====================================================
--               Order_Products_Prior
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.order_products_prior (
  CONSTRAINT order_id_valid_number EXPECT (
    TRY_CAST(order_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(order_id AS BIGINT) > 0),
  CONSTRAINT product_id_valid_number EXPECT (
    TRY_CAST(product_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(product_id AS BIGINT) > 0),
  CONSTRAINT cart_order_valid EXPECT (
    add_to_cart_order IS NULL
    OR TRY_CAST(add_to_cart_order AS INT) > 0),
  CONSTRAINT reordered_valid EXPECT (
    reordered IS NULL OR reordered IN ('0', '1')),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart order_products (prior orders)'
AS SELECT *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze_test/raw_files/order_products_prior/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data');


-- =====================================================
--               Order_Products_Train
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.order_products_train (
  CONSTRAINT order_id_valid_number EXPECT (
    TRY_CAST(order_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(order_id AS BIGINT) > 0),
  CONSTRAINT product_id_valid_number EXPECT (
    TRY_CAST(product_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(product_id AS BIGINT) > 0),
  CONSTRAINT cart_order_valid EXPECT (
    add_to_cart_order IS NULL
    OR TRY_CAST(add_to_cart_order AS INT) > 0),
  CONSTRAINT reordered_valid EXPECT (
    reordered IS NULL OR reordered IN ('0', '1')),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart order_products (train orders)'
AS SELECT *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze_test/raw_files/order_products_train/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data');


-- =====================================================
--               Products
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.products (
  CONSTRAINT product_id_valid_number EXPECT (
    TRY_CAST(product_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(product_id AS BIGINT) > 0),
  CONSTRAINT aisle_id_valid_number EXPECT (
    TRY_CAST(aisle_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(aisle_id AS BIGINT) > 0),
  CONSTRAINT department_id_valid_number EXPECT (
    TRY_CAST(department_id AS BIGINT) IS NOT NULL
    AND TRY_CAST(department_id AS BIGINT) > 0),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL)
)
COMMENT 'Lossless raw ingestion of Instacart products'
AS SELECT *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze_test/raw_files/products/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data'
);