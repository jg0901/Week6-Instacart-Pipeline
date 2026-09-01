-- =====================================================
--           Bronze Layer: Data Ingestion
-- =====================================================

-- =====================================================
--               Aisles
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.aisles (
  CONSTRAINT aisle_id_present EXPECT (
    aisle_id IS NOT NULL AND TRIM(aisle_id) <> ''  ),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart aisles'
AS
SELECT
  *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze/raw_files/aisles/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data'
);


-- =====================================================
--               Departments
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.departments (
  CONSTRAINT department_id_present EXPECT (
    department_id IS NOT NULL AND TRIM(department_id) <> ''  ),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart departments'
AS
SELECT
  *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze/raw_files/departments/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data'
);


-- =====================================================
--               Orders
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.orders (
  CONSTRAINT order_id_present EXPECT (
    order_id IS NOT NULL AND TRIM(order_id) <> ''  ),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart orders'
AS
SELECT
  *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze/raw_files/orders/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data'
);


-- =====================================================
--               Order_Products_Prior
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.order_products_prior (
  CONSTRAINT keys_present EXPECT (
    order_id IS NOT NULL AND TRIM(order_id) <> ''
    AND product_id IS NOT NULL AND TRIM(product_id) <> ''  ),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart order_products (prior orders)'
AS
SELECT
  *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze/raw_files/order_products_prior/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data'
);


-- =====================================================
--               Order_Products_Train
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.order_products_train (
  CONSTRAINT keys_present EXPECT (
    order_id IS NOT NULL AND TRIM(order_id) <> ''
    AND product_id IS NOT NULL AND TRIM(product_id) <> ''  ),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart order_products (train orders)'
AS
SELECT
  *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze/raw_files/order_products_train/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data'
);


-- =====================================================
--               Products
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze_test.products (
  CONSTRAINT product_id_present EXPECT (
    product_id IS NOT NULL AND TRIM(product_id) <> ''  ),
  CONSTRAINT row_parsed EXPECT (
    _rescued_data IS NULL  )
)
COMMENT 'Lossless raw ingestion of Instacart products'
AS SELECT  *,
  _metadata.file_path             AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_modified_at,
  CURRENT_TIMESTAMP()               AS _ingested_at
FROM STREAM read_files(
  '/Volumes/week6/bronze/raw_files/products/',
  format            => 'csv',
  header            => true,
  inferColumnTypes  => false,
  schemaEvolutionMode => 'rescue',
  rescuedDataColumn => '_rescued_data'
);

