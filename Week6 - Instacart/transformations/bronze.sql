-- =====================================================
--           Bronze Layer: Data Ingestion
-- =====================================================

-- =====================================================
--               Aisles
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze.aisles
TBLPROPERTIES (
  'delta.feature.timestampNtz' = 'supported',
  'delta.columnMapping.mode' = 'name')
AS
SELECT * FROM STREAM read_files('/Volumes/week6/bronze/raw_files/aisles/', 
  format => 'csv',
  dataAddress => 'aisles',
  headerRows => 1,
  inferColumnTypes => false,
  `cloudFiles.schemaEvolutionMode` => 'none');


-- =====================================================
--               Departments
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze.departments
TBLPROPERTIES (
  'delta.feature.timestampNtz' = 'supported',
  'delta.columnMapping.mode' = 'name')
AS
SELECT * FROM STREAM read_files('/Volumes/week6/bronze/raw_files/departments/', 
  format => 'csv',
  dataAddress => 'departments',
  headerRows => 1,
  inferColumnTypes => false,
  `cloudFiles.schemaEvolutionMode` => 'none');


-- =====================================================
--               Orders
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze.orders
TBLPROPERTIES (
  'delta.feature.timestampNtz' = 'supported',
  'delta.columnMapping.mode' = 'name')
AS
SELECT * FROM STREAM read_files('/Volumes/week6/bronze/raw_files/orders/', 
  format => 'csv',
  dataAddress => 'orders',
  headerRows => 1,
  inferColumnTypes => false,
  `cloudFiles.schemaEvolutionMode` => 'none');


-- =====================================================
--               Orders_Products_Prior
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze.order_products_prior
TBLPROPERTIES (
  'delta.feature.timestampNtz' = 'supported',
  'delta.columnMapping.mode' = 'name')
AS
SELECT * FROM STREAM read_files('/Volumes/week6/bronze/raw_files/order_products_prior/', 
  format => 'csv',
  dataAddress => 'order_products_prior',
  headerRows => 1,
  inferColumnTypes => false,
`cloudFiles.schemaEvolutionMode` => 'none');

-- =====================================================
--               Orders_Products_Train
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze.order_products_train
TBLPROPERTIES (
  'delta.feature.timestampNtz' = 'supported',
  'delta.columnMapping.mode' = 'name')
AS
SELECT * FROM STREAM read_files('/Volumes/week6/bronze/raw_files/order_products_train/', 
  format => 'csv',
  dataAddress => 'order_products_train',
  headerRows => 1,
  inferColumnTypes => false,
  `cloudFiles.schemaEvolutionMode` => 'none');


-- =====================================================
--               Products
-- =====================================================
CREATE OR REFRESH STREAMING TABLE week6.bronze.products
TBLPROPERTIES (
  'delta.feature.timestampNtz' = 'supported',
  'delta.columnMapping.mode' = 'name')
AS
SELECT * FROM STREAM read_files('/Volumes/week6/bronze/raw_files/products/', 
  format => 'csv',
  dataAddress => 'products',
  headerRows => 1,
  inferColumnTypes => false,
  `cloudFiles.schemaEvolutionMode` => 'none');
