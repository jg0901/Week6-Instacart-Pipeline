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

--

-- =====================================================
--               Orders
-- =====================================================



-- =====================================================
--               Orders_Products_Prior
-- =====================================================

---ingestion
CREATE OR REFRESH STREAMING TABLE week6.bronze.order_products_prior
AS SELECT 
  CAST(order_id AS INT),
  CAST(product_id AS INT),
  CAST(add_to_cart_order AS INT),
  CAST(reordered AS INT)
FROM STREAM read_files(
  '/Volumes/week6/bronze/raw_files/order_products__prior.csv',
  format => 'csv',
  header => 'true'
);

--ingestion automating the daily refresh
CREATE OR REFRESH STREAMING TABLE week6.bronze.order_products_prior 
SCHEDULE CRON '0 0 2 * * ?' AT TIME ZONE 'UTC' -- Runs daily at 2:00 AM UTC 
AS SELECT 
   CAST(order_id AS INT), 
   CAST(product_id AS INT), 
   CAST(add_to_cart_order AS INT), 
   CAST(reordered AS INT) 
FROM STREAM read_files( 
   '/Volumes/week6/bronze/raw_files/order_products__prior.csv', 
   format => 'csv', 
   header => 'true' 
); 

-- =====================================================
--               Orders_Products_Train
-- =====================================================



-- =====================================================
--               Products
-- =====================================================

