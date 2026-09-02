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

--Volume, Uniqueness, and Reorder Ratio
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT order_id) AS unique_orders,
  COUNT(DISTINCT product_id) AS unique_products,
  ROUND(AVG(reordered), 4) AS avg_reorder_rate
FROM week6.bronze.order_products_prior; 



--Completeness (Null Checks)
SELECT
  COUNT_IF(order_id IS NULL) AS missing_order_id,
  COUNT_IF(product_id IS NULL) AS missing_product_id,
  COUNT_IF(add_to_cart_order IS NULL) AS missing_sequence,
  COUNT_IF(reordered IS NULL) AS missing_reordered
FROM week6.bronze.order_products_prior;


--validate reordered flag
SELECT 
   reordered, 
   COUNT(*) AS row_count 
FROM week6.bronze.order_products_prior 
GROUP BY reordered 
ORDER BY reordered; 


--valudate sequence boundaries
SELECT 
   MIN(add_to_cart_order) AS min_cart_order, 
   COUNT_IF(add_to_cart_order <= 0) AS total_invalid_sequences 
FROM week6.bronze.order_products_prior; 


--validate key constraints
SELECT 
   COUNT_IF(order_id <= 0) AS invalid_order_ids, 
   COUNT_IF(product_id <= 0) AS invalid_product_ids 
FROM week6.bronze.order_products_prior; 


--Distribution and Outlier Detection
---This surfaces extreme cart sizes (e.g., hundreds of items) which often indicate wholesale buyers or bot activity that can distort market basket algorithms.
SELECT 
  order_id, 
  MAX(add_to_cart_order) AS total_items_in_cart
FROM week6.bronze.order_products_prior
GROUP BY order_id
ORDER BY total_items_in_cart DESC
LIMIT 10;


-- =====================================================
--               Orders_Products_Train
-- =====================================================



-- =====================================================
--               Products
-- =====================================================

