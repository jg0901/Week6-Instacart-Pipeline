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
--test


-- =====================================================
--               Orders
-- =====================================================
test


-- =====================================================
--               Orders_Products_Prior
-- =====================================================
shiena test

-- =====================================================
--               Orders_Products_Train
-- =====================================================



-- =====================================================
--               Products
-- =====================================================


--FOR ORDER PRODUCTS PRIOR TESTING PULL
%sql 
-- 1. Create the table with strict data types
CREATE TABLE IF NOT EXISTS week6.bronze.order_products_prior ( 
    order_id INT, 
    product_id INT, 
    add_to_cart_order INT, 
    reordered INT 
) USING DELTA;

-- 2. Incrementally load the data (handles both the initial 32M rows and future daily updates)
COPY INTO week6.bronze.order_products_prior
FROM '/Volumes/week6/bronze/raw_files/'
FILES = ('order_products__prior.csv')
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false') 
COPY_OPTIONS ('mergeSchema' = 'false', 'force' = 'false');


