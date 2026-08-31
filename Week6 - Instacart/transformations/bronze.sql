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


