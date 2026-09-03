-- =============================================================================
-- INSTACART DASHBOARD - DATASET QUERIES  (Databricks AI/BI dashboard)
-- File: Week6 - Instacart/dashboards/dashboard_queries.sql
-- Owner: Kinah
-- =============================================================================
-- These are NOT pipeline code. Each block below is one dashboard dataset.
-- They are the exact queries stored inside instacart_dashboard.lvdash.json,
-- kept here in plain SQL so the team can read them without opening the JSON.
--
-- Every dataset reads a week6.gold_test.vw_* view only. No joins here.
-- If a number looks wrong, fix the gold view, not the dashboard.
-- Swap gold_test for gold when the team runs the full load.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Dataset: KPI summary
-- -----------------------------------------------------------------------------
SELECT
  total_orders,
  total_order_items,
  distinct_products_ordered,
  distinct_users,
  overall_reorder_rate,
  avg_basket_size,
  pct_lines_with_quality_flag
FROM week6.gold_test.vw_kpi_summary;


-- -----------------------------------------------------------------------------
-- Dataset: Q1a Top products
-- -----------------------------------------------------------------------------
SELECT
  product_name,
  department_name,
  aisle_name,
  times_ordered,
  times_reordered,
  popularity_rank
FROM week6.gold_test.vw_product_popularity
ORDER BY times_ordered DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- Dataset: Q1b Department share
-- -----------------------------------------------------------------------------
SELECT
  department_name,
  times_ordered,
  orders_containing,
  pct_of_total_items
FROM week6.gold_test.vw_department_popularity
ORDER BY times_ordered DESC;


-- -----------------------------------------------------------------------------
-- Dataset: Q1c Top aisles
-- -----------------------------------------------------------------------------
SELECT
  department_name,
  aisle_name,
  times_ordered,
  pct_of_total_items
FROM week6.gold_test.vw_aisle_popularity
ORDER BY times_ordered DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- Dataset: Q2 Orders by day
-- -----------------------------------------------------------------------------
SELECT
  order_dow,
  order_day_name,
  is_weekend,
  SUM(order_count)   AS order_count,
  SUM(item_count)    AS item_count
FROM week6.gold_test.vw_orders_by_dow_hour
GROUP BY order_dow, order_day_name, is_weekend
ORDER BY order_dow;


-- -----------------------------------------------------------------------------
-- Dataset: Q2 Orders by hour
-- -----------------------------------------------------------------------------
SELECT
  order_hour_of_day,
  daypart,
  SUM(order_count)   AS order_count,
  SUM(item_count)    AS item_count
FROM week6.gold_test.vw_orders_by_dow_hour
GROUP BY order_hour_of_day, daypart
ORDER BY order_hour_of_day;


-- -----------------------------------------------------------------------------
-- Dataset: Q2 Day x hour
-- -----------------------------------------------------------------------------
SELECT
  order_dow,
  order_day_name,
  order_hour_of_day,
  order_count,
  item_count
FROM week6.gold_test.vw_orders_by_dow_hour
ORDER BY order_dow, order_hour_of_day;


-- -----------------------------------------------------------------------------
-- Dataset: Q3 Top reordered products
-- -----------------------------------------------------------------------------
SELECT
  product_name,
  department_name,
  total_orders,
  reorder_count,
  reorder_rate
FROM week6.gold_test.vw_product_reorder_rate
ORDER BY reorder_rate DESC, total_orders DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- Dataset: Q3b Department reorder rate
-- -----------------------------------------------------------------------------
SELECT
  department_name,
  total_orders,
  reorder_count,
  reorder_rate
FROM week6.gold_test.vw_department_reorder_rate
ORDER BY reorder_rate DESC;


-- -----------------------------------------------------------------------------
-- Dataset: Q4 Basket size by daypart
-- -----------------------------------------------------------------------------
SELECT
  order_dow,
  order_day_name,
  daypart,
  order_count,
  avg_basket_size
FROM week6.gold_test.vw_basket_size_by_daypart
ORDER BY order_dow,
  CASE daypart WHEN 'Morning' THEN 1 WHEN 'Afternoon' THEN 2
               WHEN 'Evening' THEN 3 WHEN 'Night' THEN 4 ELSE 5 END;


-- -----------------------------------------------------------------------------
-- Dataset: Gold validation
-- -----------------------------------------------------------------------------
SELECT
  check_name,
  expected_value,
  actual_value,
  status
FROM week6.gold_test.validation_gold;

