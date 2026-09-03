# Dashboards (Kinah)

Gold layer AI/BI dashboard for the Instacart pipeline.

## Files

- `instacart_dashboard.lvdash.json` - the whole dashboard. 11 datasets, 21 widgets.
- `dashboard_queries.sql` - the same 11 dataset queries in plain SQL, so you can read
  them without opening the JSON.

## How to open it

1. In Databricks go to Dashboards.
2. Click the arrow next to Create dashboard, then Import dashboard from file.
3. Pick `instacart_dashboard.lvdash.json`.
4. Pick a SQL warehouse and hit Publish.

## What it reads

Everything reads `week6.gold_test.vw_*` views built by `transformations/gold.sql`.
No table joins live in the dashboard. If a number looks wrong, fix the gold view.

For the full load, swap `gold_test` for `gold` in both files.

## Sections

- Top row: 6 KPI tiles (orders, order lines, customers, products, reorder rate, basket size).
- Q1: which products and departments get bought the most.
- Q2: how buying changes by day of week and hour of day.
- Q3: which products get reordered the most.
- Q4: basket size by part of day.
- Last: the gold validation checks, so you can see PASS or FAIL on the page.

## Note on order_dow

Instacart never published what 0 means. This dashboard assumes 0 = Sunday.
If the team decides otherwise, change the CASE block in `gold.sql`, not the dashboard.
