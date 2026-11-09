# Power BI Recreation Instructions

Use the Parquet/CSV files in `etl/output/` as data sources.

Steps:
1. Open Power BI Desktop. Get Data -> Folder (or Parquet/CSV individually). Point to `etl/output/`.
2. Load `online_retail_clean.parquet`, `orders_clean.parquet`, and `events_clean.parquet`.
3. Create relationships in the model view similar to `ssas/ssas_tabular_model_description.md`.
4. Create measures using the DAX provided in `ssas/measures.dax`.

Suggested visuals:
- KPI card: Total Sales, Avg Order Value, Returning Customer Rate.
- Line chart: Monthly Sales Trend (month on axis, Total Sales as value).
- Bar chart: Top 10 Products by Revenue.
- Map or stacked bar: Sales by Country.
- Donut: Customer segments.

Filters/slicers: Date range (OrderDate), Country, Channel, Product Category (if available).

Mock visuals: use images from `analysis/output/` as placeholders.
