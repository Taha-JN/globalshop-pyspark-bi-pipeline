# Power BI Recreation Instructions

## Data Ingestion
Use the Star Schema Parquet tables in `etl/output/` as your primary data sources.

### Steps:
1. Open Power BI Desktop. Click **Get Data** -> **Folder** (or choose **Parquet** individually) pointing to `etl/output/`.
2. Load the Star Schema Lakehouse tables:
   - `fact_sales` (or `fact_sales.parquet`)
   - `fact_funnel.parquet`
   - `dim_product.parquet`
   - `dim_customer.parquet`
   - `dim_date.parquet`
   - `dim_region.parquet`
3. In Model View, configure star schema relationships:
   - `FactSales[ProductKey]` -> `DimProduct[ProductKey]` (Many to One)
   - `FactSales[CustomerKey]` -> `DimCustomer[CustomerKey]` (Many to One)
   - `FactSales[DateKey]` -> `DimDate[DateKey]` (Many to One)
   - `FactSales[RegionKey]` -> `DimRegion[RegionKey]` (Many to One)
   - `DimCustomer[RegionKey]` -> `DimRegion[RegionKey]` (Many to One)
   - `FactFunnel[ProductKey]` -> `DimProduct[ProductKey]` (Many to One)
4. Add the DAX measures defined in `ssas/measures.dax` to a dedicated `_Measures` table.

### Suggested Visuals:
- **Executive KPI Cards**: Total Sales, Avg Order Value, Funnel Conversion Rate, Returning Customer Rate.
- **Line Chart**: Monthly Sales Trend (axis: `DimDate[FullDate]` or `Month`, values: `[Total Sales]`).
- **Bar Chart**: Top 10 Products by Revenue (`DimProduct[Description]`, values: `[Total Sales]`).
- **Map / Regional Bar**: Sales by Country (`DimRegion[Country]`, values: `[Total Sales]`).
- **Donut Chart**: Customer Segmentation breakdown (`q3_customer_segmentation.csv`).
- **Funnel Chart**: Behavioral Funnel (`FactFunnel[Views]` -> `FactFunnel[AddToCart]` -> `FactFunnel[Transactions]`).

Visual placeholders and reference charts are saved in `analysis/output/`.
