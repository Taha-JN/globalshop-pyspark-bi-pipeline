-- q1_top_products_by_revenue.sql
-- Returns top 10 products by revenue

WITH product_revenue AS (
  SELECT
    p.StockCode,
    p.Description,
    SUM(f.TotalPrice) AS revenue,
    SUM(f.Quantity) AS total_qty
  FROM FactSales f
  JOIN DimProduct p ON f.ProductKey = p.ProductKey
  GROUP BY p.StockCode, p.Description
)
SELECT StockCode, Description, revenue, total_qty
FROM product_revenue
ORDER BY revenue DESC
LIMIT 10;
