-- q5_country_profitability.sql
-- Country-level revenue, accurate Average Order Value (AOV), and estimated gross profit
-- Star Schema Join: FactSales -> DimRegion via RegionKey

SELECT
  r.Country,
  SUM(f.TotalPrice) AS revenue,
  SUM(f.TotalPrice) / NULLIF(COUNT(DISTINCT f.InvoiceNo), 0) AS avg_order_value,
  COUNT(DISTINCT f.InvoiceNo) AS orders,
  -- Assuming 40% gross profit margin on retail merchandise
  ROUND(SUM(f.TotalPrice) * 0.40, 2) AS estimated_profit,
  40.0 AS profit_margin_pct
FROM FactSales f
JOIN DimRegion r ON f.RegionKey = r.RegionKey
GROUP BY r.Country
ORDER BY revenue DESC;
