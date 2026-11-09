-- q5_country_profitability.sql
-- Country-level revenue and average order value

SELECT
  c.Country,
  SUM(f.TotalPrice) AS revenue,
  AVG(f.TotalPrice) AS avg_order_value,
  COUNT(DISTINCT f.InvoiceNo) AS orders
FROM FactSales f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
GROUP BY c.Country
ORDER BY revenue DESC;
