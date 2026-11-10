-- q4_peak_hours.sql
-- Identify peak hours of day for transactions using InvoiceTimestamp

SELECT
  EXTRACT(HOUR FROM InvoiceTimestamp) AS hour_of_day,
  SUM(TotalPrice) AS revenue,
  COUNT(DISTINCT InvoiceNo) AS transactions
FROM FactSales
GROUP BY hour_of_day
ORDER BY revenue DESC
LIMIT 24;
