-- q4_peak_hours.sql
-- Identify peak hours of day for transactions

SELECT
  EXTRACT(HOUR FROM DateKey) AS hour_of_day,
  SUM(TotalPrice) AS revenue,
  COUNT(*) AS transactions
FROM FactSales
GROUP BY hour_of_day
ORDER BY revenue DESC
LIMIT 24;
