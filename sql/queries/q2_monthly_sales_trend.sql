-- q2_monthly_sales_trend.sql
-- Monthly sales totals with YoY comparison

WITH monthly AS (
  SELECT
    DATE_TRUNC('month', DateKey) AS month,
    SUM(TotalPrice) AS monthly_revenue
  FROM FactSales
  GROUP BY 1
)
SELECT month, monthly_revenue
FROM monthly
ORDER BY month;
