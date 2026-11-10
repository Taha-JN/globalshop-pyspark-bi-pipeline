-- q2_monthly_sales_trend.sql
-- Monthly sales totals with Year-over-Year (YoY) comparison using LAG window function

WITH monthly AS (
  SELECT
    DATE_TRUNC('month', InvoiceTimestamp) AS month,
    SUM(TotalPrice) AS monthly_revenue
  FROM FactSales
  GROUP BY 1
)
SELECT
  month,
  monthly_revenue,
  LAG(monthly_revenue, 12) OVER (ORDER BY month) AS prior_year_revenue,
  ROUND(
    (monthly_revenue - LAG(monthly_revenue, 12) OVER (ORDER BY month)) /
    NULLIF(LAG(monthly_revenue, 12) OVER (ORDER BY month), 0) * 100,
    2
  ) AS yoy_growth_pct
FROM monthly
ORDER BY month;
