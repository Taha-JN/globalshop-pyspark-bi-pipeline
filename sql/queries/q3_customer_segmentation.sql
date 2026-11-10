-- q3_customer_segmentation.sql
-- RFM-like segmentation using transactional spend and order count

WITH customer_metrics AS (
  SELECT
    c.CustomerID,
    COUNT(DISTINCT f.InvoiceNo) AS orders_count,
    SUM(f.TotalPrice) AS total_spend,
    MAX(f.InvoiceTimestamp) AS last_order_date
  FROM FactSales f
  JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
  WHERE c.CustomerID IS NOT NULL AND c.CustomerID != 'GUEST'
  GROUP BY c.CustomerID
)
SELECT
  CustomerID,
  orders_count,
  total_spend,
  last_order_date,
  CASE
    WHEN total_spend >= 500 AND orders_count >= 5 THEN 'High-Value VIP'
    WHEN total_spend >= 200 THEN 'Loyal Customer'
    WHEN orders_count > 1 THEN 'Returning'
    ELSE 'First-Time / Occasional'
  END AS segment
FROM customer_metrics
ORDER BY total_spend DESC
LIMIT 100;
