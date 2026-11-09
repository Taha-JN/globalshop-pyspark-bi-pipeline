-- q3_customer_segmentation.sql
-- Simple RFM-like segmentation using transactional aggregates

WITH cust AS (
  SELECT
    c.CustomerID,
    COUNT(DISTINCT f.InvoiceNo) AS orders_count,
    SUM(f.TotalPrice) AS total_spend,
    MAX(DateKey) AS last_order_date
  FROM FactSales f
  JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
  GROUP BY c.CustomerID
)
SELECT
  CustomerID,
  orders_count,
  total_spend,
  CASE WHEN total_spend > 500 THEN 'High' WHEN total_spend > 100 THEN 'Medium' ELSE 'Low' END AS segment
FROM cust
ORDER BY total_spend DESC
LIMIT 100;
