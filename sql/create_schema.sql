-- create_schema.sql
-- Star schema DDL for GlobalShop demo

CREATE TABLE DimProduct (
  ProductKey BIGINT PRIMARY KEY,
  StockCode VARCHAR(50),
  Description VARCHAR(255)
);

CREATE TABLE DimCustomer (
  CustomerKey BIGINT PRIMARY KEY,
  CustomerID VARCHAR(50),
  Country VARCHAR(100)
);

CREATE TABLE DimDate (
  DateKey DATE PRIMARY KEY,
  Year INT,
  Month INT,
  Day INT
);

CREATE TABLE DimRegion (
  RegionKey BIGINT PRIMARY KEY,
  Country VARCHAR(100)
);

CREATE TABLE FactSales (
  SalesKey BIGINT PRIMARY KEY,
  InvoiceNo VARCHAR(50),
  ProductKey BIGINT,
  CustomerKey BIGINT,
  DateKey DATE,
  Quantity INT,
  UnitPrice DECIMAL(18,4),
  TotalPrice DECIMAL(18,4),
  Channel VARCHAR(50)
);

-- Example COPY (Postgres) or BULK INSERT (other) using Parquet/CSV produced by ETL
-- COPY FactSales FROM '/data/fact_sales.csv' WITH (FORMAT csv, HEADER true);
