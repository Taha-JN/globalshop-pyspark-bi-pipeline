-- create_schema.sql
-- Kimball Star Schema DDL for GlobalShop Big Data & Analytics Lakehouse
-- Target: PostgreSQL / Snowflake / Databricks / DuckDB

CREATE TABLE DimRegion (
  RegionKey BIGINT PRIMARY KEY,
  Country VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE DimProduct (
  ProductKey BIGINT PRIMARY KEY,
  StockCode VARCHAR(50) NOT NULL UNIQUE,
  Description VARCHAR(255),
  UnitPrice DECIMAL(18,4)
);

CREATE TABLE DimCustomer (
  CustomerKey BIGINT PRIMARY KEY,
  CustomerID VARCHAR(50) NOT NULL UNIQUE,
  Country VARCHAR(100),
  RegionKey BIGINT REFERENCES DimRegion(RegionKey)
);

CREATE TABLE DimDate (
  DateKey INT PRIMARY KEY,
  FullDate DATE NOT NULL,
  Year INT NOT NULL,
  Month INT NOT NULL,
  Day INT NOT NULL,
  DayOfWeek INT NOT NULL,
  Quarter INT NOT NULL
);

CREATE TABLE FactSales (
  SalesKey VARCHAR(64) PRIMARY KEY,
  InvoiceNo VARCHAR(50) NOT NULL,
  ProductKey BIGINT REFERENCES DimProduct(ProductKey),
  CustomerKey BIGINT REFERENCES DimCustomer(CustomerKey),
  DateKey INT REFERENCES DimDate(DateKey),
  RegionKey BIGINT REFERENCES DimRegion(RegionKey),
  InvoiceTimestamp TIMESTAMP NOT NULL,
  Year INT NOT NULL,
  Month INT NOT NULL,
  Quantity INT NOT NULL,
  UnitPrice DECIMAL(18,4) NOT NULL,
  TotalPrice DECIMAL(18,4) NOT NULL,
  Channel VARCHAR(50)
);

CREATE TABLE FactFunnel (
  FunnelKey VARCHAR(64) PRIMARY KEY,
  VisitorID VARCHAR(50) NOT NULL,
  StockCode VARCHAR(50) NOT NULL,
  ProductKey BIGINT REFERENCES DimProduct(ProductKey),
  Views INT NOT NULL DEFAULT 0,
  AddToCart INT NOT NULL DEFAULT 0,
  Transactions INT NOT NULL DEFAULT 0,
  Converted BOOLEAN NOT NULL DEFAULT FALSE
);
